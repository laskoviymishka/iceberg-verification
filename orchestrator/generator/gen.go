// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package generator

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
)

// Op is one generated op-log entry in a runner-agnostic form; emit.go renders
// it to the YAML authoring profile.
type Op struct {
	Kind string // append | delete | observe | promote

	// append
	Rows []Row

	// delete
	Predicate *Pred

	// observe
	At   string // "latest" | a bind name | an ordinal (as string)
	Bind string // non-empty => record this snapshot under the bind

	// promote
	FieldID int
	To      string
}

// Pred is a minimal equality predicate (term = value). The generator only emits
// predicates that match at least one live row, so a delete is always meaningful.
type Pred struct {
	Term  string
	Value TypedVal
}

// OpLog is a generated scenario: the seed it came from (for reproducibility),
// the format version, the seed schema, and the ordered ops. FinalRowKeys and
// FinalTypes are the generator's own prediction of the table after every op —
// the metamorphic oracle a single runner is checked against (no reference impl
// needed): the runner's final `observe latest` must decode exactly these
// __rowkeys, each column at these types.
type OpLog struct {
	Seed          int64
	FormatVersion int
	Schema        []Field
	Ops           []Op
	FinalRowKeys  []string          // predicted live __rowkeys after all ops
	FinalTypes    map[string]string // column name -> spec type after all ops
}

// Features restricts what the generator may emit to the claimed-feature
// intersection of the impls under test. A false flag means that op is never
// generated (so a scenario stays within every impl's support surface).
type Features struct {
	Delete    bool
	Promote   bool
	WideTypes bool // include a fixed/decimal/uuid/boolean column in some seeds
}

// legalPromotions is the ONLY spec knowledge the generator encodes: the
// widenings the Iceberg spec pins as always legal. Kept conservative so every
// generated promotion produces a valid table.
var legalPromotions = map[string][]string{
	"int":   {"long"},
	"float": {"double"},
}

// Generate produces a deterministic op-log from a seed. Same seed => identical
// op-log (no wall-clock, no global rand). count bounds the number of ops.
func Generate(seed int64, formatVersion, count int, feat Features) OpLog {
	r := rand.New(rand.NewSource(seed))
	st := newState(formatVersion)
	// Some seeds carry a wide-type column (fixed/decimal/uuid/bool) so the read
	// diff exercises the representational surface where writers diverge, not just
	// int/long/string. Kept optional so most seeds stay simple.
	if feat.WideTypes && r.Intn(2) == 0 {
		// decimal uses the no-space input form the l-log parser expects; impls
		// may render it back with a space (a divergence the diff will surface).
		wide := []string{"boolean", "fixed[4]", "decimal(9,2)", "uuid"}
		st.Schema = append(st.Schema, Field{
			ID: st.nextFieldID, Name: "w", Type: wide[r.Intn(len(wide))], Required: false,
		})
		st.nextFieldID++
	}
	seedSchema := append([]Field(nil), st.Schema...)

	var ops []Op
	// Always start with an append so later ops have live rows to act on.
	first := genAppend(r, st)
	st.applyAppend(first.Rows)
	ops = append(ops, first)

	for len(ops) < count {
		switch pickOp(r, st, feat) {
		case "append":
			op := genAppend(r, st)
			st.applyAppend(op.Rows)
			ops = append(ops, op)
		case "delete":
			op, keys := genDelete(r, st)
			if op == nil {
				continue // no live row matched; try another op
			}
			st.applyDeleteKeys(keys)
			ops = append(ops, *op)
		case "promote":
			op := genPromote(r, st)
			if op == nil {
				continue
			}
			st.applyPromote(op.FieldID, op.To)
			ops = append(ops, *op)
		case "observe":
			ops = append(ops, genObserve(r, st))
			// observe does not mutate; if it binds, record the bind name.
		}
	}
	// Always end with a latest observe so the scenario asserts a final state.
	ops = append(ops, Op{Kind: "observe", At: "latest"})

	finalTypes := map[string]string{}
	for _, f := range st.Schema {
		finalTypes[f.Name] = f.Type
	}
	return OpLog{
		Seed:          seed,
		FormatVersion: formatVersion,
		Schema:        seedSchema,
		Ops:           ops,
		FinalRowKeys:  st.liveRowKeys(),
		FinalTypes:    finalTypes,
	}
}

// pickOp weights the op choice by what is possible against live state and the
// allowed feature set.
func pickOp(r *rand.Rand, st *State, feat Features) string {
	choices := []string{"append", "observe"} // always possible
	if feat.Delete && len(st.Rows) > 0 {
		choices = append(choices, "delete")
	}
	if feat.Promote && st.hasPromotableColumn() {
		choices = append(choices, "promote")
	}
	return choices[r.Intn(len(choices))]
}

// hasPromotableColumn reports whether any column has a legal wider target.
func (s *State) hasPromotableColumn() bool {
	for _, f := range s.Schema {
		if len(legalPromotions[f.Type]) > 0 {
			return true
		}
	}
	return false
}

// genAppend builds 1-3 rows conforming to the live schema, each with a fresh
// __rowkey. Optional columns are sometimes omitted (exercising defaults/null).
func genAppend(r *rand.Rand, st *State) Op {
	n := 1 + r.Intn(3)
	rows := make([]Row, 0, n)
	for i := 0; i < n; i++ {
		rk := "r" + strconv.Itoa(st.nextRowKey)
		st.nextRowKey++
		vals := map[string]TypedVal{}
		for _, f := range st.Schema {
			// optional columns are omitted ~1/4 of the time
			if !f.Required && r.Intn(4) == 0 {
				continue
			}
			vals[f.Name] = genValue(r, f.Type)
		}
		rows = append(rows, Row{RowKey: rk, Vals: vals})
	}
	return Op{Kind: "append", Rows: rows}
}

// genDelete picks a live row and deletes by equality on its id column, so the
// predicate is guaranteed to match. Returns the op and the keys it removes.
func genDelete(r *rand.Rand, st *State) (*Op, []string) {
	keys := st.liveRowKeys()
	if len(keys) == 0 {
		return nil, nil
	}
	victim := st.Rows[keys[r.Intn(len(keys))]]
	idField := st.field(1)
	if idField == nil {
		return nil, nil
	}
	idVal, ok := victim.Vals[idField.Name]
	if !ok || idVal.Null {
		return nil, nil
	}
	// every live row with the same id value is removed (equality delete-is-set-minus)
	var removed []string
	for _, k := range keys {
		if v, ok := st.Rows[k].Vals[idField.Name]; ok && !v.Null && v.Scalar == idVal.Scalar {
			removed = append(removed, k)
		}
	}
	op := Op{
		Kind:      "delete",
		Predicate: &Pred{Term: idField.Name, Value: idVal},
	}
	return &op, removed
}

// genPromote picks a column with a legal wider target and promotes it.
func genPromote(r *rand.Rand, st *State) *Op {
	var candidates []Field
	for _, f := range st.Schema {
		if len(legalPromotions[f.Type]) > 0 {
			candidates = append(candidates, f)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	f := candidates[r.Intn(len(candidates))]
	targets := legalPromotions[f.Type]
	to := targets[r.Intn(len(targets))]
	return &Op{Kind: "promote", FieldID: f.ID, To: to}
}

// genObserve reads at latest, or time-travels to a recorded bind, and sometimes
// binds the current snapshot for a later time-travel read.
func genObserve(r *rand.Rand, st *State) Op {
	// ~1/3 of observes time-travel to an existing bind, if any.
	if len(st.Binds) > 0 && r.Intn(3) == 0 {
		return Op{Kind: "observe", At: st.Binds[r.Intn(len(st.Binds))]}
	}
	op := Op{Kind: "observe", At: "latest"}
	// ~1/2 of latest observes record a bind for later time-travel.
	if r.Intn(2) == 0 {
		bind := "b" + strconv.Itoa(len(st.Binds))
		op.Bind = bind
		st.Binds = append(st.Binds, bind)
	}
	return op
}

// genValue produces a random scalar for a spec type. Ranges are chosen so int
// stays in int32 and long can exceed it (exercising the 64-bit boundary).
func genValue(r *rand.Rand, typ string) TypedVal {
	switch typ {
	case "int":
		return TypedVal{Tag: "int", Scalar: strconv.Itoa(r.Intn(2000) - 1000)}
	case "long":
		// sometimes beyond int32 range, only valid as long
		if r.Intn(3) == 0 {
			return TypedVal{Tag: "long", Scalar: strconv.FormatInt(3_000_000_000+int64(r.Intn(1_000_000)), 10)}
		}
		return TypedVal{Tag: "long", Scalar: strconv.Itoa(r.Intn(2000) - 1000)}
	case "float":
		return TypedVal{Tag: "float", Scalar: strconv.FormatFloat(float64(r.Intn(1000))/8, 'g', -1, 32)}
	case "double":
		return TypedVal{Tag: "double", Scalar: strconv.FormatFloat(float64(r.Intn(100000))/64, 'g', -1, 64)}
	case "boolean":
		return TypedVal{Tag: "boolean", Scalar: strconv.FormatBool(r.Intn(2) == 0)}
	case "uuid":
		return TypedVal{Tag: "uuid", Scalar: randUUID(r)}
	default:
		// fixed[N] and decimal(P,S) carry their params in the type string; the
		// value tag is the bare family name (!fixed / !decimal).
		if strings.HasPrefix(typ, "fixed[") {
			return TypedVal{Tag: "fixed", Scalar: randHex(r, 4)}
		}
		if strings.HasPrefix(typ, "decimal(") {
			return TypedVal{Tag: "decimal", Scalar: fmt.Sprintf("%d.%02d", r.Intn(1000), r.Intn(100))}
		}
		return TypedVal{Tag: "string", Scalar: randString(r)}
	}
}

func randHex(r *rand.Rand, n int) string {
	const hexd = "0123456789abcdef"
	b := make([]byte, n*2)
	for i := range b {
		b[i] = hexd[r.Intn(16)]
	}
	return string(b)
}

func randUUID(r *rand.Rand) string {
	h := randHex(r, 16)
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])
}

func randString(r *rand.Rand) string {
	const alpha = "abcdefghijklmnopqrstuvwxyz"
	n := 1 + r.Intn(5)
	b := make([]byte, n)
	for i := range b {
		b[i] = alpha[r.Intn(len(alpha))]
	}
	return string(b)
}

func sortStrings(s []string) { sort.Strings(s) }

// OpSummary renders a compact human description of what a generated op-log
// exercises, e.g. "append×3, delete, promote int→long, observe×2". Ops of the
// same kind are counted; promote/delete carry a hint of their target.
func OpSummary(log OpLog) string {
	counts := map[string]int{}
	order := []string{}
	extra := map[string]string{}
	for _, op := range log.Ops {
		k := op.Kind
		if _, seen := counts[k]; !seen {
			order = append(order, k)
		}
		counts[k]++
		switch k {
		case "promote":
			extra[k] = "→" + op.To
		case "delete":
			extra[k] = " " + op.Predicate.Term + "=" + op.Predicate.Value.Scalar
		}
	}
	parts := make([]string, 0, len(order))
	for _, k := range order {
		label := k
		if counts[k] > 1 {
			label = fmt.Sprintf("%s×%d", k, counts[k])
		}
		if e := extra[k]; e != "" && counts[k] == 1 {
			label += e
		}
		parts = append(parts, label)
	}
	return strings.Join(parts, ", ")
}
