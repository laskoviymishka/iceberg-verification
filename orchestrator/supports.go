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

package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// parseSupports reads a runner's supports.yaml into name/version + read/write axes.
func parseSupports(path string) (string, Supports, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", Supports{Read: []string{}, Write: []string{}}, err
	}
	var doc struct {
		Implementation string   `yaml:"implementation"`
		Version        string   `yaml:"version"`
		Supports       Supports `yaml:"supports"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return "", Supports{}, err
	}
	if doc.Supports.Read == nil {
		doc.Supports.Read = []string{}
	}
	if doc.Supports.Write == nil {
		doc.Supports.Write = []string{}
	}
	return doc.Version, doc.Supports, nil
}

// claimedNames returns a runner's features as fully-qualified read.*/write.* keys.
func claimedNames(s Supports) map[string]bool {
	m := map[string]bool{}
	for _, x := range s.Read {
		m["read."+x] = true
	}
	for _, x := range s.Write {
		m["write."+x] = true
	}
	return m
}

// fixtureFeatures parses a fixture.yaml into (source, format-version, op summary,
// required feature set). It decodes with yaml.Node for the value-bearing bits so
// the l-log's custom type tags (!long, !struct, ...) don't trip the parser, while
// still reading enough structure to write a human-readable per-op headline.
func fixtureFeatures(data []byte) (source string, fmtVer *int, ops []Op, required []string, err error) {
	var root struct {
		Header struct {
			FormatVersion *int   `yaml:"format-version"`
			Source        string `yaml:"source"`
		} `yaml:"header"`
		Entries []struct {
			Op        string      `yaml:"op"`
			At        string      `yaml:"at"`
			Bind      string      `yaml:"bind"`
			Strategy  string      `yaml:"strategy"`
			Kind      string      `yaml:"kind"`
			Rows      []yaml.Node `yaml:"rows"`
			Predicate yaml.Node   `yaml:"predicate"`
			Changes   []yaml.Node `yaml:"changes"`
		} `yaml:"entries"`
	}
	if err = yaml.Unmarshal(data, &root); err != nil {
		return "", nil, nil, nil, fmt.Errorf("parse fixture: %w", err)
	}

	source = root.Header.Source
	if source == "" {
		source = "synthesized"
	}
	fmtVer = root.Header.FormatVersion

	reqSet := map[string]bool{}
	if source == "artifact" {
		reqSet["read.artifact"] = true
	}

	fv := 2
	if fmtVer != nil {
		fv = *fmtVer
	}
	ordinal := 0
	for _, e := range root.Entries {
		op := Op{Op: e.Op, At: e.At, Bind: e.Bind, Strategy: e.Strategy, Kind: e.Kind}
		var evolveKinds []string
		switch e.Op {
		case "append":
			keys := rowKeys(e.Rows)
			op.Summary = fmt.Sprintf("append %d row%s → snapshot %d", len(e.Rows), plural(len(e.Rows)), ordinal)
			if len(keys) > 0 {
				op.Detail = append(op.Detail, "rows "+strings.Join(keys, ", "))
			}
			ordinal++
		case "delete":
			strat := e.Strategy
			if strat == "" {
				strat = "merge-on-read"
			}
			kind := e.Kind
			if kind == "" {
				kind = "position"
			}
			op.Summary = "delete where " + predicateText(e.Predicate) + " → snapshot " + fmt.Sprint(ordinal)
			op.Detail = append(op.Detail, strat+" · "+kind+" delete")
			ordinal++
		case "overwrite":
			op.Summary = "overwrite where " + predicateText(e.Predicate) + " → snapshot " + fmt.Sprint(ordinal)
			ordinal++
		case "evolve-schema":
			lines := schemaChangeLines(e.Changes)
			op.Summary = fmt.Sprintf("evolve schema (%d change%s) → snapshot %d", len(lines), plural(len(lines)), ordinal)
			op.Detail = lines
			evolveKinds = schemaChangeKinds(e.Changes)
			ordinal++
		case "evolve-spec":
			op.Summary = fmt.Sprintf("evolve partition spec → snapshot %d", ordinal)
			ordinal++
		case "rewrite":
			op.Summary = fmt.Sprintf("rewrite / compact data files → snapshot %d", ordinal)
			op.Detail = append(op.Detail, "logical no-op on the row multiset")
			ordinal++
		case "observe":
			at := e.At
			if at == "" {
				at = "latest"
			}
			if at == "latest" {
				op.Summary = "observe · read at the latest snapshot"
			} else {
				op.Summary = "observe · time-travel to " + at
			}
			if e.Bind != "" {
				op.Detail = append(op.Detail, "bind this snapshot as \""+e.Bind+"\"")
			}
		default:
			op.Summary = e.Op
		}
		ops = append(ops, op)
		recordRequired(e.Op, e.Strategy, e.Kind, fv, evolveKinds, reqSet)
	}

	for k := range reqSet {
		required = append(required, k)
	}
	sort.Strings(required)
	return source, fmtVer, ops, required, nil
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// rowKeys pulls the __rowkey of each authored row (the synthetic identity) so the
// timeline can show WHICH rows an append introduced, not just how many.
func rowKeys(rows []yaml.Node) []string {
	var keys []string
	for _, r := range rows {
		for i := 0; i+1 < len(r.Content); i += 2 {
			if r.Content[i].Value == "__rowkey" {
				keys = append(keys, r.Content[i+1].Value)
			}
		}
	}
	return keys
}

// predicateText renders a delete/overwrite predicate as an infix string like
// "id = 2" or "category in [a, b]". Falls back to the predicate type name.
func predicateText(n yaml.Node) string {
	if len(n.Content) == 0 {
		return "all rows"
	}
	m := map[string]*yaml.Node{}
	for i := 0; i+1 < len(n.Content); i += 2 {
		m[n.Content[i].Value] = n.Content[i+1]
	}
	typ, term := valOf(m["type"]), valOf(m["term"])
	val := valOf(m["value"])
	ops := map[string]string{
		"eq": "=", "not-eq": "≠", "lt": "<", "lt-eq": "≤",
		"gt": ">", "gt-eq": "≥", "in": "in", "not-in": "not in",
	}
	if sym, ok := ops[typ]; ok && term != "" {
		if val != "" {
			return term + " " + sym + " " + val
		}
		return term + " " + sym
	}
	switch typ {
	case "is-null":
		return term + " is null"
	case "not-null":
		return term + " is not null"
	case "always-true":
		return "all rows"
	case "always-false":
		return "no rows"
	}
	if typ != "" {
		return typ + "(" + term + ")"
	}
	return "predicate"
}

// schemaChangeKinds returns the `kind` of each evolve-schema change, for the
// per-kind supports requirement.
func schemaChangeKinds(changes []yaml.Node) []string {
	var kinds []string
	for _, c := range changes {
		m := map[string]*yaml.Node{}
		for i := 0; i+1 < len(c.Content); i += 2 {
			m[c.Content[i].Value] = c.Content[i+1]
		}
		if k := valOf(m["kind"]); k != "" {
			kinds = append(kinds, k)
		}
	}
	return kinds
}

// schemaChangeLines renders each evolve-schema change as one readable line.
func schemaChangeLines(changes []yaml.Node) []string {
	var lines []string
	for _, c := range changes {
		m := map[string]*yaml.Node{}
		for i := 0; i+1 < len(c.Content); i += 2 {
			m[c.Content[i].Value] = c.Content[i+1]
		}
		switch valOf(m["kind"]) {
		case "promote-type":
			lines = append(lines, "promote field "+valOf(m["field-id"])+" → "+valOf(m["to"]))
		case "add-column":
			name, typ, dflt := fieldParts(m["field"])
			line := "add column " + name + " " + typ
			if dflt != "" {
				line += " default " + dflt
			}
			lines = append(lines, line)
		case "drop-column":
			lines = append(lines, "drop field "+valOf(m["field-id"]))
		case "rename-column":
			lines = append(lines, "rename field "+valOf(m["field-id"])+" → "+valOf(m["to"]))
		default:
			lines = append(lines, valOf(m["kind"]))
		}
	}
	return lines
}

// fieldParts extracts (name, type, default) from an add-column field node.
func fieldParts(n *yaml.Node) (name, typ, dflt string) {
	if n == nil {
		return "", "", ""
	}
	m := map[string]*yaml.Node{}
	for i := 0; i+1 < len(n.Content); i += 2 {
		m[n.Content[i].Value] = n.Content[i+1]
	}
	name, typ = valOf(m["name"]), valOf(m["type"])
	dflt = valOf(m["initial-default"])
	if dflt == "" {
		dflt = valOf(m["write-default"])
	}
	return name, typ, dflt
}

func valOf(n *yaml.Node) string {
	if n == nil {
		return ""
	}
	return n.Value
}

// recordRequired maps an op (and a delete's strategy/kind, at a format version)
// to the write feature key it exercises, for the supports.yaml cross-check.
// evolveKinds carries an evolve-schema entry's change kinds so the requirement
// is granular (write.evolve-schema.promote-type vs .add-column) — an impl can
// then claim exactly the subset of schema changes its runner wires.
func recordRequired(op, strategy, kind string, formatVersion int, evolveKinds []string, req map[string]bool) {
	switch op {
	case "append":
		req["write.append"] = true
	case "rewrite":
		req["write.rewrite"] = true
	case "evolve-schema":
		for _, k := range evolveKinds {
			req["write.evolve-schema."+k] = true
		}
	case "evolve-spec":
		req["write.evolve-spec"] = true
	case "overwrite":
		req["write.overwrite"] = true
	case "delete":
		if strategy == "copy-on-write" {
			req["write.delete.copy-on-write"] = true
			break
		}
		k := kind
		if k == "" {
			k = "position"
		}
		// In format-version 3, a position delete MUST be a deletion vector
		// (Puffin), not a parquet position-delete file — so the capability a
		// v3 position delete actually exercises is the DV writer.
		if formatVersion >= 3 && k == "position" {
			k = "deletion-vector"
		}
		req["write.delete.merge-on-read."+k] = true
	}
}
