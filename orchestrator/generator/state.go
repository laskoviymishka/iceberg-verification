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

// Package generator produces random-but-valid Iceberg op-logs for differential
// fuzzing. The key invariant: every emitted op is MEANINGFUL against the current
// table state (a delete matches at least one live row, a promotion targets an
// existing column with a legal wider type, an append conforms to the live
// schema) — raw mutation would emit ~99% spec-invalid logs and waste the fuzzer
// on rejects. State is advanced as each op is chosen so the next choice sees the
// real table.
package generator

// Field is one schema column: a stable id, a name, a spec type name, and
// whether it is required.
type Field struct {
	ID       int
	Name     string
	Type     string // spec type name: int/long/float/double/boolean/string
	Required bool
}

// TypedVal is a value with its physical type made explicit, so the int-vs-long
// distinction is authorable (rendered as a YAML tag, e.g. !long 5). A nil-scalar
// with Null=true is a present null.
type TypedVal struct {
	Tag    string // spec type name without the leading '!'
	Scalar string // scalar rendered as a string
	Null   bool
}

// Row is one logical row: its synthetic identity (__rowkey) plus a value per
// column keyed by column name. A column absent from Vals was omitted at write
// time (write-default / null).
type Row struct {
	RowKey string
	Vals   map[string]TypedVal
}

// State is the live table the generator builds ops against: the current schema,
// the live row multiset keyed by __rowkey, the bind names recorded by observes,
// and commit bookkeeping.
type State struct {
	FormatVersion int
	Schema        []Field
	Rows          map[string]Row // by __rowkey
	Binds         []string       // bind names available for time-travel observes
	Ordinal       int            // next commit ordinal
	nextRowKey    int
	nextFieldID   int
}

// newState seeds an empty table with a minimal schema (an id column plus one
// value column). The generator grows it from here.
func newState(formatVersion int) *State {
	return &State{
		FormatVersion: formatVersion,
		Schema: []Field{
			{ID: 1, Name: "id", Type: "long", Required: true},
			{ID: 2, Name: "val", Type: "string", Required: false},
		},
		Rows:        map[string]Row{},
		Ordinal:     0,
		nextRowKey:  1,
		nextFieldID: 3,
	}
}

// field returns the schema field with the given id, or nil.
func (s *State) field(id int) *Field {
	for i := range s.Schema {
		if s.Schema[i].ID == id {
			return &s.Schema[i]
		}
	}
	return nil
}

// liveRowKeys returns the current live __rowkeys in a deterministic order
// (insertion order is not tracked, so sort for reproducibility).
func (s *State) liveRowKeys() []string {
	keys := make([]string, 0, len(s.Rows))
	for k := range s.Rows {
		keys = append(keys, k)
	}
	sortStrings(keys)
	return keys
}

// applyAppend adds rows to the live set and advances the commit ordinal.
func (s *State) applyAppend(rows []Row) {
	for _, r := range rows {
		s.Rows[r.RowKey] = r
	}
	s.Ordinal++
}

// applyDeleteKeys removes the given __rowkeys and advances the commit ordinal.
func (s *State) applyDeleteKeys(keys []string) {
	for _, k := range keys {
		delete(s.Rows, k)
	}
	s.Ordinal++
}

// applyPromote widens a column's type in place (metadata-only; existing row
// values are re-tagged so later observes/emits reflect the new type) and
// advances the commit ordinal.
func (s *State) applyPromote(fieldID int, to string) {
	f := s.field(fieldID)
	if f == nil {
		return
	}
	f.Type = to
	for k, r := range s.Rows {
		if v, ok := r.Vals[f.Name]; ok && !v.Null {
			v.Tag = to
			r.Vals[f.Name] = v
			s.Rows[k] = r
		}
	}
	s.Ordinal++
}
