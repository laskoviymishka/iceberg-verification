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
	"sort"
	"strings"
)

// EmitYAML renders an op-log to the YAML authoring profile the runners parse —
// the same shape as the hand-authored fixtures, with typed value tags (!long 5,
// !string "x"). No general YAML marshaler emits custom tags the way we need, so
// this writes the format directly; the shape is small and fixed.
//
// id is the fixture id stamped in the header (e.g. "fuzz_ab12cd").
func EmitYAML(id string, log OpLog) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Generated op-log (seed %d) — do not hand-edit; reproduce with the generator.\n", log.Seed)
	b.WriteString("header:\n")
	fmt.Fprintf(&b, "  id: %s\n", id)
	fmt.Fprintf(&b, "  format-version: %d\n", log.FormatVersion)
	b.WriteString("  spec-anchor: \"generated/fuzz\"\n")
	b.WriteString("  table-uuid: \"00000000-0000-0000-0000-000000000000\"\n")
	b.WriteString("  seeds:\n")
	b.WriteString("    snapshot-id: sequential\n")
	b.WriteString("    commit-ts: \"2026-01-01T00:00:00Z\"\n")
	b.WriteString("    file-paths: templated\n")
	b.WriteString("  schema:\n")
	b.WriteString("    fields:\n")
	for _, f := range log.Schema {
		req := ""
		if f.Required {
			req = ", required: true"
		}
		// Quote types carrying params (fixed[4], decimal(9,2)) — the brackets/
		// parens are YAML-significant inside a flow mapping otherwise.
		typ := f.Type
		if strings.ContainsAny(typ, "[](), ") {
			typ = fmt.Sprintf("%q", typ)
		}
		fmt.Fprintf(&b, "      - { id: %d, name: %s, type: %s%s }\n", f.ID, f.Name, typ, req)
	}

	b.WriteString("entries:\n")
	for _, op := range log.Ops {
		emitOp(&b, op)
	}
	return b.String()
}

func emitOp(b *strings.Builder, op Op) {
	switch op.Kind {
	case "append":
		b.WriteString("  - op: append\n")
		b.WriteString("    rows:\n")
		for _, row := range op.Rows {
			b.WriteString("      - { " + emitRow(row) + " }\n")
		}
	case "delete":
		b.WriteString("  - op: delete\n")
		b.WriteString("    strategy: merge-on-read\n")
		b.WriteString("    kind: position\n")
		fmt.Fprintf(b, "    predicate: { type: eq, term: %s, value: %s }\n",
			op.Predicate.Term, emitVal(op.Predicate.Value))
	case "promote":
		b.WriteString("  - op: evolve-schema\n")
		b.WriteString("    changes:\n")
		fmt.Fprintf(b, "      - { kind: promote-type, field-id: %d, to: %s }\n", op.FieldID, op.To)
	case "observe":
		b.WriteString("  - op: observe\n")
		fmt.Fprintf(b, "    at: %s\n", op.At)
		if op.Bind != "" {
			fmt.Fprintf(b, "    bind: %s\n", op.Bind)
		}
	}
}

// emitRow renders a row's __rowkey + its columns in a stable (sorted) order.
func emitRow(row Row) string {
	parts := []string{"__rowkey: " + row.RowKey}
	names := make([]string, 0, len(row.Vals))
	for name := range row.Vals {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		parts = append(parts, name+": "+emitVal(row.Vals[name]))
	}
	return strings.Join(parts, ", ")
}

// emitVal renders a typed value as a tagged scalar: !long 5, !string "x", or
// !null null for a present null. Strings are always quoted.
func emitVal(v TypedVal) string {
	if v.Null {
		return "!null null"
	}
	if v.Tag == "string" {
		return fmt.Sprintf("!string %q", v.Scalar)
	}
	return "!" + v.Tag + " " + v.Scalar
}
