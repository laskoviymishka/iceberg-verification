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
	"encoding/json"
	"fmt"
	"sort"
)

// diff is one structured difference, renderable by the site.
type diff struct {
	Path string `json:"path"`
	Got  any    `json:"got"`
	Want any    `json:"want"`
}

// compareOutput compares emitted output to a golden on the *logical* form only:
// rows are a multiset (sorted by __rowkey = canonical field-id "0"); snapshots
// and per-observation iceberg-schema are compared exactly; observe at/bind are
// informational. Returns structured diffs (empty == match).
func compareOutput(got, want map[string]any) []diff {
	var diffs []diff

	if b(got["accept"], true) != b(want["accept"], true) {
		diffs = append(diffs, diff{"accept", got["accept"], want["accept"]})
	}

	gs := arr(got["snapshots"])
	ws := arr(want["snapshots"])
	if len(gs) != len(ws) {
		diffs = append(diffs, diff{"snapshots.length", len(gs), len(ws)})
	}
	for i := 0; i < min(len(gs), len(ws)); i++ {
		if !jsonEqual(gs[i], ws[i]) {
			diffs = append(diffs, diff{fmt.Sprintf("snapshots[%d]", i), gs[i], ws[i]})
		}
	}

	go_ := arr(got["observations"])
	wo := arr(want["observations"])
	if len(go_) != len(wo) {
		diffs = append(diffs, diff{"observations.length", len(go_), len(wo)})
	}
	for i := 0; i < min(len(go_), len(wo)); i++ {
		ga, _ := go_[i].(map[string]any)
		wa, _ := wo[i].(map[string]any)
		if ga == nil || wa == nil {
			continue
		}
		if !jsonEqual(ga["iceberg-schema"], wa["iceberg-schema"]) {
			diffs = append(diffs, diff{fmt.Sprintf("observations[%d].iceberg-schema", i), ga["iceberg-schema"], wa["iceberg-schema"]})
		}
		grows := sortedRows(arr(ga["decoded-rows"]))
		wrows := sortedRows(arr(wa["decoded-rows"]))
		if !jsonEqual(grows, wrows) {
			diffs = append(diffs, diff{fmt.Sprintf("observations[%d].decoded-rows", i), grows, wrows})
		}
	}
	return diffs
}

// sortedRows orders decoded-rows by the __rowkey cell (canonical field-id "0"),
// since a scan is unordered and the multiset is what's pinned.
func sortedRows(rows []any) []any {
	out := make([]any, len(rows))
	copy(out, rows)
	sort.SliceStable(out, func(i, j int) bool {
		return rowKey(out[i]) < rowKey(out[j])
	})
	return out
}

func rowKey(row any) string {
	m, ok := row.(map[string]any)
	if !ok {
		return ""
	}
	return canon(m["0"])
}

// canon renders a value to a stable string for comparison/sort keys.
func canon(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func jsonEqual(a, b any) bool {
	return canon(a) == canon(b)
}

func arr(v any) []any {
	if s, ok := v.([]any); ok {
		return s
	}
	return nil
}

func b(v any, def bool) bool {
	if bv, ok := v.(bool); ok {
		return bv
	}
	return def
}
