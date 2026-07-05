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

import "encoding/json"

// Report is the single artifact the front-face renders. Its shape is a
// contract with site/app.js — keep field names/tags stable.
type Report struct {
	Schema      string       `json:"schema"`
	GeneratedAt string       `json:"generated_at"`
	Runners     []RunnerInfo `json:"runners"`
	Fixtures    []Fixture    `json:"fixtures"`
	Cells       []Cell       `json:"cells"`
	// Fuzz holds a read-diff campaign result (see fuzz.FuzzReport), attached via
	// the orchestrator's --fuzz flag. Kept as raw JSON so the orchestrator stays
	// decoupled from the fuzz package; the site renders it. Omitted when absent.
	Fuzz json.RawMessage `json:"fuzz,omitempty"`
}

type RunnerInfo struct {
	Name     string   `json:"name"`
	Version  string   `json:"version"`
	Supports Supports `json:"supports"`
}

// Supports is a runner's declared capability set, split by read vs write axis.
type Supports struct {
	Read  []string `json:"read"`
	Write []string `json:"write"`
}

// Fixture is one corpus entry, with the decoded op summary + golden for the site.
type Fixture struct {
	ID            string   `json:"id"`
	Source        string   `json:"source"`
	FormatVersion *int     `json:"format_version"`
	Ops           []Op     `json:"ops"`
	Required      []string `json:"required"`
	HasGolden     bool     `json:"has_golden"`
	Golden        any      `json:"golden"`
	YAML          string   `json:"yaml"`
}

// Op is a per-entry summary for the timeline view. Summary is a human-readable
// headline (e.g. "delete where id = 2"); Detail carries supporting bullet lines
// (predicate mode, schema changes, appended row keys).
type Op struct {
	Op       string   `json:"op"`
	At       string   `json:"at,omitempty"`
	Bind     string   `json:"bind,omitempty"`
	Strategy string   `json:"strategy,omitempty"`
	Kind     string   `json:"kind,omitempty"`
	Summary  string   `json:"summary,omitempty"`
	Detail   []string `json:"detail,omitempty"`
}

// Cell is one (fixture × runner) result.
type Cell struct {
	Fixture string         `json:"fixture"`
	Runner  string         `json:"runner"`
	Status  string         `json:"status"`
	Detail  map[string]any `json:"detail"`
}

// Cell statuses.
const (
	StatusPass          = "pass"
	StatusFail          = "fail"
	StatusDeclaredGap   = "declared-gap"
	StatusUndeclaredGap = "undeclared-gap"
	StatusOracle        = "oracle"
	StatusReject        = "reject"
	StatusError         = "error"
	StatusBadFixture    = "bad-fixture"
)
