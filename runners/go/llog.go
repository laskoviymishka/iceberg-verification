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

	"gopkg.in/yaml.v3"
)

// LLog is the parsed logical op-log (the YAML authoring profile of
// spec/l-log.schema.json). Only the subset the Phase 0-2 corpus needs is
// modeled; unknown fields are ignored rather than rejected so the model can
// grow without breaking existing fixtures.
type LLog struct {
	Header  Header  `yaml:"header"`
	Entries []Entry `yaml:"entries"`
}

// Header is the determinism contract + initial table definition.
type Header struct {
	ID            string            `yaml:"id"`
	FormatVersion int               `yaml:"format-version"`
	SpecAnchor    string            `yaml:"spec-anchor"`
	TableUUID     string            `yaml:"table-uuid"`
	Schema        LSchema           `yaml:"schema"`
	PartitionSpec *LPartitionSpec   `yaml:"partition-spec"`
	Properties    map[string]string `yaml:"properties"`
}

// LSchema is the op-log schema block: an ordered list of fields.
type LSchema struct {
	Fields []LField `yaml:"fields"`
}

// LField is one schema field. Type is a raw yaml.Node because an iceberg type
// is either a primitive string (e.g. "long", "decimal(9,2)") or a nested type
// object (struct/list/map); schema.go resolves it.
type LField struct {
	ID             int         `yaml:"id"`
	Name           string      `yaml:"name"`
	Type           yaml.Node   `yaml:"type"`
	Required       bool        `yaml:"required"`
	Doc            string      `yaml:"doc"`
	InitialDefault *TypedValue `yaml:"initial-default"`
	WriteDefault   *TypedValue `yaml:"write-default"`
}

// LPartitionSpec is the header partition-spec block.
type LPartitionSpec struct {
	SpecID int               `yaml:"spec-id"`
	Fields []LPartitionField `yaml:"fields"`
}

// LPartitionField is one partition field with a transform (e.g. "bucket[4]").
type LPartitionField struct {
	SourceID  int    `yaml:"source-id"`
	FieldID   int    `yaml:"field-id"`
	Name      string `yaml:"name"`
	Transform string `yaml:"transform"`
}

// Entry is one op-log entry. Op discriminates; the remaining fields are the
// union of every op's payload (only the fields relevant to Op are populated).
type Entry struct {
	Op string `yaml:"op"`

	// append / overwrite
	Rows []Row `yaml:"rows"`

	// delete / overwrite
	Predicate      *Predicate `yaml:"predicate"`
	Strategy       string     `yaml:"strategy"`
	Kind           string     `yaml:"kind"`
	EqualityFields []int      `yaml:"equality-fields"`

	// evolve-schema
	Changes []yaml.Node `yaml:"changes"`

	// evolve-spec
	Spec *LPartitionSpec `yaml:"spec"`

	// observe
	At   yaml.Node `yaml:"at"`
	Bind string    `yaml:"bind"`

	// authored-golden / oracle payloads are for the orchestrator; the runner
	// parses but does not act on them.
	Expect    yaml.Node `yaml:"expect"`
	Invariant yaml.Node `yaml:"invariant"`
}

// Row is one logical row: a required synthetic __rowkey plus field values
// keyed by column name, each value a TypedValue carrying its physical type.
type Row struct {
	RowKey string
	Values map[string]*TypedValue
}

// UnmarshalYAML decodes a row mapping, peeling off __rowkey and decoding every
// other entry as a TypedValue (which understands the !int/!long/... tags).
func (r *Row) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("row must be a mapping, got kind %d", node.Kind)
	}
	r.Values = map[string]*TypedValue{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i].Value
		val := node.Content[i+1]
		if key == "__rowkey" {
			r.RowKey = val.Value
			continue
		}
		tv := &TypedValue{}
		if err := tv.fromNode(val); err != nil {
			return fmt.Errorf("row field %q: %w", key, err)
		}
		r.Values[key] = tv
	}
	if r.RowKey == "" {
		return fmt.Errorf("row missing __rowkey")
	}
	return nil
}

// Predicate mirrors the l-log predicate grammar reduced to what the corpus
// uses. Only the eq form (term + typed value) is needed for Phase 2.
type Predicate struct {
	Type   string        `yaml:"type"`
	Term   string        `yaml:"term"`
	Value  *TypedValue   `yaml:"value"`
	Values []*TypedValue `yaml:"values"`
	Args   []*Predicate  `yaml:"args"`
	Arg    *Predicate    `yaml:"arg"`
}
