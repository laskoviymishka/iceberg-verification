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
	"strconv"

	"github.com/apache/iceberg-go"
	"gopkg.in/yaml.v3"
)

// CanonicalOutput is the runner's emit shape (expected-output.schema.json). A
// blessed run of this output becomes a golden.
type CanonicalOutput struct {
	SpecID       string        `json:"spec-id"`
	Accept       bool          `json:"accept"`
	Snapshots    []SnapshotOut `json:"snapshots,omitempty"`
	Observations []Observation `json:"observations"`
}

// SnapshotOut is one commit-ordered snapshot with only spec-pinned, layout-
// independent facts. The random snapshot-id is replaced by the commit ordinal.
type SnapshotOut struct {
	Ordinal        int          `json:"ordinal"`
	Parent         *int         `json:"parent"`
	Operation      string       `json:"operation"`
	SequenceNumber *int64       `json:"sequence-number,omitempty"`
	Summary        *SummaryOut  `json:"summary,omitempty"`
	DeleteFiles    []DeleteFile `json:"delete-files,omitempty"`
}

// SummaryOut carries only layout-independent aggregate metrics.
type SummaryOut struct {
	TotalRecords     *int `json:"total-records,omitempty"`
	AddedRecords     *int `json:"added-records,omitempty"`
	DeletedRecords   *int `json:"deleted-records,omitempty"`
	TotalDeleteFiles *int `json:"total-delete-files,omitempty"`
}

// DeleteFile is a spec-pinned fact about a delete file added in a snapshot.
type DeleteFile struct {
	Content int    `json:"content"` // 1=positional (incl. DV), 2=equality
	Format  string `json:"format,omitempty"`
}

// Observation is a decoded read of the table at one observe point.
type Observation struct {
	At            any                     `json:"at"`
	Bind          string                  `json:"bind,omitempty"`
	IcebergSchema []schemaField           `json:"iceberg-schema"`
	DecodedRows   []map[string]*valueNode `json:"decoded-rows"`
}

// summaryOut extracts the layout-independent aggregate metrics from a
// snapshot's summary properties (all string-valued in iceberg-go). Missing
// keys are left nil so they are omitted from output.
func summaryOut(props iceberg.Properties) *SummaryOut {
	s := &SummaryOut{}
	s.TotalRecords = intProp(props, "total-records")
	s.AddedRecords = intProp(props, "added-records")
	s.DeletedRecords = intProp(props, "deleted-records")
	// total-delete-files is only meaningful once deletes exist; the goldens
	// omit it on append snapshots, so suppress a zero value.
	if tdf := intProp(props, "total-delete-files"); tdf != nil && *tdf > 0 {
		s.TotalDeleteFiles = tdf
	}
	if s.TotalRecords == nil && s.AddedRecords == nil && s.DeletedRecords == nil && s.TotalDeleteFiles == nil {
		return nil
	}
	return s
}

func intProp(props iceberg.Properties, key string) *int {
	v, ok := props[key]
	if !ok {
		return nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return nil
	}
	return &n
}

// observePoint is the resolved target of an observe entry.
type observeKind int

const (
	atLatest  observeKind = iota
	atBind                // time-travel to a bound observation
	atOrdinal             // time-travel to a commit ordinal
)

type observePoint struct {
	kind    observeKind
	value   any    // the value echoed into output ('latest', a bind name, or an ordinal)
	bind    string // bind name when kind==atBind
	ordinal int    // ordinal when kind==atOrdinal
}

// observeAt resolves an observe entry's 'at' node.
func observeAt(e Entry) (observePoint, error) {
	node := &e.At
	if node.Kind == 0 {
		return observePoint{}, fmt.Errorf("observe missing 'at'")
	}
	if node.Kind == yaml.ScalarNode {
		if node.Tag == "!!int" {
			n, err := strconv.Atoi(node.Value)
			if err != nil {
				return observePoint{}, err
			}
			return observePoint{kind: atOrdinal, value: n, ordinal: n}, nil
		}
		if node.Value == "latest" {
			return observePoint{kind: atLatest, value: "latest"}, nil
		}
		return observePoint{kind: atBind, value: node.Value, bind: node.Value}, nil
	}
	return observePoint{}, fmt.Errorf("unsupported 'at' node")
}
