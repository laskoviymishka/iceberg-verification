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
// required feature set). It decodes into yaml.Node so the l-log's custom type
// tags (!long, !struct, ...) don't trip the parser — we only read structure.
func fixtureFeatures(data []byte) (source string, fmtVer *int, ops []Op, required []string, err error) {
	var root struct {
		Header struct {
			FormatVersion *int   `yaml:"format-version"`
			Source        string `yaml:"source"`
		} `yaml:"header"`
		Entries []struct {
			Op       string `yaml:"op"`
			At       string `yaml:"at"`
			Bind     string `yaml:"bind"`
			Strategy string `yaml:"strategy"`
			Kind     string `yaml:"kind"`
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

	for _, e := range root.Entries {
		op := Op{Op: e.Op, At: e.At, Bind: e.Bind, Strategy: e.Strategy, Kind: e.Kind}
		ops = append(ops, op)
		recordRequired(e.Op, e.Strategy, e.Kind, reqSet)
	}

	for k := range reqSet {
		required = append(required, k)
	}
	sort.Strings(required)
	return source, fmtVer, ops, required, nil
}

// recordRequired maps an op (and a delete's strategy/kind) to the write feature
// key it exercises, for the supports.yaml cross-check.
func recordRequired(op, strategy, kind string, req map[string]bool) {
	switch op {
	case "append":
		req["write.append"] = true
	case "rewrite":
		req["write.rewrite"] = true
	case "evolve-schema":
		req["write.evolve-schema"] = true
	case "evolve-spec":
		req["write.evolve-spec"] = true
	case "overwrite":
		req["write.overwrite"] = true
	case "delete":
		if strategy == "copy-on-write" {
			req["write.delete.copy-on-write"] = true
		} else {
			k := kind
			if k == "" {
				k = "position"
			}
			req["write.delete.merge-on-read."+k] = true
		}
	}
}
