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
	"strings"

	"github.com/apache/iceberg-go"
	"gopkg.in/yaml.v3"
)

// TypedValue is one authored value carrying its physical type, decoded from a
// YAML tag (!int, !long, !decimal, !struct, ...). The tag makes the int-vs-long
// distinction (iceberg-go#880) authorable and pinnable. Exactly one of the
// fields below is populated depending on tagName.
type TypedValue struct {
	tagName string // spec type name without the leading '!': int, long, struct, list, map, null, ...
	scalar  string // primitive scalar rendered as a string (parsed via iceberg casting)
	isNull  bool

	// nested payloads
	structFields map[string]*TypedValue
	listElems    []*TypedValue
	mapEntries   []mapEntry
}

type mapEntry struct {
	key string // map keys in the corpus are always strings
	val *TypedValue
}

// UnmarshalYAML lets TypedValue be decoded directly by the yaml library where
// it appears as a struct field (predicate value, field defaults).
func (t *TypedValue) UnmarshalYAML(node *yaml.Node) error {
	return t.fromNode(node)
}

// fromNode decodes a yaml node into a TypedValue based on its custom tag. The
// tag is the spec type name (e.g. "!long"); nested tags carry mapping/sequence
// bodies.
func (t *TypedValue) fromNode(node *yaml.Node) error {
	t.tagName = strings.TrimPrefix(node.Tag, "!")
	switch t.tagName {
	case "null":
		t.isNull = true
		return nil
	case "struct":
		if node.Kind != yaml.MappingNode {
			return fmt.Errorf("!struct expects a mapping")
		}
		t.structFields = map[string]*TypedValue{}
		for i := 0; i+1 < len(node.Content); i += 2 {
			k := node.Content[i].Value
			child := &TypedValue{}
			if err := child.fromNode(node.Content[i+1]); err != nil {
				return fmt.Errorf("struct field %q: %w", k, err)
			}
			t.structFields[k] = child
		}
		return nil
	case "list":
		if node.Kind != yaml.SequenceNode {
			return fmt.Errorf("!list expects a sequence")
		}
		for i, item := range node.Content {
			child := &TypedValue{}
			if err := child.fromNode(item); err != nil {
				return fmt.Errorf("list elem %d: %w", i, err)
			}
			t.listElems = append(t.listElems, child)
		}
		return nil
	case "map":
		if node.Kind != yaml.MappingNode {
			return fmt.Errorf("!map expects a mapping")
		}
		for i := 0; i+1 < len(node.Content); i += 2 {
			k := node.Content[i].Value
			child := &TypedValue{}
			if err := child.fromNode(node.Content[i+1]); err != nil {
				return fmt.Errorf("map value for %q: %w", k, err)
			}
			t.mapEntries = append(t.mapEntries, mapEntry{key: k, val: child})
		}
		return nil
	default:
		// primitive: keep the scalar as a string; iceberg casting parses it.
		if t.tagName == "" {
			return fmt.Errorf("value %q has no physical-type tag", node.Value)
		}
		t.scalar = node.Value
		if node.Tag == "!!null" || node.Value == "null" {
			t.isNull = true
		}
		return nil
	}
}

// toLiteral converts a primitive TypedValue to an iceberg.Literal of the given
// target type. Parsing goes through iceberg.StringLiteral(...).To(typ), the
// single caster that handles every primitive type in the corpus. Nested and
// null values have no scalar Literal and return an error.
func (t *TypedValue) toLiteral(typ iceberg.Type) (iceberg.Literal, error) {
	if t.isNull {
		return nil, fmt.Errorf("null has no literal")
	}
	if t.isNested() {
		return nil, fmt.Errorf("nested value has no scalar literal")
	}
	lit, err := iceberg.StringLiteral(t.scalar).To(typ)
	if err != nil {
		return nil, fmt.Errorf("cast %q to %s: %w", t.scalar, typ, err)
	}
	return lit, nil
}

func (t *TypedValue) isNested() bool {
	switch t.tagName {
	case "struct", "list", "map":
		return true
	}
	return false
}
