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
	"regexp"
	"strconv"

	"github.com/apache/iceberg-go"
	"gopkg.in/yaml.v3"
)

// rowKeyName is the reserved synthetic identity column carried on every row so
// the orchestrator can compare row multisets across physical layouts. It is
// materialized as a real string column inside the table; the emitter relabels
// its field-id to the canonical 0 in output.
const rowKeyName = "__rowkey"

// rowKeyFieldID is the internal iceberg field-id assigned to __rowkey. User
// field-ids in the corpus are small (1..~1001 including partition field-ids);
// this id sits well above them but clear of iceberg's 2147483xxx reserved
// metadata-column range. The emitter maps this back to canonical field-id 0.
const rowKeyFieldID = 100000

var (
	decimalRe = regexp.MustCompile(`^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$`)
	fixedRe   = regexp.MustCompile(`^fixed\[\s*(\d+)\s*\]$`)
)

// buildCanonIDs maps each top-level column name to the canonical output
// field-id used in the golden: __rowkey is 0, user columns keep the field-id
// the fixture declared. iceberg-go reassigns fresh field-ids at CreateTable
// (metadata.go AssignFreshSchemaIDs), so the runner must relabel by name back
// to these authored ids rather than emit the table's internal ids.
func buildCanonIDs(ls LSchema) map[string]int {
	m := map[string]int{rowKeyName: 0}
	for _, f := range ls.Fields {
		m[f.Name] = f.ID
	}
	return m
}

// buildSchema turns the L-log schema block into an iceberg.Schema, prepending
// the synthetic __rowkey string column. Returns the schema and the ordered
// list of user field names (schema order) for row materialization.
func buildSchema(ls LSchema) (*iceberg.Schema, []string, error) {
	fields := []iceberg.NestedField{{
		ID:       rowKeyFieldID,
		Name:     rowKeyName,
		Type:     iceberg.PrimitiveTypes.String,
		Required: false,
	}}
	names := make([]string, 0, len(ls.Fields))
	for _, f := range ls.Fields {
		nf, err := buildField(f)
		if err != nil {
			return nil, nil, err
		}
		fields = append(fields, nf)
		names = append(names, f.Name)
	}
	return iceberg.NewSchema(0, fields...), names, nil
}

// buildField resolves one L-log field (including initial/write defaults) into
// an iceberg.NestedField.
func buildField(f LField) (iceberg.NestedField, error) {
	typ, err := resolveType(&f.Type)
	if err != nil {
		return iceberg.NestedField{}, fmt.Errorf("field %q: %w", f.Name, err)
	}
	nf := iceberg.NestedField{
		ID:       f.ID,
		Name:     f.Name,
		Type:     typ,
		Required: f.Required,
		Doc:      f.Doc,
	}
	if f.InitialDefault != nil {
		lit, err := f.InitialDefault.toLiteral(typ)
		if err != nil {
			return iceberg.NestedField{}, fmt.Errorf("field %q initial-default: %w", f.Name, err)
		}
		nf.InitialDefault = lit.Any()
	}
	if f.WriteDefault != nil {
		lit, err := f.WriteDefault.toLiteral(typ)
		if err != nil {
			return iceberg.NestedField{}, fmt.Errorf("field %q write-default: %w", f.Name, err)
		}
		nf.WriteDefault = lit.Any()
	}
	return nf, nil
}

// resolveType maps an L-log type node (a primitive string like "long" /
// "decimal(9,2)" / "fixed[4]", or a nested struct/list/map object) to an
// iceberg.Type.
func resolveType(node *yaml.Node) (iceberg.Type, error) {
	switch node.Kind {
	case yaml.ScalarNode:
		return primitiveType(node.Value)
	case yaml.MappingNode:
		var probe struct {
			Type string `yaml:"type"`
		}
		if err := node.Decode(&probe); err != nil {
			return nil, err
		}
		switch probe.Type {
		case "struct":
			return resolveStruct(node)
		case "list":
			return resolveList(node)
		case "map":
			return resolveMap(node)
		default:
			return nil, fmt.Errorf("unknown nested type %q", probe.Type)
		}
	default:
		return nil, fmt.Errorf("unsupported type node kind %d", node.Kind)
	}
}

// primitiveType maps a spec primitive type name to its iceberg.Type.
func primitiveType(name string) (iceberg.Type, error) {
	switch name {
	case "boolean":
		return iceberg.PrimitiveTypes.Bool, nil
	case "int":
		return iceberg.PrimitiveTypes.Int32, nil
	case "long":
		return iceberg.PrimitiveTypes.Int64, nil
	case "float":
		return iceberg.PrimitiveTypes.Float32, nil
	case "double":
		return iceberg.PrimitiveTypes.Float64, nil
	case "date":
		return iceberg.PrimitiveTypes.Date, nil
	case "time":
		return iceberg.PrimitiveTypes.Time, nil
	case "timestamp":
		return iceberg.PrimitiveTypes.Timestamp, nil
	case "timestamptz":
		return iceberg.PrimitiveTypes.TimestampTz, nil
	case "timestamp_ns":
		return iceberg.PrimitiveTypes.TimestampNs, nil
	case "timestamptz_ns":
		return iceberg.PrimitiveTypes.TimestampTzNs, nil
	case "string":
		return iceberg.PrimitiveTypes.String, nil
	case "uuid":
		return iceberg.PrimitiveTypes.UUID, nil
	case "binary":
		return iceberg.PrimitiveTypes.Binary, nil
	}
	if m := decimalRe.FindStringSubmatch(name); m != nil {
		prec, _ := strconv.Atoi(m[1])
		scale, _ := strconv.Atoi(m[2])
		return iceberg.DecimalTypeOf(prec, scale), nil
	}
	if m := fixedRe.FindStringSubmatch(name); m != nil {
		n, _ := strconv.Atoi(m[1])
		return iceberg.FixedTypeOf(n), nil
	}
	return nil, fmt.Errorf("unsupported primitive type %q", name)
}

func resolveStruct(node *yaml.Node) (iceberg.Type, error) {
	var st struct {
		Fields []LField `yaml:"fields"`
	}
	if err := node.Decode(&st); err != nil {
		return nil, err
	}
	fields := make([]iceberg.NestedField, 0, len(st.Fields))
	for _, f := range st.Fields {
		nf, err := buildField(f)
		if err != nil {
			return nil, err
		}
		fields = append(fields, nf)
	}
	return &iceberg.StructType{FieldList: fields}, nil
}

func resolveList(node *yaml.Node) (iceberg.Type, error) {
	var lt struct {
		ElementID       int       `yaml:"element-id"`
		Element         yaml.Node `yaml:"element"`
		ElementRequired bool      `yaml:"element-required"`
	}
	if err := node.Decode(&lt); err != nil {
		return nil, err
	}
	elem, err := resolveType(&lt.Element)
	if err != nil {
		return nil, err
	}
	return &iceberg.ListType{
		ElementID:       lt.ElementID,
		Element:         elem,
		ElementRequired: lt.ElementRequired,
	}, nil
}

func resolveMap(node *yaml.Node) (iceberg.Type, error) {
	var mt struct {
		KeyID         int       `yaml:"key-id"`
		Key           yaml.Node `yaml:"key"`
		ValueID       int       `yaml:"value-id"`
		Value         yaml.Node `yaml:"value"`
		ValueRequired bool      `yaml:"value-required"`
	}
	if err := node.Decode(&mt); err != nil {
		return nil, err
	}
	keyType, err := resolveType(&mt.Key)
	if err != nil {
		return nil, err
	}
	valType, err := resolveType(&mt.Value)
	if err != nil {
		return nil, err
	}
	return &iceberg.MapType{
		KeyID:         mt.KeyID,
		KeyType:       keyType,
		ValueID:       mt.ValueID,
		ValueType:     valType,
		ValueRequired: mt.ValueRequired,
	}, nil
}
