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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
)

// valueNode is the type-annotated value tree from expected-output.schema.json:
// primitive => {type, value}; null cell => bare JSON null (nil valueNode).
type valueNode struct {
	Type  string `json:"type"`
	Value any    `json:"value"`
}

// schemaField is one entry of an observation's iceberg-schema array.
type schemaField struct {
	FieldID int    `json:"field-id"`
	Name    string `json:"name"`
	Type    string `json:"type"`
}

// decodeScan reads a scanned arrow.Table into (iceberg-schema, decoded-rows)
// for one observation. Columns are aligned positionally against the scanned
// iceberg schema (scanFields), whose fields carry the true iceberg field-ids
// in the same order as the arrow columns — the scan output does NOT stamp
// PARQUET:field_id on arrow fields, so positional alignment is the contract.
// Each column's output field-id comes from canonIDs (keyed by column name):
// iceberg-go reassigns internal field-ids at CreateTable, so the emitter maps
// back to the fixture's authored ids (__rowkey => 0) by name.
func decodeScan(tbl arrow.Table, scanFields []iceberg.NestedField, canonIDs map[string]int) ([]schemaField, []map[string]*valueNode, error) {
	numCols := int(tbl.NumCols())
	if numCols != len(scanFields) {
		return nil, nil, fmt.Errorf("column count %d != schema field count %d", numCols, len(scanFields))
	}

	// iceberg-schema: one entry per top-level column, canonical field-ids.
	icebergSchema := make([]schemaField, 0, numCols)
	for _, f := range scanFields {
		id, ok := canonIDs[f.Name]
		if !ok {
			return nil, nil, fmt.Errorf("no canonical field-id for column %q", f.Name)
		}
		icebergSchema = append(icebergSchema, schemaField{
			FieldID: id,
			Name:    f.Name,
			Type:    f.Type.Type(),
		})
	}

	numRows := int(tbl.NumRows())
	rows := make([]map[string]*valueNode, numRows)
	for r := range rows {
		rows[r] = map[string]*valueNode{}
	}

	for c := 0; c < numCols; c++ {
		field := scanFields[c]
		key := strconv.Itoa(canonIDs[field.Name])
		col := tbl.Column(c)

		rowBase := 0
		for _, chunk := range col.Data().Chunks() {
			for i := 0; i < chunk.Len(); i++ {
				vn, err := decodeCell(chunk, i, field.Type)
				if err != nil {
					return nil, nil, fmt.Errorf("column %q row %d: %w", field.Name, rowBase+i, err)
				}
				if vn != nil {
					rows[rowBase+i][key] = vn
				}
				// nil vn = SQL null; leave the key absent so JSON omits it.
			}
			rowBase += chunk.Len()
		}
	}

	return icebergSchema, rows, nil
}

// decodeCell converts one arrow array cell to a valueNode. Returns nil for a
// SQL null. Phase 0-2 covers the primitive types in the corpus; nested types
// are a Phase 4 extension.
func decodeCell(arr arrow.Array, i int, typ iceberg.Type) (*valueNode, error) {
	if arr.IsNull(i) {
		return nil, nil
	}
	name := typ.Type()
	switch a := arr.(type) {
	case *array.Boolean:
		return &valueNode{Type: name, Value: a.Value(i)}, nil
	case *array.Int32:
		return &valueNode{Type: name, Value: a.Value(i)}, nil
	case *array.Int64:
		// 64-bit integers are emitted as JSON strings (PR #2 rule).
		return &valueNode{Type: name, Value: strconv.FormatInt(a.Value(i), 10)}, nil
	case *array.Float32:
		return &valueNode{Type: name, Value: a.Value(i)}, nil
	case *array.Float64:
		return &valueNode{Type: name, Value: a.Value(i)}, nil
	case *array.String:
		return &valueNode{Type: name, Value: a.Value(i)}, nil
	case *array.LargeString:
		return &valueNode{Type: name, Value: a.Value(i)}, nil
	}
	return nil, fmt.Errorf("unsupported arrow type %s for iceberg type %s (Phase 4)", arr.DataType(), name)
}
