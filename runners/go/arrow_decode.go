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
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
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
	case *array.Date32:
		// ISO date, matching the java reference (LocalDate.toString): YYYY-MM-DD.
		return &valueNode{Type: name, Value: a.Value(i).ToTime().Format("2006-01-02")}, nil
	case *array.Time64:
		unit := a.DataType().(*arrow.Time64Type).Unit
		return &valueNode{Type: name, Value: formatTime(a.Value(i).ToTime(unit))}, nil
	case *array.Timestamp:
		tt := a.DataType().(*arrow.TimestampType)
		return &valueNode{Type: name, Value: formatTimestamp(a.Value(i).ToTime(tt.Unit), tt.TimeZone != "")}, nil
	case *array.Decimal128:
		// exact string with the type's scale, matching BigDecimal.toPlainString.
		scale := a.DataType().(*arrow.Decimal128Type).Scale
		return &valueNode{Type: name, Value: a.Value(i).ToString(scale)}, nil
	case *array.FixedSizeBinary:
		// uuid and fixed both surface as fixed-size binary; emit lowercase hex.
		return &valueNode{Type: name, Value: hexEncode(a.Value(i))}, nil
	case *array.Binary:
		return &valueNode{Type: name, Value: hexEncode(a.Value(i))}, nil
	case *extensions.UUIDArray:
		// iceberg uuid may surface as the arrow UUID extension: emit canonical string.
		return &valueNode{Type: name, Value: a.Value(i).String()}, nil
	}
	return nil, fmt.Errorf("unsupported arrow type %s for iceberg type %s (Phase 4)", arr.DataType(), name)
}

// formatTime renders a time value as HH:MM:SS, appending a fractional part only
// when non-zero, matching java's LocalTime.toString.
func formatTime(t time.Time) string {
	if t.Nanosecond() == 0 {
		return t.Format("15:04:05")
	}
	return t.Format("15:04:05.999999999")
}

// formatTimestamp renders a timestamp as ISO-8601 matching the java reference:
// LocalDateTime for no-tz (no suffix), OffsetDateTime for tz (UTC -> "Z").
func formatTimestamp(t time.Time, withTZ bool) string {
	base := "2006-01-02T15:04:05"
	if t.Nanosecond() != 0 {
		base = "2006-01-02T15:04:05.999999999"
	}
	if withTZ {
		return t.UTC().Format(base) + "Z"
	}
	return t.Format(base)
}

// hexEncode renders bytes as lowercase hex with no prefix (matches the java hex()).
func hexEncode(b []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, x := range b {
		out[i*2] = digits[x>>4]
		out[i*2+1] = digits[x&0xf]
	}
	return string(out)
}
