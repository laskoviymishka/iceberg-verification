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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
)

// buildArrowTable materializes the given rows into an arrow.Table matching the
// table's live iceberg schema (sc must be the schema AFTER CreateTable, whose
// field-ids iceberg-go has reassigned). Columns are built in schema order
// (field 0 is __rowkey); each row value is matched to its column by NAME.
// Missing values are appended as null so write-default fills them.
//
// Phase 0-2 supports primitive columns (string/int/long and other scalars
// castable via the arrow builder's AppendValueFromString). Nested struct/list/
// map columns are a Phase 4 extension and return an error here.
func buildArrowTable(sc *iceberg.Schema, rows []Row) (arrow.Table, error) {
	arrowSchema, err := icetable.SchemaToArrowSchema(sc, nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("schema to arrow: %w", err)
	}

	fields := sc.Fields()
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer bldr.Release()

	for rowIdx, row := range rows {
		for colIdx, f := range fields {
			fb := bldr.Field(colIdx)
			if f.Name == rowKeyName {
				if err := fb.AppendValueFromString(row.RowKey); err != nil {
					return nil, fmt.Errorf("row %d __rowkey: %w", rowIdx, err)
				}
				continue
			}
			tv, ok := row.Values[f.Name]
			if !ok || tv == nil || tv.isNull {
				fb.AppendNull()
				continue
			}
			if tv.isNested() {
				return nil, fmt.Errorf("row %d field %q: nested values not supported before Phase 4", rowIdx, f.Name)
			}
			if err := fb.AppendValueFromString(tv.scalar); err != nil {
				return nil, fmt.Errorf("row %d field %q (%s %q): %w", rowIdx, f.Name, tv.tagName, tv.scalar, err)
			}
		}
	}

	rec := bldr.NewRecordBatch()
	defer rec.Release()

	return array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec}), nil
}
