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

//! Decode scanned Arrow batches into the type-annotated tree keyed by canonical
//! field-id. Columns are aligned positionally against the scanned iceberg
//! schema (whose `NestedField`s carry the real field-ids + names in scan
//! order); each column's output id comes from `canon_ids` by name, so the
//! fixture's authored ids (`__rowkey` => 0) are emitted regardless of what
//! iceberg-rust assigned internally. 64-bit ints are emitted as JSON strings.

use std::collections::{BTreeMap, HashMap};

use anyhow::{Result, anyhow, bail};
use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray,
};
use iceberg::spec::NestedFieldRef;

use crate::emit::{DecodedRow, SchemaField, ValueNode};

/// Decode all batches of one observation into (iceberg-schema, decoded-rows).
/// `scan_fields` are the scanned schema's top-level fields, in the same order
/// as the arrow columns.
pub fn decode_scan(
    batches: &[RecordBatch],
    scan_fields: &[NestedFieldRef],
    canon_ids: &HashMap<String, i32>,
) -> Result<(Vec<SchemaField>, Vec<DecodedRow>)> {
    // iceberg-schema: one entry per top-level column, canonical field-ids.
    let mut iceberg_schema = Vec::with_capacity(scan_fields.len());
    for f in scan_fields {
        let id = *canon_ids
            .get(f.name.as_str())
            .ok_or_else(|| anyhow!("no canonical field-id for column {:?}", f.name))?;
        iceberg_schema.push(SchemaField {
            field_id: id,
            name: f.name.clone(),
            type_name: f.field_type.to_string(),
        });
    }

    let mut rows: Vec<DecodedRow> = Vec::new();
    for batch in batches {
        if batch.num_columns() != scan_fields.len() {
            bail!(
                "column count {} != schema field count {}",
                batch.num_columns(),
                scan_fields.len()
            );
        }
        let base = rows.len();
        for _ in 0..batch.num_rows() {
            rows.push(BTreeMap::new());
        }
        for (c, f) in scan_fields.iter().enumerate() {
            let id = *canon_ids.get(f.name.as_str()).unwrap();
            let key = id.to_string();
            let col = batch.column(c);
            for r in 0..batch.num_rows() {
                if let Some(node) = decode_cell(col.as_ref(), r, &f.field_type.to_string())? {
                    rows[base + r].insert(key.clone(), node);
                }
                // absent key = SQL null
            }
        }
    }
    Ok((iceberg_schema, rows))
}

/// Convert one arrow cell to a ValueNode; None for SQL null. Phase 0-2 covers
/// the primitive types in the base fixtures.
fn decode_cell(arr: &dyn Array, i: usize, type_name: &str) -> Result<Option<ValueNode>> {
    if arr.is_null(i) {
        return Ok(None);
    }
    if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
        return Ok(Some(ValueNode {
            type_name: type_name.to_string(),
            value: serde_json::Value::String(a.value(i).to_string()),
        }));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int32Array>() {
        return Ok(Some(ValueNode {
            type_name: type_name.to_string(),
            value: serde_json::Value::Number(a.value(i).into()),
        }));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        // 64-bit integers are emitted as JSON strings (PR #2 rule).
        return Ok(Some(ValueNode {
            type_name: type_name.to_string(),
            value: serde_json::Value::String(a.value(i).to_string()),
        }));
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return Ok(Some(ValueNode {
            type_name: type_name.to_string(),
            value: serde_json::Value::Bool(a.value(i)),
        }));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float32Array>() {
        return Ok(Some(ValueNode {
            type_name: type_name.to_string(),
            value: json_number_f64(a.value(i) as f64)?,
        }));
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return Ok(Some(ValueNode {
            type_name: type_name.to_string(),
            value: json_number_f64(a.value(i))?,
        }));
    }
    bail!(
        "unsupported arrow type {:?} for iceberg type {type_name} (Phase 4)",
        arr.data_type()
    )
}

/// A finite f64 as a JSON number. NaN/±Inf have no JSON representation, so they
/// are an explicit error rather than a silent null.
fn json_number_f64(v: f64) -> Result<serde_json::Value> {
    serde_json::Number::from_f64(v)
        .map(serde_json::Value::Number)
        .ok_or_else(|| anyhow!("non-finite float {v} has no JSON representation"))
}
