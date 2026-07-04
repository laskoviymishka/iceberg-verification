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

//! Build an Arrow [`RecordBatch`] from op-log rows, matching the table's live
//! iceberg schema. Rows are matched to columns by NAME; missing values become
//! null so write-default fills them. Phase 0-2 supports primitive columns
//! (string / int / long and the other scalars in the base fixtures); nested
//! columns are a later extension and return an error.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use arrow_array::builder::{
    BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema as ArrowSchema};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::Schema;

use crate::schema::ROW_KEY_NAME;
use crate::values::TypedValue;

/// One decoded op-log row: the synthetic `__rowkey` plus values keyed by column
/// name.
pub struct Row {
    pub row_key: String,
    pub values: BTreeMap<String, TypedValue>,
}

/// Materialize rows into a RecordBatch whose schema is derived from the table's
/// live iceberg schema (`sc` must be the schema AFTER create_table — its
/// field-ids are what iceberg-rust assigned). Columns are built in schema order
/// (column 0 is `__rowkey`).
pub fn build_record_batch(sc: &Schema, rows: &[Row]) -> Result<RecordBatch> {
    let arrow_schema: ArrowSchema =
        schema_to_arrow_schema(sc).map_err(|e| anyhow!("schema_to_arrow_schema: {e}"))?;
    let arrow_schema = Arc::new(arrow_schema);

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());
    for field in arrow_schema.fields() {
        let name = field.name();
        let col = build_column(field.data_type(), name, rows)?;
        columns.push(col);
    }

    RecordBatch::try_new(arrow_schema, columns).map_err(|e| anyhow!("record batch: {e}"))
}

/// Build one column array by pulling each row's value for `name`.
fn build_column(dt: &DataType, name: &str, rows: &[Row]) -> Result<ArrayRef> {
    match dt {
        DataType::Utf8 => {
            let mut b = StringBuilder::new();
            for row in rows {
                if name == ROW_KEY_NAME {
                    b.append_value(&row.row_key);
                    continue;
                }
                match row.values.get(name) {
                    Some(TypedValue::Primitive { scalar, .. }) => b.append_value(scalar),
                    Some(TypedValue::Null) | None => b.append_null(),
                    Some(other) => bail!("column {name}: expected primitive, got {other:?}"),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int32 => {
            let mut b = Int32Builder::new();
            for row in rows {
                match row.values.get(name) {
                    Some(TypedValue::Primitive { scalar, .. }) => {
                        b.append_value(scalar.parse::<i32>()?)
                    }
                    Some(TypedValue::Null) | None => b.append_null(),
                    Some(other) => bail!("column {name}: expected primitive, got {other:?}"),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Int64 => {
            let mut b = Int64Builder::new();
            for row in rows {
                match row.values.get(name) {
                    Some(TypedValue::Primitive { scalar, .. }) => {
                        b.append_value(scalar.parse::<i64>()?)
                    }
                    Some(TypedValue::Null) | None => b.append_null(),
                    Some(other) => bail!("column {name}: expected primitive, got {other:?}"),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::new();
            for row in rows {
                match row.values.get(name) {
                    Some(TypedValue::Primitive { scalar, .. }) => {
                        b.append_value(scalar.parse::<bool>()?)
                    }
                    Some(TypedValue::Null) | None => b.append_null(),
                    Some(other) => bail!("column {name}: expected primitive, got {other:?}"),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float32 => {
            let mut b = Float32Builder::new();
            for row in rows {
                match row.values.get(name) {
                    Some(TypedValue::Primitive { scalar, .. }) => {
                        b.append_value(scalar.parse::<f32>()?)
                    }
                    Some(TypedValue::Null) | None => b.append_null(),
                    Some(other) => bail!("column {name}: expected primitive, got {other:?}"),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        DataType::Float64 => {
            let mut b = Float64Builder::new();
            for row in rows {
                match row.values.get(name) {
                    Some(TypedValue::Primitive { scalar, .. }) => {
                        b.append_value(scalar.parse::<f64>()?)
                    }
                    Some(TypedValue::Null) | None => b.append_null(),
                    Some(other) => bail!("column {name}: expected primitive, got {other:?}"),
                }
            }
            Ok(Arc::new(b.finish()))
        }
        other => bail!("column {name}: arrow type {other:?} not supported before Phase 4"),
    }
}
