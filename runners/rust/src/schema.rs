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

//! Build an iceberg [`Schema`] from the op-log schema block, prepending the
//! synthetic `__rowkey` column, and map column names to the canonical output
//! field-ids used in the golden.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use iceberg::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type};
use serde_yaml::Value as YamlValue;

use crate::llog::{LField, LSchema};
use crate::values::TypedValue;

/// Reserved synthetic identity column carried on every row so the orchestrator
/// can compare row multisets across physical layouts. Materialized as a real
/// string column; the emitter relabels its field-id to the canonical 0.
pub const ROW_KEY_NAME: &str = "__rowkey";

/// Internal iceberg field-id for `__rowkey`. Sits above the small corpus ids
/// but clear of iceberg's reserved 2147483xxx metadata-column range.
const ROW_KEY_FIELD_ID: i32 = 100_000;

/// Build the iceberg schema (with a leading `__rowkey` string column) from the
/// op-log schema block.
pub fn build_schema(ls: &LSchema) -> Result<Schema> {
    let mut fields: Vec<Arc<NestedField>> = Vec::with_capacity(ls.fields.len() + 1);
    fields.push(Arc::new(NestedField::optional(
        ROW_KEY_FIELD_ID,
        ROW_KEY_NAME,
        Type::Primitive(PrimitiveType::String),
    )));
    for f in &ls.fields {
        fields.push(Arc::new(build_field(f)?));
    }
    Schema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()
        .map_err(|e| anyhow!("schema build: {e}"))
}

/// Map each top-level column name to the canonical output field-id: `__rowkey`
/// is 0, user columns keep the id the fixture declared. iceberg-rust reassigns
/// fresh field-ids at create_table, so the emitter relabels by name back to
/// these authored ids.
pub fn build_canon_ids(ls: &LSchema) -> HashMap<String, i32> {
    let mut m = HashMap::new();
    m.insert(ROW_KEY_NAME.to_string(), 0);
    for f in &ls.fields {
        m.insert(f.name.clone(), f.id);
    }
    m
}

/// Derive canonical output field-ids from a loaded (read mode) table schema by
/// top-level column POSITION: __rowkey => 0, then user columns 1,2,... in schema
/// order. Read fixtures carry no authored ids and the impl's internal ids differ,
/// so position is the stable canonical labeling shared across implementations.
pub fn canon_ids_from_schema(schema: &Schema) -> HashMap<String, i32> {
    let mut m = HashMap::new();
    let mut next = 1;
    for f in schema.as_struct().fields() {
        if f.name == ROW_KEY_NAME {
            m.insert(f.name.clone(), 0);
        } else {
            m.insert(f.name.clone(), next);
            next += 1;
        }
    }
    m
}

fn build_field(f: &LField) -> Result<NestedField> {
    let ty = resolve_type(&f.field_type)?;
    let mut nf = if f.required {
        NestedField::required(f.id, &f.name, ty.clone())
    } else {
        NestedField::optional(f.id, &f.name, ty.clone())
    };
    if let Some(doc) = &f.doc {
        nf = nf.with_doc(doc);
    }
    if let Some(dv) = &f.initial_default {
        let tv = TypedValue::from_yaml(dv)?;
        nf = nf.with_initial_default(tv.to_literal(&ty)?);
    }
    if let Some(dv) = &f.write_default {
        let tv = TypedValue::from_yaml(dv)?;
        nf = nf.with_write_default(tv.to_literal(&ty)?);
    }
    Ok(nf)
}

/// Resolve an op-log type node (a primitive string like "long" /
/// "decimal(9,2)" / "fixed[4]", or a nested struct/list/map object) to a Type.
pub fn resolve_type(v: &YamlValue) -> Result<Type> {
    match v {
        YamlValue::String(s) => primitive_type(s),
        YamlValue::Mapping(_) => {
            let kind = v
                .get("type")
                .and_then(|t| t.as_str())
                .ok_or_else(|| anyhow!("nested type missing 'type'"))?;
            match kind {
                "struct" => resolve_struct(v),
                "list" => resolve_list(v),
                "map" => resolve_map(v),
                other => bail!("unknown nested type {other:?}"),
            }
        }
        other => bail!("unsupported type node {other:?}"),
    }
}

fn primitive_type(name: &str) -> Result<Type> {
    let p = match name {
        "boolean" => PrimitiveType::Boolean,
        "int" => PrimitiveType::Int,
        "long" => PrimitiveType::Long,
        "float" => PrimitiveType::Float,
        "double" => PrimitiveType::Double,
        "date" => PrimitiveType::Date,
        "time" => PrimitiveType::Time,
        "timestamp" => PrimitiveType::Timestamp,
        "timestamptz" => PrimitiveType::Timestamptz,
        "timestamp_ns" => PrimitiveType::TimestampNs,
        "timestamptz_ns" => PrimitiveType::TimestamptzNs,
        "string" => PrimitiveType::String,
        "uuid" => PrimitiveType::Uuid,
        "binary" => PrimitiveType::Binary,
        _ => {
            if let Some(rest) = name
                .strip_prefix("decimal(")
                .and_then(|s| s.strip_suffix(')'))
            {
                let (p, s) = rest
                    .split_once(',')
                    .ok_or_else(|| anyhow!("bad decimal type {name:?}"))?;
                return Ok(Type::Primitive(PrimitiveType::Decimal {
                    precision: p.trim().parse()?,
                    scale: s.trim().parse()?,
                }));
            }
            if let Some(n) = name
                .strip_prefix("fixed[")
                .and_then(|s| s.strip_suffix(']'))
            {
                return Ok(Type::Primitive(PrimitiveType::Fixed(n.trim().parse()?)));
            }
            bail!("unsupported primitive type {name:?}");
        }
    };
    Ok(Type::Primitive(p))
}

fn resolve_struct(v: &YamlValue) -> Result<Type> {
    let fields = v
        .get("fields")
        .and_then(|f| f.as_sequence())
        .ok_or_else(|| anyhow!("struct missing fields"))?;
    let mut out = Vec::with_capacity(fields.len());
    for fv in fields {
        let lf: LField = serde_yaml::from_value(fv.clone())?;
        out.push(Arc::new(build_field(&lf)?));
    }
    Ok(Type::Struct(StructType::new(out)))
}

fn resolve_list(v: &YamlValue) -> Result<Type> {
    let element_id = v
        .get("element-id")
        .and_then(|e| e.as_i64())
        .ok_or_else(|| anyhow!("list missing element-id"))? as i32;
    let element = resolve_type(
        v.get("element")
            .ok_or_else(|| anyhow!("list missing element"))?,
    )?;
    let required = v
        .get("element-required")
        .and_then(|b| b.as_bool())
        .unwrap_or(false);
    let field = if required {
        NestedField::required(element_id, "element", element)
    } else {
        NestedField::optional(element_id, "element", element)
    };
    Ok(Type::List(ListType::new(Arc::new(field))))
}

fn resolve_map(v: &YamlValue) -> Result<Type> {
    let key_id = v
        .get("key-id")
        .and_then(|e| e.as_i64())
        .ok_or_else(|| anyhow!("map missing key-id"))? as i32;
    let value_id = v
        .get("value-id")
        .and_then(|e| e.as_i64())
        .ok_or_else(|| anyhow!("map missing value-id"))? as i32;
    let key = resolve_type(v.get("key").ok_or_else(|| anyhow!("map missing key"))?)?;
    let value = resolve_type(v.get("value").ok_or_else(|| anyhow!("map missing value"))?)?;
    let required = v
        .get("value-required")
        .and_then(|b| b.as_bool())
        .unwrap_or(false);
    let key_field = Arc::new(NestedField::required(key_id, "key", key));
    let value_field = if required {
        NestedField::required(value_id, "value", value)
    } else {
        NestedField::optional(value_id, "value", value)
    };
    Ok(Type::Map(MapType::new(key_field, Arc::new(value_field))))
}
