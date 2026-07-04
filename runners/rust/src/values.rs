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

//! Typed-value model and its conversion to iceberg [`Datum`]/[`Literal`].
//!
//! A value in the op-log carries its physical type via a YAML tag (`!int`,
//! `!long`, `!decimal`, `!struct`, ...). That tag makes the int-vs-long
//! distinction (iceberg-go#880) authorable and pinnable. This module decodes a
//! `serde_yaml::Value` into a [`TypedValue`] and converts a primitive one to a
//! [`Datum`] for the target iceberg type (used for schema defaults and delete
//! predicates).

use std::collections::BTreeMap;

use anyhow::{Result, anyhow, bail};
use iceberg::spec::{Datum, Literal, PrimitiveType, Type};
use serde_yaml::Value as YamlValue;

/// One authored value with its physical type. Exactly one shape is populated
/// depending on `tag`.
#[derive(Debug, Clone)]
#[allow(dead_code)] // `tag` records the authored physical type (int vs long) for later phases.
pub enum TypedValue {
    /// A primitive scalar rendered as a string (parsed via iceberg casting).
    /// `tag` is the spec type name without the leading '!': int, long, ...
    Primitive { tag: String, scalar: String },
    /// An explicit null (`!null`).
    Null,
    /// A nested struct: field name -> value.
    Struct(BTreeMap<String, TypedValue>),
    /// A nested list.
    List(Vec<TypedValue>),
    /// A nested map (corpus keys are always strings).
    Map(Vec<(String, TypedValue)>),
}

impl TypedValue {
    /// Decode a tagged YAML value into a TypedValue. The tag is the spec type
    /// name; nested tags carry mapping/sequence bodies.
    pub fn from_yaml(v: &YamlValue) -> Result<TypedValue> {
        match v {
            YamlValue::Tagged(tagged) => {
                let tag = tag_name(&tagged.tag);
                match tag.as_str() {
                    "null" => Ok(TypedValue::Null),
                    "struct" => {
                        let mut fields = BTreeMap::new();
                        let m = tagged
                            .value
                            .as_mapping()
                            .ok_or_else(|| anyhow!("!struct expects a mapping"))?;
                        for (k, val) in m {
                            let key = k
                                .as_str()
                                .ok_or_else(|| anyhow!("struct key must be a string"))?
                                .to_string();
                            fields.insert(key, TypedValue::from_yaml(val)?);
                        }
                        Ok(TypedValue::Struct(fields))
                    }
                    "list" => {
                        let seq = tagged
                            .value
                            .as_sequence()
                            .ok_or_else(|| anyhow!("!list expects a sequence"))?;
                        let mut elems = Vec::with_capacity(seq.len());
                        for item in seq {
                            elems.push(TypedValue::from_yaml(item)?);
                        }
                        Ok(TypedValue::List(elems))
                    }
                    "map" => {
                        let m = tagged
                            .value
                            .as_mapping()
                            .ok_or_else(|| anyhow!("!map expects a mapping"))?;
                        let mut entries = Vec::with_capacity(m.len());
                        for (k, val) in m {
                            let key = k
                                .as_str()
                                .ok_or_else(|| anyhow!("map key must be a string"))?
                                .to_string();
                            entries.push((key, TypedValue::from_yaml(val)?));
                        }
                        Ok(TypedValue::Map(entries))
                    }
                    _ => {
                        // primitive: keep the scalar as a string.
                        let scalar = yaml_scalar_to_string(&tagged.value)?;
                        Ok(TypedValue::Primitive { tag, scalar })
                    }
                }
            }
            YamlValue::Null => Ok(TypedValue::Null),
            other => Err(anyhow!("value {other:?} has no physical-type tag")),
        }
    }

    // Reserved shape predicates for later phases (delete predicates, nested
    // Phase 4 build/decode). Kept alongside the model they describe.
    #[allow(dead_code)]
    pub fn is_null(&self) -> bool {
        matches!(self, TypedValue::Null)
    }

    #[allow(dead_code)]
    pub fn is_nested(&self) -> bool {
        matches!(
            self,
            TypedValue::Struct(_) | TypedValue::List(_) | TypedValue::Map(_)
        )
    }

    /// The primitive scalar string, if this is a primitive.
    #[allow(dead_code)]
    pub fn scalar(&self) -> Option<&str> {
        match self {
            TypedValue::Primitive { scalar, .. } => Some(scalar),
            _ => None,
        }
    }

    /// Convert a primitive TypedValue to an iceberg Datum of the target type.
    /// Parsing routes through iceberg's typed / from_str constructors. Nested
    /// and null values have no scalar Datum.
    pub fn to_datum(&self, ty: &Type) -> Result<Datum> {
        let scalar = match self {
            TypedValue::Primitive { scalar, .. } => scalar,
            TypedValue::Null => bail!("null has no datum"),
            _ => bail!("nested value has no scalar datum"),
        };
        let prim = match ty {
            Type::Primitive(p) => p,
            _ => bail!("cannot build a scalar datum for non-primitive type {ty}"),
        };
        datum_for_primitive(prim, scalar)
    }

    /// Convert to an iceberg Literal for schema defaults.
    pub fn to_literal(&self, ty: &Type) -> Result<Literal> {
        Ok(self.to_datum(ty)?.into())
    }
}

/// Build a Datum for a primitive type from its string scalar.
fn datum_for_primitive(p: &PrimitiveType, s: &str) -> Result<Datum> {
    let d = match p {
        PrimitiveType::Boolean => Datum::bool_from_str(s)?,
        PrimitiveType::Int => Datum::int(s.parse::<i32>()?),
        PrimitiveType::Long => Datum::long(s.parse::<i64>()?),
        PrimitiveType::Float => Datum::float(s.parse::<f32>()?),
        PrimitiveType::Double => Datum::double(s.parse::<f64>()?),
        PrimitiveType::Date => Datum::date_from_str(s)?,
        PrimitiveType::Time => Datum::time_from_str(s)?,
        PrimitiveType::Timestamp => Datum::timestamp_from_str(s)?,
        PrimitiveType::Timestamptz => Datum::timestamptz_from_str(s)?,
        PrimitiveType::String => Datum::string(s),
        PrimitiveType::Uuid => Datum::uuid_from_str(s)?,
        PrimitiveType::Decimal { .. } => Datum::decimal_from_str(s)?,
        other => bail!("unsupported primitive type for datum: {other:?}"),
    };
    Ok(d)
}

/// The tag name without the leading '!'. serde_yaml's Tag Display renders as
/// "!name"; nobang-normalized equality means we compare the bare name.
fn tag_name(tag: &serde_yaml::value::Tag) -> String {
    tag.to_string().trim_start_matches('!').to_string()
}

/// Render a YAML scalar (string/int/float/bool) as its string form for typed
/// parsing.
fn yaml_scalar_to_string(v: &YamlValue) -> Result<String> {
    match v {
        YamlValue::String(s) => Ok(s.clone()),
        YamlValue::Number(n) => Ok(n.to_string()),
        YamlValue::Bool(b) => Ok(b.to_string()),
        YamlValue::Null => Ok("null".to_string()),
        other => Err(anyhow!("cannot render scalar from {other:?}")),
    }
}
