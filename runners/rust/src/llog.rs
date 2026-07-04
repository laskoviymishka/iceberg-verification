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

//! Parsed logical op-log (the YAML authoring profile of spec/l-log.schema.json).
//!
//! The outer structure is decoded with serde derive; per-value physical types
//! (the `!int` / `!long` / `!struct` YAML tags) are preserved as raw
//! `serde_yaml::Value` and interpreted by [`crate::values`], since serde's
//! derive cannot dispatch on YAML tags directly.

use serde::Deserialize;
use serde_yaml::Value as YamlValue;

/// The whole op-log: determinism/DDL header + ordered DML entries.
#[derive(Debug, Deserialize)]
pub struct LLog {
    pub header: Header,
    pub entries: Vec<Entry>,
}

/// Determinism contract + initial table definition. Unknown fields are ignored
/// so the model can grow without breaking existing fixtures. `partition_spec`
/// is parsed but not yet applied (partition-spec support is Phase 4 work).
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct Header {
    #[serde(default)]
    pub id: String,
    #[serde(rename = "format-version", default)]
    pub format_version: i32,
    /// "synthesized" (default) builds the table from schema+entries; "artifact"
    /// loads a checked-in table read-only (read conformance).
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub artifact: Option<LArtifact>,
    /// Required for source=synthesized; absent for read fixtures.
    #[serde(default)]
    pub schema: Option<LSchema>,
    #[serde(rename = "partition-spec", default)]
    pub partition_spec: Option<YamlValue>,
    #[serde(default)]
    pub properties: std::collections::HashMap<String, String>,
}

/// Locates a checked-in table for read (source: artifact) mode.
#[derive(Debug, Deserialize)]
pub struct LArtifact {
    /// Fixture-relative dir holding bytes/ + bytes/ROOT.
    pub path: String,
}

/// The op-log schema block: an ordered list of fields.
#[derive(Debug, Deserialize)]
pub struct LSchema {
    pub fields: Vec<LField>,
}

/// One schema field. `field_type` is a raw YAML value because an iceberg type
/// is either a primitive string ("long", "decimal(9,2)") or a nested
/// struct/list/map object; [`crate::schema`] resolves it.
#[derive(Debug, Deserialize)]
pub struct LField {
    pub id: i32,
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: YamlValue,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub doc: Option<String>,
    #[serde(rename = "initial-default", default)]
    pub initial_default: Option<YamlValue>,
    #[serde(rename = "write-default", default)]
    pub write_default: Option<YamlValue>,
}

/// One op-log entry. `op` discriminates; the remaining fields are the union of
/// every op's payload (only those relevant to `op` are populated).
///
/// Some fields are parsed (so serde accepts the fixtures) but not yet read by
/// the emit-only runner: `predicate`/`strategy`/`changes` are consumed once
/// delete/evolve leave declared-gap status; `expect`/`invariant` are always
/// orchestrator-only.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct Entry {
    pub op: String,

    // append: rows is a sequence of row mappings (kept raw for tagged values).
    #[serde(default)]
    pub rows: Option<YamlValue>,

    // delete
    #[serde(default)]
    pub predicate: Option<YamlValue>,
    #[serde(default)]
    pub strategy: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,

    // evolve-schema
    #[serde(default)]
    pub changes: Option<YamlValue>,

    // observe: 'latest', a bound name, or a commit ordinal.
    #[serde(default)]
    pub at: Option<YamlValue>,
    #[serde(default)]
    pub bind: Option<String>,

    // authored-golden / oracle payloads are for the orchestrator; parsed but
    // not acted on by the emit-only runner.
    #[serde(default)]
    pub expect: Option<YamlValue>,
    #[serde(default)]
    pub invariant: Option<YamlValue>,
}
