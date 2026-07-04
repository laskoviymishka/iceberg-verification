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

//! Canonical output shape (expected-output.schema.json). A blessed run of this
//! output becomes a golden. Serialized with serde_json; `#[serde(skip_serializing_if)]`
//! keeps optional facts out when absent so the shape matches the corpus goldens.

use std::collections::BTreeMap;

use serde::Serialize;

/// The runner's top-level emit shape.
#[derive(Debug, Serialize)]
pub struct CanonicalOutput {
    #[serde(rename = "spec-id")]
    pub spec_id: String,
    pub accept: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub snapshots: Vec<SnapshotOut>,
    pub observations: Vec<Observation>,
}

/// One commit-ordered snapshot with only spec-pinned, layout-independent facts.
/// The random snapshot-id is replaced by the commit ordinal.
#[derive(Debug, Serialize)]
pub struct SnapshotOut {
    pub ordinal: i64,
    pub parent: Option<i64>,
    pub operation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<SummaryOut>,
    #[serde(rename = "delete-files", skip_serializing_if = "Vec::is_empty")]
    pub delete_files: Vec<DeleteFile>,
}

/// Layout-independent aggregate metrics.
#[derive(Debug, Serialize)]
pub struct SummaryOut {
    #[serde(rename = "total-records", skip_serializing_if = "Option::is_none")]
    pub total_records: Option<i64>,
    #[serde(rename = "added-records", skip_serializing_if = "Option::is_none")]
    pub added_records: Option<i64>,
    #[serde(rename = "deleted-records", skip_serializing_if = "Option::is_none")]
    pub deleted_records: Option<i64>,
    #[serde(rename = "total-delete-files", skip_serializing_if = "Option::is_none")]
    pub total_delete_files: Option<i64>,
}

/// A spec-pinned fact about a delete file added in a snapshot.
#[derive(Debug, Serialize)]
pub struct DeleteFile {
    pub content: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
}

/// A decoded read of the table at one observe point.
#[derive(Debug, Serialize)]
pub struct Observation {
    pub at: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bind: Option<String>,
    #[serde(rename = "iceberg-schema")]
    pub iceberg_schema: Vec<SchemaField>,
    #[serde(rename = "decoded-rows")]
    pub decoded_rows: Vec<DecodedRow>,
}

/// One entry of an observation's iceberg-schema array.
#[derive(Debug, Serialize)]
pub struct SchemaField {
    #[serde(rename = "field-id")]
    pub field_id: i32,
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
}

/// One decoded row: field-id (as string key) -> value node. Row order is not
/// significant (the orchestrator sorts by __rowkey); null cells are omitted.
pub type DecodedRow = BTreeMap<String, ValueNode>;

/// PR #2 type-annotated value tree. Primitive: {type, value}. (Nested variants
/// are a Phase 4 extension.) 64-bit integers are serialized as JSON strings.
#[derive(Debug, Serialize)]
pub struct ValueNode {
    #[serde(rename = "type")]
    pub type_name: String,
    pub value: serde_json::Value,
}
