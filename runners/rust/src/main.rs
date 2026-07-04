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

//! iceberg-rust conformance runner for the iceberg-verification corpus.
//!
//! Parses a logical op-log (the YAML authoring profile), materializes a fresh
//! SQLite + `file://` table, drives iceberg-rust's native API, scans, and emits
//! canonical output matching `expected-output.schema.json`. Emit-only: it never
//! compares — all judgment lives in the central orchestrator.
//!
//! Usage: `runner --spec <l-log.yaml> --warehouse <dir> --out <canonical.json>`
//!
//! Exit codes (spec/runner-contract.md):
//!   0  executed, canonical output written
//!   2  spec invalid / unparseable
//!   3  input correctly rejected
//!   4  op/kind unsupported by iceberg-rust (declared gap)
//!   1  crash / internal error

mod arrow_build;
mod arrow_decode;
mod emit;
mod interpret;
mod llog;
mod materialize;
mod schema;
mod values;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use iceberg::CatalogBuilder;
use iceberg::io::LocalFsStorageFactory;
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalog, SqlCatalogBuilder,
};

use crate::interpret::{RunError, Runner};
use crate::llog::LLog;

const EXIT_OK: u8 = 0;
const EXIT_CRASH: u8 = 1;
const EXIT_SPEC_INVALID: u8 = 2;
#[allow(dead_code)]
const EXIT_REJECTED: u8 = 3;
const EXIT_UNSUPPORTED: u8 = 4;

#[derive(Parser)]
#[command(about = "iceberg-rust conformance runner (emit-only)")]
struct Args {
    /// Path to the L-log spec (YAML authoring profile).
    #[arg(long)]
    spec: PathBuf,
    /// Empty directory to use as a file:// warehouse.
    #[arg(long)]
    warehouse: PathBuf,
    /// Path to write canonical output JSON.
    #[arg(long)]
    out: PathBuf,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();

    // Parse + validate the spec. On failure -> exit 2.
    let log = match parse_spec(&args.spec) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("spec parse error: {e}");
            return ExitCode::from(EXIT_SPEC_INVALID);
        }
    };

    let catalog = match open_catalog(&args.warehouse).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("catalog setup error: {e}");
            return ExitCode::from(EXIT_CRASH);
        }
    };

    match run(catalog, &log, &args.spec, &args.out).await {
        Ok(()) => ExitCode::from(EXIT_OK),
        Err(RunError::Unsupported { entry, feature }) => {
            eprintln!("unsupported: entry {entry}: {feature}");
            ExitCode::from(EXIT_UNSUPPORTED)
        }
        Err(RunError::Other(e)) => {
            eprintln!("execution error: {e:#}");
            ExitCode::from(EXIT_CRASH)
        }
    }
}

async fn run(catalog: SqlCatalog, log: &LLog, spec: &Path, out: &PathBuf) -> Result<(), RunError> {
    let spec_dir = spec
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));
    let mut runner = Runner::new(catalog, log, spec_dir).await?;
    runner.run(log).await?;
    let output = runner.into_output();
    write_output(out, &output).map_err(RunError::Other)?;
    Ok(())
}

/// Read and decode the L-log. The authoring profile is YAML with custom type
/// tags; canonical JSON is also valid YAML.
fn parse_spec(path: &PathBuf) -> anyhow::Result<LLog> {
    let data = std::fs::read_to_string(path)?;
    let log: LLog = serde_yaml::from_str(&data)?;
    if log.entries.is_empty() {
        anyhow::bail!("spec has no entries");
    }
    if log.header.format_version < 1 || log.header.format_version > 3 {
        anyhow::bail!("invalid format-version {}", log.header.format_version);
    }
    Ok(log)
}

/// Stand up a SQLite catalog with a `file://` warehouse rooted at the given
/// directory. The runner owns the warehouse contents.
async fn open_catalog(warehouse: &PathBuf) -> anyhow::Result<SqlCatalog> {
    let warehouse = std::fs::canonicalize(warehouse)?;
    let warehouse = warehouse
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("non-utf8 warehouse path"))?;
    let db_uri = format!("sqlite:{warehouse}/catalog.db");

    // The SQLite db file must exist before the catalog opens it.
    use sqlx::migrate::MigrateDatabase;
    sqlx::Sqlite::create_database(&db_uri)
        .await
        .map_err(|e| anyhow::anyhow!("create sqlite db: {e}"))?;

    let props = HashMap::from([
        (SQL_CATALOG_PROP_URI.to_string(), db_uri),
        (
            SQL_CATALOG_PROP_WAREHOUSE.to_string(),
            warehouse.to_string(),
        ),
        (
            SQL_CATALOG_PROP_BIND_STYLE.to_string(),
            SqlBindStyle::QMark.to_string(),
        ),
    ]);

    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("conformance", props)
        .await
        .map_err(|e| anyhow::anyhow!("load catalog: {e}"))
}

fn write_output(path: &PathBuf, out: &emit::CanonicalOutput) -> anyhow::Result<()> {
    let mut json = serde_json::to_string_pretty(out)?;
    json.push('\n');
    std::fs::write(path, json)?;
    Ok(())
}
