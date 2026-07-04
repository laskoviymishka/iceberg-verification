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

//! The runner state machine: materialize the table from the header, then apply
//! each op-log entry, assembling canonical output. Emit-only.
//!
//! Capability note (iceberg-rust HEAD): append + scan + time-travel are
//! supported. Merge-on-read delete WRITE, schema type-promotion, and
//! compaction are not committable via any public API, so those ops raise
//! [`RunError::Unsupported`] (mapped to exit 4 — a declared gap in the matrix).

use std::collections::HashMap;

use anyhow::{Result, anyhow, bail};
use arrow_array::RecordBatch;
use futures::TryStreamExt;
use iceberg::spec::{DataFileFormat, FormatVersion};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use iceberg_catalog_sql::SqlCatalog;
use parquet::file::properties::WriterProperties;
use serde_yaml::Value as YamlValue;

use crate::arrow_build::{Row, build_record_batch};
use crate::arrow_decode::decode_scan;
use crate::emit::{CanonicalOutput, Observation, SnapshotOut, SummaryOut};
use crate::llog::{Entry, LLog};
use crate::schema::{ROW_KEY_NAME, build_canon_ids, build_schema};
use crate::values::TypedValue;

/// An error carrying its category so main can map it to the right exit code.
#[derive(Debug)]
pub enum RunError {
    /// Op/kind unsupported by iceberg-rust (declared gap) -> exit 4.
    Unsupported { entry: usize, feature: String },
    /// Any other execution failure -> exit 1.
    Other(anyhow::Error),
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RunError::Unsupported { entry, feature } => {
                write!(f, "entry {entry}: unsupported feature {feature:?}")
            }
            RunError::Other(e) => write!(f, "{e}"),
        }
    }
}

impl From<anyhow::Error> for RunError {
    fn from(e: anyhow::Error) -> Self {
        RunError::Other(e)
    }
}

/// Where an observe reads from.
enum At {
    Latest,
    Bind(String),
    Ordinal(i64),
}

pub struct Runner {
    catalog: SqlCatalog,
    table: iceberg::table::Table,
    canon_ids: HashMap<String, i32>,
    /// iceberg snapshot-id -> commit ordinal (0,1,2...).
    snapshot_ordinal: HashMap<i64, i64>,
    next_ordinal: i64,
    /// bind name -> snapshot-id current at that observe.
    binds: HashMap<String, i64>,
    out: CanonicalOutput,
}

impl Runner {
    /// Create the namespace + table from the header, returning a ready Runner.
    pub async fn materialize(catalog: SqlCatalog, log: &LLog) -> Result<Runner, RunError> {
        let schema = build_schema(&log.header.schema)?;
        let canon_ids = build_canon_ids(&log.header.schema);

        let ns = NamespaceIdent::new("default".to_string());
        catalog
            .create_namespace(&ns, HashMap::new())
            .await
            .map_err(|e| RunError::Other(anyhow!("create namespace: {e}")))?;

        let format_version = match log.header.format_version {
            1 => FormatVersion::V1,
            3 => FormatVersion::V3,
            _ => FormatVersion::V2,
        };

        let creation = TableCreation::builder()
            .name("t".to_string())
            .schema(schema)
            .format_version(format_version)
            .properties(log.header.properties.clone())
            .build();
        let table = catalog
            .create_table(&ns, creation)
            .await
            .map_err(|e| RunError::Other(anyhow!("create table: {e}")))?;

        Ok(Runner {
            catalog,
            table,
            canon_ids,
            snapshot_ordinal: HashMap::new(),
            next_ordinal: 0,
            binds: HashMap::new(),
            out: CanonicalOutput {
                spec_id: log.header.id.clone(),
                accept: true,
                snapshots: Vec::new(),
                observations: Vec::new(),
            },
        })
    }

    pub async fn run(&mut self, log: &LLog) -> Result<(), RunError> {
        for (idx, entry) in log.entries.iter().enumerate() {
            self.apply_entry(idx, entry).await?;
        }
        Ok(())
    }

    pub fn into_output(self) -> CanonicalOutput {
        self.out
    }

    async fn apply_entry(&mut self, idx: usize, e: &Entry) -> Result<(), RunError> {
        match e.op.as_str() {
            "append" => self.do_append(idx, e).await,
            "observe" => self.do_observe(idx, e).await,
            // Declared gaps: iceberg-rust has no committable MoR delete write
            // path, no type-promotion, and no compaction action.
            "delete" => Err(RunError::Unsupported {
                entry: idx,
                feature: delete_feature(e),
            }),
            "overwrite" => Err(RunError::Unsupported {
                entry: idx,
                feature: "op.overwrite".to_string(),
            }),
            "rewrite" => Err(RunError::Unsupported {
                entry: idx,
                feature: "rewrite".to_string(),
            }),
            "evolve-schema" => Err(RunError::Unsupported {
                entry: idx,
                feature: "evolve-schema.promote-type".to_string(),
            }),
            "evolve-spec" => Err(RunError::Unsupported {
                entry: idx,
                feature: "evolve-spec".to_string(),
            }),
            other => Err(RunError::Unsupported {
                entry: idx,
                feature: format!("op.{other}"),
            }),
        }
    }

    async fn do_append(&mut self, idx: usize, e: &Entry) -> Result<(), RunError> {
        let rows = parse_rows(e)
            .map_err(|err| RunError::Other(err.context(format!("entry {idx} append"))))?;
        let batch = build_record_batch(self.table.metadata().current_schema(), &rows)
            .map_err(|err| RunError::Other(err.context(format!("entry {idx} append"))))?;

        // Unique file-name prefix per append: DefaultFileNameGenerator resets
        // its counter to 00000 for each new writer, so a fixed prefix would
        // collide across appends and fast_append's duplicate check would reject
        // the second file. Keying on the entry index keeps every file distinct.
        let prefix = format!("data-{idx}");
        let data_files = self
            .write_data_files(batch, &prefix)
            .await
            .map_err(|err| RunError::Other(err.context(format!("entry {idx} write"))))?;

        let tx = Transaction::new(&self.table);
        let tx = tx
            .fast_append()
            .add_data_files(data_files)
            .apply(tx)
            .map_err(|e| RunError::Other(anyhow!("entry {idx} fast_append: {e}")))?;
        self.table = tx
            .commit(&self.catalog)
            .await
            .map_err(|e| RunError::Other(anyhow!("entry {idx} commit: {e}")))?;

        self.record_snapshot()?;
        Ok(())
    }

    /// Run the writer chain (ParquetWriter -> RollingFileWriter -> DataFileWriter)
    /// to turn one RecordBatch into committable DataFiles.
    async fn write_data_files(
        &self,
        batch: RecordBatch,
        prefix: &str,
    ) -> Result<Vec<iceberg::spec::DataFile>> {
        let schema = self.table.metadata().current_schema().clone();
        let loc = DefaultLocationGenerator::new(self.table.metadata())
            .map_err(|e| anyhow!("location generator: {e}"))?;
        let names =
            DefaultFileNameGenerator::new(prefix.to_string(), None, DataFileFormat::Parquet);
        let pw = ParquetWriterBuilder::new(WriterProperties::default(), schema);
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            pw,
            self.table.file_io().clone(),
            loc,
            names,
        );
        let mut w = DataFileWriterBuilder::new(rolling)
            .build(None)
            .await
            .map_err(|e| anyhow!("data file writer build: {e}"))?;
        w.write(batch).await.map_err(|e| anyhow!("write: {e}"))?;
        w.close().await.map_err(|e| anyhow!("close: {e}"))
    }

    async fn do_observe(&mut self, idx: usize, e: &Entry) -> Result<(), RunError> {
        let at = parse_at(e).map_err(RunError::Other)?;

        let scan = match &at {
            At::Latest => self.table.scan().build(),
            At::Bind(name) => {
                let snap = *self.binds.get(name).ok_or_else(|| {
                    RunError::Other(anyhow!("entry {idx}: unknown bind {name:?}"))
                })?;
                self.table.scan().snapshot_id(snap).build()
            }
            At::Ordinal(ord) => {
                let snap = self.ordinal_snapshot(*ord).ok_or_else(|| {
                    RunError::Other(anyhow!("entry {idx}: unknown ordinal {ord}"))
                })?;
                self.table.scan().snapshot_id(snap).build()
            }
        }
        .map_err(|e| RunError::Other(anyhow!("entry {idx} scan build: {e}")))?;

        let batches: Vec<RecordBatch> = scan
            .to_arrow()
            .await
            .map_err(|e| RunError::Other(anyhow!("entry {idx} to_arrow: {e}")))?
            .try_collect()
            .await
            .map_err(|e| RunError::Other(anyhow!("entry {idx} collect: {e}")))?;

        let scan_fields = self.table.metadata().current_schema().as_struct().fields();
        let (iceberg_schema, decoded_rows) = decode_scan(&batches, scan_fields, &self.canon_ids)
            .map_err(|err| RunError::Other(err.context(format!("entry {idx} decode"))))?;

        // 'at' echoes the resolved point: a bind name when the observe binds,
        // else the literal target (matches the golden vocabulary).
        let at_value = if let Some(bind) = &e.bind {
            serde_json::Value::String(bind.clone())
        } else {
            match &at {
                At::Latest => serde_json::Value::String("latest".to_string()),
                At::Bind(n) => serde_json::Value::String(n.clone()),
                At::Ordinal(o) => serde_json::Value::Number((*o).into()),
            }
        };

        self.out.observations.push(Observation {
            at: at_value,
            bind: e.bind.clone(),
            iceberg_schema,
            decoded_rows,
        });

        if let Some(bind) = &e.bind
            && let Some(snap) = self.table.metadata().current_snapshot()
        {
            self.binds.insert(bind.clone(), snap.snapshot_id());
        }
        Ok(())
    }

    fn ordinal_snapshot(&self, ordinal: i64) -> Option<i64> {
        self.snapshot_ordinal
            .iter()
            .find_map(|(id, ord)| if *ord == ordinal { Some(*id) } else { None })
    }

    /// Assign the newest snapshot its commit ordinal and record its facts.
    fn record_snapshot(&mut self) -> Result<(), RunError> {
        let snap = self
            .table
            .metadata()
            .current_snapshot()
            .ok_or_else(|| RunError::Other(anyhow!("no current snapshot after commit")))?
            .clone();
        if self.snapshot_ordinal.contains_key(&snap.snapshot_id()) {
            return Ok(());
        }
        let ordinal = self.next_ordinal;
        self.snapshot_ordinal.insert(snap.snapshot_id(), ordinal);
        self.next_ordinal += 1;

        let parent = snap
            .parent_snapshot_id()
            .and_then(|p| self.snapshot_ordinal.get(&p).copied());

        let summary = snap.summary();
        let props = &summary.additional_properties;
        self.out.snapshots.push(SnapshotOut {
            ordinal,
            parent,
            operation: summary.operation.as_str().to_string(),
            summary: summary_out(props),
            delete_files: Vec::new(), // iceberg-rust cannot write delete files (declared gap)
        });
        Ok(())
    }
}

/// Extract the layout-independent aggregate metrics from a snapshot's summary
/// properties. `total-delete-files` is suppressed when zero to match goldens.
fn summary_out(props: &HashMap<String, String>) -> Option<SummaryOut> {
    let get = |k: &str| props.get(k).and_then(|v| v.parse::<i64>().ok());
    let total_records = get("total-records");
    let added_records = get("added-records");
    let deleted_records = get("deleted-records");
    let total_delete_files = get("total-delete-files").filter(|n| *n > 0);
    if total_records.is_none()
        && added_records.is_none()
        && deleted_records.is_none()
        && total_delete_files.is_none()
    {
        return None;
    }
    Some(SummaryOut {
        total_records,
        added_records,
        deleted_records,
        total_delete_files,
    })
}

/// The declared-gap feature name for a delete entry, by kind.
fn delete_feature(e: &Entry) -> String {
    match e.kind.as_deref() {
        Some("equality") => "delete.merge-on-read.equality".to_string(),
        Some("deletion-vector") => "delete.merge-on-read.deletion-vector".to_string(),
        _ => "delete.merge-on-read.position".to_string(),
    }
}

/// Parse the append entry's rows into Row structs.
fn parse_rows(e: &Entry) -> Result<Vec<Row>> {
    let seq = e
        .rows
        .as_ref()
        .and_then(|v| v.as_sequence())
        .ok_or_else(|| anyhow!("append missing rows"))?;
    let mut rows = Vec::with_capacity(seq.len());
    for rv in seq {
        rows.push(parse_row(rv)?);
    }
    Ok(rows)
}

fn parse_row(v: &YamlValue) -> Result<Row> {
    let m = v
        .as_mapping()
        .ok_or_else(|| anyhow!("row must be a mapping"))?;
    let mut row_key = None;
    let mut values = std::collections::BTreeMap::new();
    for (k, val) in m {
        let key = k
            .as_str()
            .ok_or_else(|| anyhow!("row key must be a string"))?;
        if key == ROW_KEY_NAME {
            row_key = Some(
                val.as_str()
                    .ok_or_else(|| anyhow!("__rowkey must be a string"))?
                    .to_string(),
            );
            continue;
        }
        values.insert(key.to_string(), TypedValue::from_yaml(val)?);
    }
    Ok(Row {
        row_key: row_key.ok_or_else(|| anyhow!("row missing __rowkey"))?,
        values,
    })
}

/// Resolve an observe entry's 'at' node.
fn parse_at(e: &Entry) -> Result<At> {
    let at =
        e.at.as_ref()
            .ok_or_else(|| anyhow!("observe missing 'at'"))?;
    match at {
        YamlValue::String(s) if s == "latest" => Ok(At::Latest),
        YamlValue::String(s) => Ok(At::Bind(s.clone())),
        YamlValue::Number(n) => Ok(At::Ordinal(
            n.as_i64()
                .ok_or_else(|| anyhow!("ordinal must be an integer"))?,
        )),
        other => bail!("unsupported 'at' node {other:?}"),
    }
}
