<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# iceberg-rust conformance runner

Emit-only runner for the iceberg-verification corpus, built on
[`iceberg-rust`](https://github.com/apache/iceberg-rust). It parses a logical
op-log (the YAML authoring profile), materializes a fresh SQLite + `file://`
table, drives iceberg-rust's native API, scans, and emits canonical output
matching `spec/expected-output.schema.json`. It never compares — all judgment
lives in the central orchestrator.

## Dependency on iceberg-rust

`Cargo.toml` uses **path dependencies** to a local iceberg-rust checkout, so the
runner tracks the implementation's working copy:

```toml
iceberg = { path = "../../../iceberg-rust/crates/iceberg" }
iceberg-catalog-sql = { path = "../../../iceberg-rust/crates/catalog/sql" }
```

Adjust the paths if your iceberg-rust checkout is elsewhere. `arrow-*` and
`parquet` are pinned to the same major (58) iceberg-rust uses so `RecordBatch`
and `WriterProperties` share type identity across the crate boundary.

## Build & run

```
cargo build
./target/debug/runner \
    --spec ../../fixtures/v2_append_timetravel/fixture.yaml \
    --warehouse "$(mktemp -d)" \
    --out /tmp/out.json
```

Exit codes (see `spec/runner-contract.md`): `0` ok · `2` spec invalid · `3`
input correctly rejected · `4` op/kind unsupported (declared gap) · `1` crash.

## Capability status

See [`supports.yaml`](supports.yaml). In short: **append + scan + time-travel**
are green; **merge-on-read delete write, type promotion, and compaction** are
declared gaps (exit 4) because iceberg-rust has no committable public API for
them today. `v2_append_timetravel` is the universal-write baseline that runs
green on every implementation.
