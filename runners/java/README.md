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

# iceberg-java (reference) conformance runner

Emit-only runner for the iceberg-verification corpus, built on Apache Iceberg's Java
library — the reference implementation. It parses a logical op-log (the YAML authoring
profile), materializes a fresh JDBC(SQLite) + `file://` table, drives Iceberg's Java API,
scans, and emits canonical output matching `spec/expected-output.schema.json`. It never
compares — all judgment lives in the central orchestrator.

## Dependencies

Depends on the **published** Iceberg artifacts (`iceberg-core`, `iceberg-data`,
`iceberg-parquet` 1.11.0) plus Hadoop (local FS `Path`/`Configuration`), Parquet,
`sqlite-jdbc` (JDBC catalog, consistent with the Go/Rust runners), SnakeYAML (op-log), and
Jackson (JSON emit). Requires **JDK 17**.

## Build & run

```
export JAVA_HOME=<path to a JDK 17>
./gradlew build
./gradlew run --args="--spec ../../fixtures/v2_append_timetravel/fixture.yaml \
    --warehouse $(mktemp -d) --out /tmp/java-out.json"
# or run the built distribution / fat classpath directly.
```

Exit codes (see `spec/runner-contract.md`): `0` ok · `2` spec invalid · `3` input correctly
rejected · `4` op/kind unsupported (declared gap) · `1` crash.

### Maven repository

`build.gradle` resolves from Maven Central by default. In an environment where Central is
unreachable, override the repository:

```
./gradlew build -PmavenRepoUrl=<your maven mirror>
```

or set `mavenRepoUrl=...` in a local (gitignored) `gradle.properties`. The runner was
developed against the Databricks maven proxy for exactly this reason.

## Capability status

See [`supports.yaml`](supports.yaml). Java is the reference implementation and can run the
full DML surface. This runner currently implements **append + scan + time-travel** (Phases
0–2, including merge-on-read position deletes with self-computed positions). Schema evolution
and compaction are core-committable and are follow-up phases.
