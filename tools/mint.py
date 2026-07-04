#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Mint a read (`source: artifact`) fixture from a synthesized seed op-log.

This is the reproducible counterpart to materialize.py (which only *restores* an
already-checked-in byte tree). Minting runs a WRITE op-log through the reference
runner (iceberg-java) into a table rooted at the fixture's pinned absolute path,
then snapshots that warehouse into ``fixtures/<name>/bytes/`` and captures the
canonical logical output as ``expected.json`` (the golden).

Why iceberg-java: it is the reference implementation, so the bytes it writes and
the values it decodes define the contract the other runners are checked against.

Pipeline:
    seed.yaml (synthesized write ops)
      -> java runner --warehouse <pinned-root>          # writes default.db/t
      -> copy <pinned-root> into fixtures/<name>/bytes/ + write bytes/ROOT
      -> java runner over the ARTIFACT fixture.yaml       # read-only scan
      -> that canonical output becomes expected.json (golden; observations only)

The seed op-log is kept as ``fixtures/<name>/seed.yaml`` for provenance; it is
NOT what runners execute — they execute the artifact-mode ``fixture.yaml``.

Usage:
    JAVA_HOME=... tools/mint.py <fixture-dir> --java-runner <path-to-java-runner> \\
        [--root <fixed-root>]

Expects <fixture-dir> to already contain seed.yaml and fixture.yaml (artifact mode
pointing at bytes/). Writes bytes/, bytes/ROOT, and expected.json.
"""

import argparse
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile

DEFAULT_ROOT_BASE = "/tmp/iceberg-verification-fixtures"


def run_java(runner: str, spec: pathlib.Path, warehouse: pathlib.Path, out: pathlib.Path) -> None:
    """Invoke the java runner per the runner contract; fail loudly on non-zero exit."""
    warehouse.mkdir(parents=True, exist_ok=True)
    res = subprocess.run(
        [runner, "--spec", str(spec), "--warehouse", str(warehouse), "--out", str(out)],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        sys.exit(f"java runner failed (exit {res.returncode}) on {spec}:\n{res.stderr}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("fixture", help="fixture directory (must contain seed.yaml + fixture.yaml)")
    ap.add_argument("--java-runner", required=True, help="path to the iceberg-java runner binary")
    ap.add_argument("--root", help="pinned absolute root (default: %s/<name>)" % DEFAULT_ROOT_BASE)
    args = ap.parse_args()

    fixture = pathlib.Path(args.fixture).resolve()
    name = fixture.name
    seed = fixture / "seed.yaml"
    artifact_spec = fixture / "fixture.yaml"
    if not seed.is_file():
        sys.exit(f"{seed} not found (mint needs a synthesized seed op-log)")
    if not artifact_spec.is_file():
        sys.exit(f"{artifact_spec} not found (mint needs the artifact-mode fixture.yaml)")

    root = pathlib.Path(args.root or f"{DEFAULT_ROOT_BASE}/{name}")

    # 1. Write the seed op-log into the pinned root via the reference runner.
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        run_java(args.java_runner, seed, root, pathlib.Path(tmp) / "seed-out.json")

    # 2. Snapshot the written warehouse into fixtures/<name>/bytes/ + ROOT marker.
    #    Exclude what is not part of the Iceberg table: the runner's SQLite catalog
    #    (the artifact is loaded by bare metadata path, not via a catalog) and
    #    Hadoop's .crc checksum sidecars (write-tool noise, not table data).
    dst = fixture / "bytes"
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(
        root,
        dst,
        ignore=shutil.ignore_patterns("catalog.db", "*.crc"),
    )
    (dst / "ROOT").write_text(str(root) + "\n")

    # 3. Capture the golden by reading back through the ARTIFACT fixture (so the
    #    golden has the artifact-mode shape: observations only, no snapshots).
    #    materialize.py restores bytes/ to the pinned root the artifact runner loads.
    with tempfile.TemporaryDirectory() as tmp:
        golden_out = pathlib.Path(tmp) / "golden.json"
        run_java(args.java_runner, artifact_spec, pathlib.Path(tmp) / "wh", golden_out)
        golden = json.loads(golden_out.read_text())

    # Belt-and-suspenders: artifact mode should not carry snapshots; drop if present.
    golden.pop("snapshots", None)
    (fixture / "expected.json").write_text(json.dumps(golden, indent=2) + "\n")

    print(f"minted {name}: bytes/ ({sum(1 for _ in dst.rglob('*') if _.is_file())} files), "
          f"expected.json ({len(golden.get('observations', []))} observations), root {root}")


if __name__ == "__main__":
    main()
