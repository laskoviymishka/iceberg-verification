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

"""Conformance orchestrator: run every fixture against every runner, classify
each cell, and emit report.json — the single artifact the front-face renders.

The orchestrator is the only component that judges. Each runner is a black box:
it is invoked with --spec/--warehouse/--out, and its exit code + emitted output
are classified (cross-checked against its supports.yaml) into a matrix cell.

Usage:
    run.py --corpus <dir> --out report.json \\
           --runner go=<bin> --runner rust=<bin> --runner java=<bin>
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from compare import compare
from supports import fixture_features, parse_supports

# Exit codes the runner contract defines.
EXIT_OK = 0
EXIT_SPEC_INVALID = 2
EXIT_REJECT = 3
EXIT_UNSUPPORTED = 4


def discover_fixtures(corpus: Path) -> list[dict]:
    """Load every fixtures/<id>/ into a descriptor (features, ops, golden, yaml text)."""
    out = []
    for d in sorted((corpus / "fixtures").iterdir()):
        spec = d / "fixture.yaml"
        if not spec.is_file():
            continue
        text = spec.read_text()
        feats = fixture_features(text)
        golden_path = d / "expected.json"
        golden = json.loads(golden_path.read_text()) if golden_path.is_file() else None
        out.append(
            {
                "id": d.name,
                "spec_path": str(spec),
                "source": feats["source"],
                "format_version": feats["format_version"],
                "ops": feats["ops"],
                "required": feats["required"],
                "has_golden": golden is not None,
                "golden": golden,
                "yaml": text,
            }
        )
    return out


def run_cell(runner_bin: str, spec_path: str) -> dict:
    """Invoke a runner on one fixture; return {exit, stdout, stderr, output}."""
    with tempfile.TemporaryDirectory() as wh:
        out_path = Path(wh) / "out.json"
        proc = subprocess.run(
            [runner_bin, "--spec", spec_path, "--warehouse", wh, "--out", str(out_path)],
            capture_output=True,
            text=True,
            timeout=300,
        )
        output = None
        if out_path.is_file():
            try:
                output = json.loads(out_path.read_text())
            except json.JSONDecodeError:
                output = None
        return {
            "exit": proc.returncode,
            "stderr": proc.stderr.strip(),
            "output": output,
        }


def classify(fixture: dict, supports: dict, result: dict) -> dict:
    """Turn a runner result into a matrix cell {status, detail}.

    status ∈ pass | fail | declared-gap | undeclared-gap | oracle | reject
             | bad-fixture | error
    """
    code = result["exit"]

    if code == EXIT_UNSUPPORTED:
        # Which required feature is missing from what the runner claimed?
        claimed = _claimed_names(supports)
        missing = [r for r in fixture["required"] if r not in claimed]
        if missing:
            return {"status": "declared-gap", "detail": {"missing": missing, "stderr": result["stderr"]}}
        # runner exited 4 for a feature it DID claim → drift, a real failure.
        return {"status": "undeclared-gap", "detail": {"stderr": result["stderr"], "required": fixture["required"]}}

    if code == EXIT_SPEC_INVALID:
        # A crash can alias exit 2 (Go panics exit 2, same as the runner's own
        # "spec parse error"). Disambiguate on stderr so a panic never masquerades
        # as a corpus verdict — this is the robustness gap the runner contract warns
        # about, made explicit.
        if _looks_like_crash(result["stderr"]):
            return {"status": "error", "detail": {"exit": code, "stderr": result["stderr"]}}
        return {"status": "bad-fixture", "detail": {"stderr": result["stderr"]}}
    if code == EXIT_REJECT:
        return {"status": "reject", "detail": {"stderr": result["stderr"]}}
    if code != EXIT_OK:
        return {"status": "error", "detail": {"exit": code, "stderr": result["stderr"]}}

    # exit 0
    if result["output"] is None:
        return {"status": "error", "detail": {"stderr": "exit 0 but no valid output JSON"}}
    if not fixture["has_golden"]:
        # oracle-mode fixture (e.g. compaction no-op): executed, invariant not yet checked.
        return {"status": "oracle", "detail": {"observations": len(result["output"].get("observations", []))}}
    diffs = compare(result["output"], fixture["golden"])
    if diffs:
        return {"status": "fail", "detail": {"diffs": diffs}}
    return {"status": "pass", "detail": {}}


_CRASH_SIGNATURES = ("panic:", "goroutine ", "traceback", "thread 'main' panicked", "exception in thread")


def _looks_like_crash(stderr: str) -> bool:
    low = stderr.lower()
    return any(sig in low for sig in _CRASH_SIGNATURES)


def _claimed_names(supports: dict) -> set[str]:
    """The runner's claimed features as fully-qualified read.*/write.* names."""
    names = set()
    for x in supports["read"]:
        names.add(f"read.{x}")
    for x in supports["write"]:
        names.add(f"write.{x}")
    return names


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", default=".", help="corpus root (contains fixtures/ and runners/)")
    ap.add_argument("--out", default="report.json")
    ap.add_argument(
        "--runner",
        action="append",
        default=[],
        metavar="name=binary",
        help="a runner: name=path-to-binary (repeatable)",
    )
    args = ap.parse_args()

    corpus = Path(args.corpus).resolve()
    runners = []
    for spec in args.runner:
        name, _, binary = spec.partition("=")
        sup_path = corpus / "runners" / name / "supports.yaml"
        supports = parse_supports(sup_path) if sup_path.is_file() else {"read": [], "write": [], "version": ""}
        runners.append({"name": name, "binary": binary, "supports": supports})

    fixtures = discover_fixtures(corpus)

    cells = []
    for fx in fixtures:
        for rn in runners:
            result = run_cell(rn["binary"], fx["spec_path"])
            cell = classify(fx, rn["supports"], result)
            cell["fixture"] = fx["id"]
            cell["runner"] = rn["name"]
            cells.append(cell)
            print(f"  {fx['id']:34s} {rn['name']:5s} -> {cell['status']}", file=sys.stderr)

    report = {
        "schema": "iceberg-verification/report/v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "runners": [
            {
                "name": rn["name"],
                "version": rn["supports"].get("version", ""),
                "supports": {"read": rn["supports"]["read"], "write": rn["supports"]["write"]},
            }
            for rn in runners
        ],
        "fixtures": [
            {
                "id": fx["id"],
                "source": fx["source"],
                "format_version": fx["format_version"],
                "ops": fx["ops"],
                "required": fx["required"],
                "has_golden": fx["has_golden"],
                "golden": fx["golden"],
                "yaml": fx["yaml"],
            }
            for fx in fixtures
        ],
        "cells": cells,
    }
    Path(args.out).write_text(json.dumps(report, indent=2) + "\n")
    print(f"wrote {args.out}: {len(fixtures)} fixtures x {len(runners)} runners = {len(cells)} cells", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
