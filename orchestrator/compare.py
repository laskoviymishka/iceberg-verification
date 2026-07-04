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

"""The canonical-output comparator.

Compares a runner's emitted output against a golden on the *logical* form only —
normalizing where the spec leaves it open, pinning what the spec pins:

- rows are a multiset: sort each observation's decoded-rows by __rowkey
  (canonical field-id "0") before comparing, since a scan is unordered;
- `at`/`bind` on observations are informational (the resolved point), not pinned;
- everything else (snapshots, per-observation iceberg-schema, the row values) is
  compared exactly.

Returns a list of structured diffs (empty == match) so callers can render them.
"""

from __future__ import annotations

import json
from typing import Any


def _rowkey(row: dict | None) -> str:
    if row is None:
        return ""
    return json.dumps(row.get("0"), sort_keys=True)


def _sorted_rows(rows: list) -> list:
    return sorted(rows, key=_rowkey)


class Diff:
    """One structured difference, renderable by the site."""

    def __init__(self, path: str, got: Any, want: Any):
        self.path = path
        self.got = got
        self.want = want

    def to_dict(self) -> dict:
        return {"path": self.path, "got": self.got, "want": self.want}


def compare(got: dict, want: dict) -> list[dict]:
    """Compare emitted output `got` to golden `want`; return structured diffs."""
    diffs: list[Diff] = []

    # accept flag (read/write both carry it)
    if got.get("accept", True) != want.get("accept", True):
        diffs.append(Diff("accept", got.get("accept"), want.get("accept")))

    # snapshots (write side); absent in read/oracle goldens
    gs = got.get("snapshots", [])
    ws = want.get("snapshots", [])
    if len(gs) != len(ws):
        diffs.append(Diff("snapshots.length", len(gs), len(ws)))
    for i, (a, b) in enumerate(zip(gs, ws)):
        if a != b:
            diffs.append(Diff(f"snapshots[{i}]", a, b))

    # observations
    go = got.get("observations", [])
    wo = want.get("observations", [])
    if len(go) != len(wo):
        diffs.append(Diff("observations.length", len(go), len(wo)))
    for i, (a, b) in enumerate(zip(go, wo)):
        if a.get("iceberg-schema") != b.get("iceberg-schema"):
            diffs.append(
                Diff(f"observations[{i}].iceberg-schema", a.get("iceberg-schema"), b.get("iceberg-schema"))
            )
        arows = _sorted_rows(a.get("decoded-rows", []))
        brows = _sorted_rows(b.get("decoded-rows", []))
        if arows != brows:
            diffs.append(Diff(f"observations[{i}].decoded-rows", arows, brows))

    return [d.to_dict() for d in diffs]
