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

"""Capability declarations and per-fixture feature derivation.

No YAML library is assumed (stdlib only), so these read the corpus's *narrow*
authoring profile with targeted line scanning rather than a general parser:
- supports.yaml: implementation/version scalars + supports.read/supports.write lists.
- a fixture's required features: derived from header.source + the op of each entry
  (and a delete's strategy/kind), enough to cross-check a `4 unsupported` exit
  against what the runner claimed.

This is deliberately corpus-specific; it is not a general YAML reader.
"""

from __future__ import annotations

import re
from pathlib import Path


def _strip_comment(s: str) -> str:
    # our corpus never quotes '#' inside these lines, so a bare split is safe.
    return s.split("#", 1)[0].rstrip()


def parse_supports(path: Path) -> dict:
    """Parse a runner supports.yaml into {implementation, version, read:[], write:[]}."""
    impl = ""
    version = ""
    read: list[str] = []
    write: list[str] = []
    section = None  # None | "read" | "write"
    in_supports = False

    for raw in path.read_text().splitlines():
        line = _strip_comment(raw)
        if not line.strip():
            continue
        stripped = line.strip()
        indent = len(line) - len(line.lstrip())

        if stripped.startswith("implementation:"):
            impl = stripped.split(":", 1)[1].strip()
            continue
        if stripped.startswith("version:"):
            version = stripped.split(":", 1)[1].strip()
            continue
        if stripped == "supports:":
            in_supports = True
            continue
        if in_supports and stripped in ("read:", "write:"):
            section = stripped[:-1]
            continue
        if in_supports and section and stripped.startswith("- "):
            # a supports item; deeper indent than the read:/write: key
            item = stripped[2:].strip()
            (read if section == "read" else write).append(item)
            continue
        # a non-list, non-section line at supports' own indent ends the block
        if in_supports and indent == 0 and not stripped.startswith("-"):
            in_supports = False
            section = None

    return {"implementation": impl, "version": version, "read": read, "write": write}


def fixture_features(fixture_yaml: str) -> dict:
    """Return {source, format_version, ops:[…], required:[features]} from a fixture.

    `ops` is a light per-entry summary for rendering; `required` is the set of
    capability keys the fixture exercises, for the supports.yaml cross-check.
    """
    source = "synthesized"
    fmt = None
    ops: list[dict] = []
    required: set[str] = set()

    lines = fixture_yaml.splitlines()

    # header scalars (appear before `entries:`)
    for raw in lines:
        line = _strip_comment(raw)
        m = re.match(r"\s*source:\s*(\S+)", line)
        if m:
            source = m.group(1)
        m = re.match(r"\s*format-version:\s*(\d+)", line)
        if m and fmt is None:
            fmt = int(m.group(1))
        if re.match(r"^entries:", line):
            break

    if source == "artifact":
        required.add("read.artifact")

    # entries: each starts with `- op: <name>` at 2-space indent
    cur: dict | None = None
    in_entries = False
    for raw in lines:
        line = _strip_comment(raw)
        if re.match(r"^entries:", line):
            in_entries = True
            continue
        if not in_entries:
            continue
        m = re.match(r"\s*-\s*op:\s*(\S+)", line)
        if m:
            cur = {"op": m.group(1)}
            ops.append(cur)
            _record_required(cur["op"], source, required)
            continue
        if cur is None:
            continue
        # capture a few params on the current entry (same or deeper indent)
        for key in ("at", "bind", "strategy", "kind"):
            km = re.match(rf"\s*{key}:\s*(\S+)", line)
            if km:
                cur[key] = km.group(1)
                if key in ("strategy", "kind") and cur["op"] == "delete":
                    _record_delete_required(cur, required)

    return {
        "source": source,
        "format_version": fmt,
        "ops": ops,
        "required": sorted(required),
    }


def _record_required(op: str, source: str, required: set[str]) -> None:
    prefix = "read" if source == "artifact" else "write"
    if op == "append":
        required.add(f"{prefix}.append" if source == "artifact" else "write.append")
    elif op == "observe":
        # time-travel is only implied by a non-'latest' observe; recorded in the
        # entry scan below is overkill — observe alone needs no feature.
        pass
    elif op == "rewrite":
        required.add("write.rewrite")
    elif op == "evolve-schema":
        required.add("write.evolve-schema")
    elif op == "evolve-spec":
        required.add("write.evolve-spec")
    elif op == "overwrite":
        required.add("write.overwrite")


def _record_delete_required(entry: dict, required: set[str]) -> None:
    strategy = entry.get("strategy", "merge-on-read")
    kind = entry.get("kind", "position")
    if strategy == "copy-on-write":
        required.add("write.delete.copy-on-write")
    else:
        required.add(f"write.delete.merge-on-read.{kind}")
