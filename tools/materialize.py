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

"""Materialize a checked-in read fixture so its embedded absolute paths resolve.

Iceberg table metadata embeds absolute ``file://`` paths at every level (metadata
json, the deflate-compressed avro manifest-lists and manifests, and data files),
and every implementation opens those paths verbatim. Rather than rewrite the whole
chain (which would mean re-encoding deflate avro without a library), a read fixture
is *minted* rooted at a fixed conventional absolute path and checked in under
``fixtures/<name>/bytes/``. Materializing = restoring those bytes to the exact path
they claim, so all embedded paths resolve with zero rewriting.

Usage:
    materialize.py <fixture-dir> [--root <fixed-root>]

Prints the absolute path of the fixture's current (latest) metadata.json, which the
runner then loads. ``--root`` defaults to the fixture's pinned root recorded in
``bytes/ROOT`` (written at mint time); pass it to override for parallel runs.
"""

import argparse
import pathlib
import re
import shutil
import sys


def latest_metadata(root: pathlib.Path) -> pathlib.Path:
    """The current metadata.json under a table dir: highest NNNNN-*.metadata.json."""
    candidates = sorted(root.rglob("*.metadata.json"))
    if not candidates:
        sys.exit(f"no *.metadata.json found under {root}")
    # metadata files are named NNNNN-<uuid>.metadata.json; the numeric prefix orders them.
    def version(p: pathlib.Path) -> int:
        m = re.match(r"(\d+)-", p.name)
        return int(m.group(1)) if m else -1
    return max(candidates, key=version)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("fixture", help="fixture directory containing bytes/")
    ap.add_argument("--root", help="fixed absolute root to restore bytes to (overrides bytes/ROOT)")
    args = ap.parse_args()

    fixture = pathlib.Path(args.fixture).resolve()
    src = fixture / "bytes"
    if not src.is_dir():
        sys.exit(f"{src} not found (fixture must contain a bytes/ dir)")

    root = args.root
    root_file = src / "ROOT"
    if root is None:
        if not root_file.is_file():
            sys.exit(f"no --root given and {root_file} missing")
        root = root_file.read_text().strip()
    root_path = pathlib.Path(root)

    # Restore the byte tree to the exact absolute path the metadata was minted at.
    if root_path.exists():
        shutil.rmtree(root_path)
    root_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src, root_path)
    # The ROOT marker is not part of the table; drop it from the materialized copy.
    (root_path / "ROOT").unlink(missing_ok=True)

    print(latest_metadata(root_path))


if __name__ == "__main__":
    main()
