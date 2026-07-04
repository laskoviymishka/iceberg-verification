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

//! Restore a checked-in read fixture's bytes to their pinned absolute root (so
//! the metadata's embedded `file://` paths resolve) via the shared
//! `tools/materialize.py`. The rewrite logic lives in the one Python script, not
//! per-runner; this just invokes it and returns the metadata.json path to load.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Result, anyhow, bail};

/// Materialize `fixture_dir` (which contains `bytes/`) and return the path to the
/// current metadata.json to load.
pub fn materialize(fixture_dir: &Path) -> Result<String> {
    let script = find_materializer(fixture_dir)?;
    let output = Command::new("python3")
        .arg(&script)
        .arg(fixture_dir)
        .output()
        .map_err(|e| anyhow!("run materialize.py: {e}"))?;
    if !output.status.success() {
        bail!(
            "materialize.py failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let meta = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if meta.is_empty() {
        bail!("materialize.py produced no metadata path");
    }
    Ok(meta)
}

/// Walk up from the fixture dir to locate `tools/materialize.py` at the corpus root.
fn find_materializer(fixture_dir: &Path) -> Result<PathBuf> {
    let mut dir = fixture_dir
        .canonicalize()
        .map_err(|e| anyhow!("canonicalize {}: {e}", fixture_dir.display()))?;
    loop {
        let candidate = dir.join("tools").join("materialize.py");
        if candidate.is_file() {
            return Ok(candidate);
        }
        match dir.parent() {
            Some(parent) => dir = parent.to_path_buf(),
            None => bail!(
                "tools/materialize.py not found above {}",
                fixture_dir.display()
            ),
        }
    }
}
