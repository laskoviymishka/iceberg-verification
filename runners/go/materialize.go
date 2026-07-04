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

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// materialize restores a checked-in read fixture's bytes to the pinned absolute
// root (so the metadata's embedded file:// paths resolve) via the shared
// tools/materialize.py, and returns the path to the current metadata.json to
// load. The rewrite logic lives in the one Python script, not per-runner.
func materialize(fixtureDir string) (string, error) {
	script, err := findMaterializer(fixtureDir)
	if err != nil {
		return "", err
	}
	cmd := exec.Command("python3", script, fixtureDir)
	var out, stderr strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("materialize.py: %w: %s", err, stderr.String())
	}
	metaPath := strings.TrimSpace(out.String())
	if metaPath == "" {
		return "", fmt.Errorf("materialize.py produced no metadata path")
	}
	return metaPath, nil
}

// findMaterializer walks up from the fixture dir to locate tools/materialize.py
// at the corpus root.
func findMaterializer(fixtureDir string) (string, error) {
	dir, err := filepath.Abs(fixtureDir)
	if err != nil {
		return "", err
	}
	for {
		candidate := filepath.Join(dir, "tools", "materialize.py")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("tools/materialize.py not found above %s", fixtureDir)
		}
		dir = parent
	}
}
