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

	"github.com/laskoviymishka/iceberg-verification/orchestrator/generator"
)

// runShrink takes a seed known to diverge on read-diff, delta-minimizes its
// op-log to the smallest still-failing scenario, and writes it as a committed
// fixture (bytes/ + golden), so every fuzz find becomes a permanent regression.
//
//	fuzz shrink --java <bin> --reader <name>=<bin> --seed S [--ops K]
//	    [--format-version V] [--out fixtures/<name>]
func runShrink(args []string) error {
	var javaBin string
	var reader readerBin
	var seed int64
	ops, fmtVer := 12, 2
	outDir := ""
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--java":
			i++
			javaBin = args[i]
		case "--reader":
			i++
			name, bin, _ := strings.Cut(args[i], "=")
			reader = readerBin{name, bin}
		case "--seed":
			i++
			seed = int64(atoi(args[i]))
		case "--ops":
			i++
			ops = atoi(args[i])
		case "--format-version":
			i++
			fmtVer = atoi(args[i])
		case "--out":
			i++
			outDir = args[i]
		default:
			return fmt.Errorf("unknown arg %q", args[i])
		}
	}
	if javaBin == "" || reader.bin == "" || seed == 0 {
		return fmt.Errorf("need --java, --reader name=bin, and --seed")
	}

	feat := generator.Features{Delete: true, Promote: fmtVer >= 2, WideTypes: true}
	full := generator.Generate(seed, fmtVer, ops, feat)

	// Confirm the seed diverges before shrinking.
	if !divergesOn(javaBin, reader, full) {
		return fmt.Errorf("seed %d does not diverge on %s — nothing to shrink", seed, reader.name)
	}
	fmt.Printf("seed %d diverges on %s; shrinking %d ops...\n", seed, reader.name, len(full.Ops))

	shrunk := shrink(javaBin, reader, full)
	fmt.Printf("minimized to %d ops\n", len(shrunk.Ops))

	if outDir == "" {
		outDir = filepath.Join("fixtures", fmt.Sprintf("fuzz_%d_%s", seed, reader.name))
	}
	if err := writeFixture(javaBin, outDir, seed, shrunk); err != nil {
		return err
	}
	fmt.Printf("wrote regression fixture %s\n", outDir)
	return nil
}

// shrink repeatedly drops ops (and trailing appends' rows) while the divergence
// persists — classic delta minimization over the op list. The final observe is
// always kept so the scenario still asserts a state.
func shrink(javaBin string, reader readerBin, log generator.OpLog) generator.OpLog {
	cur := log
	changed := true
	for changed {
		changed = false
		for i := 0; i < len(cur.Ops); i++ {
			// never drop the terminal observe
			if i == len(cur.Ops)-1 && cur.Ops[i].Kind == "observe" {
				continue
			}
			trial := cur
			trial.Ops = append(append([]generator.Op(nil), cur.Ops[:i]...), cur.Ops[i+1:]...)
			// recompute predicted final state is unnecessary for read-diff (Java
			// mints ground truth); just re-run the diff.
			if divergesOn(javaBin, reader, trial) {
				cur = trial
				changed = true
				break
			}
		}
	}
	return cur
}

// divergesOn re-runs one op-log through mint + the single reader and reports
// whether the reader's canonical output still differs from the Java golden.
func divergesOn(javaBin string, reader readerBin, log generator.OpLog) bool {
	findings, err := readDiffSeed(javaBin, []readerBin{reader}, log.Seed, log)
	if err != nil {
		return false // setup error is not a divergence
	}
	return len(findings) > 0
}

// writeFixture mints the shrunk op-log into a permanent fixture directory
// (seed.yaml + artifact fixture.yaml + bytes/ + expected.json).
func writeFixture(javaBin, outDir string, seed int64, log generator.OpLog) error {
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	id := filepath.Base(outDir)
	if err := os.WriteFile(filepath.Join(outDir, "seed.yaml"), []byte(generator.EmitYAML(id, log)), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, "fixture.yaml"), []byte(artifactFixtureYAML(id, log.FormatVersion)), 0o644); err != nil {
		return err
	}
	// mint.py fills bytes/ + expected.json from the reference; the pinned root is
	// the fixture's own convention (mint.py defaults it under /tmp).
	mint := exec.Command("python3", "tools/mint.py", outDir, "--java-runner", javaBin)
	var stderr strings.Builder
	mint.Stderr = &stderr
	if err := mint.Run(); err != nil {
		return fmt.Errorf("mint fixture: %w: %s", err, oneLine(stderr.String()))
	}
	return nil
}
