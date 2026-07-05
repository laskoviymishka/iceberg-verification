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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/laskoviymishka/iceberg-verification/orchestrator/generator"
)

// readerBin is one reader impl under test.
type readerBin struct {
	name string
	bin  string
}

// runReadDiff generates op-logs, mints each into a real table with the Java
// reference, then points every reader at the minted bytes and diffs their
// canonical scan against Java's — a majority vote triages each divergence.
//
//	fuzz readdiff --java <bin> --reader go=<bin> --reader rust=<bin> \
//	    --seeds N [--from S] [--ops K] [--format-version V]
func runReadDiff(args []string) error {
	var javaBin string
	var readers []readerBin
	seeds, from, ops, fmtVer := 50, int64(1), 12, 2
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--java":
			i++
			javaBin = args[i]
		case "--reader":
			i++
			name, bin, _ := strings.Cut(args[i], "=")
			readers = append(readers, readerBin{name, bin})
		case "--seeds":
			i++
			seeds = atoi(args[i])
		case "--from":
			i++
			from = int64(atoi(args[i]))
		case "--ops":
			i++
			ops = atoi(args[i])
		case "--format-version":
			i++
			fmtVer = atoi(args[i])
		default:
			return fmt.Errorf("unknown arg %q", args[i])
		}
	}
	if javaBin == "" || len(readers) == 0 {
		return fmt.Errorf("need --java <bin> and at least one --reader name=bin")
	}

	feat := generator.Features{Delete: true, Promote: fmtVer >= 2, WideTypes: true}
	var divergences int
	for s := from; s < from+int64(seeds); s++ {
		log := generator.Generate(s, fmtVer, ops, feat)
		findings, err := readDiffSeed(javaBin, readers, s, log)
		if err != nil {
			fmt.Printf("  seed %d: SETUP ERROR: %s\n", s, err)
			continue
		}
		for _, f := range findings {
			divergences++
			fmt.Printf("  seed %d [%s] %s: %s\n", s, f.verdict, f.reader, f.detail)
		}
	}
	fmt.Printf("\n=== %d seeds x %d readers: %d divergence(s) ===\n", seeds, len(readers), divergences)
	return nil
}

// finding is one reader diverging from the Java reference on one seed.
type finding struct {
	reader  string
	verdict string // "reader-bug" | "escalate"
	detail  string
}

// readDiffSeed mints the op-log with Java, reads it back with Java (the golden)
// and each reader, and returns the readers whose canonical output diverges.
func readDiffSeed(javaBin string, readers []readerBin, seed int64, log generator.OpLog) ([]finding, error) {
	// The fixture dir must sit inside the repo tree so each runner's upward walk
	// finds tools/materialize.py (they search parents for it). ./tmp is gitignored.
	if err := os.MkdirAll("tmp", 0o755); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("tmp", "readdiff-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	// The mint seed is the full generated op-log (writes + evolution); the read
	// fixture observes only the final state.
	id := fmt.Sprintf("fuzz_%d", seed)
	if err := os.WriteFile(filepath.Join(dir, "seed.yaml"), []byte(generator.EmitYAML(id, log)), 0o644); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "fixture.yaml"), []byte(artifactFixtureYAML(id, log.FormatVersion)), 0o644); err != nil {
		return nil, err
	}

	// Mint bytes with Java into a per-seed pinned root (no cross-seed collision).
	root := filepath.Join(os.TempDir(), "iceberg-verification-fuzz", id)
	mint := exec.Command("python3", "tools/mint.py", dir, "--java-runner", javaBin, "--root", root)
	var mintErr strings.Builder
	mint.Stderr = &mintErr
	if err := mint.Run(); err != nil {
		// A mint failure is a Java-side finding worth surfacing, not a silent skip.
		return []finding{{reader: "java(mint)", verdict: "escalate", detail: "mint failed: " + oneLine(mintErr.String())}}, nil
	}

	// Java golden: mint.py already read the artifact back through Java and wrote
	// expected.json — reuse it rather than launch a third JVM per seed.
	goldenData, err := os.ReadFile(filepath.Join(dir, "expected.json"))
	if err != nil {
		return nil, fmt.Errorf("read golden: %w", err)
	}
	var golden map[string]any
	if err := json.Unmarshal(goldenData, &golden); err != nil {
		return nil, fmt.Errorf("parse golden: %w", err)
	}

	var findings []finding
	for _, rd := range readers {
		out, err := readerOutput(rd.bin, dir)
		if err != nil {
			findings = append(findings, finding{rd.name, "reader-bug", "read failed: " + err.Error()})
			continue
		}
		if d := diffObservation(golden, out); d != "" {
			// Majority vote is trivial with one reference + one reader; when more
			// readers agree with each other but not Java, that flips to escalate.
			findings = append(findings, finding{rd.name, "reader-bug", d})
		}
	}
	return findings, nil
}

// readerOutput runs one reader on the artifact fixture dir and returns its
// parsed canonical output.
func readerOutput(bin, fixtureDir string) (map[string]any, error) {
	wh, err := os.MkdirTemp("", "rd-wh-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(wh)
	outPath := filepath.Join(wh, "out.json")
	spec := filepath.Join(fixtureDir, "fixture.yaml")
	cmd := exec.Command(bin, "--spec", spec, "--warehouse", wh, "--out", outPath)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	_ = cmd.Run()
	if code := cmd.ProcessState.ExitCode(); code != 0 {
		return nil, fmt.Errorf("exit %d: %s", code, oneLine(stderr.String()))
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// diffObservation compares the single observation of two canonical outputs on
// the assertions that matter for a read: the schema (types) and the decoded
// row multiset. Returns "" when they agree.
func diffObservation(golden, got map[string]any) string {
	go1 := firstObs(golden)
	gt1 := firstObs(got)
	if go1 == nil || gt1 == nil {
		return "missing observation"
	}
	// schema types by column name
	gs := schemaByName(go1)
	ts := schemaByName(gt1)
	for name, typ := range gs {
		if tt, ok := ts[name]; !ok || tt != typ {
			return fmt.Sprintf("schema type for %q: java=%q reader=%q", name, typ, ts[name])
		}
	}
	// decoded row multiset (as canonicalized JSON strings, order-insensitive)
	grows := canonRows(go1)
	trows := canonRows(gt1)
	if !equalStrings(grows, trows) {
		return fmt.Sprintf("decoded rows differ: java=%d rows, reader=%d rows\n      java: %v\n      read: %v",
			len(grows), len(trows), grows, trows)
	}
	return ""
}

func firstObs(out map[string]any) map[string]any {
	obs := arr(out["observations"])
	if len(obs) == 0 {
		return nil
	}
	m, _ := obs[0].(map[string]any)
	return m
}

func schemaByName(obs map[string]any) map[string]string {
	m := map[string]string{}
	for _, f := range arr(obs["iceberg-schema"]) {
		field, _ := f.(map[string]any)
		name, _ := field["name"].(string)
		typ, _ := field["type"].(string)
		m[name] = typ
	}
	return m
}

func canonRows(obs map[string]any) []string {
	var rows []string
	for _, r := range arr(obs["decoded-rows"]) {
		b, _ := json.Marshal(r)
		rows = append(rows, string(b))
	}
	sort.Strings(rows)
	return rows
}

// artifactFixtureYAML is the read fixture the minted bytes are scanned through:
// source: artifact, one observe of the final state.
func artifactFixtureYAML(id string, formatVersion int) string {
	return fmt.Sprintf(`# Generated read fixture for %s (final-state scan of the minted table).
header:
  id: %s
  format-version: %d
  spec-anchor: "generated/fuzz"
  source: artifact
  artifact:
    path: .
entries:
  - op: observe
    at: latest
`, id, id, formatVersion)
}
