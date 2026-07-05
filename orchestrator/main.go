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

// Command orchestrator runs every fixture against every runner, classifies each
// (fixture × runner) cell, and writes report.json — the single artifact the
// front-face renders. The orchestrator is the only component that judges; each
// runner is a black box invoked with --spec/--warehouse/--out.
//
// Usage:
//
//	orchestrator --corpus <dir> --out report.json \
//	    --runner go=<bin> --runner rust=<bin> --runner java=<bin>
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Runner-contract exit codes.
const (
	exitOK          = 0
	exitSpecInvalid = 2
	exitReject      = 3
	exitUnsupported = 4
)

type runnerFlags []string

func (r *runnerFlags) String() string { return strings.Join(*r, ",") }
func (r *runnerFlags) Set(v string) error {
	*r = append(*r, v)
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "orchestrator:", err)
		os.Exit(1)
	}
}

func run() error {
	corpus := "."
	outPath := "report.json"
	fuzzPath := ""
	var runners runnerFlags
	// tiny hand-rolled flag parse (avoids flag pkg's single-value limit noise)
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--corpus":
			i++
			corpus = args[i]
		case "--out":
			i++
			outPath = args[i]
		case "--runner":
			i++
			runners = append(runners, args[i])
		case "--fuzz":
			i++
			fuzzPath = args[i]
		default:
			return fmt.Errorf("unknown arg %q", args[i])
		}
	}
	if len(runners) == 0 {
		return fmt.Errorf("need at least one --runner name=binary")
	}

	absCorpus, err := filepath.Abs(corpus)
	if err != nil {
		return err
	}

	type runnerRec struct {
		name     string
		binary   string
		version  string
		supports Supports
	}
	var recs []runnerRec
	for _, spec := range runners {
		name, binary, ok := strings.Cut(spec, "=")
		if !ok {
			return fmt.Errorf("bad --runner %q (want name=binary)", spec)
		}
		ver, sup, _ := parseSupports(filepath.Join(absCorpus, "runners", name, "supports.yaml"))
		recs = append(recs, runnerRec{name: name, binary: binary, version: ver, supports: sup})
	}

	fixtures, err := discoverFixtures(absCorpus)
	if err != nil {
		return err
	}

	var cells []Cell
	for _, fx := range fixtures {
		for _, rn := range recs {
			res := runCell(rn.binary, fx.specPath)
			cell := classify(fx, rn.supports, res)
			cell.Fixture = fx.ID
			cell.Runner = rn.name
			cells = append(cells, cell)
			fmt.Fprintf(os.Stderr, "  %-34s %-5s -> %s\n", fx.ID, rn.name, cell.Status)
		}
	}

	report := Report{
		Schema:      "iceberg-verification/report/v1",
		GeneratedAt: time.Now().UTC().Format("2006-01-02T15:04:05Z"),
	}
	for _, rn := range recs {
		report.Runners = append(report.Runners, RunnerInfo{Name: rn.name, Version: rn.version, Supports: rn.supports})
	}
	for _, fx := range fixtures {
		report.Fixtures = append(report.Fixtures, fx.Fixture)
	}
	report.Cells = cells

	// Attach a read-diff fuzz campaign result if one was produced (--fuzz). Kept
	// as raw JSON: the orchestrator does not interpret it, the site renders it.
	if fuzzPath != "" {
		fb, ferr := os.ReadFile(fuzzPath)
		if ferr != nil {
			return fmt.Errorf("read --fuzz %s: %w", fuzzPath, ferr)
		}
		if !json.Valid(fb) {
			return fmt.Errorf("--fuzz %s is not valid JSON", fuzzPath)
		}
		report.Fuzz = json.RawMessage(fb)
	}

	buf, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(outPath, append(buf, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %s: %d fixtures x %d runners = %d cells\n", outPath, len(fixtures), len(recs), len(cells))
	return nil
}

type fixtureRec struct {
	Fixture
	specPath string
}

func discoverFixtures(corpus string) ([]fixtureRec, error) {
	dir := filepath.Join(corpus, "fixtures")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []fixtureRec
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		specPath := filepath.Join(dir, e.Name(), "fixture.yaml")
		data, err := os.ReadFile(specPath)
		if err != nil {
			continue
		}
		source, fmtVer, ops, required, ferr := fixtureFeatures(data)
		if ferr != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), ferr)
		}
		var golden any
		hasGolden := false
		if gb, gerr := os.ReadFile(filepath.Join(dir, e.Name(), "expected.json")); gerr == nil {
			if json.Unmarshal(gb, &golden) == nil {
				hasGolden = true
			}
		}
		if ops == nil {
			ops = []Op{}
		}
		if required == nil {
			required = []string{}
		}
		out = append(out, fixtureRec{
			Fixture: Fixture{
				ID:            e.Name(),
				Source:        source,
				FormatVersion: fmtVer,
				Ops:           ops,
				Required:      required,
				HasGolden:     hasGolden,
				Golden:        golden,
				YAML:          string(data),
			},
			specPath: specPath,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

type cellResult struct {
	exitCode int
	stderr   string
	output   map[string]any
	hasOut   bool
}

// runCell invokes a runner on one fixture in a throwaway warehouse.
func runCell(binary, specPath string) cellResult {
	wh, err := os.MkdirTemp("", "wh-")
	if err != nil {
		return cellResult{exitCode: -1, stderr: err.Error()}
	}
	defer os.RemoveAll(wh)
	outFile := filepath.Join(wh, "out.json")

	cmd := exec.Command(binary, "--spec", specPath, "--warehouse", wh, "--out", outFile)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	_ = cmd.Run()
	code := cmd.ProcessState.ExitCode()

	res := cellResult{exitCode: code, stderr: strings.TrimSpace(stderr.String())}
	if data, rerr := os.ReadFile(outFile); rerr == nil {
		var out map[string]any
		if json.Unmarshal(data, &out) == nil {
			res.output = out
			res.hasOut = true
		}
	}
	return res
}

var crashSignatures = []string{"panic:", "goroutine ", "traceback", "thread 'main' panicked", "exception in thread"}

func looksLikeCrash(stderr string) bool {
	low := strings.ToLower(stderr)
	for _, sig := range crashSignatures {
		if strings.Contains(low, sig) {
			return true
		}
	}
	return false
}

// classify turns a runner result into a matrix cell.
func classify(fx fixtureRec, supports Supports, res cellResult) Cell {
	switch res.exitCode {
	case exitUnsupported:
		claimed := claimedNames(supports)
		var missing []string
		for _, r := range fx.Required {
			if !claimed[r] {
				missing = append(missing, r)
			}
		}
		if len(missing) > 0 {
			return Cell{Status: StatusDeclaredGap, Detail: map[string]any{"missing": missing, "stderr": res.stderr}}
		}
		// exited 4 for a feature it DID claim -> drift, a real failure.
		return Cell{Status: StatusUndeclaredGap, Detail: map[string]any{"required": fx.Required, "stderr": res.stderr}}

	case exitSpecInvalid:
		// A crash can alias exit 2 (Go panics exit 2, same as a real spec-invalid);
		// disambiguate on stderr so a crash never masquerades as a corpus verdict.
		if looksLikeCrash(res.stderr) {
			return Cell{Status: StatusError, Detail: map[string]any{"exit": res.exitCode, "stderr": res.stderr}}
		}
		return Cell{Status: StatusBadFixture, Detail: map[string]any{"stderr": res.stderr}}

	case exitReject:
		return Cell{Status: StatusReject, Detail: map[string]any{"stderr": res.stderr}}

	case exitOK:
		if !res.hasOut {
			return Cell{Status: StatusError, Detail: map[string]any{"stderr": "exit 0 but no valid output JSON"}}
		}
		if !fx.HasGolden {
			return Cell{Status: StatusOracle, Detail: map[string]any{"observations": len(arr(res.output["observations"]))}}
		}
		golden, _ := fx.Golden.(map[string]any)
		diffs := compareOutput(res.output, golden)
		if len(diffs) > 0 {
			return Cell{Status: StatusFail, Detail: map[string]any{"diffs": diffs}}
		}
		return Cell{Status: StatusPass, Detail: map[string]any{}}

	default:
		return Cell{Status: StatusError, Detail: map[string]any{"exit": res.exitCode, "stderr": res.stderr}}
	}
}
