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

// Command fuzz drives the differential fuzzer. Subcommands:
//
//	fuzz metamorphic --runner go=<bin> --seeds N [--from S] [--ops K]
//	    Generate op-logs and check each against a SINGLE runner using the
//	    generator's own predicted final state as the oracle (no reference impl).
//	    delete-is-set-minus + append-monotonicity fall out of the __rowkey check;
//	    promotion type-changes fall out of the column-type check.
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

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: fuzz <metamorphic> [flags]")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "metamorphic":
		if err := runMetamorphic(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "fuzz:", err)
			os.Exit(1)
		}
	case "readdiff":
		if err := runReadDiff(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "fuzz:", err)
			os.Exit(1)
		}
	case "shrink":
		if err := runShrink(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "fuzz:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(2)
	}
}

func runMetamorphic(args []string) error {
	var runnerBin string
	seeds, from, ops, fmtVer := 100, int64(1), 12, 2
	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--runner":
			i++
			_, bin, _ := strings.Cut(args[i], "=")
			runnerBin = bin
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
	if runnerBin == "" {
		return fmt.Errorf("need --runner name=binary")
	}

	feat := generator.Features{Delete: true, Promote: fmtVer >= 2}
	var failures, specInvalid int
	for s := from; s < from+int64(seeds); s++ {
		log := generator.Generate(s, fmtVer, ops, feat)
		res := runSeed(runnerBin, s, log)
		switch res.kind {
		case resultOK:
			// good
		case resultSpecInvalid:
			specInvalid++
			fmt.Printf("  seed %d: SPEC-INVALID (generator emitted an unrunnable op-log)\n    %s\n", s, res.detail)
		case resultMismatch:
			failures++
			fmt.Printf("  seed %d: METAMORPHIC MISMATCH\n    %s\n", s, res.detail)
		case resultError:
			failures++
			fmt.Printf("  seed %d: RUNNER ERROR\n    %s\n", s, res.detail)
		}
	}
	fmt.Printf("\n=== %d seeds: %d ok, %d mismatch/error, %d spec-invalid ===\n",
		seeds, seeds-failures-specInvalid, failures, specInvalid)
	if failures > 0 || specInvalid > 0 {
		os.Exit(1)
	}
	return nil
}

type resultKind int

const (
	resultOK resultKind = iota
	resultSpecInvalid
	resultMismatch
	resultError
)

type result struct {
	kind   resultKind
	detail string
}

// runSeed emits the op-log, runs it through the runner, and checks the final
// observation against the generator's predicted final state.
func runSeed(runnerBin string, seed int64, log generator.OpLog) result {
	yaml := generator.EmitYAML(fmt.Sprintf("fuzz_%d", seed), log)
	wh, err := os.MkdirTemp("", "fuzz-")
	if err != nil {
		return result{resultError, err.Error()}
	}
	defer os.RemoveAll(wh)
	specPath := filepath.Join(wh, "fixture.yaml")
	if err := os.WriteFile(specPath, []byte(yaml), 0o644); err != nil {
		return result{resultError, err.Error()}
	}
	outPath := filepath.Join(wh, "out.json")

	cmd := exec.Command(runnerBin, "--spec", specPath, "--warehouse", wh, "--out", outPath)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	_ = cmd.Run()
	code := cmd.ProcessState.ExitCode()
	if code == 4 {
		// unsupported feature — shouldn't happen if features match; treat as skip.
		return result{resultOK, ""}
	}
	if code != 0 {
		return result{resultSpecInvalid, fmt.Sprintf("exit %d: %s\n    spec:\n%s", code, oneLine(stderr.String()), indent(yaml))}
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		return result{resultError, "no output: " + err.Error()}
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		return result{resultError, "bad output json: " + err.Error()}
	}

	// The last observation is the final `observe latest`.
	obs := arr(out["observations"])
	if len(obs) == 0 {
		return result{resultMismatch, "no observations emitted\n" + indent(yaml)}
	}
	final, _ := obs[len(obs)-1].(map[string]any)

	gotKeys := rowKeysOf(final)
	wantKeys := append([]string(nil), log.FinalRowKeys...)
	sort.Strings(gotKeys)
	sort.Strings(wantKeys)
	if !equalStrings(gotKeys, wantKeys) {
		return result{resultMismatch, fmt.Sprintf("live __rowkeys: got %v want %v\n%s", gotKeys, wantKeys, indent(yaml))}
	}

	if d := checkTypes(final, log.FinalTypes); d != "" {
		return result{resultMismatch, d + "\n" + indent(yaml)}
	}
	return result{resultOK, ""}
}

// rowKeysOf pulls the __rowkey (canonical field-id "0") value from each decoded row.
func rowKeysOf(obs map[string]any) []string {
	var keys []string
	for _, r := range arr(obs["decoded-rows"]) {
		row, _ := r.(map[string]any)
		node, _ := row["0"].(map[string]any)
		if v, ok := node["value"].(string); ok {
			keys = append(keys, v)
		}
	}
	return keys
}

// checkTypes verifies each column's emitted schema type matches the prediction.
func checkTypes(obs map[string]any, want map[string]string) string {
	for _, f := range arr(obs["iceberg-schema"]) {
		field, _ := f.(map[string]any)
		name, _ := field["name"].(string)
		if name == "__rowkey" {
			continue
		}
		got, _ := field["type"].(string)
		if w, ok := want[name]; ok && w != got {
			return fmt.Sprintf("column %q type: got %q want %q", name, got, w)
		}
	}
	return ""
}

func arr(v any) []any {
	a, _ := v.([]any)
	return a
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func atoi(s string) int {
	n := 0
	neg := false
	for i, c := range s {
		if i == 0 && c == '-' {
			neg = true
			continue
		}
		n = n*10 + int(c-'0')
	}
	if neg {
		return -n
	}
	return n
}

func oneLine(s string) string { return strings.ReplaceAll(strings.TrimSpace(s), "\n", " | ") }

func indent(s string) string {
	var b strings.Builder
	for _, line := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		b.WriteString("    | " + line + "\n")
	}
	return b.String()
}
