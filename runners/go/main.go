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

// Command runner executes an Iceberg logical op-log (L-log) against iceberg-go
// and emits canonical output for the conformance orchestrator to judge. It is
// emit-only: it parses, executes, scans, and prints — it never compares.
//
// Usage:
//
//	runner --spec <l-log.(yaml|json)> --warehouse <dir> --out <canonical.json>
//
// Exit codes (see spec/runner-contract.md):
//
//	0  executed, canonical output written
//	2  spec invalid / unparseable
//	3  input correctly rejected
//	4  op/kind unsupported by iceberg-go (declared gap)
//	1  crash / internal error
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/uptrace/bun/driver/sqliteshim"
	"gopkg.in/yaml.v3"

	// Register the file:// warehouse IO backend.
	_ "github.com/apache/iceberg-go/io"
)

const (
	exitOK          = 0
	exitCrash       = 1
	exitSpecInvalid = 2
	exitRejected    = 3
	exitUnsupported = 4
)

func main() {
	os.Exit(run())
}

func run() int {
	var specPath, warehouse, outPath string
	flag.StringVar(&specPath, "spec", "", "path to L-log spec (YAML or JSON)")
	flag.StringVar(&warehouse, "warehouse", "", "empty directory to use as a file:// warehouse")
	flag.StringVar(&outPath, "out", "", "path to write canonical output JSON")
	flag.Parse()

	if specPath == "" || warehouse == "" || outPath == "" {
		fmt.Fprintln(os.Stderr, "usage: runner --spec <l-log> --warehouse <dir> --out <canonical.json>")
		return exitCrash
	}

	// Parse + validate the spec. On failure -> exit 2.
	log, err := parseSpec(specPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "spec parse error: %v\n", err)
		return exitSpecInvalid
	}

	ctx := compute.WithAllocator(context.Background(), memory.DefaultAllocator)

	cat, err := openCatalog(ctx, warehouse)
	if err != nil {
		fmt.Fprintf(os.Stderr, "catalog setup error: %v\n", err)
		return exitCrash
	}

	r := &runner{
		ctx:   ctx,
		cat:   cat,
		ident: icetable.Identifier{"default", "t"},
	}
	if err := r.execute(log); err != nil {
		var unsup *unsupportedError
		if errors.As(err, &unsup) {
			fmt.Fprintf(os.Stderr, "unsupported: %v\n", unsup)
			return exitUnsupported
		}
		fmt.Fprintf(os.Stderr, "execution error: %v\n", err)
		return exitCrash
	}

	if err := writeOutput(outPath, r.out); err != nil {
		fmt.Fprintf(os.Stderr, "write output error: %v\n", err)
		return exitCrash
	}
	return exitOK
}

// parseSpec reads and decodes the L-log. The corpus authoring profile is YAML
// with custom type tags; canonical JSON is accepted too (JSON is valid YAML).
func parseSpec(path string) (*LLog, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var log LLog
	if err := yaml.Unmarshal(data, &log); err != nil {
		return nil, err
	}
	if len(log.Entries) == 0 {
		return nil, fmt.Errorf("spec has no entries")
	}
	if log.Header.FormatVersion < 1 || log.Header.FormatVersion > 3 {
		return nil, fmt.Errorf("invalid format-version %d", log.Header.FormatVersion)
	}
	return &log, nil
}

// openCatalog stands up a SQLite catalog with an in-memory metastore and a
// file:// warehouse rooted at the given directory. The runner owns the
// warehouse contents.
func openCatalog(ctx context.Context, warehouse string) (catalog.Catalog, error) {
	abs, err := filepath.Abs(warehouse)
	if err != nil {
		return nil, err
	}
	cat, err := catalog.Load(ctx, "conformance", iceberg.Properties{
		"type":            "sql",
		"uri":             ":memory:",
		sqlcat.DriverKey:  sqliteshim.ShimName,
		sqlcat.DialectKey: string(sqlcat.SQLite),
		"warehouse":       "file://" + abs,
	})
	if err != nil {
		return nil, err
	}
	return cat, nil
}

func writeOutput(path string, out *CanonicalOutput) error {
	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}
