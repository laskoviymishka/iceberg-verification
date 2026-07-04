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
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"
)

// unsupportedError signals an op/kind this implementation does not support
// (declared gap). main maps it to exit code 4.
type unsupportedError struct {
	entryIdx int
	feature  string
}

func (e *unsupportedError) Error() string {
	return fmt.Sprintf("entry %d: unsupported feature %q", e.entryIdx, e.feature)
}

// runner executes an L-log against iceberg-go and accumulates canonical output.
type runner struct {
	ctx      context.Context
	cat      catalog.Catalog
	ident    icetable.Identifier
	tbl      *icetable.Table
	specPath string // path to the l-log file (to resolve fixture-relative artifact dirs)

	schema   *iceberg.Schema
	canonIDs map[string]int // column name -> canonical output field-id (__rowkey => 0)

	// snapshotOrdinal maps an iceberg snapshot-id to its commit ordinal
	// (0,1,2...) so output references the ordinal, not the random id.
	snapshotOrdinal map[int64]int
	nextOrdinal     int

	// binds maps an observe 'bind' name to the snapshot-id current at that
	// point, so a later 'observe at: <name>' time-travels to it.
	binds map[string]int64

	out *CanonicalOutput
}

// execute acquires the table (synthesized from the header, or loaded from a
// checked-in artifact) then processes each entry in order, assembling canonical
// output.
func (r *runner) execute(log *LLog) error {
	r.snapshotOrdinal = map[int64]int{}
	r.binds = map[string]int64{}
	r.out = &CanonicalOutput{SpecID: log.Header.ID, Accept: true}

	if log.Header.Source == "artifact" {
		if err := r.loadArtifact(log.Header); err != nil {
			return fmt.Errorf("load artifact: %w", err)
		}
	} else {
		sc, _, err := buildSchema(log.Header.Schema)
		if err != nil {
			return fmt.Errorf("build schema: %w", err)
		}
		r.schema = sc
		r.canonIDs = buildCanonIDs(log.Header.Schema)
		if err := r.createTable(log.Header); err != nil {
			return fmt.Errorf("create table: %w", err)
		}
	}

	for i, e := range log.Entries {
		if err := r.applyEntry(i, e); err != nil {
			return err
		}
	}
	return nil
}

// loadArtifact materializes a checked-in table (restoring its bytes to the
// pinned root so embedded absolute paths resolve) and loads it read-only. The
// canonical field-ids are derived by name from the loaded schema; __rowkey => 0
// and user columns keep the field-id the table carries (same relabel rule the
// synthesized path uses).
func (r *runner) loadArtifact(h Header) error {
	if h.Artifact == nil {
		return fmt.Errorf("source: artifact requires an 'artifact' block")
	}
	fixtureDir := filepath.Join(filepath.Dir(r.specPath), h.Artifact.Path)
	metaPath, err := materialize(fixtureDir)
	if err != nil {
		return err
	}
	tbl, err := icetable.NewFromLocation(
		r.ctx, icetable.Identifier{"default", "t"}, metaPath,
		iceio.LoadFSFunc(nil, metaPath), nil,
	)
	if err != nil {
		return fmt.Errorf("NewFromLocation %s: %w", metaPath, err)
	}
	r.tbl = tbl
	r.canonIDs = canonIDsFromSchema(tbl.Schema())
	return nil
}

// createTable creates the namespace + table from the header schema and
// properties.
func (r *runner) createTable(h Header) error {
	ns := icetable.Identifier{"default"}
	if err := r.cat.CreateNamespace(r.ctx, ns, nil); err != nil {
		return err
	}
	opts := []catalog.CreateTableOpt{}
	props := iceberg.Properties{
		"format-version": fmt.Sprintf("%d", h.FormatVersion),
	}
	for k, v := range h.Properties {
		props[k] = v
	}
	opts = append(opts, catalog.WithProperties(props))

	tbl, err := r.cat.CreateTable(r.ctx, r.ident, r.schema, opts...)
	if err != nil {
		return err
	}
	r.tbl = tbl
	return nil
}

// applyEntry dispatches one op-log entry.
func (r *runner) applyEntry(idx int, e Entry) error {
	switch e.Op {
	case "append":
		return r.doAppend(idx, e)
	case "observe":
		return r.doObserve(idx, e)
	case "delete":
		return r.doDelete(idx, e)
	default:
		return &unsupportedError{entryIdx: idx, feature: "op." + e.Op}
	}
}

// doAppend builds an arrow table from the rows and commits one append snapshot.
func (r *runner) doAppend(idx int, e Entry) error {
	arrowTbl, err := buildArrowTable(r.tbl.Schema(), e.Rows)
	if err != nil {
		return fmt.Errorf("entry %d append: %w", idx, err)
	}
	defer arrowTbl.Release()

	tx := r.tbl.NewTransaction()
	if err := tx.AppendTable(r.ctx, arrowTbl, arrowTbl.NumRows(), nil); err != nil {
		return fmt.Errorf("entry %d append: %w", idx, err)
	}
	tbl, err := tx.Commit(r.ctx)
	if err != nil {
		return fmt.Errorf("entry %d commit: %w", idx, err)
	}
	r.tbl = tbl
	return r.recordSnapshot()
}

// doDelete performs a delete. The spec-open strategy is mapped to the
// write.delete.mode table property before Transaction.Delete: merge-on-read
// writes position deletes (content=1); copy-on-write rewrites data files.
// Equality deletes and deletion vectors are declared gaps (exit 4).
func (r *runner) doDelete(idx int, e Entry) error {
	if e.Kind == "equality" {
		return &unsupportedError{entryIdx: idx, feature: "delete.merge-on-read.equality"}
	}
	if e.Kind == "deletion-vector" {
		return &unsupportedError{entryIdx: idx, feature: "delete.merge-on-read.deletion-vector"}
	}

	var mode string
	switch e.Strategy {
	case "merge-on-read":
		mode = icetable.WriteModeMergeOnRead
	case "copy-on-write":
		mode = icetable.WriteModeCopyOnWrite
	default:
		return fmt.Errorf("entry %d delete: unknown strategy %q", idx, e.Strategy)
	}

	if e.Predicate == nil {
		return fmt.Errorf("entry %d delete: missing predicate", idx)
	}
	filter, err := r.buildExpr(e.Predicate)
	if err != nil {
		return fmt.Errorf("entry %d delete: %w", idx, err)
	}

	tx := r.tbl.NewTransaction()
	if err := tx.SetProperties(iceberg.Properties{icetable.WriteDeleteModeKey: mode}); err != nil {
		return fmt.Errorf("entry %d set delete mode: %w", idx, err)
	}
	if err := tx.Delete(r.ctx, filter, nil); err != nil {
		return fmt.Errorf("entry %d delete: %w", idx, err)
	}
	tbl, err := tx.Commit(r.ctx)
	if err != nil {
		return fmt.Errorf("entry %d commit: %w", idx, err)
	}
	r.tbl = tbl
	return r.recordSnapshot()
}

// buildExpr converts an L-log predicate to an iceberg BooleanExpression. The
// term's column type drives literal parsing.
func (r *runner) buildExpr(p *Predicate) (iceberg.BooleanExpression, error) {
	switch p.Type {
	case "true":
		return iceberg.AlwaysTrue{}, nil
	case "false":
		return iceberg.AlwaysFalse{}, nil
	case "is-null":
		return iceberg.UnaryPredicate(iceberg.OpIsNull, iceberg.Reference(p.Term)), nil
	case "not-null":
		return iceberg.UnaryPredicate(iceberg.OpNotNull, iceberg.Reference(p.Term)), nil
	case "eq", "ne", "lt", "lt-eq", "gt", "gt-eq":
		lit, err := r.termLiteral(p.Term, p.Value)
		if err != nil {
			return nil, err
		}
		return iceberg.LiteralPredicate(cmpOp(p.Type), iceberg.Reference(p.Term), lit), nil
	case "in", "not-in":
		lits := make([]iceberg.Literal, 0, len(p.Values))
		for _, v := range p.Values {
			lit, err := r.termLiteral(p.Term, v)
			if err != nil {
				return nil, err
			}
			lits = append(lits, lit)
		}
		op := iceberg.OpIn
		if p.Type == "not-in" {
			op = iceberg.OpNotIn
		}
		return iceberg.SetPredicate(op, iceberg.Reference(p.Term), lits), nil
	case "and", "or":
		if len(p.Args) < 2 {
			return nil, fmt.Errorf("%s needs >= 2 args", p.Type)
		}
		sub := make([]iceberg.BooleanExpression, 0, len(p.Args))
		for _, a := range p.Args {
			e, err := r.buildExpr(a)
			if err != nil {
				return nil, err
			}
			sub = append(sub, e)
		}
		if p.Type == "and" {
			return iceberg.NewAnd(sub[0], sub[1], sub[2:]...), nil
		}
		return iceberg.NewOr(sub[0], sub[1], sub[2:]...), nil
	case "not":
		if p.Arg == nil {
			return nil, fmt.Errorf("not needs an arg")
		}
		e, err := r.buildExpr(p.Arg)
		if err != nil {
			return nil, err
		}
		return iceberg.NewNot(e), nil
	default:
		return nil, fmt.Errorf("unsupported predicate type %q", p.Type)
	}
}

// termLiteral parses a typed value against the column type named by term.
func (r *runner) termLiteral(term string, tv *TypedValue) (iceberg.Literal, error) {
	if tv == nil {
		return nil, fmt.Errorf("predicate on %q missing value", term)
	}
	typ, ok := r.tbl.Schema().FindTypeByName(term)
	if !ok {
		return nil, fmt.Errorf("predicate term %q not in schema", term)
	}
	return tv.toLiteral(typ)
}

// cmpOp maps an l-log comparison predicate type to an iceberg Operation.
func cmpOp(t string) iceberg.Operation {
	switch t {
	case "eq":
		return iceberg.OpEQ
	case "ne":
		return iceberg.OpNEQ
	case "lt":
		return iceberg.OpLT
	case "lt-eq":
		return iceberg.OpLTEQ
	case "gt":
		return iceberg.OpGT
	case "gt-eq":
		return iceberg.OpGTEQ
	}
	return iceberg.OpEQ
}

// doObserve scans the table at the requested point and appends an observation.
// Phase 0/1 only handle 'latest'; time-travel to a bound name is Phase 2.
func (r *runner) doObserve(idx int, e Entry) error {
	at, err := observeAt(e)
	if err != nil {
		return fmt.Errorf("entry %d observe: %w", idx, err)
	}

	var scan *icetable.Scan
	switch at.kind {
	case atLatest:
		scan = r.tbl.Scan()
	case atBind:
		snapID, ok := r.binds[at.bind]
		if !ok {
			return fmt.Errorf("entry %d observe: unknown bind %q", idx, at.bind)
		}
		scan = r.tbl.Scan(icetable.WithSnapshotID(snapID))
	case atOrdinal:
		snapID, ok := r.ordinalSnapshot(at.ordinal)
		if !ok {
			return fmt.Errorf("entry %d observe: unknown ordinal %d", idx, at.ordinal)
		}
		scan = r.tbl.Scan(icetable.WithSnapshotID(snapID))
	default:
		return &unsupportedError{entryIdx: idx, feature: "time-travel"}
	}

	arrowTbl, err := scan.ToArrowTable(r.ctx)
	if err != nil {
		return fmt.Errorf("entry %d scan: %w", idx, err)
	}
	defer arrowTbl.Release()

	fields := r.tbl.Schema().Fields()
	icebergSchema, rows, err := decodeScan(arrowTbl, fields, r.canonIDs)
	if err != nil {
		return fmt.Errorf("entry %d decode: %w", idx, err)
	}

	// 'at' echoes the resolved observation point: when the observe binds a
	// name, that name is the resolved point (the golden reports the bind name,
	// not the literal 'latest' the author wrote); otherwise the literal target.
	atValue := at.value
	if e.Bind != "" {
		atValue = e.Bind
	}

	obs := Observation{
		At:            atValue,
		Bind:          e.Bind,
		IcebergSchema: icebergSchema,
		DecodedRows:   rows,
	}
	r.out.Observations = append(r.out.Observations, obs)

	// record the bind -> current snapshot for later time-travel
	if e.Bind != "" {
		if snap := r.tbl.CurrentSnapshot(); snap != nil {
			r.binds[e.Bind] = snap.SnapshotID
		}
	}
	return nil
}

// recordSnapshot assigns the newest snapshot its commit ordinal and appends a
// SnapshotOut with the spec-pinned facts.
func (r *runner) recordSnapshot() error {
	snap := r.tbl.CurrentSnapshot()
	if snap == nil {
		return fmt.Errorf("no current snapshot after commit")
	}
	if _, seen := r.snapshotOrdinal[snap.SnapshotID]; seen {
		return nil
	}
	ordinal := r.nextOrdinal
	r.snapshotOrdinal[snap.SnapshotID] = ordinal
	r.nextOrdinal++

	so := SnapshotOut{Ordinal: ordinal}
	if snap.ParentSnapshotID != nil {
		if p, ok := r.snapshotOrdinal[*snap.ParentSnapshotID]; ok {
			so.Parent = &p
		}
	}
	if snap.Summary != nil {
		so.Operation = string(snap.Summary.Operation)
		so.Summary = summaryOut(snap.Summary.Properties)
	}

	deleteFiles, err := r.snapshotDeleteFiles(snap)
	if err != nil {
		return fmt.Errorf("collect delete files: %w", err)
	}
	so.DeleteFiles = deleteFiles

	r.out.Snapshots = append(r.out.Snapshots, so)
	return nil
}

// ordinalSnapshot reverse-looks-up the snapshot-id for a commit ordinal.
func (r *runner) ordinalSnapshot(ordinal int) (int64, bool) {
	for id, ord := range r.snapshotOrdinal {
		if ord == ordinal {
			return id, true
		}
	}
	return 0, false
}

// snapshotDeleteFiles walks the delete manifests added by this snapshot and
// emits the spec-pinned facts (content, format, equality-ids) for each delete
// file — no paths, sizes, or counts. Only delete files added in this snapshot
// (matching its sequence number) are reported.
func (r *runner) snapshotDeleteFiles(snap *icetable.Snapshot) ([]DeleteFile, error) {
	fs, err := r.tbl.FS(r.ctx)
	if err != nil {
		return nil, err
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, err
	}

	var out []DeleteFile
	for _, m := range manifests {
		if m.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		// Only delete manifests added by this snapshot (the manifest's
		// added-snapshot-id is reliably set; per-entry snapshot ids may be
		// inherited/nil for freshly written files).
		if m.SnapshotID() != snap.SnapshotID {
			continue
		}
		entries, err := m.FetchEntries(fs, true) // discardDeleted: only live delete files
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			df := entry.DataFile()
			out = append(out, DeleteFile{
				Content: int(df.ContentType()),
				Format:  fileFormatName(df.FileFormat()),
			})
		}
	}
	return out, nil
}

// fileFormatName lowercases iceberg's uppercase FileFormat enum to match the
// golden vocabulary ("parquet", "puffin").
func fileFormatName(f iceberg.FileFormat) string {
	return strings.ToLower(string(f))
}
