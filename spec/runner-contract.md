# Runner contract

The boundary every implementation codes against. A **runner** is a small program, shipped by an implementation, that reads an L-log, executes it against that implementation's native API, and emits **canonical output**. The runner is a **pure translator + emitter — it never judges.** All comparison lives in the central orchestrator, so there is one ruler, not one per language.

## Why emit-only (non-negotiable)

1. **No comparator drift** — if each impl judged its own output, four languages would encode "does this match?" four ways. Emit-only keeps one comparator.
2. **No compensating-bug trap** — a runner that executed *and* checked against its own read-back could let a write-bug and a read-bug cancel. Because the runner only emits and the golden is central + blessed, self-conformance compares emitted output against *someone else's* committed truth.
3. **One runner, three modes for free** — self-conformance (emit → orchestrator diffs vs golden), differential (emit from N impls → orchestrator diffs against each other, no golden), fuzz (emit → orchestrator/oracle checks a metamorphic relation). The runner does not know which mode it is in.

## CLI (v0, one-shot)

```
runner --spec <l-log.json> --warehouse <dir> --out <canonical.json>
```

- `--spec` — path to a canonical L-log JSON (validates against `l-log.schema.json`).
- `--warehouse` — an empty directory the runner may use as a `file://` warehouse. The runner stands up whatever catalog it wants inside (iceberg-go: SQLite + `file://`). Fresh per fixture; the runner owns its contents.
- `--out` — path to write canonical output JSON (validates against `expected-output.schema.json` — the runner emits the same shape the golden is written in; a blessed run *becomes* a golden).

The runner:
1. Parse + schema-validate the L-log. On invalid spec → exit `2`.
2. Acquire the table under observation, per `header.source`:
   - `synthesized` (default) — **create** the table from `header` (schema, partition-spec, sort-order, format-version, pinned uuid/seeds, properties), then apply the op-log's mutating entries. This is **write conformance**.
   - `artifact` — **load** a pre-existing checked-in table (read conformance). The runner materializes `header.artifact.path`'s `bytes/` (restoring them to the pinned root in `bytes/ROOT`, so the metadata's embedded absolute paths resolve) and loads the table read-only; there are no mutating entries. The `acquire → observe → emit` back half is identical to synthesized mode.
3. For each `entry` in order:
   - mutating op → translate to native API, commit one snapshot. Map spec-open choices to the impl (iceberg-go: `strategy` → `write.delete.mode` table property before `Delete`).
   - `observe` → scan at `at`, capture canonical state, tag with `bind` if present.
   - If the op or a required `kind` is **not supported** by this impl → exit `4` (unsupported), naming the entry index and feature. Do not partially execute.
4. Assemble canonical output (below) and write to `--out`. Exit `0`.

The runner writes canonical output whether or not the L-log carries `expect`/`invariant` — those are for the orchestrator, not the runner. The runner records the observed state at every `observe` and the per-snapshot physical facts the canonical schema asks for; it asserts nothing.

In `artifact` (read) mode the runner emits **observations only** — no `snapshots` section. The loaded table's snapshot history is an artifact of how the fixture was written, not a read-conformance fact; a read case collapses to exactly the PR #2 shape (observations of the decoded row-set). The `snapshots` section is the write-side (DML) extension, emitted only in `synthesized` mode.

## Exit codes

| Code | Meaning | Orchestrator interpretation |
|---|---|---|
| `0` | executed, canonical output written | compare output (vs golden or oracle) |
| `2` | spec invalid / unparseable | harness error (bad corpus, not an impl verdict) |
| `3` | input correctly rejected | pass **iff** a step's `expect`/spec marked it reject; else fail |
| `4` | op/kind unsupported by this impl | **unsupported** matrix cell (declared gap, distinct from fail) |
| other | crash / internal error | **error** (not a correct rejection, not a gap) |

Reserving `3` for explicit rejection (vs `other` for a crash) means a crash can never masquerade as a correct rejection — the robustness gap in a plain "any non-zero = reject" convention.

Reserving `4` for unsupported is what makes the matrix honest: an impl that hasn't implemented equality deletes exits `4`, the orchestrator renders an empty/declared-gap cell, and `supports.yaml` omits the feature — the gap is *visible*, not a silent pass or a red failure.

## Canonical output

The runner emits the table's **logical state at each observed point** plus the spec-pinned physical facts, in the impl-neutral form defined by `expected-output.schema.json` (the same format the golden is written in). Key rules:

- **Rows** are a multiset keyed by `__rowkey`, values as typed literals (spec-semantic: physical type retained where the spec pins one). `__rowkey` is carried through so the orchestrator can compare row-sets across physical layouts and across engines; it is excluded from user-data comparison.
- **Snapshots** are reported in commit order with their `operation`; snapshot-ids are the sequential ordinals from the header seed (the runner does not invent random ids into the output — it emits the ordinal).
- **Physical facts** the runner emits per snapshot are only the spec-pinned ones the canonical schema names (delete-file `content`, `equality-ids element-type`, sequence numbers, partition tuples, aggregate metrics) — never file paths, sizes, counts, `created_by`, compression, or byte layout. Those are stripped at the source so the orchestrator never has to.

## supports.yaml (per impl, in the impl's repo)

```yaml
implementation: iceberg-go
version: <git-sha or release>
supports:
  - append
  - delete.copy-on-write
  - delete.merge-on-read.position
  - rewrite
  - evolve-schema.add-column
  - evolve-schema.promote-type
  - time-travel
  # NOT listed (=> declared gaps, exit 4 if a fixture needs them):
  #   delete.merge-on-read.equality
  #   delete.merge-on-read.deletion-vector
```

The orchestrator runs each runner only over fixtures whose required features it claims. A feature key present here but a fixture that then fails is a real failure; a feature absent here is an unsupported cell. Modeled on protobuf-conformance's failure-list discipline: a gap is an explicit, reviewable line, and an impl that starts supporting a feature must add the line (which lights up the matrix cell) — drift can't hide.

## Extension path (do not build in v0, do not preclude)

**Server mode** for fuzz throughput: a long-lived process reading L-logs from stdin and emitting canonical output to stdout, one message per line or length-prefixed (protobuf-conformance framing). Same parse → execute → emit contract, amortized process startup. The one-shot CLI is the degenerate single-message case, so adopting server mode later is an additive capability, not a redesign.
