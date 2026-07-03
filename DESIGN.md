# Design

The rationale behind the op-log, the runner contract, and the orchestrator. This is a
prototype; the design is meant to be argued with.

## The problem

Each Iceberg implementation is verified internally, not interoperably. When a spec
feature ships (variant, deletion vectors, row lineage, defaults, nanosecond timestamps),
nothing automatically checks that the other implementations read and write it the same
way. The hardest cases are not bugs but places where two implementations each follow the
spec faithfully and still disagree — the spec left something open. Writing down a single
expected value is what forces that ambiguity to be resolved.

Running an engine (usually Spark) in every implementation's CI to verify reads is a cost
that grows with each implementation and each spec version, and still leaves the community
without one agreed answer for what a correct read produces.

## The op-log (L-log)

A test is a **logical, engine-agnostic op-log**: DDL (schema, partition spec) plus an
ordered sequence of DML operations (append, delete, overwrite, rewrite, evolve-schema),
each producing one snapshot, with optional per-step expectations.

**Altitude — logical DML with the spec-open choices named.** Not pure logical (two
conformant writers legitimately diverge on copy-on-write vs merge-on-read, so a golden
couldn't be shared), and not metadata-update scripting (that isn't DML and high-level
writers couldn't conform). The op-log stays at DML altitude but forces the author to name
every degree of freedom the spec leaves open (`strategy: merge-on-read`,
`kind: equality`), and pins everything the spec pins (an `equality_ids` element type is
`int`, no knob). This is the organizing rule: **normalize where the spec is open, retain
where the spec pins.**

**Typed literals.** Every value carries its physical type (`!long 1` is not `!int 1`), so
representational distinctions the spec cares about are authorable and checkable.

**A synthetic row key.** Each appended row carries a `__rowkey`. Iceberg has no stable row
identity across a rewrite, so comparing "the same rows" across physical layouts (and
across engines) is only possible with a carried key. `__rowkey` is reserved and excluded
from user-data comparison.

## Why golden bytes are not the cross-implementation contract

Two fully-conformant writers legitimately differ in bytes: file boundaries, file/row
ordering, column-stats precision, compression codec, Parquet `created_by`, Avro sync
markers, and assigned identifiers (snapshot ids, table uuid, timestamps, absolute paths).
So the comparison is on a **canonical logical form**, not bytes:

- **kept**: logical row multiset (by `__rowkey`, spec-typed), snapshot `operation`
  sequence, sequence numbers, delete-file `content`, `equality_ids` element type,
  partition tuples, aggregate metrics.
- **canonicalized**: snapshot ids renumbered to commit ordinals (they are *referenced*, so
  they are rewritten, not stripped).
- **stripped**: file paths / counts / sizes, `last-updated-ms`, table uuid, `created_by`,
  compression, row-group boundaries, sync markers.

Byte-equality remains the right tool for **single-implementation regression** (an
implementation's own output today vs its committed golden). It is the wrong tool *across*
implementations. This is the same conclusion Arrow reached: compare decoded values.

## The runner is emit-only

A runner reads an op-log, executes it via one implementation's native API, and emits
canonical output. It **never compares**. All judgment lives in the central orchestrator.

Three reasons:
1. **No comparator drift** — one comparator, not one re-implemented per language.
2. **No compensating-bug trap** — a runner that executed *and* judged against its own
   read-back could let a write bug and a read bug cancel. Emit-only, compared against a
   central blessed golden, breaks that.
3. **One runner, three modes for free** — self-conformance (diff vs golden), differential
   (diff N implementations against each other, no golden), and property/fuzz (check a
   metamorphic relation). The runner doesn't know which mode it's in.

See [`spec/runner-contract.md`](spec/runner-contract.md) for the CLI, exit codes, and the
`supports.yaml` mechanism.

## Topology: implementations depend on this repo

An implementation pins this repo (submodule, like parquet-testing) and ships a small
runner that links its own library. The corpus, the contract, the comparator, and the
orchestrator are central; only the runner is per-implementation. This means the runner
tracks the implementation's HEAD, the implementation owns it, and conformance can run in
the implementation's own CI. The same runner binary is also invoked by the central
orchestrator to build the cross-implementation matrix.

## Support declaration and the matrix

Each implementation ships a `supports.yaml` listing the features it claims. The
orchestrator runs a runner only over fixtures whose required features it claims; a needed
feature that is *not* claimed yields an `unsupported` cell (the runner exits with the
reserved unsupported code), distinct from a failure. A claimed feature that then fails is
a real failure.

This is modeled on the Protobuf conformance failure list: a gap is an explicit, reviewable
line, and an implementation that starts supporting a feature must add the line — which
lights up the matrix cell — so the matrix cannot silently drift from reality.

## Generating op-logs

The op-log is one format with several front-ends:
- **hand-authored** — the example corpus here.
- **AI-authored** — from spec clauses and known interop bugs.
- **replayed from production** — a committed snapshot history lowered into an op-log
  (this recovers *effect*, not always *intent* — position deletes and copy-on-write
  rewrites record which rows changed, not the predicate — so replay primarily serves
  read-conformance and regression).
- **fuzzed** — a generator that maps entropy to a valid op-log, checked by the metamorphic
  oracles. Because the generator is a total function from bytes to a runnable op-log,
  coverage-guided fuzzing composes with the same oracles used elsewhere.

## Metamorphic relations (oracle mode)

Relations that must hold with no authored expected value:
- **compaction is a logical no-op** on the live row multiset;
- **append commutativity** — order of independent appends doesn't change the multiset;
- **delete is set-minus** — after `delete(φ)`, the rows are the prior rows minus those
  matching `φ`;
- **time-travel equals replay** — reading at snapshot N equals replaying ops 0..N;
- **count equals summary** — `count(scan)` equals the snapshot summary's total-records.

These let an implementation be checked for self-consistency without designating any engine
as the source of truth.

## Open questions

- `__rowkey` as a reserved schema column vs harness-side identity.
- How much physical invariant to pin beyond the spec-required minimum.
- Column-stats / bounds are writer-choice (truncation length varies); v0 excludes bounds
  from comparison rather than pretend to check them.
- Exact reserved field ids for v3 row lineage — expressed logically here pending
  confirmation against the spec.
