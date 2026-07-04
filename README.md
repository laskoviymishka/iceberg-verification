# iceberg-verification

A prototype for **cross-implementation DML conformance testing** of Apache Iceberg.

Every Iceberg implementation (iceberg-java, -go, -rust, -cpp, PyIceberg) has its own
tests, but nothing checks that a table one implementation *writes* is read the same way
by another, or that the same logical DML operation produces a spec-conformant result
across implementations. Divergences surface as bug reports weeks after a release.

This repo prototypes an approach: describe DML as a **logical, engine-agnostic op-log**,
have each implementation *execute* it and emit a **canonical result**, and check that
result against a spec-derived expectation. A central orchestrator runs every
implementation's runner over the shared corpus and produces a compliance report.

> Status: **early prototype / design exploration.** The spec and example corpus are
> here; the iceberg-go runner and orchestrator are specified but not yet implemented.

## Idea in one picture

```
  L-log (logical op-log, YAML/JSON)         # append / delete / compact / evolve-schema
        │                                     with the spec-open choices named
        ▼
  runner  (per implementation, emit-only)   # executes ops via the impl's native API,
        │                                     emits canonical output — never judges
        ▼
  canonical output (impl-neutral JSON)       # logical row-multiset + spec-pinned facts
        │
        ▼
  orchestrator  (central, nightly)           # diff vs golden, or check a metamorphic
        │                                     relation (oracle mode), across N impls
        ▼
  compliance matrix + report                 # feature × impl → pass / fail / unsupported
```

## Two verification modes, one format

- **Authored golden** — the op-log carries an expected result (`expect:`); the runner's
  output must match it. Writing the expected value down is also what forces a spec
  ambiguity to be settled rather than rediscovered.
- **Oracle (no source of truth)** — the op-log carries a metamorphic relation
  (`invariant:`) that must hold with no authored expected value and no reference engine.
  Example: compaction must be a logical no-op on the live row multiset.

## What's here

| Path | What |
|---|---|
| [`spec/l-log.schema.json`](spec/l-log.schema.json) | The formal spec: the logical op-log JSON Schema |
| [`spec/expected-output.schema.json`](spec/expected-output.schema.json) | The golden format each fixture is checked against (decoded-rows keyed by field-id) |
| [`spec/runner-contract.md`](spec/runner-contract.md) | What a runner binary accepts/emits; exit codes; emit-only rule |
| [`fixtures/`](fixtures/) | Example fixtures — an op-log (`fixture.yaml`, YAML authoring profile) + its `expected.json` golden |
| [`runners/go/supports.yaml`](runners/go/supports.yaml) | What iceberg-go claims to support + declared gaps |
| [`DESIGN.md`](DESIGN.md) | The design rationale and how it relates to prior art |

## The example corpus

> Terminology: **spec** is the framework (the format, schemas, and runner contract under
> `spec/`); a **fixture** is one individual test under `fixtures/` — an op-log
> (`fixture.yaml`) plus its `expected.json` golden.

- **`v2_append_timetravel`** — two appends + time-travel scans at each snapshot
  ordinal. Authored golden.
- **`v2_append_posdelete_timetravel`** — append + merge-on-read position delete +
  time-travel. Authored golden.
- **`v2_read_append_timetravel`** — read side: loads a checked-in table artifact
  (`source: artifact`), then appends and time-travels over it. Authored golden.
- **`v3_schema_evolution_defaults`** — type promotion + add-column with defaults +
  time-travel across the change. Authored golden.

## Golden format

Each authored fixture has an `expected.json` in the read-side fixture vocabulary from
[sungwy/iceberg-testing#2](https://github.com/sungwy/iceberg-testing/pull/2): rows keyed
by field-id, values as a type-annotated tree (`{type, value}`, `{type: object, fields}`,
`{type: array, elements}`, `{type: null}`; 64-bit integers as JSON strings). It extends
that read-only shape with a `snapshots` section for the write side (an op-log mutates a
table across snapshots and observation points; a read-only case collapses to exactly the
PR #2 shape). Convention in the op-log: **mutations assert snapshot facts; `observe`
entries assert row-state**, so each `observe` maps to one entry in the golden's
`observations` array.

## Format: JSON canonical, YAML authoring

The canonical form is JSON validated against `spec/l-log.schema.json` — the schema *is*
the formal spec. Humans and tools author in a strict YAML profile that maps 1:1 to the
JSON, using type tags so physical types are explicit (`!long 1` is distinct from
`!int 1` — the distinction behind real interop bugs like
[apache/iceberg-go#880](https://github.com/apache/iceberg-go/pull/880)).

## Prior art this borrows from

- [apache/parquet-testing](https://github.com/apache/parquet-testing) — a shared file
  corpus each implementation tests against; consumed as a submodule.
- Apache Arrow's `archery` integration tests — a neutral orchestrator, a thin per-language
  runner, comparison on decoded values rather than bytes.
- The Protobuf conformance suite — a per-implementation failure list, green-by-default,
  so a support matrix cannot silently drift.

## License

Apache License 2.0. See [LICENSE](LICENSE).
