# pg_relay

A Postgres extension framework for exposing on-demand computed data through SQL
table functions, backed by a daemon process. Replace stored tables with
computation; keep the SQL interface.

**Status:** v0.0.1 — pre-alpha. The daemon framework runs and is tested. The
pgrx extension side is sketched but not built into the workspace yet (see
[`docs/extension-sketch.md`](docs/extension-sketch.md)). Not ready for
production use.

## What's in v0.0.1

- `pg-relay-core` — traits, types, IPC protocol
- `pg-relay-server` — daemon framework with:
  - Per-key compute cache with read coalescing (`OnceCell`-based)
  - Per-key write coordinator with fail-fast `try_lock` and idempotency
  - Object-storage `Storage` trait + in-memory and local-FS implementations
  - Length-prefixed JSON IPC server over Unix domain socket
  - Structured stdout audit log
- `examples/kv-store` — minimal working example daemon with read + write table
  functions, plus end-to-end integration test

## Quick start

```bash
# Build everything
cargo build --workspace

# Run the example daemon
cargo run -p kv-store-example -- /tmp/pg_relay.sock &

# Run the test suite (includes E2E test against a spawned daemon)
cargo test --workspace
```

## Concepts

**Table function.** A SQL-callable function that returns a set of rows. From
Postgres's perspective it's a normal SRF; under the hood it's an RPC to the
daemon.

**Read table function (`ReadTableFunction`).** STABLE semantics. Concurrent
callers with the same `compute_key` share work through a `OnceCell` — first
arrival computes, others wait.

**Write table function (`WriteTableFunction`).** VOLATILE semantics. Serialized
per `lock_key`; conflicts return immediately rather than queueing. Idempotent
by `request_id`.

**Storage.** Object-storage-shaped trait: `get`, `put`, `put_if_match`, `list`,
`delete`. Atomicity at the application level (manifest commits) is built on
`put_if_match` (ETag CAS).

**Audit log.** Every read and write emits a structured event with caller
attribution (Postgres role, application_name, client_addr), cache outcome,
latency breakdown, and error code.

## What's missing (intentionally) in v0.0.1

- The `#[derive(TableFunction)]` macro — users write raw trait impls
- YAML config — Rust API only
- S3 / GCS / Azure storage backends — only in-memory and local FS
- The pgrx extension as a real crate (sketched in docs)
- Manifest commit helper — user code does its own commits via the Storage trait
- Read-cache invalidation on writes (today, reads see stale data until the
  cache entry naturally evicts)
- Bounded cache size / TTL
- HTTP introspection endpoint
- OpenTelemetry tracing and Prometheus metrics
- Iceberg commit protocol
- OPA-style authorization

See `ROADMAP.md` for the order I'd add them.

## Project layout

```
pg-relay/
├── Cargo.toml
├── crates/
│   ├── pg-relay-core/       # types, traits, protocol — no IO, no runtime
│   └── pg-relay-server/     # daemon framework
├── examples/
│   └── kv-store/            # minimal end-to-end example
├── docs/
│   ├── architecture.md      # design overview
│   └── extension-sketch.md  # what the pgrx extension looks like
└── ROADMAP.md
```

## License

Dual MIT / Apache-2.0.
