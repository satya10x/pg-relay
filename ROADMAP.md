# ROADMAP

The order I'd build things, with rough effort estimates. Items earlier
in the list block items later; nothing later in the list is needed to
prove the core architecture.

## v0.1 — make it usable for a single internal team

Goal: someone at Zerodha can migrate one DungBeetle task onto pg_relay
in a controlled environment and have it run.

- [ ] **pgrx extension crate.** Generic SRF wrapper that opens a Unix
      socket per backend, marshals inputs/outputs, handles advisory
      locks for writes. Sketched in `docs/extension-sketch.md`.
- [ ] **Build & install instructions.** End-to-end recipe: compile the
      daemon, compile the extension, `CREATE EXTENSION`, run a query.
- [ ] **DDL generation.** From a `TableFunctionSchema`, emit the
      `CREATE FUNCTION` SQL. The extension uses this at install time.
- [ ] **Manifest commit helper.** A `ManifestProtocol` in `pg-relay-core`
      so users don't roll the pending-prefix-plus-CAS dance themselves.
- [ ] **Read-cache invalidation on writes.** Writes to a `LockKey` invalidate
      cache entries whose `ComputeKey` includes that key. Requires a
      key-prefix relationship between LockKey and ComputeKey.
- [ ] **Bounded read cache.** LRU with byte/entry limits per table function.
- [ ] **Bounded idempotency cache.** TTL or size cap, configurable.

## v0.2 — production readiness

Goal: it runs in production at Zerodha for at least one report. The
bugs we'd find on day one are now fixed.

- [ ] **S3 storage backend.** Same `Storage` trait; uses `aws-sdk-s3`.
- [ ] **GCS storage backend.** Same trait.
- [ ] **Prometheus metrics.** Per-table histograms via the `metrics` crate.
      Hosted on a small HTTP listener separate from the IPC socket.
- [ ] **OpenTelemetry tracing.** `trace_id` propagated through caller →
      daemon → storage. Spans for each phase.
- [ ] **Audit sinks.** File rotation, Kafka, S3 append-only logs. Composite
      sink for fan-out. Configurable sampling.
- [ ] **Graceful shutdown.** Drain in-flight requests on SIGTERM; reject
      new ones; flush audit log.
- [ ] **Health endpoint.** HTTP `/health` for liveness, `/ready` for
      readiness (daemon connected to storage, has cache warmed if configured).
- [ ] **Timeout enforcement.** Honor `deadline_ms` from the request; cancel
      compute on deadline.

## v0.3 — make it pleasant to use

Goal: someone outside your team can pick this up without reading source.

- [ ] **`#[derive(TableFunction)]` macro.** Generates the `ReadHandler` or
      `WriteHandler` impl from a struct. The bulk of registration becomes
      one annotation.
- [ ] **YAML config.** Schema declaration via `pg-relay.yaml`. Loads at
      startup, generates DDL, looks up Rust impls by name.
- [ ] **`pg_relay.list()`, `pg_relay.describe()`, `pg_relay.stats()`.**
      In-extension introspection table functions surfacing the registry
      and runtime metrics over SQL.
- [ ] **HTTP introspection endpoint.** `/pg_relay/tables`, `/pg_relay/health`.
- [ ] **CLI exporter.** `pg-relay export --format=markdown|json|openapi`
      generates docs from the registry.
- [ ] **Documentation site.** mdBook with architecture, quickstart, recipes,
      production deployment, perf tuning.

## v0.4 — distinguishing features

Goal: there's a real reason to choose pg_relay over rolling your own.

- [ ] **Snapshot isolation.** Pin a snapshot ETag at the start of each
      read; storage trait grows a `pin_snapshot` method. Long reads see
      a consistent view across the whole compute.
- [ ] **Lineage / DAG model.** Declare `derives_from` between table
      functions; framework reasons about shared compute and invalidation
      transitively. `pg-relay lineage --dot` emits the graph.
- [ ] **`ForwardWalk` helper.** Common helper for "baseline + events →
      state" computations with incremental extension.
- [ ] **Deterministic simulation testing.** Run the daemon under `madsim`
      or similar; inject crashes mid-write, network partitions, clock
      skew. Property-test "no reader observes partial commits."

## v0.5+ — beyond v1

- [ ] **Arrow Flight IPC.** Replace JSON framing with Flight; zero-copy
      columnar batches.
- [ ] **Cell-based isolation.** Hash-range routing across multiple daemon
      processes. Blast radius bounded per cell.
- [ ] **Iceberg commit protocol.** Pluggable alongside the simple manifest
      protocol; lets pg_relay-managed data be read by Spark/Trino/DuckDB.
- [ ] **OPA-style authorization.** Pluggable policy evaluation per call.
- [ ] **Incremental view maintenance.** For users who subscribe their
      daemon to an event stream and want to maintain materialized state
      in real time.
- [ ] **Hot reload.** SIGHUP reloads YAML, refreshes Postgres DDL for
      compatible changes.

## Things explicitly NOT on the roadmap

- A query language inside pg_relay. SQL is your interface; resist
  reinventing it.
- Cross-shard distributed transactions. The manifest CAS gives you
  what you need within a single shard.
- Multi-region active-active. If you need it, you're at a scale where
  you're building infra yourself anyway.
- Windows support for the daemon. pgrx supports Windows builds; the
  daemon process model targets Linux first, macOS for dev.
