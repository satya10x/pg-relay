# Architecture

This is the *why* document. The crate docs explain how the code works;
this explains why it's shaped that way. Read it once when you're cold
on the project and you'll have your bearings back.

## The problem

DungBeetle reads pre-registered `.sql` task files with positional args
and runs them against Postgres. Today the data those queries touch lives
in Postgres tables (and historically ClickHouse) that have to be ETL'd
and kept fresh. For a class of computations — portfolio holdings,
realized P&L, exit/entry events — the source of truth is actually a
forward-walk over a stream of trade events, baselined periodically into
checkpoints in S3.

We don't want to store the derived state. We want to *compute* it on
demand from S3, but keep SQL as the consumer interface so DungBeetle's
existing model (parameterized `.sql` files, throttled worker pool,
results-cache DB) keeps working unchanged.

## The solution shape

```
┌──────────────┐       SQL        ┌──────────────┐    Unix    ┌──────────────┐    S3    ┌──────────┐
│  DungBeetle  │ ───────────────▶ │   Postgres   │ ─socket──▶ │   pg_relay   │ ───────▶ │   S3 /   │
│ worker pool  │                  │ + pg_relay   │   JSON     │    daemon    │          │ local FS │
└──────────────┘                  │  extension   │ ◀──rows─── │              │ ◀──────  └──────────┘
                                  └──────────────┘            └──────────────┘
                                         │
                                         ▼
                                  bhavcopy + other
                                  slowly-changing
                                  reference data
                                  (real tables)
```

Postgres holds only reference data (bhavcopy, master tables) where local
indexed joins and full-text search matter. Everything that's expensive
or derived comes from the daemon through table functions that look like
SRFs to the planner.

## Why this split

**Postgres for the SQL surface.** DungBeetle is built around SQL. Replacing
it would be a huge migration. Keeping SQL means existing tasks port with
mechanical edits — `FROM eq_holdings` becomes
`FROM daemon_eq_holdings_latest(...)` and the rest is unchanged.

**Daemon for the compute.** Per-client forward-walk state is expensive to
build and benefits enormously from in-process caching. Doing it inside a
Postgres backend per query would either (a) blow memory, since each
backend would build its own, or (b) require shared-memory plumbing we
don't want to maintain. A daemon with one cache shared across all
backends is the natural shape.

**Object storage for persistence.** Trade-event streams + periodic
checkpoints are already in S3. Daemon-managed Postgres tables for derived
state would duplicate that. Object storage with atomic manifest commits
gives us snapshot-style consistency without a separate consistency tier.

## The core abstraction: TableFunction

Two flavors. Both are declared in user code, registered with the daemon,
and exposed as SRFs by the pgrx extension.

### `ReadTableFunction` — STABLE, parallel_safe

```rust
trait ReadTableFunction {
    fn schema() -> TableFunctionSchema;
    fn compute_key(inputs: &Inputs) -> Result<ComputeKey>;
    async fn compute(inputs: &Inputs, ctx: &ComputeContext) -> Result<SharedState>;
    fn project(inputs: &Inputs, state: &SharedState) -> Result<Vec<Row>>;
}
```

Three operations, intentionally separated:

- **`compute_key`** — identity of the underlying computation. Two table
  functions can share a `compute_key` to indicate they read from the same
  derived state. `holdings_latest(client, date)` and
  `holdings_series(client, from, to)` both project from the same
  `client → date` forward-walk; sharing the key means they share work.

- **`compute`** — cache miss path. The framework guarantees this is called
  at most once per `ComputeKey` across concurrent readers. First arrival
  computes, others wait on the result (`OnceCell` semantics).

- **`project`** — fast pure projection from cached state for *this* call's
  specific inputs. No IO. The split lets the cache hold "all data for
  client+date" and project different views cheaply on demand.

### `WriteTableFunction` — VOLATILE, parallel_unsafe

```rust
trait WriteTableFunction {
    fn schema() -> TableFunctionSchema;
    fn lock_key(inputs: &Inputs) -> Result<LockKey>;
    fn idempotency_key(inputs: &Inputs) -> Option<String>;
    async fn execute(inputs: &Inputs, ctx: &mut WriteContext) -> Result<Row>;
}
```

- **`lock_key`** — the daemon `try_lock`s this. Concurrent writes to the
  same key fail-fast with `WriteConflict`, not queue. This is intentional:
  DungBeetle has its own retry layer; queueing in the daemon would just
  hide contention while making things look healthier than they are.

- **`idempotency_key`** — DungBeetle's `job_id` flows through here. If a
  write with the same key succeeded before, the framework returns the
  prior outcome without invoking `execute` again. This is what makes
  retries safe even though writes are VOLATILE.

- **`execute`** — the actual side-effectful work. The framework wraps it
  in lock acquisition, idempotency lookup, and outcome caching.

## Concurrency story

Two-layer fail-fast, no queueing anywhere.

```
┌─ Postgres backend ──────────────────────────────────────┐
│                                                         │
│  pg_try_advisory_xact_lock(hash(table_function, key))   │ ← Layer 1
│           │                                             │
│           ▼                                             │
│  send Write request to daemon ──────────┐               │
└─────────────────────────────────────────┼───────────────┘
                                          │
┌─ Daemon ─────────────────────────────────────────────── ┐
│                                          ▼              │
│              try_lock on per-key Mutex<()>              │ ← Layer 2
│                          │                              │
│                          ▼                              │
│              check idempotency cache                    │
│                          │                              │
│                          ▼                              │
│                  user execute()                         │
└─────────────────────────────────────────────────────────┘
```

Why both layers? The advisory lock fails before the daemon round-trip,
saving an RTT under contention. The daemon-side lock is the actual
correctness boundary — if multiple Postgres instances share one daemon,
the advisory lock is per-instance.

The advisory lock is `pg_try_*` (non-blocking). The daemon-side mutex is
`try_lock`. No one ever waits.

## Read coalescing — the interesting part

```
Reader A  ─┐
Reader B  ─┼──▶ compute_key=X  ─▶  OnceCell<X> ─▶ compute() runs once
Reader C  ─┤                              │
Reader D  ─┘                              ▼
                                   all four get the same result
```

`OnceCell` from `tokio::sync` lets N futures await the same one-shot
initialization. If readers C and D arrive while A is computing, they
block on the cell, not on a lock. When A's compute completes, the cell
fires, and all of B/C/D wake with the result simultaneously.

Failed computes are *evicted*, not cached. A subsequent reader retries
fresh. The alternative — caching errors — would compound transient
failures into permanent ones.

The cache outcome (`Hit`/`Miss`/`Coalesced`) is reported back to the
audit log so you can see in production how much coalescing is actually
happening. A high `Coalesced` ratio is the system working as designed.

## Manifest commit — atomicity without a database

For writes that update state in S3, the protocol is:

1. **Write data files to a `pending/<request_id>/` prefix.** Many objects,
   non-atomic, fine — nobody reads from here.
2. **Read current manifest** (or `(none)` if first commit). Note its ETag.
3. **Build new manifest** with `data_objects` referencing the pending paths
   and `request_id` set.
4. **`PUT current/manifest.json` with `If-Match: <old-etag>`.** Atomic
   point-of-no-return. Either this succeeds and the new state is live,
   or it fails (someone else committed) and you abandon the pending
   files; a janitor reaps them later.

```
                   ┌──── put data files ────┐
   Write begin ───▶│ pending/req_42/        │
                   │   data-0.json          │
                   │   data-1.json          │
                   │   ...                  │
                   └────────────┬───────────┘
                                │
                   ┌────────────▼───────────┐
                   │ read current/manifest  │ ← get ETag E1
                   │ build new manifest M2  │
                   └────────────┬───────────┘
                                │
                   ┌────────────▼───────────┐
                   │ PUT current/manifest   │ ← If-Match: E1
                   │   .json (= M2)         │
                   └────────────┬───────────┘
                                │
                  ┌─ 200 OK ───┴───── 412 ─┐
                  ▼                        ▼
              committed               WriteConflict
                                  (janitor reaps pending/req_42/)
```

The pattern is the simplest possible thing that gives you snapshot
isolation: the manifest pointer flips atomically, readers see either
the old state or the new state, never partial. No transactions, no
coordination protocols.

This is Iceberg-shaped without being Iceberg. v0.5 may add an actual
Iceberg backend behind the same `Storage` trait for ecosystem
compatibility (Spark/Trino/DuckDB can read Iceberg directly).

## The IPC protocol

Length-prefixed JSON over Unix domain socket. One frame = 4-byte BE
length + JSON payload.

```rust
Request::Read {
    table_function: "kv_get",
    inputs: { "key": "foo" },
    caller: { pg_role: "trader", application_name: "dungbeetle", ... },
    trace_id: "abc-123",
    deadline_ms: Some(5000),
}

Response::Rows {
    rows: [ ... ],
    cache: Coalesced,
    compute_ms: 12.4,
}
```

Why JSON in v1: dead simple to debug. Anyone can `socat - UNIX-CONNECT:...`
and poke the daemon. Anyone can grep a packet capture. The performance
ceiling is below Arrow Flight but well above our throughput needs at
v0.1 scale.

Why a fresh framing instead of stdio or gRPC: Unix socket gives us
backend-to-daemon affinity (thread_local socket per Postgres backend),
clean per-connection lifecycle, no port management. Each Postgres
backend opens one socket on first use and reuses it. The daemon's IPC
accept loop spawns one task per connection.

The protocol enum uses serde's *adjacently tagged* form (`tag = "op"`,
`content = "args"`) rather than internally tagged. Internally tagged
collided with `TableFunctionSchema.kind` — a field of the payload was
named the same as the discriminator. Adjacently tagged nests the payload
under an `"args"` key, so the discriminator can never collide with
anything inside the variant.

## The audit log

Every read and every write emits one structured event:

```json
{
  "ts": "2026-...",
  "trace_id": "...",
  "request_id": "...",
  "caller": {
    "pg_role": "trader",
    "application_name": "dungbeetle",
    "client_addr": "10.0.4.7",
    "backend_pid": 12345,
    "session_user": "trader"
  },
  "operation": {
    "table_function": "eq_holdings",
    "kind": "read",
    "inputs": { ... }
  },
  "result": {
    "status": "ok",
    "cache": "coalesced",
    "rows": 412,
    "bytes_out": 89234,
    "latency_ms": 4.2,
    "compute_ms": 0.1,
    "storage_ms": 0,
    "error_code": null
  }
}
```

Caller attribution is the point. When something goes wrong at 3am, you
need to know which Postgres role on which app from which IP did what.
The pgrx extension fills the `caller` field by SPI-querying
`current_user`, `application_name`, `inet_client_addr()`, and
`pg_backend_pid()` at call time.

Reads are sampleable; writes always log. v0.2 adds pluggable sinks
(file rotation, Kafka, S3 append-only) behind the `AuditLog` trait.

## The Storage trait

```rust
trait Storage {
    async fn get(&self, key: &str) -> StorageResult<Bytes>;
    async fn get_with_etag(&self, key: &str) -> StorageResult<(Bytes, String)>;
    async fn put(&self, key: &str, data: Bytes) -> StorageResult<ObjectMeta>;
    async fn put_if_match(&self, key, data, if_match) -> StorageResult<ObjectMeta>;
    async fn put_if_absent(&self, key, data) -> StorageResult<ObjectMeta>;
    async fn list(&self, prefix: &str) -> StorageResult<Vec<ObjectMeta>>;
    async fn delete(&self, key: &str) -> StorageResult<()>;
}
```

Object-storage-shaped on purpose. S3 fits trivially. So does GCS, Azure
Blob. Local FS works via sidecar `.etag` files (good enough for dev,
not for production at scale). The in-memory impl is for tests.

What this *doesn't* let you do: transactions across multiple objects.
That's fine — the manifest commit pattern doesn't need them. Single-
object atomic PUT with conditional headers is the whole atomicity
substrate.

## What's deliberately not in v0.0.1

- **No `#[derive(TableFunction)]` macro.** API is still moving. Macros
  freeze interfaces; we want to learn what the real shape should be
  from a few hand-written impls first.
- **No YAML config.** Same reason. The Rust API is the source of truth
  until it stabilizes.
- **No S3 backend.** Easy to add behind the trait; uninteresting until
  someone needs it.
- **No read-cache invalidation on writes.** Today, a successful write
  doesn't invalidate stale read entries. The fix is "writes to a
  `LockKey` invalidate `ComputeKey`s that include that key as a prefix,"
  which needs a prefix relation between the two key types. v0.1.
- **No bounded cache.** v0.0.1 has unbounded growth. v0.1 adds LRU with
  configurable byte/entry limits.
- **No Iceberg, OPA, Arrow Flight, cell routing, lineage DAG, simulation
  testing.** All sketched in `ROADMAP.md` for v0.4+.

## Reading order

If you're picking this up cold:

1. This document.
2. `crates/pg-relay-core/src/table_function.rs` — the contracts.
3. `crates/pg-relay-server/src/read_cache.rs` — the most interesting
   primitive.
4. `crates/pg-relay-server/src/write_coordinator.rs` — the second
   primitive.
5. `examples/kv-store/src/main.rs` and `tests/e2e.rs` — how it all
   wires together.
6. `docs/extension-sketch.md` — what the pgrx side will look like.
7. `ROADMAP.md` — what's next.
