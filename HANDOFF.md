# Handoff

Status snapshot for picking this up cold in a new session, a new
machine, or after a few weeks away.

## Where we are

**v0.0.1 scaffold, on disk and tested.** The daemon framework runs.
The Postgres extension does not exist yet — it's sketched in code in
`docs/extension-sketch.md` and will become a real crate in v0.1.

What works:

- `cargo build --workspace` builds clean
- `cargo test --workspace` passes 9 tests including an end-to-end test
  that spawns the daemon on a Unix socket and exercises the wire protocol
- `cargo run -p kv-store-example -- /tmp/pg_relay.sock` starts a working
  example daemon
- The example handles Read, Write, idempotent retry, Describe, and
  ListTableFunctions

What's stubbed or absent:

- No pgrx extension crate (the code that runs *inside* Postgres)
- No S3 storage (only in-memory and local-FS)
- No read-cache invalidation on writes — known correctness gap
- No bounded caches — memory grows unbounded today
- No manifest commit helper — users would do it themselves through the
  Storage trait if they need it
- No metrics, no tracing, no graceful shutdown
- No derive macro, no YAML config — users write raw trait impls

## How to come back to this

Open `pg-relay/` in your editor. Read in this order:

1. **`docs/context.md`** — the original Zerodha problem. Refreshes why
   any of this exists.
2. **`docs/architecture.md`** — the design rationale. Why the code is
   shaped this way.
3. **`crates/pg-relay-core/src/table_function.rs`** — the trait
   contracts. Everything else builds on these.
4. **`crates/pg-relay-server/src/read_cache.rs`** — the most interesting
   primitive. The OnceCell-based coalescing pattern is the conceptual
   center of the project.
5. **`crates/pg-relay-server/src/write_coordinator.rs`** — the second
   primitive. Read it alongside the manifest commit section of
   architecture.md.
6. **`examples/kv-store/src/main.rs`** and **`tests/e2e.rs`** — how a
   real user wires up table functions and how the wire protocol looks
   from outside.
7. **`docs/extension-sketch.md`** — what the pgrx side looks like. This
   is the template for v0.1's first real piece of work.
8. **`ROADMAP.md`** — the prioritized backlog.

## Open decisions you may want to revisit

These came up in design but were deferred. Worth a fresh look if
something feels off:

- **JSON wire protocol.** Chosen for debuggability. Arrow Flight is
  faster but bigger surface area. Revisit at v0.4 if profiling shows
  IPC is the bottleneck — it almost certainly won't be.

- **`Arc<dyn Any>` for cached state.** Cost: users downcast in
  `project()`, which is mildly awkward. Benefit: registry is fully type-
  erased, no generic explosion. Worth considering a typed alternative
  once derive macros land (the macro could generate typed wrappers and
  keep `Any` purely as an internal detail).

- **Two-layer fail-fast (advisory lock + daemon mutex).** Belt-and-
  suspenders that saves one RTT under contention. If the advisory-lock
  layer turns out to be a maintenance burden (hash collisions, GUC
  config, audit confusion), drop it and rely on the daemon mutex alone.

- **Adjacently-tagged enum for protocol.** We were burned by a collision
  between the discriminator and a payload field name. Adjacently tagged
  prevents recurrence. If you ever want a flatter JSON shape (for
  pasting into Postman, etc.), switch to internally tagged and rename
  the colliding field. Not worth it.

- **Per-Postgres-backend socket.** One socket per backend, reused via
  `thread_local!`. If the daemon-side accept queue becomes a hot path,
  multiplex many backends onto fewer sockets. Not on the radar.

## Where to start when you come back

The next concrete chunk of work is the pgrx extension crate. The plan:

1. Read `docs/extension-sketch.md` end to end.
2. Create `crates/pg-relay-extension/` with the layout from the sketch.
3. Get `cargo pgrx run pg18` succeeding on the basic structure.
4. Implement `pg_relay_read` and `pg_relay_write` as generic SRFs that
   take a JSONB inputs blob. The sketch has working code for all of this.
5. Implement `caller.rs` — the SPI-based caller attribution.
6. Implement `client.rs` — the blocking socket client.
7. Write a `pg_relay.control` file and a placeholder
   `sql/pg_relay--0.0.1.sql`.
8. End-to-end smoke test: start the kv-store daemon, install the
   extension, run `SELECT * FROM pg_relay_read('kv_get', '{...}')`.

Once that works, the typed-DDL story (v0.3) becomes the obvious next
unlock — generating `CREATE FUNCTION` per registered table function so
the SQL stops being JSON-shaped.

Or you may want to pick a different direction. The four reasonable
ones are still:

- **pgrx extension** — get to "running against Postgres" status (above)
- **Manifest commits + S3** — get to "real persistence" status
- **Read-cache invalidation** — fix the known correctness gap
- **Derive macro + DDL generation** — make the API pleasant before it
  ossifies

I'd go pgrx-first because nothing's a *real* v0.1 until it runs against
Postgres. The other three each provide value but they're improvements
to a thing that already works. The pgrx extension is the thing that
makes pg_relay actually pg_relay.

## How tests are organized

- Per-module unit tests in each crate (`#[cfg(test)] mod tests`)
- E2E integration test in `examples/kv-store/tests/e2e.rs` — exercises
  the wire protocol against a daemon spun up on a temp Unix socket.
  This is the closest thing we have to a smoke test today.

When the pgrx extension lands, an additional integration test will run
against a real Postgres via `pgrx-tests`. Don't try to test pgrx code
outside `cargo pgrx test`; it doesn't work.

## Pinned versions

The workspace pins exact versions of every workspace dep, because some
2026-era crates require `edition2024` which the CI host's cargo 1.75
can't handle. The pins are:

- chrono 0.4.38
- dashmap 5.5.3
- serde 1.0.210, serde_json 1.0.128
- thiserror 1.0.64
- tokio 1.40.0
- tracing 0.1.40, tracing-subscriber 0.3.18
- uuid 1.10.0

Locally on macOS with rustup-installed Rust 1.78+ you can unpin these.
The pins are only for environments stuck on cargo 1.75.

## Where the conversation lived

The decisions in `architecture.md` and `context.md` came out of a long
design conversation. If you want the receipts:

- DungBeetle context, the P&L SQL, the storage shape — in `docs/context.md`
- Why two-layer fail-fast, why coalescing, why manifest CAS — in
  `docs/architecture.md`
- What's deferred and why — in `ROADMAP.md`

Everything material from that conversation is captured in those docs.
You don't need the chat log to make progress.
