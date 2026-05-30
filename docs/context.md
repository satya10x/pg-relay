# Context

The problem that motivated pg_relay, and the specific Zerodha use case
it's being designed for. This is the "what was I solving" doc — read
it after `architecture.md` when you've forgotten why anything matters.

## The Zerodha situation

Today's stack:

- **DungBeetle** (github.com/zerodha/dungbeetle) is the consumer. It's a
  distributed async SQL job server. Apps register `.sql` task files with
  positional arguments. DungBeetle runs them against a SQL backend,
  throttled by worker concurrency (somewhere in the 30–100 concurrent
  range per node), and writes results to an ephemeral results-cache DB
  that the calling app polls.

- **Backend today: Postgres + ClickHouse.** Most of the data lives in
  Postgres tables that have to be ETL'd from upstream systems. Some
  aggregates live in ClickHouse. Both are storing derived state — the
  result of forward-walking trade events for each client — which we'd
  rather compute on demand than maintain.

- **Real source of truth: S3.** Trade events stream to S3. Periodic
  baseline checkpoints (current positions, opening balances) also live
  in S3. Forward-walking from `baseline + events` reconstructs any
  derived state — holdings, realized P&L, exit events — deterministically.

## What we want

Move the derived-state tables out of Postgres. Keep DungBeetle's `.sql`
task files as the interface. Have the daemon compute the same answers
from S3, transparently.

Two reasons this is worth doing:

1. **Storage cost and ETL maintenance.** Daemon-computed state has no
   storage overhead and no ETL pipeline.
2. **Consistency.** Today derived state can drift from source-of-truth
   events. Daemon-computed state is always consistent by definition.

## The constraint that shapes everything

**DungBeetle is the consumer and isn't changing.** It speaks SQL, runs
parameterized queries, has its own retry semantics and worker pool. So
the interface pg_relay exposes has to be SQL. Anything else is a much
bigger migration.

That's why pg_relay is a Postgres extension and not a separate service
with its own client library.

## What stays in Postgres

- **bhavcopy** and other reference / master data. Slowly-changing,
  indexed local joins, full-text search.
- Anything else where Postgres's storage and indexing is genuinely useful.

## What moves to the daemon

- **eq_holdings** — current holdings per client per instrument
- **eq_exitentry** — realized exit/entry events
- Anything else derivable from the trade event stream + baseline
  checkpoints

These get exposed as table functions:

```sql
-- Instead of:
SELECT * FROM eq_holdings WHERE client_id = $1 AND date = $3;

-- We write:
SELECT * FROM daemon_eq_holdings_latest($1, $3);
```

The CTE-wrapping pattern in DungBeetle tasks (`WITH x AS MATERIALIZED
(SELECT FROM daemon_eq_holdings_latest(...))`) ensures the SRF is called
once per query, not once per join expansion.

## The P&L query — before and after

This is the realistic shape of what gets ported. Original query (lightly
sanitized):

```sql
-- Original — Postgres-only, derived state stored in tables
WITH holdings AS (
    SELECT h.*
    FROM eq_holdings h
    JOIN (
        SELECT instrument_id, MAX(date) AS max_date
        FROM eq_holdings
        WHERE client_id = $1 AND date <= $3
        GROUP BY instrument_id
    ) latest ON h.instrument_id = latest.instrument_id
            AND h.date = latest.max_date
    WHERE h.client_id = $1
),
exits AS (
    SELECT e.instrument_id,
           SUM(e.exit_qty * e.exit_price - e.entry_qty * e.entry_price) AS realized_pnl
    FROM eq_exitentry e
    WHERE e.client_id = $1
      AND e.exit_date BETWEEN $2 AND $3
    GROUP BY e.instrument_id
),
combined AS (
    SELECT
        COALESCE(h.instrument_id, e.instrument_id) AS instrument_id,
        h.qty,
        h.avg_cost,
        e.realized_pnl
    FROM holdings h
    FULL OUTER JOIN exits e USING (instrument_id)
)
SELECT
    c.instrument_id,
    b.symbol,
    b.name,
    c.qty,
    c.avg_cost,
    (c.qty * b.close_price) - (c.qty * c.avg_cost) AS unrealized_pnl,
    c.realized_pnl
FROM combined c
JOIN bhavcopy b
  ON b.instrument_id = c.instrument_id
 AND b.date = $3
WHERE to_tsvector('english', b.name) @@ plainto_tsquery('english', $4)
ORDER BY b.symbol;
```

Same query, ported to pg_relay:

```sql
-- Ported — derived state computed by the daemon
WITH holdings AS MATERIALIZED (
    SELECT *
    FROM daemon_eq_holdings_latest($1, $3)
    -- ^ returns one row per instrument; the dedup self-join disappears
),
exits AS MATERIALIZED (
    SELECT instrument_id,
           SUM(exit_qty * exit_price - entry_qty * entry_price) AS realized_pnl
    FROM daemon_eq_exitentry($1, $2, $3)
    GROUP BY instrument_id
),
combined AS (
    SELECT
        COALESCE(h.instrument_id, e.instrument_id) AS instrument_id,
        h.qty,
        h.avg_cost,
        e.realized_pnl
    FROM holdings h
    FULL OUTER JOIN exits e USING (instrument_id)
)
SELECT
    c.instrument_id,
    b.symbol,
    b.name,
    c.qty,
    c.avg_cost,
    (c.qty * b.close_price) - (c.qty * c.avg_cost) AS unrealized_pnl,
    c.realized_pnl
FROM combined c
JOIN bhavcopy b
  ON b.instrument_id = c.instrument_id
 AND b.date = $3
WHERE to_tsvector('english', b.name) @@ plainto_tsquery('english', $4)
ORDER BY b.symbol;
```

What changed:

- `FROM eq_holdings ... GROUP BY` self-join → `FROM daemon_eq_holdings_latest(...)`.
  The dedup disappears because `_latest` already returns one row per
  instrument.
- `FROM eq_exitentry` → `FROM daemon_eq_exitentry(...)`. Same shape, same
  GROUP BY downstream.
- Both wrapped in `AS MATERIALIZED` to guarantee one SRF call per CTE.
- Everything else — bhavcopy join, `to_tsvector` full-text search,
  ORDER BY — unchanged. That's the whole point: the SQL surface stays
  recognizable.

What the daemon does under the hood for *both* CTEs:

- Loads the most recent baseline checkpoint for `client_id` from S3.
- Replays trade events from baseline date through `$3` (the as-of date).
- Caches the forward-walked state under
  `ComputeKey("client_id=$1,as_of=$3")`.
- `daemon_eq_holdings_latest` projects "current holdings" from that state.
- `daemon_eq_exitentry` projects "exit events in the date range" from
  the same state — same compute, two views.

Read coalescing means the second SRF call within the query uses the
already-computed state without recomputing. Cross-query coalescing means
two concurrent DungBeetle jobs for the same client share the work too.

## Why "pg_relay"

The Postgres extension "relays" SQL calls to the daemon. Daemon-side is
where the actual computation happens. The name signals that Postgres is
the interface, not the compute layer.

(We considered `pg_pulley`, `pg_sluice`, `pg_forge`. Relay won because
it's the most descriptive without overclaiming. We are not a query
optimizer, we are not a federated query engine, we are not a streaming
processor — we relay one shape of query through to a daemon. Pick a
name that doesn't promise more than it is.)

## Throughput math

DungBeetle worker pool ≈ 30–100 concurrent jobs per node. Each job ≈
1–10 SRF calls. At the top end: 1000 concurrent SRF calls.

But: read coalescing collapses concurrent same-key calls into one
compute. The realistic "1000 concurrent SRF calls" maps to maybe 50–100
unique compute keys after coalescing. Each compute is bounded by S3
latency (≈10–100ms for the checkpoint read) plus the forward-walk
(microseconds per event, milliseconds total).

A single daemon node handling sub-second responses for ~100 concurrent
unique computations is well within reach. The 10k QPS scaremongering
you'd see in a naive analysis ignores coalescing.

## Existing similar tools and why we're not using them

- **Foreign Data Wrappers (FDW).** Right interface, wrong abstraction.
  FDWs are for connecting Postgres to *other databases*, with a query
  planner that pushes predicates down. We don't have a query to push
  down to; we have a function to call.

- **pg_duckdb.** Embeds DuckDB in Postgres for analytics. Doesn't help —
  the data still has to live somewhere, and DuckDB doesn't know how to
  forward-walk trade events.

- **Materialized views.** Stale by definition. Refresh-on-write breaks
  the storage-free property. The whole point is to *not* materialize.

- **Custom DungBeetle backend.** Would work, but means abandoning SQL
  as the interface. Pg_relay keeps SQL working unchanged.

## Open design questions

These are worth thinking about but not blocking v0.1:

- **Cache invalidation across instances.** If two daemon processes serve
  the same Postgres cluster, how does a write through one invalidate
  the read cache in the other? Today: separate caches, eventually
  consistent. v0.4+: maybe a pub/sub channel.

- **Cold start.** First call after deploy has no cache. For large
  forward-walks (months of events), cold-start can be slow. Pre-warming
  on common client_id+as_of_date keys at startup? Or lazy is fine?

- **Partial-result reads during writes.** Today: snapshot isolation
  isn't enforced. A read that starts before a write but finishes after
  might see partial state. The Iceberg-style snapshot pinning (v0.4)
  fixes this.

- **Multi-tenancy isolation.** Currently no per-tenant rate limiting or
  quota. A bad client could starve others. Cell-based architecture
  (v0.5) addresses this; until then, hope.
