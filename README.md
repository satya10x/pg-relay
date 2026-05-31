# pg_relay

A Postgres extension that exposes SQL functions which **relay** calls to an
external daemon over HTTP and return the daemon's rows. SQL stays the
interface; all data and logic live in the daemon.

The extension is deliberately **dumb plumbing**: it knows nothing about any
specific table function. It forwards a name + arguments to the daemon and
returns whatever rows come back. Adding, changing, or removing a table
function is done entirely in the daemon — the extension never changes.

```
 SQL query ──▶  pg_relay (in Postgres)  ──HTTP──▶  daemon (separate server)  ──▶  data source
                     │                                  │
                     │ marshals call, attaches          │ owns every table function
                     │ caller identity, takes a         │ (kv_get, kv_put, ...) and
                     │ per-client write lock.           │ all the logic. returns rows.
                     │ ZERO business logic.             │
```

## Status

Early development. Built and compiling against **pgrx 0.18 / Postgres 18** on
macOS. The daemon is currently a throwaway Python stub (`stub/daemon.py`);
the real daemon is expected to live on a separate server.

## SQL interface

Two generic functions. Both take the **table function name** as a string and
its **arguments** as `jsonb`, and both return a set of rows, one `jsonb`
value per row (column name `row`).

```sql
-- READ  (STABLE)   -> GET  /<name>?args=<json>
SELECT * FROM pg_relay_read('kv_get', '{"key":"foo"}'::jsonb);

-- WRITE (VOLATILE)  -> PUT  /<name>   body = <args json>
SELECT * FROM pg_relay_write('kv_put', '{"key":"foo","value":"bar"}'::jsonb);

-- pull fields out of the jsonb rows
SELECT row->>'value' AS value
FROM pg_relay_read('kv_get', '{"key":"foo"}'::jsonb);
```

| | `pg_relay_read` | `pg_relay_write` |
|---|---|---|
| Volatility | `STABLE`, `parallel_restricted` | `VOLATILE`, `parallel_unsafe` |
| HTTP method | `GET /<name>?args=<json>` | `PUT /<name>` (args in body) |
| Side effects | none | mutates the daemon's data source |
| Per-client lock | no | yes (see below) |

Why two functions and not one: the read/write split is what tells the Postgres
planner whether a call is side-effect-free (so it may cache/parallelize it) or
must run exactly as written.

## Wire protocol

HTTP + JSON, because the daemon runs on a **separate server** (a Unix socket
can't cross machines). The table function name is the URL path; the HTTP verb
carries read-vs-write:

- **Read:** `GET /<name>?args=<url-encoded-json>` — responses are sent with
  `Cache-Control: no-store` so no proxy can serve stale reads.
- **Write:** `PUT /<name>` with the args as the JSON request body.

The daemon responds with `{"rows": [ ... ]}`. Each element becomes one result
row. Any failure (couldn't connect, non-2xx status, non-JSON body, or a
missing `rows` array) is turned into a clear Postgres `ERROR`.

> Security note: for production the daemon URL should be **HTTPS with an auth
> token**, never plain HTTP. The default points at a local dev stub.

## Configuration (GUCs)

Settings are registered in `_PG_init` and can be set in `postgresql.conf`, via
`ALTER SYSTEM`, or per-session with `SET`.

| GUC | Type | Default | Meaning |
|---|---|---|---|
| `pg_relay.daemon_url` | string | `http://127.0.0.1:8080` | Base URL of the daemon. |
| `pg_relay.write_lock` | enum | `wait` | Behaviour on a per-shard write conflict: `wait` blocks until free; `nowait` fails fast with a write-conflict error. |
| `pg_relay.lock_key_field` | string | (unset) | Args field whose value identifies the write shard to serialize on. Unset = writes are not locked. |

```sql
SHOW pg_relay.daemon_url;
SET  pg_relay.daemon_url = 'https://daemon.internal:8080';
SET  pg_relay.write_lock = 'nowait';
SET  pg_relay.lock_key_field = 'key';   -- enable per-shard write locking
```

The *allowed values* and *defaults* live in the compiled extension (in
`src/lib.rs`); `postgresql.conf` only stores the chosen override.

## Caller attribution

Every read and write logs one audit line to the Postgres server log, capturing
who made the call. The identity is read from the session via SPI:

- `current_user` (role), `session_user`
- `application_name`
- `inet_client_addr()` (NULL for local socket connections)
- `pg_backend_pid()`

The same identity is also sent to the daemon as request headers
(`X-Pg-Role`, `X-Pg-Application`, `X-Pg-Client-Addr`, `X-Pg-Backend-Pid`,
`X-Pg-Session-User`), so the daemon can log or authorize on it too.

To see the audit lines in `psql`: `SET client_min_messages = 'log';`

## Per-shard write locking

`pg_relay_write` can serialize concurrent writes that share the same shard
using a **transaction-scoped Postgres advisory lock**. The shard is identified
by the args field named in `pg_relay.lock_key_field`. The lock auto-releases at
the end of the statement's transaction.

- Set `pg_relay.lock_key_field` to the field that identifies your write shard
  (e.g. a tenant or entity id). Writes sharing that field's value serialize.
- `pg_relay.write_lock = 'wait'` → `pg_advisory_xact_lock` (the second writer
  for a shard blocks until the first finishes).
- `pg_relay.write_lock = 'nowait'` → `pg_try_advisory_xact_lock` (the second
  writer gets an immediate `write conflict` error).

If `pg_relay.lock_key_field` is unset, or the args don't contain that field,
the write is not locked.

**Scope:** this lock only serializes writers going through *this* Postgres
instance. The authoritative data lives in the remote daemon; if the daemon can
be reached from elsewhere, the daemon must enforce its own per-shard
serialization for full safety. (The current single-threaded Python stub
processes one request at a time, so it's fine for development.)

## Project layout

```
pg-relay/
├── Cargo.toml            # crate manifest; pgrx + ureq + serde_json
├── pg_relay.control      # extension metadata read by CREATE EXTENSION
├── .cargo/config.toml    # macOS linker flag (undefined symbols resolved at runtime)
├── src/lib.rs            # the entire extension
└── stub/daemon.py        # throwaway dev daemon (HTTP + JSON, in-memory store)
```

## Build, install, run

Prerequisites: Rust, `cargo-pgrx 0.18`, and a Postgres 18 registered with pgrx
(`cargo pgrx init --pg18 $(which pg_config)`).

```bash
# Fast dev loop: build, install into a pgrx-managed sandbox, open psql
cargo pgrx run

# Or install into your own (e.g. Homebrew) Postgres
cargo pgrx install --pg-config $(which pg_config)
#   then, in any database:  CREATE EXTENSION pg_relay;
```

Start the stub daemon in another terminal so the extension has something to
talk to:

```bash
python3 stub/daemon.py
# GET  /kv_get?args=<json>   reads the in-memory store
# PUT  /kv_put               writes the in-memory store
```

Smoke test inside `psql`:

```sql
CREATE EXTENSION pg_relay;
SET client_min_messages = 'log';

SELECT * FROM pg_relay_write('kv_put',
    '{"key":"foo","value":"bar"}'::jsonb);
SELECT row->>'value'
FROM   pg_relay_read('kv_get', '{"key":"foo"}'::jsonb);   -- bar
```

## Not done yet / next steps

- HTTPS + auth token for the daemon connection.
- A timeout on `wait` mode so a write can't block indefinitely.
- Typed SQL wrappers (e.g. `kv_get('foo')`) generated from a daemon-published
  schema, instead of the generic `pg_relay_read('kv_get', ...)` form.
- A real daemon to replace the Python stub.
