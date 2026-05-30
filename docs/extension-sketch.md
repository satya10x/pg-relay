# pgrx extension sketch

The pgrx extension is *not* built in this workspace yet — pgrx version churn
and the need for a real Postgres install make it awkward to keep inside the
core workspace. The plan is a separate crate (`crates/pg-relay-extension`)
once v0.1 lands.

This document is the template for that crate. Copy these files when you're
ready to wire pg_relay up to a real Postgres.

## Crate layout

```
pg-relay-extension/
├── Cargo.toml
├── pg_relay.control          # extension control file
├── sql/pg_relay--0.0.1.sql   # initial DDL (generated, see below)
└── src/
    ├── lib.rs                # pgrx entry + extension wrappers
    ├── client.rs             # blocking IPC client to the daemon
    ├── caller.rs             # builds Caller from current session info
    └── ddl.rs                # generates CREATE FUNCTION DDL
```

## Cargo.toml

```toml
[package]
name = "pg-relay-extension"
version = "0.0.1"
edition = "2021"

[lib]
crate-type = ["cdylib", "lib"]

[features]
default = ["pg18"]
pg14 = ["pgrx/pg14", "pgrx-tests/pg14"]
pg15 = ["pgrx/pg15", "pgrx-tests/pg15"]
pg16 = ["pgrx/pg16", "pgrx-tests/pg16"]
pg17 = ["pgrx/pg17", "pgrx-tests/pg17"]
pg18 = ["pgrx/pg18", "pgrx-tests/pg18"]
pg_test = []

[dependencies]
pg-relay-core = { path = "../pg-relay-core" }
pgrx = "=0.18.0"
serde = { workspace = true }
serde_json = { workspace = true }
uuid = { workspace = true }

[dev-dependencies]
pgrx-tests = "=0.18.0"
```

## src/lib.rs

```rust
use pg_relay_core::{
    audit::Caller,
    protocol::{Request, Response},
    Column, Inputs,
};
use pgrx::prelude::*;

mod client;
mod caller;

::pgrx::pg_module_magic!();

/// The Unix socket path. Set via `pg_relay.socket_path` GUC; defaults
/// to `/var/run/pg_relay.sock`.
fn socket_path() -> String {
    // PgRX gives access to custom GUC values; wire this up properly in real code.
    std::env::var("PG_RELAY_SOCKET")
        .unwrap_or_else(|_| "/var/run/pg_relay.sock".to_string())
}

// ─── Read SRF: returned rows projected from a daemon Response::Rows ─

#[pg_extern(stable, parallel_safe)]
fn pg_relay_read(
    table_function: &str,
    args_json: pgrx::Json,
) -> Result<TableIterator<'static, (name!(row_json, pgrx::Json),)>, spi::Error> {
    let inputs = decode_inputs(&args_json.0)?;
    let trace_id = new_trace_id();
    let caller = caller::current_caller();

    let req = Request::Read {
        table_function: table_function.to_string(),
        inputs,
        caller,
        trace_id,
        deadline_ms: Some(5000),
    };

    let resp = client::call(&socket_path(), &req)
        .map_err(|e| spi::Error::SpiError(pgrx::pg_sys::SPI_result_code_string(-1)))?;

    match resp {
        Response::Rows { rows, .. } => Ok(TableIterator::new(rows.into_iter().map(|r| {
            (pgrx::Json(serde_json::to_value(r).unwrap()),)
        }))),
        Response::Error { code, message } => {
            error!("pg_relay {code}: {message}");
        }
        other => error!("unexpected daemon response: {other:?}"),
    }
}

// ─── Write SRF: similar, but VOLATILE and parallel_unsafe ────────────

#[pg_extern(volatile, parallel_unsafe)]
fn pg_relay_write(
    table_function: &str,
    args_json: pgrx::Json,
    request_id: &str,
) -> Result<TableIterator<'static, (name!(row_json, pgrx::Json),)>, spi::Error> {
    let inputs = decode_inputs(&args_json.0)?;
    let trace_id = new_trace_id();
    let caller = caller::current_caller();

    // Advisory lock for fast-fail concurrent-write detection.
    // Hash the LockKey to a bigint for pg_try_advisory_xact_lock.
    let lock_key_hash = hash_lock_key(table_function, &inputs);
    let acquired: bool = Spi::get_one_with_args(
        "SELECT pg_try_advisory_xact_lock($1::bigint)",
        vec![(pgrx::PgBuiltInOids::INT8OID.oid(), lock_key_hash.into_datum())],
    )?
    .unwrap_or(false);
    if !acquired {
        error!("pg_relay write_conflict: another write in progress");
    }

    let req = Request::Write {
        table_function: table_function.to_string(),
        inputs,
        request_id: request_id.to_string(),
        caller,
        trace_id,
        deadline_ms: Some(60_000),
    };

    let resp = client::call(&socket_path(), &req)
        .map_err(|e| spi::Error::SpiError(pgrx::pg_sys::SPI_result_code_string(-1)))?;

    match resp {
        Response::Outcome { row, .. } => {
            Ok(TableIterator::new(std::iter::once((
                pgrx::Json(serde_json::to_value(row).unwrap()),
            ))))
        }
        Response::Error { code, message } => error!("pg_relay {code}: {message}"),
        other => error!("unexpected daemon response: {other:?}"),
    }
}

fn decode_inputs(v: &serde_json::Value) -> Result<Inputs, spi::Error> {
    serde_json::from_value(v.clone())
        .map_err(|e| spi::Error::SpiError(pgrx::pg_sys::SPI_result_code_string(-1)))
}

fn new_trace_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn hash_lock_key(table_function: &str, inputs: &Inputs) -> i64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    table_function.hash(&mut h);
    // Hash just the input *values*; the table function's own lock_key()
    // logic runs daemon-side. The hash here is just for the advisory lock.
    for (k, v) in &inputs.values {
        k.hash(&mut h);
        serde_json::to_string(v).unwrap_or_default().hash(&mut h);
    }
    h.finish() as i64
}
```

## src/caller.rs

```rust
use pg_relay_core::audit::Caller;
use pgrx::prelude::*;

pub fn current_caller() -> Caller {
    let pg_role: Option<String> = Spi::get_one("SELECT current_user::text").ok().flatten();
    let app_name: Option<String> = Spi::get_one(
        "SELECT current_setting('application_name', true)::text",
    )
    .ok()
    .flatten();
    let client_addr: Option<String> = Spi::get_one("SELECT inet_client_addr()::text")
        .ok()
        .flatten();
    let session_user: Option<String> = Spi::get_one("SELECT session_user::text").ok().flatten();
    let backend_pid: Option<i32> = Spi::get_one("SELECT pg_backend_pid()").ok().flatten();

    Caller {
        pg_role,
        application_name: app_name,
        client_addr,
        backend_pid,
        session_user,
    }
}
```

## src/client.rs

```rust
//! Blocking IPC client. One connection per Postgres backend, reused
//! across calls in that backend's lifetime via a `thread_local!`.

use pg_relay_core::protocol::{Request, Response};
use std::cell::RefCell;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

thread_local! {
    static SOCKET: RefCell<Option<UnixStream>> = RefCell::new(None);
}

pub fn call(path: &str, req: &Request) -> Result<Response, std::io::Error> {
    SOCKET.with(|cell| {
        let mut slot = cell.borrow_mut();
        let stream = match slot.as_mut() {
            Some(s) => s,
            None => {
                *slot = Some(UnixStream::connect(path)?);
                slot.as_mut().unwrap()
            }
        };

        match do_call(stream, req) {
            Ok(resp) => Ok(resp),
            Err(_) => {
                // Broken pipe — reconnect and retry once.
                *slot = Some(UnixStream::connect(path)?);
                do_call(slot.as_mut().unwrap(), req)
            }
        }
    })
}

fn do_call(stream: &mut UnixStream, req: &Request) -> Result<Response, std::io::Error> {
    let body = serde_json::to_vec(req)?;
    stream.write_all(&(body.len() as u32).to_be_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf)?;
    Ok(serde_json::from_slice(&buf)?)
}
```

## Usage from SQL

For v0.1 the table functions are accessed through generic wrappers. The
`#[derive(TableFunction)]` macro will later generate typed wrappers per
table function so users get `SELECT * FROM kv_get('foo')` rather than the
JSON-shaped generic form below.

```sql
CREATE EXTENSION pg_relay;

-- Read
SELECT *
FROM pg_relay_read(
    'kv_get',
    '{"values":[["key",{"t":"Text","v":"foo"}]]}'::jsonb
);

-- Write
SELECT *
FROM pg_relay_write(
    'kv_put',
    '{"values":[["key",{"t":"Text","v":"foo"}],
                 ["value",{"t":"Text","v":"hello"}]]}'::jsonb,
    'req_1'
);
```

This is ugly on purpose for v0.1 — it gets you a working SQL surface
quickly. v0.2+'s macro generation makes the call sites look like normal
typed functions:

```sql
SELECT * FROM kv_get('foo');
SELECT * FROM kv_put('foo', 'hello', 'req_1');
```

## pg_relay.control

```ini
comment = 'Active compute layer exposed through SQL table functions'
default_version = '0.0.1'
module_pathname = '$libdir/pg_relay'
relocatable = false
superuser = false
```

## Where this is going

The `ddl.rs` module will subscribe to the daemon at extension startup,
fetch the full `Response::SchemaList`, and emit one typed `CREATE FUNCTION`
per registered table function. That replaces the generic
`pg_relay_read`/`pg_relay_write` calls with typed ones, and the SQL becomes
indistinguishable from "real" tables.

For v0.1, the generic form above ships first. The typed-DDL story is part
of v0.3 (alongside the derive macro).
