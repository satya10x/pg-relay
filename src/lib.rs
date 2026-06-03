use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting, PostgresGucEnum};
use pgrx::prelude::*;
use pgrx::JsonB;
use std::ffi::CString;
use std::hash::{Hash, Hasher};

::pgrx::pg_module_magic!(name, version);

// ─── Configuration (a GUC: a Postgres setting) ────────────────────────
//
// `pg_relay.daemon_url` can be set in postgresql.conf, via ALTER SYSTEM,
// or per-session with SET. Defaults to the local dev stub.
static DAEMON_URL: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"http://127.0.0.1:8080"));

/// What to do when another write already holds a client's lock.
#[derive(Copy, Clone, PostgresGucEnum)]
enum WriteLock {
    /// Block until the in-progress write finishes (pg_advisory_xact_lock).
    #[name = c"wait"]
    Wait,
    /// Fail immediately with a write-conflict error (pg_try_advisory_xact_lock).
    #[name = c"nowait"]
    Nowait,
}

/// `pg_relay.write_lock` = 'wait' (default) | 'nowait'.
static WRITE_LOCK: GucSetting<WriteLock> = GucSetting::<WriteLock>::new(WriteLock::Wait);

/// `pg_relay.lock_key_field` — name of the args field whose value
/// identifies the write shard to serialize on. Unset (default) means
/// writes are not locked; set it (e.g. to your tenant/key field) to
/// enable per-shard write serialization.
static LOCK_KEY_FIELD: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

/// Postgres calls this once when the library is loaded. We register the
/// GUC here so the setting exists and can be read/overridden.
#[pg_guard]
extern "C-unwind" fn _PG_init() {
    GucRegistry::define_string_guc(
        c"pg_relay.daemon_url",
        c"Base URL of the pg_relay daemon.",
        c"Where read and write relays send their HTTP requests.",
        &DAEMON_URL,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_enum_guc(
        c"pg_relay.write_lock",
        c"Behaviour when a concurrent write holds a client's lock.",
        c"'wait' blocks until it's free; 'nowait' fails fast with a write conflict.",
        &WRITE_LOCK,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        c"pg_relay.lock_key_field",
        c"Args field whose value identifies the write shard to lock on.",
        c"When set, concurrent writes sharing this field's value are serialized; unset means no write locking.",
        &LOCK_KEY_FIELD,
        GucContext::Userset,
        GucFlags::default(),
    );
}

/// Current daemon base URL from the GUC (falls back to the dev stub).
fn daemon_base() -> String {
    DAEMON_URL
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_else(|| "http://127.0.0.1:8080".to_string())
}

// ─── Caller attribution ───────────────────────────────────────────────

/// Who is making this call — pulled from the session via SPI.
struct Caller {
    role: Option<String>,
    application: Option<String>,
    addr: Option<String>,
    pid: Option<i32>,
    session_user: Option<String>,
}

fn current_caller() -> Caller {
    Caller {
        role: Spi::get_one::<String>("SELECT current_user::text").ok().flatten(),
        application: Spi::get_one::<String>("SELECT current_setting('application_name', true)")
            .ok()
            .flatten(),
        addr: Spi::get_one::<String>("SELECT inet_client_addr()::text").ok().flatten(),
        pid: Spi::get_one::<i32>("SELECT pg_backend_pid()").ok().flatten(),
        session_user: Spi::get_one::<String>("SELECT session_user::text").ok().flatten(),
    }
}

/// Write one audit line to the Postgres server log.
fn log_call(verb: &str, table_function: &str, args: &serde_json::Value, c: &Caller) {
    log!(
        "pg_relay {verb} {table_function} args={args} \
         caller(role={:?} app={:?} addr={:?} pid={:?} session_user={:?})",
        c.role,
        c.application,
        c.addr,
        c.pid,
        c.session_user,
    );
}

/// Attach the caller identity to an outgoing request as headers, so the
/// daemon sees who made the call too.
fn with_caller(req: ureq::Request, c: &Caller) -> ureq::Request {
    req.set("X-Pg-Role", c.role.as_deref().unwrap_or(""))
        .set("X-Pg-Application", c.application.as_deref().unwrap_or(""))
        .set("X-Pg-Client-Addr", c.addr.as_deref().unwrap_or(""))
        .set(
            "X-Pg-Backend-Pid",
            &c.pid.map(|p| p.to_string()).unwrap_or_default(),
        )
        .set("X-Pg-Session-User", c.session_user.as_deref().unwrap_or(""))
}

// ─── Shared tail ──────────────────────────────────────────────────────

/// Turn a daemon HTTP result into rows, raising a clear Postgres error on
/// any failure (couldn't connect, bad status, bad JSON, or missing rows).
fn rows_from(result: Result<ureq::Response, ureq::Error>) -> Vec<serde_json::Value> {
    let response = match result {
        Ok(r) => r,
        Err(ureq::Error::Status(code, r)) => {
            let body = r.into_string().unwrap_or_default();
            error!("pg_relay: daemon returned HTTP {code}: {body}");
        }
        Err(ureq::Error::Transport(t)) => {
            error!("pg_relay: could not reach daemon: {t}");
        }
    };

    let json: serde_json::Value = response
        .into_json()
        .unwrap_or_else(|e| error!("pg_relay: invalid daemon response: {e}"));

    match json.get("rows").and_then(|r| r.as_array()) {
        Some(arr) => arr.clone(),
        None => error!("pg_relay: daemon response missing a 'rows' array"),
    }
}

// ─── Relays ───────────────────────────────────────────────────────────

/// Read relay. STABLE + parallel_restricted: logically read-only, but it
/// does SPI lookups and an external HTTP call, so it must run in the
/// leader process (not farmed out to parallel workers).
#[pg_extern(stable, parallel_restricted)]
fn pg_relay_read(
    table_function: &str,
    args: JsonB,
) -> TableIterator<'static, (name!(row, JsonB),)> {
    let caller = current_caller();
    log_call("GET", table_function, &args.0, &caller);

    let url = format!("{}/{table_function}", daemon_base());
    let req = with_caller(ureq::get(&url).query("args", &args.0.to_string()), &caller);
    TableIterator::new(rows_from(req.call()).into_iter().map(|v| (JsonB(v),)))
}

/// Write relay. STABLE + parallel_restricted (like the read relay): it
/// does SPI lookups and an external HTTP call, so it must run in the
/// leader process. Sends the call to the daemon as a POST with `args` as
/// the JSON request body. Unlike `pg_relay_update`, it does no per-shard
/// write serialization.
#[pg_extern(stable, parallel_restricted)]
fn pg_relay_write(
    table_function: &str,
    args: JsonB,
) -> TableIterator<'static, (name!(row, JsonB),)> {
    let caller = current_caller();
    log_call("POST", table_function, &args.0, &caller);

    let url = format!("{}/{table_function}", daemon_base());
    let req = with_caller(ureq::post(&url), &caller);
    TableIterator::new(rows_from(req.send_json(args.0)).into_iter().map(|v| (JsonB(v),)))
}

/// Update relay. VOLATILE + parallel_unsafe. Before sending, it serializes
/// concurrent writes that share the same shard (within this Postgres
/// instance) via a transaction-scoped advisory lock keyed on the args
/// field named by `pg_relay.lock_key_field`.
#[pg_extern(volatile, parallel_unsafe)]
fn pg_relay_update(
    table_function: &str,
    args: JsonB,
) -> TableIterator<'static, (name!(row, JsonB),)> {
    let caller = current_caller();

    // Per-shard write serialization. The shard field is configured via
    // pg_relay.lock_key_field; when set and present in args, concurrent
    // writes sharing that field's value are serialized. The lock
    // auto-releases at end of the statement's transaction. 'wait' blocks;
    // 'nowait' fails fast — controlled by pg_relay.write_lock.
    if let Some(shard) = shard_value(&args.0) {
        let key = lock_key(&format!("{table_function}:{shard}"));
        match WRITE_LOCK.get() {
            WriteLock::Wait => {
                Spi::run(&format!("SELECT pg_advisory_xact_lock({key})"))
                    .unwrap_or_else(|e| error!("pg_relay: failed to take write lock: {e}"));
            }
            WriteLock::Nowait => {
                let got: Option<bool> =
                    Spi::get_one(&format!("SELECT pg_try_advisory_xact_lock({key})"))
                        .unwrap_or_else(|e| error!("pg_relay: failed to take write lock: {e}"));
                if got != Some(true) {
                    error!(
                        "pg_relay: write conflict on shard {shard} \
                         (another write is in progress)"
                    );
                }
            }
        }
    }

    log_call("PUT", table_function, &args.0, &caller);

    let url = format!("{}/{table_function}", daemon_base());
    let req = with_caller(ureq::put(&url), &caller);
    TableIterator::new(rows_from(req.send_json(args.0)).into_iter().map(|v| (JsonB(v),)))
}

/// The value of the configured shard field in `args`, if both the
/// `pg_relay.lock_key_field` GUC is set and that field is present.
/// `None` means "don't lock this write."
fn shard_value(args: &serde_json::Value) -> Option<String> {
    let field = LOCK_KEY_FIELD.get()?;
    let field = field.to_string_lossy();
    if field.is_empty() {
        return None;
    }
    args.get(field.as_ref()).map(|v| v.to_string())
}

/// Stable 64-bit hash of a string, for use as an advisory-lock key.
/// Interpolated into SQL as a bare integer, so it's injection-safe.
fn lock_key(s: &str) -> i64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish() as i64
}
