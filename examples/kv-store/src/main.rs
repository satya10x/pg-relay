//! Minimal end-to-end example.
//!
//! Two table functions:
//!   - kv_get(key) → (key, value) row (read, STABLE)
//!   - kv_put(key, value, request_id) → confirmation row (write, VOLATILE)
//!
//! Both use the in-memory Storage backend so you can run this with
//! no external dependencies. Run:
//!
//!     cargo run -p kv-store-example -- /tmp/pg_relay.sock
//!
//! Then poke it with the included client test (see tests/ in this crate).

use async_trait::async_trait;
use pg_relay_server::storage_memory::MemoryStorage;
use pg_relay_server::{
    App, ColumnSchema, ComputeContext, ComputeKey, Inputs, LockKey, ReadHandler, Row,
    SharedState, TableFunctionKind, TableFunctionSchema, Type, WriteContext, WriteHandler,
};
use std::sync::Arc;

// ─── kv_get ────────────────────────────────────────────────────────────

struct KvGet;

#[async_trait]
impl ReadHandler for KvGet {
    fn schema(&self) -> TableFunctionSchema {
        TableFunctionSchema {
            name: "kv_get".to_string(),
            kind: TableFunctionKind::Read,
            inputs: vec![ColumnSchema {
                name: "key".to_string(),
                ty: Type::Text,
                nullable: false,
                description: "Key to look up".to_string(),
            }],
            columns: vec![
                ColumnSchema {
                    name: "key".to_string(),
                    ty: Type::Text,
                    nullable: false,
                    description: String::new(),
                },
                ColumnSchema {
                    name: "value".to_string(),
                    ty: Type::Text,
                    nullable: true,
                    description: "Null if key absent".to_string(),
                },
            ],
            description: "Read a value by key from the in-memory KV store.".to_string(),
            shard_key: None,
            estimated_rows: 1,
            timeout_ms: Some(1000),
        }
    }

    fn compute_key(&self, inputs: &Inputs) -> pg_relay_server::Result<ComputeKey> {
        let key = inputs.get_text("key")?;
        Ok(ComputeKey::from_parts([b"kv_get:", key.as_bytes()]))
    }

    async fn compute(
        &self,
        inputs: &Inputs,
        ctx: &ComputeContext<'_>,
    ) -> pg_relay_server::Result<SharedState> {
        let key = inputs.get_text("key")?;
        let storage_key = format!("kv/{key}");

        let value = match ctx.storage.get(&storage_key).await {
            Ok(b) => Some(String::from_utf8_lossy(&b).to_string()),
            Err(pg_relay_server::storage::StorageError::NotFound(_)) => None,
            Err(e) => return Err(pg_relay_server::Error::Storage(e.to_string())),
        };

        let state: SharedState = Arc::new(value);
        Ok(state)
    }

    fn project(
        &self,
        inputs: &Inputs,
        state: &SharedState,
    ) -> pg_relay_server::Result<Vec<Row>> {
        let value = pg_relay_server::table_function::downcast_state::<Option<String>>(state)?;
        let key = inputs.get_text("key")?.to_string();
        let row = Row::new().push(key).push(value.clone());
        Ok(vec![row])
    }
}

// ─── kv_put ────────────────────────────────────────────────────────────

struct KvPut;

#[async_trait]
impl WriteHandler for KvPut {
    fn schema(&self) -> TableFunctionSchema {
        TableFunctionSchema {
            name: "kv_put".to_string(),
            kind: TableFunctionKind::Write,
            inputs: vec![
                ColumnSchema {
                    name: "key".to_string(),
                    ty: Type::Text,
                    nullable: false,
                    description: String::new(),
                },
                ColumnSchema {
                    name: "value".to_string(),
                    ty: Type::Text,
                    nullable: false,
                    description: String::new(),
                },
                ColumnSchema {
                    name: "request_id".to_string(),
                    ty: Type::Text,
                    nullable: false,
                    description: "Idempotency key".to_string(),
                },
            ],
            columns: vec![
                ColumnSchema {
                    name: "key".to_string(),
                    ty: Type::Text,
                    nullable: false,
                    description: String::new(),
                },
                ColumnSchema {
                    name: "bytes_written".to_string(),
                    ty: Type::Int64,
                    nullable: false,
                    description: String::new(),
                },
                ColumnSchema {
                    name: "committed_at".to_string(),
                    ty: Type::Timestamp,
                    nullable: false,
                    description: String::new(),
                },
            ],
            description: "Write a key/value pair atomically.".to_string(),
            shard_key: Some("key".to_string()),
            estimated_rows: 1,
            timeout_ms: Some(5000),
        }
    }

    fn lock_key(&self, inputs: &Inputs) -> pg_relay_server::Result<LockKey> {
        Ok(LockKey::from(inputs.get_text("key")?))
    }

    fn idempotency_key(&self, inputs: &Inputs) -> Option<String> {
        inputs.get_text("request_id").ok().map(|s| s.to_string())
    }

    async fn execute(
        &self,
        inputs: &Inputs,
        ctx: &mut WriteContext<'_>,
    ) -> pg_relay_server::Result<Row> {
        let key = inputs.get_text("key")?.to_string();
        let value = inputs.get_text("value")?.to_string();
        let storage_key = format!("kv/{key}");

        let bytes = bytes::Bytes::from(value.into_bytes());
        let meta = ctx
            .storage
            .put(&storage_key, bytes)
            .await
            .map_err(|e| pg_relay_server::Error::Storage(e.to_string()))?;

        Ok(Row::new()
            .push(key)
            .push(meta.size as i64)
            .push(meta.last_modified))
    }
}

// ─── main ──────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let socket_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/pg_relay.sock".to_string());

    let storage = Arc::new(MemoryStorage::new());

    let app = App::new().storage(storage);
    app.register_read(Arc::new(KvGet));
    app.register_write(Arc::new(KvPut));

    tracing::info!(socket = %socket_path, "starting kv-store example");
    app.serve_unix(socket_path).await
}
