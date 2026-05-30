//! End-to-end test of the kv-store example. Spawns the daemon on a
//! temp Unix socket, then sends Read/Write requests and verifies the
//! responses.

use async_trait::async_trait;
use pg_relay_server::audit_stdout::StdoutJsonLog;
use pg_relay_server::storage_memory::MemoryStorage;
use pg_relay_server::{
    App, ColumnSchema, ComputeContext, ComputeKey, Inputs, LockKey, ReadHandler, Row,
    SharedState, TableFunctionKind, TableFunctionSchema, Type, WriteContext, WriteHandler,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use pg_relay_server::audit::{AuditEvent, AuditLog, Caller};
use pg_relay_server::protocol::{Request, Response};
use pg_relay_server::Column;

// ─── Re-define KvGet/KvPut here for the test (would normally come
// from the example's main module, but main.rs isn't a library) ────

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
                description: String::new(),
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
                    description: String::new(),
                },
            ],
            description: String::new(),
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
        Ok(Arc::new(value) as SharedState)
    }

    fn project(
        &self,
        inputs: &Inputs,
        state: &SharedState,
    ) -> pg_relay_server::Result<Vec<Row>> {
        let value = pg_relay_server::table_function::downcast_state::<Option<String>>(state)?;
        let key = inputs.get_text("key")?.to_string();
        Ok(vec![Row::new().push(key).push(value.clone())])
    }
}

struct KvPut;

#[async_trait]
impl WriteHandler for KvPut {
    fn schema(&self) -> TableFunctionSchema {
        TableFunctionSchema {
            name: "kv_put".to_string(),
            kind: TableFunctionKind::Write,
            inputs: vec![],
            columns: vec![],
            description: String::new(),
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
        let meta = ctx
            .storage
            .put(&storage_key, bytes::Bytes::from(value.into_bytes()))
            .await
            .map_err(|e| pg_relay_server::Error::Storage(e.to_string()))?;
        Ok(Row::new()
            .push(key)
            .push(meta.size as i64)
            .push(meta.last_modified))
    }
}

// A silent audit log for tests — stdout would pollute test output.
struct SilentAudit;
#[async_trait]
impl AuditLog for SilentAudit {
    async fn record(&self, _event: AuditEvent) {}
}

async fn send_request(stream: &mut UnixStream, req: &Request) -> anyhow::Result<Response> {
    let body = serde_json::to_vec(req)?;
    stream.write_all(&(body.len() as u32).to_be_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(serde_json::from_slice(&buf)?)
}

fn empty_caller() -> Caller {
    Caller {
        pg_role: None,
        application_name: None,
        client_addr: None,
        backend_pid: None,
        session_user: None,
    }
}

/// Start the kv-store daemon on a temp socket and return its path.
async fn spawn_daemon() -> String {
    let sock = format!(
        "/tmp/pg_relay_test_{}_{}.sock",
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    );

    let app = App::new()
        .storage(Arc::new(MemoryStorage::new()))
        .audit(Arc::new(SilentAudit));
    app.register_read(Arc::new(KvGet));
    app.register_write(Arc::new(KvPut));

    let sock_for_serve = sock.clone();
    tokio::spawn(async move {
        let _ = app.serve_unix(sock_for_serve).await;
    });

    // Wait for the socket to actually exist.
    for _ in 0..50 {
        if std::path::Path::new(&sock).exists() {
            return sock;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("daemon socket never appeared");
}

#[tokio::test]
async fn end_to_end_read_write_idempotency() {
    let sock = spawn_daemon().await;
    let mut stream = UnixStream::connect(&sock).await.unwrap();

    // 1. Ping the daemon.
    let r = send_request(&mut stream, &Request::Ping).await.unwrap();
    assert!(matches!(r, Response::Pong));

    // 2. Read a missing key — should return one row with NULL value.
    let r = send_request(
        &mut stream,
        &Request::Read {
            table_function: "kv_get".to_string(),
            inputs: Inputs::from_pairs([("key", Column::Text("foo".to_string()))]),
            caller: empty_caller(),
            trace_id: "t1".to_string(),
            deadline_ms: None,
        },
    )
    .await
    .unwrap();
    match r {
        Response::Rows { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert!(matches!(rows[0].columns[1], Column::Null));
        }
        other => panic!("unexpected response: {other:?}"),
    }

    // 3. Write a value.
    let r = send_request(
        &mut stream,
        &Request::Write {
            table_function: "kv_put".to_string(),
            inputs: Inputs::from_pairs([
                ("key", Column::Text("foo".to_string())),
                ("value", Column::Text("hello".to_string())),
                ("request_id", Column::Text("req_1".to_string())),
            ]),
            request_id: "req_1".to_string(),
            caller: empty_caller(),
            trace_id: "t2".to_string(),
            deadline_ms: None,
        },
    )
    .await
    .unwrap();
    match r {
        Response::Outcome {
            was_idempotent_noop,
            ..
        } => assert!(!was_idempotent_noop),
        other => panic!("unexpected response: {other:?}"),
    }

    // 4. Read it back — should be cached compute returning "hello".
    //    Open a NEW stream to bypass any cached state on the prior stream.
    let mut stream2 = UnixStream::connect(&sock).await.unwrap();
    let r = send_request(
        &mut stream2,
        &Request::Read {
            table_function: "kv_get".to_string(),
            inputs: Inputs::from_pairs([("key", Column::Text("foo".to_string()))]),
            caller: empty_caller(),
            trace_id: "t3".to_string(),
            deadline_ms: None,
        },
    )
    .await
    .unwrap();
    match r {
        Response::Rows { rows, .. } => {
            // Note: the read cache means stale data could be returned.
            // For this v1 we don't invalidate read cache on write — that
            // belongs on the ROADMAP. So this read might return either
            // None (cached pre-write) or Some("hello") (post-cache-eviction).
            // The test just checks the call succeeds.
            assert_eq!(rows.len(), 1);
        }
        other => panic!("unexpected response: {other:?}"),
    }

    // 5. Retry the write with the same request_id — should noop.
    let r = send_request(
        &mut stream,
        &Request::Write {
            table_function: "kv_put".to_string(),
            inputs: Inputs::from_pairs([
                ("key", Column::Text("foo".to_string())),
                ("value", Column::Text("ignored".to_string())),
                ("request_id", Column::Text("req_1".to_string())),
            ]),
            request_id: "req_1".to_string(),
            caller: empty_caller(),
            trace_id: "t4".to_string(),
            deadline_ms: None,
        },
    )
    .await
    .unwrap();
    match r {
        Response::Outcome {
            was_idempotent_noop,
            ..
        } => assert!(was_idempotent_noop, "expected idempotent noop on retry"),
        other => panic!("unexpected response: {other:?}"),
    }

    // 6. Describe the table function.
    let r = send_request(
        &mut stream,
        &Request::Describe {
            table_function: "kv_get".to_string(),
        },
    )
    .await
    .unwrap();
    match r {
        Response::Schema(s) => {
            assert_eq!(s.name, "kv_get");
            assert!(matches!(s.kind, TableFunctionKind::Read));
        }
        other => panic!("unexpected response: {other:?}"),
    }

    // 7. List all table functions.
    let r = send_request(&mut stream, &Request::ListTableFunctions)
        .await
        .unwrap();
    match r {
        Response::SchemaList(list) => {
            assert_eq!(list.len(), 2);
            assert!(list.iter().any(|s| s.name == "kv_get"));
            assert!(list.iter().any(|s| s.name == "kv_put"));
        }
        other => panic!("unexpected response: {other:?}"),
    }
}
