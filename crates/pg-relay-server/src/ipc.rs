//! Length-prefixed JSON IPC server over Unix domain socket.
//!
//! Each frame: 4-byte big-endian length + JSON payload. One
//! request/response per frame. Blocking read on the socket
//! corresponds to one Postgres backend waiting for an SRF result.

use crate::{audit_stdout::StdoutJsonLog, read_cache::ReadCache, write_coordinator::WriteCoordinator, Registry};
use pg_relay_core::audit::{AuditEvent, AuditLog, CacheResult, CallResult, Operation};
use pg_relay_core::protocol::{Request, Response};
use pg_relay_core::storage::Storage;
use pg_relay_core::{ComputeContext, Error, TableFunctionKind, WriteContext};
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

const MAX_FRAME: u32 = 16 * 1024 * 1024; // 16 MiB

pub struct IpcServer {
    pub registry: Arc<Registry>,
    pub read_cache: Arc<ReadCache>,
    pub write_coord: Arc<WriteCoordinator>,
    pub storage: Arc<dyn Storage>,
    pub audit: Arc<dyn AuditLog>,
}

impl IpcServer {
    pub fn new(
        registry: Arc<Registry>,
        storage: Arc<dyn Storage>,
        audit: Option<Arc<dyn AuditLog>>,
    ) -> Self {
        IpcServer {
            registry,
            read_cache: Arc::new(ReadCache::new()),
            write_coord: Arc::new(WriteCoordinator::new()),
            storage,
            audit: audit.unwrap_or_else(|| Arc::new(StdoutJsonLog::new())),
        }
    }

    pub async fn serve_unix(self: Arc<Self>, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = path.as_ref();
        // Remove a stale socket file if present.
        let _ = std::fs::remove_file(path);
        let listener = UnixListener::bind(path)?;
        tracing::info!(socket = %path.display(), "pg_relay listening");

        loop {
            let (stream, _addr) = listener.accept().await?;
            let server = self.clone();
            tokio::spawn(async move {
                if let Err(e) = server.handle_connection(stream).await {
                    tracing::warn!(error = %e, "connection ended with error");
                }
            });
        }
    }

    async fn handle_connection(self: Arc<Self>, mut stream: UnixStream) -> anyhow::Result<()> {
        loop {
            let req = match read_frame(&mut stream).await? {
                Some(r) => r,
                None => return Ok(()), // peer closed
            };

            let resp = self.handle_request(req).await;
            write_frame(&mut stream, &resp).await?;
        }
    }

    async fn handle_request(&self, req: Request) -> Response {
        match req {
            Request::Ping => Response::Pong,

            Request::Describe { table_function } => {
                if let Some(h) = self.registry.get_read(&table_function) {
                    Response::Schema(h.schema())
                } else if let Some(h) = self.registry.get_write(&table_function) {
                    Response::Schema(h.schema())
                } else {
                    Response::Error {
                        code: "unknown_table_function".to_string(),
                        message: table_function,
                    }
                }
            }

            Request::ListTableFunctions => Response::SchemaList(self.registry.list_schemas()),

            Request::Read {
                table_function,
                inputs,
                caller,
                trace_id,
                deadline_ms: _,
            } => {
                let start = std::time::Instant::now();
                let handler = match self.registry.get_read(&table_function) {
                    Some(h) => h,
                    None => {
                        return Response::Error {
                            code: "unknown_table_function".to_string(),
                            message: table_function,
                        }
                    }
                };

                let key = match handler.compute_key(&inputs) {
                    Ok(k) => k,
                    Err(e) => return error_response(&e),
                };

                let storage = self.storage.clone();
                let trace_id_for_compute = trace_id.clone();
                let handler_for_compute = handler.clone();
                let inputs_for_compute = inputs.clone();

                let compute_start = std::time::Instant::now();
                let result = self
                    .read_cache
                    .get_or_compute(key, move || async move {
                        let ctx = ComputeContext {
                            storage: storage.as_ref(),
                            trace_id: &trace_id_for_compute,
                        };
                        handler_for_compute.compute(&inputs_for_compute, &ctx).await
                    })
                    .await;

                let (rows_out, cache_outcome, error) = match result {
                    Ok((state, outcome)) => match handler.project(&inputs, &state) {
                        Ok(rows) => (rows, outcome, None),
                        Err(e) => (Vec::new(), outcome, Some(e)),
                    },
                    Err(e) => (Vec::new(), crate::read_cache::CacheOutcome::Miss, Some(e)),
                };

                let compute_ms = compute_start.elapsed().as_secs_f64() * 1000.0;
                let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

                let bytes_out = rows_out
                    .iter()
                    .map(|r| serde_json::to_vec(r).map(|b| b.len() as u64).unwrap_or(0))
                    .sum();

                let audit_cache = match cache_outcome {
                    crate::read_cache::CacheOutcome::Hit => CacheResult::Hit,
                    crate::read_cache::CacheOutcome::Miss => CacheResult::Miss,
                    crate::read_cache::CacheOutcome::Coalesced => CacheResult::Coalesced,
                };

                self.emit_audit(
                    &table_function,
                    TableFunctionKind::Read,
                    &caller,
                    &trace_id,
                    None,
                    audit_cache,
                    rows_out.len() as u64,
                    bytes_out,
                    latency_ms,
                    compute_ms,
                    0.0,
                    error.as_ref(),
                    &inputs,
                )
                .await;

                if let Some(e) = error {
                    error_response(&e)
                } else {
                    Response::Rows {
                        rows: rows_out,
                        cache: audit_cache,
                        compute_ms,
                    }
                }
            }

            Request::Write {
                table_function,
                inputs,
                request_id,
                caller,
                trace_id,
                deadline_ms: _,
            } => {
                let start = std::time::Instant::now();
                let handler = match self.registry.get_write(&table_function) {
                    Some(h) => h,
                    None => {
                        return Response::Error {
                            code: "unknown_table_function".to_string(),
                            message: table_function,
                        }
                    }
                };

                let lock_key = match handler.lock_key(&inputs) {
                    Ok(k) => k,
                    Err(e) => return error_response(&e),
                };
                let idem = handler.idempotency_key(&inputs);

                let storage = self.storage.clone();
                let trace_id_for_exec = trace_id.clone();
                let request_id_for_exec = request_id.clone();
                let handler_for_exec = handler.clone();
                let inputs_for_exec = inputs.clone();

                let result = self
                    .write_coord
                    .execute(lock_key, idem.as_deref(), move || async move {
                        let mut ctx = WriteContext {
                            storage: storage.as_ref(),
                            trace_id: &trace_id_for_exec,
                            request_id: &request_id_for_exec,
                        };
                        handler_for_exec.execute(&inputs_for_exec, &mut ctx).await
                    })
                    .await;

                let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
                let (response, error_for_audit) = match result {
                    Ok(wr) => {
                        let row = wr.row.clone();
                        let bytes = serde_json::to_vec(&row).map(|b| b.len() as u64).unwrap_or(0);
                        self.emit_audit(
                            &table_function,
                            TableFunctionKind::Write,
                            &caller,
                            &trace_id,
                            Some(&request_id),
                            CacheResult::NotApplicable,
                            1,
                            bytes,
                            latency_ms,
                            0.0,
                            0.0,
                            None,
                            &inputs,
                        )
                        .await;
                        (
                            Response::Outcome {
                                row,
                                was_idempotent_noop: wr.was_idempotent_noop,
                            },
                            None::<Error>,
                        )
                    }
                    Err(e) => {
                        self.emit_audit(
                            &table_function,
                            TableFunctionKind::Write,
                            &caller,
                            &trace_id,
                            Some(&request_id),
                            CacheResult::NotApplicable,
                            0,
                            0,
                            latency_ms,
                            0.0,
                            0.0,
                            Some(&e),
                            &inputs,
                        )
                        .await;
                        (error_response(&e), Some(e))
                    }
                };
                let _ = error_for_audit;
                response
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn emit_audit(
        &self,
        table_function: &str,
        kind: TableFunctionKind,
        caller: &pg_relay_core::audit::Caller,
        trace_id: &str,
        request_id: Option<&str>,
        cache: CacheResult,
        rows: u64,
        bytes_out: u64,
        latency_ms: f64,
        compute_ms: f64,
        storage_ms: f64,
        error: Option<&Error>,
        inputs: &pg_relay_core::Inputs,
    ) {
        let event = AuditEvent {
            ts: chrono::Utc::now(),
            trace_id: trace_id.to_string(),
            request_id: request_id.map(|s| s.to_string()),
            caller: caller.clone(),
            operation: Operation {
                table_function: table_function.to_string(),
                kind,
                inputs: serde_json::to_value(inputs).unwrap_or(serde_json::Value::Null),
            },
            result: CallResult {
                status: match error {
                    None => "ok".to_string(),
                    Some(e) => e.code().to_string(),
                },
                cache,
                rows,
                bytes_out,
                latency_ms,
                compute_ms,
                storage_ms,
                error_code: error.map(|e| e.code().to_string()),
            },
        };
        self.audit.record(event).await;
    }
}

fn error_response(e: &Error) -> Response {
    Response::Error {
        code: e.code().to_string(),
        message: e.to_string(),
    }
}

async fn read_frame(stream: &mut UnixStream) -> anyhow::Result<Option<Request>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(len_buf);
    if len > MAX_FRAME {
        anyhow::bail!("frame too large: {len} bytes");
    }
    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf).await?;
    let req: Request = serde_json::from_slice(&buf)?;
    Ok(Some(req))
}

async fn write_frame(stream: &mut UnixStream, resp: &Response) -> anyhow::Result<()> {
    let body = serde_json::to_vec(resp)?;
    let len = body.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}
