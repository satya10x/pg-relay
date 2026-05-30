//! Structured JSON audit log to stdout. Suitable for capture by
//! log shippers (vector, fluent-bit, otel-collector, Promtail).

use async_trait::async_trait;
use pg_relay_core::audit::{AuditEvent, AuditLog};

pub struct StdoutJsonLog;

impl Default for StdoutJsonLog {
    fn default() -> Self {
        StdoutJsonLog
    }
}

impl StdoutJsonLog {
    pub fn new() -> Self {
        StdoutJsonLog
    }
}

#[async_trait]
impl AuditLog for StdoutJsonLog {
    async fn record(&self, event: AuditEvent) {
        match serde_json::to_string(&event) {
            Ok(line) => println!("{line}"),
            Err(e) => eprintln!(
                "pg_relay: failed to serialize audit event: {e} (event was for {})",
                event.operation.table_function
            ),
        }
    }
}
