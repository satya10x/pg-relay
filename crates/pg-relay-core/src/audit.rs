use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Caller {
    pub pg_role: Option<String>,
    pub application_name: Option<String>,
    pub client_addr: Option<String>,
    pub backend_pid: Option<i32>,
    pub session_user: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Operation {
    pub table_function: String,
    pub kind: crate::schema::TableFunctionKind,
    pub inputs: serde_json::Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheResult {
    Hit,
    Miss,
    Coalesced,
    NotApplicable,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CallResult {
    pub status: String,
    pub cache: CacheResult,
    pub rows: u64,
    pub bytes_out: u64,
    pub latency_ms: f64,
    pub compute_ms: f64,
    pub storage_ms: f64,
    pub error_code: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditEvent {
    pub ts: chrono::DateTime<chrono::Utc>,
    pub trace_id: String,
    pub request_id: Option<String>,
    pub caller: Caller,
    pub operation: Operation,
    pub result: CallResult,
}

#[async_trait]
pub trait AuditLog: Send + Sync {
    async fn record(&self, event: AuditEvent);
}
