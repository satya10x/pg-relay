//! IPC protocol between the pgrx extension and the daemon.
//!
//! Wire format is length-prefixed JSON for v1. Each frame is:
//!
//!   [4 bytes BE u32 length] [JSON payload]
//!
//! The simplicity is intentional. Switching to Arrow Flight or
//! a binary format is a v2 concern; v1 wants observability and
//! debuggability over performance.

use crate::audit::Caller;
use crate::{Inputs, Row};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", content = "args", rename_all = "snake_case")]
pub enum Request {
    /// Sanity probe.
    Ping,

    /// Schema introspection for a single table function.
    Describe { table_function: String },

    /// Schema introspection across all registered table functions.
    ListTableFunctions,

    /// Read-side invocation.
    Read {
        table_function: String,
        inputs: Inputs,
        caller: Caller,
        trace_id: String,
        deadline_ms: Option<u64>,
    },

    /// Write-side invocation.
    Write {
        table_function: String,
        inputs: Inputs,
        request_id: String,
        caller: Caller,
        trace_id: String,
        deadline_ms: Option<u64>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "op", content = "args", rename_all = "snake_case")]
pub enum Response {
    Pong,

    Schema(crate::TableFunctionSchema),

    SchemaList(Vec<crate::TableFunctionSchema>),

    /// Read returned rows.
    Rows {
        rows: Vec<Row>,
        cache: crate::audit::CacheResult,
        compute_ms: f64,
    },

    /// Write returned a single confirmation row.
    Outcome {
        row: Row,
        was_idempotent_noop: bool,
    },

    Error {
        code: String,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trip() {
        let req = Request::Ping;
        let json = serde_json::to_string(&req).unwrap();
        let back: Request = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, Request::Ping));
    }
}
