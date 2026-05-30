use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("table function '{0}' not registered")]
    UnknownTableFunction(String),

    #[error("missing input '{0}'")]
    MissingInput(String),

    #[error("type mismatch for input '{name}': expected {expected}, got {got}")]
    InputType {
        name: String,
        expected: &'static str,
        got: &'static str,
    },

    #[error("concurrent write in progress for shard key {0}")]
    WriteConflict(String),

    #[error("idempotent no-op: prior outcome returned")]
    IdempotentNoop,

    #[error("storage error: {0}")]
    Storage(String),

    #[error("compute error: {0}")]
    Compute(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("timeout after {0:?}")]
    Timeout(std::time::Duration),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl Error {
    /// Stable error code suitable for the audit log and the SQL-side
    /// error name. Kept short and machine-readable.
    pub fn code(&self) -> &'static str {
        match self {
            Error::UnknownTableFunction(_) => "unknown_table_function",
            Error::MissingInput(_) => "missing_input",
            Error::InputType { .. } => "input_type_mismatch",
            Error::WriteConflict(_) => "write_conflict",
            Error::IdempotentNoop => "idempotent_noop",
            Error::Storage(_) => "storage_error",
            Error::Compute(_) => "compute_error",
            Error::Protocol(_) => "protocol_error",
            Error::Timeout(_) => "timeout",
            Error::Other(_) => "internal_error",
        }
    }
}
