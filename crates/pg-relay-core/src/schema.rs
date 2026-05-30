use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TableFunctionKind {
    Read,
    Write,
}

/// SQL-shaped types that pg_relay can marshal end-to-end.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Bool,
    Int64,
    Float64,
    Text,
    Bytes,
    Date,
    Timestamp,
    Json,
}

impl Type {
    /// The Postgres type name this maps to in `CREATE FUNCTION` DDL.
    pub fn pg_type(&self) -> &'static str {
        match self {
            Type::Bool => "boolean",
            Type::Int64 => "bigint",
            Type::Float64 => "double precision",
            Type::Text => "text",
            Type::Bytes => "bytea",
            Type::Date => "date",
            Type::Timestamp => "timestamp with time zone",
            Type::Json => "jsonb",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub ty: Type,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableFunctionSchema {
    pub name: String,
    pub kind: TableFunctionKind,
    pub inputs: Vec<ColumnSchema>,
    pub columns: Vec<ColumnSchema>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub shard_key: Option<String>,
    /// Estimated row count for the planner. Defaults to 100.
    #[serde(default = "default_rows")]
    pub estimated_rows: u32,
    /// Per-call deadline. None means no daemon-side timeout
    /// (the Postgres statement_timeout still applies).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

fn default_rows() -> u32 {
    100
}
