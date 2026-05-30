//! Core types, traits, and protocol for pg_relay.
//!
//! This crate defines the contract between the daemon framework
//! (`pg-relay-server`) and the user code that plugs into it. It
//! intentionally has no runtime dependencies — no tokio, no IO —
//! so it can be embedded anywhere the types are needed.

pub mod audit;
pub mod error;
pub mod keys;
pub mod protocol;
pub mod row;
pub mod schema;
pub mod storage;
pub mod table_function;

pub use error::{Error, Result};
pub use keys::{ComputeKey, LockKey};
pub use row::{Column, Inputs, Row};
pub use schema::{ColumnSchema, TableFunctionKind, TableFunctionSchema, Type};
pub use table_function::{
    ComputeContext, ReadTableFunction, SharedState, TableFunction, WriteContext,
    WriteTableFunction,
};
