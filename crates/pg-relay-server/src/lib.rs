//! pg_relay daemon framework.
//!
//! Users build an `App`, register their table function impls,
//! configure storage and audit, then call `App::serve`. The
//! framework owns the IPC loop, the per-key caches, the write
//! coordinator, and the audit pipeline.

pub mod app;
pub mod audit_stdout;
pub mod ipc;
pub mod read_cache;
pub mod registry;
pub mod storage_local;
pub mod storage_memory;
pub mod write_coordinator;

pub use app::App;
pub use registry::{ReadHandler, Registry, WriteHandler};

// Re-export core types so users only depend on pg-relay-server.
pub use pg_relay_core::*;
