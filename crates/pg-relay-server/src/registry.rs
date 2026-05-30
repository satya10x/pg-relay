//! Type-erased registry of read and write handlers.
//!
//! User code registers concrete handlers (closures or trait-impl wrappers)
//! via `App::register_read` / `App::register_write`. The registry erases
//! the static types and holds them by name.

use crate::{
    audit::{CacheResult, Caller},
    ComputeContext, Inputs, Row, SharedState, TableFunctionSchema, WriteContext,
};
use async_trait::async_trait;
use std::sync::Arc;

/// What the registry dispatches a Read request to. Already wraps the
/// concrete `ReadTableFunction` impl so the registry only stores
/// trait objects.
#[async_trait]
pub trait ReadHandler: Send + Sync {
    fn schema(&self) -> TableFunctionSchema;

    fn compute_key(&self, inputs: &Inputs) -> pg_relay_core::Result<pg_relay_core::ComputeKey>;

    async fn compute(
        &self,
        inputs: &Inputs,
        ctx: &ComputeContext<'_>,
    ) -> pg_relay_core::Result<SharedState>;

    fn project(
        &self,
        inputs: &Inputs,
        state: &SharedState,
    ) -> pg_relay_core::Result<Vec<Row>>;
}

#[async_trait]
pub trait WriteHandler: Send + Sync {
    fn schema(&self) -> TableFunctionSchema;

    fn lock_key(&self, inputs: &Inputs) -> pg_relay_core::Result<pg_relay_core::LockKey>;

    fn idempotency_key(&self, inputs: &Inputs) -> Option<String>;

    async fn execute(
        &self,
        inputs: &Inputs,
        ctx: &mut WriteContext<'_>,
    ) -> pg_relay_core::Result<Row>;
}

pub struct Registry {
    reads: dashmap::DashMap<String, Arc<dyn ReadHandler>>,
    writes: dashmap::DashMap<String, Arc<dyn WriteHandler>>,
}

impl Default for Registry {
    fn default() -> Self {
        Registry::new()
    }
}

impl Registry {
    pub fn new() -> Self {
        Registry {
            reads: dashmap::DashMap::new(),
            writes: dashmap::DashMap::new(),
        }
    }

    pub fn register_read(&self, handler: Arc<dyn ReadHandler>) {
        let name = handler.schema().name.clone();
        self.reads.insert(name, handler);
    }

    pub fn register_write(&self, handler: Arc<dyn WriteHandler>) {
        let name = handler.schema().name.clone();
        self.writes.insert(name, handler);
    }

    pub fn get_read(&self, name: &str) -> Option<Arc<dyn ReadHandler>> {
        self.reads.get(name).map(|h| h.clone())
    }

    pub fn get_write(&self, name: &str) -> Option<Arc<dyn WriteHandler>> {
        self.writes.get(name).map(|h| h.clone())
    }

    pub fn list_schemas(&self) -> Vec<TableFunctionSchema> {
        let mut out: Vec<TableFunctionSchema> = self
            .reads
            .iter()
            .map(|e| e.value().schema())
            .chain(self.writes.iter().map(|e| e.value().schema()))
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }
}

// ─── Unused for now but useful for the audit pipeline downstream ──
#[allow(dead_code)]
pub(crate) struct CallContext {
    pub caller: Caller,
    pub trace_id: String,
    pub cache_outcome: CacheResult,
}
