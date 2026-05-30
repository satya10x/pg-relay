use crate::{ComputeKey, Error, Inputs, LockKey, Result, Row, TableFunctionSchema};
use async_trait::async_trait;
use std::any::Any;
use std::sync::Arc;

/// A type-erased handle to cached compute state. The framework stores
/// this; user code downcasts to its concrete `State` type via
/// `downcast_state` in `project()`.
pub type SharedState = Arc<dyn Any + Send + Sync>;

/// Common metadata for both read and write table functions.
pub trait TableFunction: Send + Sync + 'static {
    fn schema() -> TableFunctionSchema;
}

/// Context passed to `compute()`. Holds references to storage and
/// other framework facilities a compute call might need.
pub struct ComputeContext<'a> {
    pub storage: &'a dyn crate::storage::Storage,
    pub trace_id: &'a str,
}

/// Context passed to `execute()`. Same as ComputeContext plus
/// the write-specific bits.
pub struct WriteContext<'a> {
    pub storage: &'a dyn crate::storage::Storage,
    pub trace_id: &'a str,
    pub request_id: &'a str,
}

/// A read table function. STABLE semantics; concurrent calls with
/// the same `compute_key` coalesce through a OnceCell in the framework.
#[async_trait]
pub trait ReadTableFunction: TableFunction {
    /// Identifier the framework uses to key the compute cache.
    /// Two different table functions sharing the same key share the
    /// underlying compute (e.g., holdings_latest + holdings_series).
    fn compute_key(inputs: &Inputs) -> Result<ComputeKey>;

    /// Cache miss path. The framework guarantees this is called at
    /// most once per unique `compute_key` across concurrent readers;
    /// other callers wait on the result.
    async fn compute(inputs: &Inputs, ctx: &ComputeContext<'_>) -> Result<SharedState>;

    /// Project rows from the (possibly cached) state for this call's
    /// specific inputs. Must be pure and fast.
    fn project(inputs: &Inputs, state: &SharedState) -> Result<Vec<Row>>;
}

/// Helper for the common `state.downcast_ref::<S>()` pattern.
pub fn downcast_state<S: 'static>(state: &SharedState) -> Result<&S> {
    state
        .downcast_ref::<S>()
        .ok_or_else(|| Error::Compute("cached state type mismatch".to_string()))
}

/// A write table function. VOLATILE semantics; serialized per
/// `lock_key`, idempotent by `idempotency_key`.
#[async_trait]
pub trait WriteTableFunction: TableFunction {
    /// Per-shard write serialization key. Two writes with the same
    /// LockKey try-lock; the second to arrive fails fast.
    fn lock_key(inputs: &Inputs) -> Result<LockKey>;

    /// Idempotency key. If a prior call with the same key succeeded,
    /// the framework returns its cached outcome without re-executing.
    /// Return None to disable idempotency for this call.
    fn idempotency_key(inputs: &Inputs) -> Option<String>;

    /// The actual write. The framework wraps this in lock acquisition,
    /// idempotency lookup, manifest commit, and outcome caching.
    async fn execute(inputs: &Inputs, ctx: &mut WriteContext<'_>) -> Result<Row>;
}
