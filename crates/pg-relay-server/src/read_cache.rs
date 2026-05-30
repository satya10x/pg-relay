//! Per-key compute cache with read coalescing.
//!
//! The interesting property: N concurrent callers asking for the
//! same `ComputeKey` trigger exactly ONE underlying compute. The
//! first arrival populates an in-flight slot; subsequent arrivals
//! await it. After completion the result is cached for future
//! callers within the configured TTL/size bounds.
//!
//! Implemented with `tokio::sync::OnceCell` per-key, held in a
//! `DashMap<ComputeKey, Arc<Slot>>`. Eviction is naive in v1
//! (no TTL or size limit) — see ROADMAP.

use crate::SharedState;
use dashmap::DashMap;
use pg_relay_core::{ComputeKey, Error, Result};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// One cache slot per ComputeKey. Holds either an in-flight
/// computation or the materialized result.
struct Slot {
    cell: OnceCell<std::result::Result<SharedState, String>>,
}

impl Slot {
    fn new() -> Self {
        Slot {
            cell: OnceCell::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheOutcome {
    /// First caller; we ran the compute.
    Miss,
    /// Cell already complete; we read from cache.
    Hit,
    /// Cell was in-flight; we waited on someone else's compute.
    Coalesced,
}

pub struct ReadCache {
    slots: DashMap<ComputeKey, Arc<Slot>>,
}

impl Default for ReadCache {
    fn default() -> Self {
        ReadCache::new()
    }
}

impl ReadCache {
    pub fn new() -> Self {
        ReadCache {
            slots: DashMap::new(),
        }
    }

    /// Get-or-compute. `f` is invoked at most once per `key` across
    /// concurrent callers. The cache outcome (`Hit`/`Miss`/`Coalesced`)
    /// is returned alongside the value so the audit log can record it.
    pub async fn get_or_compute<F, Fut>(
        &self,
        key: ComputeKey,
        f: F,
    ) -> Result<(SharedState, CacheOutcome)>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<SharedState>>,
    {
        // Get or insert the slot. Cheap — no lock on the cell yet.
        let slot = self
            .slots
            .entry(key.clone())
            .or_insert_with(|| Arc::new(Slot::new()))
            .clone();

        // Detect whether the cell was already complete before we touch it.
        let was_initialized = slot.cell.initialized();

        // get_or_init runs `f` at most once across all concurrent callers.
        // We wrap our Result<_> in another Result<_, String> for OnceCell's
        // FnOnce-but-not-Try interface; the inner error is recreated.
        let outcome_marker = std::sync::atomic::AtomicBool::new(false);
        let result = slot
            .cell
            .get_or_init(|| async {
                outcome_marker.store(true, std::sync::atomic::Ordering::SeqCst);
                match f().await {
                    Ok(v) => Ok(v),
                    Err(e) => Err(e.to_string()),
                }
            })
            .await
            .clone();

        let we_ran_compute = outcome_marker.load(std::sync::atomic::Ordering::SeqCst);

        let outcome = if was_initialized {
            CacheOutcome::Hit
        } else if we_ran_compute {
            CacheOutcome::Miss
        } else {
            CacheOutcome::Coalesced
        };

        // If the cached computation failed, surface it as a fresh error.
        // Don't keep failed computations cached — invalidate so the next
        // caller can retry.
        match result {
            Ok(state) => Ok((state, outcome)),
            Err(msg) => {
                self.slots.remove(&key);
                Err(Error::Compute(msg))
            }
        }
    }

    /// Evict a single key. Used for explicit invalidation (e.g.,
    /// after a write commits new state for that key).
    pub fn invalidate(&self, key: &ComputeKey) {
        self.slots.remove(key);
    }

    /// Number of currently-cached entries (live + in-flight).
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn coalesces_concurrent_calls() {
        let cache = Arc::new(ReadCache::new());
        let counter = Arc::new(AtomicU32::new(0));
        let key = ComputeKey::from_parts([b"client_42"]);

        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache = cache.clone();
            let counter = counter.clone();
            let key = key.clone();
            handles.push(tokio::spawn(async move {
                cache
                    .get_or_compute(key, || async move {
                        // Simulate slow compute so concurrent callers actually overlap.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        counter.fetch_add(1, Ordering::SeqCst);
                        let state: SharedState = Arc::new(42u32);
                        Ok(state)
                    })
                    .await
                    .unwrap()
            }));
        }

        let mut hits = 0;
        let mut misses = 0;
        let mut coalesced = 0;
        for h in handles {
            let (_, outcome) = h.await.unwrap();
            match outcome {
                CacheOutcome::Hit => hits += 1,
                CacheOutcome::Miss => misses += 1,
                CacheOutcome::Coalesced => coalesced += 1,
            }
        }

        // Exactly one compute should have happened.
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        // Exactly one Miss across the 10 callers.
        assert_eq!(misses, 1);
        // The remaining 9 are either Coalesced (raced in) or Hit (arrived
        // after completion). Both are correct outcomes.
        assert_eq!(hits + coalesced, 9);
    }

    #[tokio::test]
    async fn failed_compute_does_not_stick() {
        let cache = Arc::new(ReadCache::new());
        let key = ComputeKey::from_parts([b"client_99"]);

        // First call: compute fails.
        let r1 = cache
            .get_or_compute(key.clone(), || async {
                Err::<SharedState, _>(Error::Compute("boom".to_string()))
            })
            .await;
        assert!(r1.is_err());

        // Second call: compute can succeed because the failure was evicted.
        let r2 = cache
            .get_or_compute(key.clone(), || async {
                Ok(Arc::new("ok".to_string()) as SharedState)
            })
            .await;
        assert!(r2.is_ok());
    }
}
