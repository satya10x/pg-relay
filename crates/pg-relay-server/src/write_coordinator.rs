//! Per-key write serialization plus idempotency.
//!
//! Two-step protection:
//!
//!   1. Look up `request_id` in the idempotency cache. Hit ⇒ return
//!      the prior outcome without executing.
//!   2. `try_lock` the per-key mutex. Held ⇒ fail-fast with a
//!      WriteConflict error.
//!
//! If both pass, execute the user's write closure, store the result
//! under `request_id`, release the lock.

use crate::Row;
use dashmap::DashMap;
use pg_relay_core::{Error, LockKey, Result};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;

/// The cached outcome of a previously-successful write call.
#[derive(Clone)]
struct IdempotentRecord {
    row: Row,
    #[allow(dead_code)] // used for TTL eviction once we add bounded retention
    inserted_at: chrono::DateTime<chrono::Utc>,
}

pub struct WriteCoordinator {
    locks: DashMap<LockKey, Arc<Mutex<()>>>,
    idempotency: DashMap<String, IdempotentRecord>,
}

impl Default for WriteCoordinator {
    fn default() -> Self {
        WriteCoordinator::new()
    }
}

pub struct WriteResult {
    pub row: Row,
    pub was_idempotent_noop: bool,
}

impl WriteCoordinator {
    pub fn new() -> Self {
        WriteCoordinator {
            locks: DashMap::new(),
            idempotency: DashMap::new(),
        }
    }

    /// Run `f` under the per-key lock with idempotency protection.
    ///
    /// - If `request_id` was successfully processed before, returns
    ///   the prior outcome without invoking `f`.
    /// - If the per-key lock is held, returns `Error::WriteConflict`
    ///   immediately.
    /// - Otherwise: acquires lock, runs `f`, caches outcome, releases.
    pub async fn execute<F, Fut>(
        &self,
        lock_key: LockKey,
        request_id: Option<&str>,
        f: F,
    ) -> Result<WriteResult>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Row>>,
    {
        // Step 1: idempotency check (cheap, no lock).
        if let Some(rid) = request_id {
            if let Some(record) = self.idempotency.get(rid) {
                return Ok(WriteResult {
                    row: record.row.clone(),
                    was_idempotent_noop: true,
                });
            }
        }

        // Step 2: try_lock for fail-fast on concurrent writes.
        let lock = self
            .locks
            .entry(lock_key.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();

        let _guard = lock
            .try_lock()
            .map_err(|_| Error::WriteConflict(lock_key.to_string()))?;

        // Step 3: double-check idempotency under the lock — someone else
        // may have committed our request_id while we were entering.
        if let Some(rid) = request_id {
            if let Some(record) = self.idempotency.get(rid) {
                return Ok(WriteResult {
                    row: record.row.clone(),
                    was_idempotent_noop: true,
                });
            }
        }

        // Step 4: actually do the write.
        let row = f().await?;

        // Step 5: cache the outcome for future retries.
        if let Some(rid) = request_id {
            self.idempotency.insert(
                rid.to_string(),
                IdempotentRecord {
                    row: row.clone(),
                    inserted_at: chrono::Utc::now(),
                },
            );
        }

        Ok(WriteResult {
            row,
            was_idempotent_noop: false,
        })
    }

    pub fn idempotency_len(&self) -> usize {
        self.idempotency.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pg_relay_core::Column;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    fn ok_row() -> Row {
        Row::new().push(Column::Text("committed".to_string()))
    }

    #[tokio::test]
    async fn idempotency_returns_prior_result() {
        let coord = Arc::new(WriteCoordinator::new());
        let lk = LockKey::from("client_1");

        let r1 = coord
            .execute(lk.clone(), Some("req_A"), || async { Ok(ok_row()) })
            .await
            .unwrap();
        assert!(!r1.was_idempotent_noop);

        let r2 = coord
            .execute(lk.clone(), Some("req_A"), || async {
                // This must not run.
                panic!("idempotent path should not invoke f");
            })
            .await
            .unwrap();
        assert!(r2.was_idempotent_noop);
    }

    #[tokio::test]
    async fn concurrent_writes_fail_fast() {
        let coord = Arc::new(WriteCoordinator::new());
        let lk = LockKey::from("client_2");
        let counter = Arc::new(AtomicU32::new(0));

        // Hold the lock with a slow first writer.
        let coord_a = coord.clone();
        let lk_a = lk.clone();
        let counter_a = counter.clone();
        let slow = tokio::spawn(async move {
            coord_a
                .execute(lk_a, Some("req_slow"), move || async move {
                    counter_a.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok(ok_row())
                })
                .await
        });

        // Give the slow writer a moment to take the lock.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Second writer for the same lock_key should fail fast.
        let r = coord
            .execute(lk.clone(), Some("req_fast"), || async {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(ok_row())
            })
            .await;

        assert!(matches!(r, Err(Error::WriteConflict(_))));

        // Wait for slow to finish; verify only slow's closure ran.
        slow.await.unwrap().unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn different_keys_dont_conflict() {
        let coord = Arc::new(WriteCoordinator::new());

        let r1 = coord
            .execute(LockKey::from("a"), Some("req_1"), || async { Ok(ok_row()) })
            .await
            .unwrap();
        let r2 = coord
            .execute(LockKey::from("b"), Some("req_2"), || async { Ok(ok_row()) })
            .await
            .unwrap();

        assert!(!r1.was_idempotent_noop);
        assert!(!r2.was_idempotent_noop);
    }
}
