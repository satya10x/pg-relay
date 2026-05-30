//! In-memory `Storage` implementation. For tests, examples, and
//! development. Behavior is identical to a real backend for the
//! manifest commit protocol: per-object atomic PUT, conditional
//! PUT via ETag.

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use pg_relay_core::storage::{ObjectMeta, Storage, StorageError, StorageResult};
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

struct Object {
    data: Bytes,
    etag: String,
    last_modified: chrono::DateTime<chrono::Utc>,
}

pub struct MemoryStorage {
    objects: DashMap<String, Object>,
    writes: AtomicU64,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        MemoryStorage::new()
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        MemoryStorage {
            objects: DashMap::new(),
            writes: AtomicU64::new(0),
        }
    }

    pub fn writes_count(&self) -> u64 {
        self.writes.load(Ordering::SeqCst)
    }

    fn fresh_etag(&self) -> String {
        Uuid::new_v4().to_string()
    }

    fn meta_of(&self, key: &str, obj: &Object) -> ObjectMeta {
        ObjectMeta {
            key: key.to_string(),
            size: obj.data.len() as u64,
            etag: obj.etag.clone(),
            last_modified: obj.last_modified,
        }
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn get(&self, key: &str) -> StorageResult<Bytes> {
        self.objects
            .get(key)
            .map(|o| o.data.clone())
            .ok_or_else(|| StorageError::NotFound(key.to_string()))
    }

    async fn get_with_etag(&self, key: &str) -> StorageResult<(Bytes, String)> {
        self.objects
            .get(key)
            .map(|o| (o.data.clone(), o.etag.clone()))
            .ok_or_else(|| StorageError::NotFound(key.to_string()))
    }

    async fn put(&self, key: &str, data: Bytes) -> StorageResult<ObjectMeta> {
        let etag = self.fresh_etag();
        let obj = Object {
            data,
            etag: etag.clone(),
            last_modified: chrono::Utc::now(),
        };
        let meta = self.meta_of(key, &obj);
        self.objects.insert(key.to_string(), obj);
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(meta)
    }

    async fn put_if_match(
        &self,
        key: &str,
        data: Bytes,
        if_match: &str,
    ) -> StorageResult<ObjectMeta> {
        // Hold the per-key slot under a single critical section to avoid
        // a torn read-then-write race. DashMap entry gives us that.
        let mut entry = self
            .objects
            .entry(key.to_string())
            .or_insert_with(|| Object {
                data: Bytes::new(),
                etag: String::new(),
                last_modified: chrono::Utc::now(),
            });

        if entry.etag != if_match {
            return Err(StorageError::EtagMismatch {
                key: key.to_string(),
                expected: if_match.to_string(),
                got: entry.etag.clone(),
            });
        }

        let etag = self.fresh_etag();
        entry.data = data;
        entry.etag = etag.clone();
        entry.last_modified = chrono::Utc::now();
        let meta = self.meta_of(key, &entry);
        self.writes.fetch_add(1, Ordering::SeqCst);
        Ok(meta)
    }

    async fn put_if_absent(&self, key: &str, data: Bytes) -> StorageResult<ObjectMeta> {
        if self.objects.contains_key(key) {
            return Err(StorageError::EtagMismatch {
                key: key.to_string(),
                expected: "(absent)".to_string(),
                got: "(present)".to_string(),
            });
        }
        self.put(key, data).await
    }

    async fn list(&self, prefix: &str) -> StorageResult<Vec<ObjectMeta>> {
        let mut out = Vec::new();
        for entry in self.objects.iter() {
            if entry.key().starts_with(prefix) {
                out.push(self.meta_of(entry.key(), entry.value()));
            }
        }
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        self.objects.remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn put_if_match_round_trip() {
        let s = MemoryStorage::new();

        let m1 = s.put("k", Bytes::from_static(b"v1")).await.unwrap();
        let m2 = s
            .put_if_match("k", Bytes::from_static(b"v2"), &m1.etag)
            .await
            .unwrap();
        assert_ne!(m1.etag, m2.etag);

        // Stale etag → mismatch error.
        let r = s
            .put_if_match("k", Bytes::from_static(b"v3"), &m1.etag)
            .await;
        assert!(matches!(r, Err(StorageError::EtagMismatch { .. })));
    }

    #[tokio::test]
    async fn list_by_prefix() {
        let s = MemoryStorage::new();
        s.put("foo/a", Bytes::from_static(b"1")).await.unwrap();
        s.put("foo/b", Bytes::from_static(b"2")).await.unwrap();
        s.put("bar/c", Bytes::from_static(b"3")).await.unwrap();

        let foos = s.list("foo/").await.unwrap();
        assert_eq!(foos.len(), 2);
        assert_eq!(foos[0].key, "foo/a");
        assert_eq!(foos[1].key, "foo/b");
    }
}
