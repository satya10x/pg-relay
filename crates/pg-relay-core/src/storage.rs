use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub last_modified: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("etag mismatch on {key}: expected {expected}, got {got}")]
    EtagMismatch {
        key: String,
        expected: String,
        got: String,
    },

    #[error("backend error: {0}")]
    Backend(String),
}

pub type StorageResult<T> = Result<T, StorageError>;

/// Object-storage-shaped interface. Implementations: in-memory,
/// local FS, S3, GCS, Azure. All built on the same atomicity
/// primitives: per-object atomic PUT, conditional PUT via ETag.
#[async_trait]
pub trait Storage: Send + Sync {
    async fn get(&self, key: &str) -> StorageResult<Bytes>;

    async fn get_with_etag(&self, key: &str) -> StorageResult<(Bytes, String)>;

    async fn put(&self, key: &str, data: Bytes) -> StorageResult<ObjectMeta>;

    /// Conditional PUT: succeeds only if the current ETag of `key`
    /// matches `if_match`. Used as the atomic commit point in the
    /// manifest protocol.
    async fn put_if_match(
        &self,
        key: &str,
        data: Bytes,
        if_match: &str,
    ) -> StorageResult<ObjectMeta>;

    /// Conditional PUT that succeeds only if `key` does not yet exist.
    async fn put_if_absent(&self, key: &str, data: Bytes) -> StorageResult<ObjectMeta>;

    async fn list(&self, prefix: &str) -> StorageResult<Vec<ObjectMeta>>;

    async fn delete(&self, key: &str) -> StorageResult<()>;
}

/// The committed-state document. A single object PUT switches the
/// live pointer atomically.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u32,
    pub committed_at: chrono::DateTime<chrono::Utc>,
    pub request_id: String,
    pub data_objects: Vec<String>,
    pub metadata: serde_json::Value,
}

impl Manifest {
    pub const CURRENT_VERSION: u32 = 1;
}
