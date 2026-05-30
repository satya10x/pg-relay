//! Local filesystem `Storage` implementation. Uses sidecar `.etag`
//! files to emulate per-object ETags. Good enough for dev,
//! integration tests against MinIO are still recommended.

use async_trait::async_trait;
use bytes::Bytes;
use pg_relay_core::storage::{ObjectMeta, Storage, StorageError, StorageResult};
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use uuid::Uuid;

pub struct LocalFsStorage {
    root: PathBuf,
    /// Serializes all writes for the whole storage. Coarse but
    /// correct; v1 doesn't try to scale local FS storage.
    write_lock: Mutex<()>,
}

impl LocalFsStorage {
    pub async fn open(root: impl Into<PathBuf>) -> StorageResult<Self> {
        let root = root.into();
        tokio::fs::create_dir_all(&root)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        Ok(LocalFsStorage {
            root,
            write_lock: Mutex::new(()),
        })
    }

    fn path_for(&self, key: &str) -> PathBuf {
        self.root.join(key)
    }

    fn etag_path_for(&self, key: &str) -> PathBuf {
        self.root.join(format!("{key}.etag"))
    }

    async fn ensure_parent(path: &Path) -> StorageResult<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;
        }
        Ok(())
    }

    async fn read_etag(&self, key: &str) -> StorageResult<String> {
        tokio::fs::read_to_string(self.etag_path_for(key))
            .await
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => StorageError::NotFound(key.to_string()),
                _ => StorageError::Backend(e.to_string()),
            })
    }

    async fn write_object(&self, key: &str, data: Bytes) -> StorageResult<ObjectMeta> {
        let path = self.path_for(key);
        Self::ensure_parent(&path).await?;

        // Write atomically via tmp + rename.
        let tmp = path.with_extension(format!("tmp-{}", Uuid::new_v4()));
        tokio::fs::write(&tmp, &data)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        tokio::fs::rename(&tmp, &path)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        let etag = Uuid::new_v4().to_string();
        tokio::fs::write(self.etag_path_for(key), &etag)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        let meta = tokio::fs::metadata(&path)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;

        Ok(ObjectMeta {
            key: key.to_string(),
            size: meta.len(),
            etag,
            last_modified: chrono::Utc::now(),
        })
    }
}

#[async_trait]
impl Storage for LocalFsStorage {
    async fn get(&self, key: &str) -> StorageResult<Bytes> {
        match tokio::fs::read(self.path_for(key)).await {
            Ok(bytes) => Ok(Bytes::from(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(StorageError::NotFound(key.to_string()))
            }
            Err(e) => Err(StorageError::Backend(e.to_string())),
        }
    }

    async fn get_with_etag(&self, key: &str) -> StorageResult<(Bytes, String)> {
        let data = self.get(key).await?;
        let etag = self.read_etag(key).await?;
        Ok((data, etag))
    }

    async fn put(&self, key: &str, data: Bytes) -> StorageResult<ObjectMeta> {
        let _guard = self.write_lock.lock().await;
        self.write_object(key, data).await
    }

    async fn put_if_match(
        &self,
        key: &str,
        data: Bytes,
        if_match: &str,
    ) -> StorageResult<ObjectMeta> {
        let _guard = self.write_lock.lock().await;
        let current = self.read_etag(key).await?;
        if current != if_match {
            return Err(StorageError::EtagMismatch {
                key: key.to_string(),
                expected: if_match.to_string(),
                got: current,
            });
        }
        self.write_object(key, data).await
    }

    async fn put_if_absent(&self, key: &str, data: Bytes) -> StorageResult<ObjectMeta> {
        let _guard = self.write_lock.lock().await;
        if self.path_for(key).exists() {
            return Err(StorageError::EtagMismatch {
                key: key.to_string(),
                expected: "(absent)".to_string(),
                got: "(present)".to_string(),
            });
        }
        self.write_object(key, data).await
    }

    async fn list(&self, prefix: &str) -> StorageResult<Vec<ObjectMeta>> {
        let prefix_path = self.root.join(prefix);
        let mut out = Vec::new();

        let scan_root = if prefix_path.is_dir() {
            prefix_path
        } else {
            self.root.clone()
        };

        let mut stack = vec![scan_root];
        while let Some(dir) = stack.pop() {
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
                Err(e) => return Err(StorageError::Backend(e.to_string())),
            };

            while let Some(entry) =
                entries.next_entry().await.map_err(|e| StorageError::Backend(e.to_string()))?
            {
                let path = entry.path();
                let file_type =
                    entry.file_type().await.map_err(|e| StorageError::Backend(e.to_string()))?;

                if file_type.is_dir() {
                    stack.push(path);
                    continue;
                }

                // Ignore the .etag sidecars themselves.
                if path.extension().and_then(|x| x.to_str()) == Some("etag") {
                    continue;
                }

                let rel = path
                    .strip_prefix(&self.root)
                    .map_err(|e| StorageError::Backend(e.to_string()))?
                    .to_string_lossy()
                    .replace('\\', "/");

                if !rel.starts_with(prefix) {
                    continue;
                }

                let etag = self.read_etag(&rel).await.unwrap_or_default();
                let meta =
                    entry.metadata().await.map_err(|e| StorageError::Backend(e.to_string()))?;
                out.push(ObjectMeta {
                    key: rel,
                    size: meta.len(),
                    etag,
                    last_modified: chrono::Utc::now(),
                });
            }
        }

        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    async fn delete(&self, key: &str) -> StorageResult<()> {
        let _guard = self.write_lock.lock().await;
        let _ = tokio::fs::remove_file(self.path_for(key)).await;
        let _ = tokio::fs::remove_file(self.etag_path_for(key)).await;
        Ok(())
    }
}
