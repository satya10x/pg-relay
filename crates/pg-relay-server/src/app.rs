//! Builder for assembling a pg_relay daemon.
//!
//! Wire up storage, audit, and table function handlers, then `serve_unix`.

use crate::audit_stdout::StdoutJsonLog;
use crate::ipc::IpcServer;
use crate::registry::{ReadHandler, Registry, WriteHandler};
use pg_relay_core::audit::AuditLog;
use pg_relay_core::storage::Storage;
use std::path::PathBuf;
use std::sync::Arc;

pub struct App {
    registry: Arc<Registry>,
    storage: Option<Arc<dyn Storage>>,
    audit: Option<Arc<dyn AuditLog>>,
}

impl Default for App {
    fn default() -> Self {
        App::new()
    }
}

impl App {
    pub fn new() -> Self {
        App {
            registry: Arc::new(Registry::new()),
            storage: None,
            audit: None,
        }
    }

    pub fn register_read(&self, handler: Arc<dyn ReadHandler>) -> &Self {
        self.registry.register_read(handler);
        self
    }

    pub fn register_write(&self, handler: Arc<dyn WriteHandler>) -> &Self {
        self.registry.register_write(handler);
        self
    }

    pub fn storage(mut self, s: Arc<dyn Storage>) -> Self {
        self.storage = Some(s);
        self
    }

    pub fn audit(mut self, a: Arc<dyn AuditLog>) -> Self {
        self.audit = Some(a);
        self
    }

    pub fn registry(&self) -> Arc<Registry> {
        self.registry.clone()
    }

    /// Serve on a Unix domain socket. Blocks; spawn it under tokio.
    pub async fn serve_unix(self, socket_path: impl Into<PathBuf>) -> anyhow::Result<()> {
        let storage = self
            .storage
            .ok_or_else(|| anyhow::anyhow!("App::serve_unix requires storage()"))?;
        let audit = self
            .audit
            .unwrap_or_else(|| Arc::new(StdoutJsonLog::new()));

        let server = Arc::new(IpcServer::new(self.registry, storage, Some(audit)));
        server.serve_unix(socket_path.into()).await
    }
}
