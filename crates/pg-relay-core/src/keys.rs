use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::{Hash, Hasher};

/// A cache key for a computation. Multiple table functions can share
/// the same ComputeKey to indicate "we'd reuse each other's compute."
///
/// Internally a stable byte representation so the hash is consistent
/// across processes and serde round-trips.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputeKey(pub Vec<u8>);

impl ComputeKey {
    pub fn from_parts<I, S>(parts: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<[u8]>,
    {
        let mut buf = Vec::new();
        for part in parts {
            let bytes = part.as_ref();
            buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(bytes);
        }
        ComputeKey(buf)
    }
}

impl Hash for ComputeKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl fmt::Display for ComputeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Hex display, capped — keys can be long.
        for byte in self.0.iter().take(32) {
            write!(f, "{:02x}", byte)?;
        }
        if self.0.len() > 32 {
            write!(f, "…")?;
        }
        Ok(())
    }
}

/// A shard key for write serialization. Two writes with the same
/// LockKey will serialize through the daemon's per-key mutex.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LockKey(pub String);

impl LockKey {
    pub fn from<S: Into<String>>(s: S) -> Self {
        LockKey(s.into())
    }
}

impl fmt::Display for LockKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
