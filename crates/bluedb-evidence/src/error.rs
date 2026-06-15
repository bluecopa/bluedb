use thiserror::Error;

#[derive(Debug, Error)]
pub enum EvidenceError {
    #[error("idempotency key reused with a different payload")]
    IdemConflict,
    #[error("chain '{0}' already exists with a different mode")]
    ChainModeConflict(String),
    #[error("entry {seq} not found in chain '{chain}'")]
    EntryNotFound { chain: String, seq: i64 },
    #[error("node is a read-only replica (no writer)")]
    NotWriter,
    #[error(transparent)]
    Storage(#[from] anyhow::Error),
}
