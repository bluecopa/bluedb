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
    #[error("chain '{0}' is not verified; Merkle proofs are unavailable")]
    NotVerified(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error(transparent)]
    Storage(#[from] anyhow::Error),
}
