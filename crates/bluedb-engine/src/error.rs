//! Engine error type — folds the pillars' error types into one.

use thiserror::Error;

/// Errors surfaced by the engine facade.
#[derive(Debug, Error)]
pub enum EngineError {
    /// A `bluedb-rest` DSL request failed to translate to SQL (bad identifier,
    /// malformed value, unfiltered mutation, ...).
    #[error("rest translation: {0}")]
    Rest(#[from] bluedb_rest::RestError),

    /// The SQL engine rejected or failed a statement.
    #[error("sql: {0}")]
    Sql(#[from] gluesql_core::error::Error),

    /// A statement was rejected by a restricted surface (DDL/multi-statement on `/sql`).
    #[error("statement not allowed on this surface: {0}")]
    Rejected(String),

    /// Anything from the storage / FTS layers (blob I/O, split open, compaction,
    /// manifest (de)serialization, ...), carried as `anyhow`.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Engine result alias.
pub type Result<T> = std::result::Result<T, EngineError>;
