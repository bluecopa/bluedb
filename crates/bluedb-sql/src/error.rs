//! Error type for the bluedb SQL store.
//!
//! GlueSQL's `Store`/`StoreMut` methods return `gluesql_core::result::Result`,
//! whose storage-layer escape hatch is the [`Error::StorageMsg`] string
//! variant. Internally we use the richer [`SqlError`] below and convert it to
//! `StorageMsg` at the trait boundary so the cause survives into GlueSQL's
//! error output.

use thiserror::Error;

/// Errors raised by the SlateDB-backed GlueSQL store.
#[derive(Debug, Error)]
pub enum SqlError {
    /// A SlateDB operation (`get`/`put`/`delete`/`scan`) failed.
    #[error("slatedb error: {0}")]
    SlateDb(String),

    /// Serializing/deserializing a `Schema` or `DataRow` value failed.
    #[error("serde error: {0}")]
    Serde(String),

    /// Encoding a primary [`gluesql_core::data::Key`] to comparable bytes failed.
    #[error("key encode error: {0}")]
    KeyEncode(String),

    /// Evaluating a secondary-index expression against a row failed, or its
    /// result could not be turned into an indexable [`gluesql_core::data::Key`]
    /// (e.g. the expression produced a map/list/point value).
    #[error("index eval error: {0}")]
    IndexEval(String),

    /// A schema-registry write-time validation check failed (column count,
    /// type, or NOT NULL mismatch). See [`crate::SchemaRegistry`].
    #[error("schema validation error: {0}")]
    SchemaValidation(String),

    /// A composite-primary-key rewrite failed: a malformed `PRIMARY KEY(a,b)`
    /// DDL, a reserved-name collision, a NULL/non-literal PK component on insert,
    /// or an unsupported statement against a composite-PK table. See
    /// [`crate::compositepk`].
    #[error("composite primary key error: {0}")]
    CompositePk(String),

    /// A concurrent connection committed a row with the same primary key after
    /// this transaction checked it was free — detected at commit while holding
    /// the write lease (first committer wins; the loser aborts). See
    /// [`crate::storage::SlateDbStorage`]'s commit-time uniqueness validation.
    #[error("unique constraint violation: a row with key {0} already exists")]
    UniqueViolation(String),
}

impl From<slatedb::Error> for SqlError {
    fn from(err: slatedb::Error) -> Self {
        SqlError::SlateDb(err.to_string())
    }
}

impl From<anyhow::Error> for SqlError {
    // Substrate read/lifecycle ops (and the "this node is a read-only replica"
    // guard) surface as `anyhow`; carry the message through.
    fn from(err: anyhow::Error) -> Self {
        SqlError::SlateDb(err.to_string())
    }
}

impl From<serde_json::Error> for SqlError {
    fn from(err: serde_json::Error) -> Self {
        SqlError::Serde(err.to_string())
    }
}

impl From<SqlError> for gluesql_core::error::Error {
    fn from(err: SqlError) -> Self {
        gluesql_core::error::Error::StorageMsg(err.to_string())
    }
}
