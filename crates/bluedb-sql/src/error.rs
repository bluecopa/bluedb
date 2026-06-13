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
}

impl From<slatedb::Error> for SqlError {
    fn from(err: slatedb::Error) -> Self {
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
