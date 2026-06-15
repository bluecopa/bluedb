//! bluedb lakehouse — Iceberg CDC mirror.
//!
//! Mirrors bluedb tables into object storage as Apache Iceberg tables (full CRUD via
//! equality deletes), driven by the SQL commit tap's durable CDC log. The Iceberg commit is
//! **self-authored** (manifests + snapshot + `metadata.json`) and published through bluedb's
//! own catalog — see `docs/superpowers/specs/2026-06-15-bluedb-lakehouse-iceberg-mirror-design.md` §5.2.
pub mod catalog;
pub mod cdc;
pub mod compaction;
pub mod engine;
pub mod objstore_io;
pub mod schema;
pub mod writer;

pub use objstore_io::object_store_file_io;

/// Errors raised by the lakehouse mirror.
#[derive(Debug, thiserror::Error)]
pub enum LakehouseError {
    /// An Iceberg-layer failure (manifest/snapshot/metadata authoring or read-back).
    #[error("iceberg: {0}")]
    Iceberg(String),
    /// A schema/type-mapping failure (gluesql → Iceberg).
    #[error("schema: {0}")]
    Schema(String),
    /// A bluedb-sql failure (CDC log scan, substrate I/O).
    #[error("sql: {0}")]
    Sql(#[from] bluedb_sql::SqlError),
    /// Any other error.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<iceberg::Error> for LakehouseError {
    fn from(err: iceberg::Error) -> Self {
        LakehouseError::Iceberg(err.to_string())
    }
}

impl From<serde_json::Error> for LakehouseError {
    fn from(err: serde_json::Error) -> Self {
        LakehouseError::Iceberg(format!("metadata json: {err}"))
    }
}

/// Convenience result type for the crate.
pub type Result<T> = std::result::Result<T, LakehouseError>;

pub use engine::LakehouseEngine;
