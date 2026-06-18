//! `bluedb-sql` — SQL on the object-storage substrate.
//!
//! This crate is bluedb's **SQL pillar**: it implements [GlueSQL]'s pluggable
//! custom-storage traits over [SlateDB] (an LSM key-value store on object
//! storage), so the platform gets a full SQL engine — `CREATE TABLE`,
//! `INSERT`/`UPDATE`/`DELETE`, `SELECT ... WHERE ... ORDER BY` — directly on
//! the same substrate as the blob ([`bluedb-storage`]) and full-text
//! ([`bluedb-fts`]) crates, with no DDL/migration tax (GlueSQL supports
//! schemaless tables).
//!
//! # How it fits together
//!
//! [`SlateDbStorage`] wraps an `Arc<slatedb::Db>` and implements GlueSQL's
//! [`Store`](gluesql_core::store::Store) (read path) and
//! [`StoreMut`](gluesql_core::store::StoreMut) (write path), plus real
//! [`Transaction`](gluesql_core::store::Transaction) (overlay + atomic batch
//! commit, snapshot isolation) and [`Index`](gluesql_core::store::Index)/
//! [`IndexMut`](gluesql_core::store::IndexMut) (secondary indexes). The
//! remaining `GStore`/`GStoreMut` traits — `Metadata`, `CustomFunction(Mut)`,
//! `AlterTable`, `Planner` — are satisfied by gluesql-core's default method
//! implementations via empty marker `impl`s. Hand the storage to
//! `gluesql_core::prelude::Glue::new` and run SQL strings through
//! `Glue::execute`.
//!
//! Every key is **tenant-namespaced**: many tenants can share one `Db` with no
//! cross-tenant reads. See [`SlateDbStorage::new_for_tenant`].
//!
//! [`SchemaRegistry`] is a schema-as-data facade over the same storage: list,
//! fetch, register/replace schemas without DDL, and validate a row against a
//! registered schema.
//!
//! ```no_run
//! use std::sync::Arc;
//! use slatedb::Db;
//! use slatedb::object_store::memory::InMemory;
//! use gluesql_core::prelude::Glue;
//! use bluedb_sql::SlateDbStorage;
//!
//! # async fn run() -> anyhow::Result<()> {
//! let db = Db::open("bluedb-sql", Arc::new(InMemory::new())).await?;
//! let storage = SlateDbStorage::new(Arc::new(db));
//! let mut glue = Glue::new(storage);
//! glue.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);").await?;
//! glue.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b');").await?;
//! let payloads = glue.execute("SELECT * FROM t ORDER BY id;").await?;
//! # let _ = payloads;
//! # Ok(())
//! # }
//! ```
//!
//! # Key encoding
//!
//! Ordered scans (and therefore `ORDER BY <pk>`) come straight out of SlateDB's
//! byte-ordered range scan because of the storage-key layout in
//! [`keyspace`] — schema and data live in tag-separated, length-prefixed
//! namespaces and row keys end in GlueSQL's order-preserving
//! `Key::to_cmp_be_bytes`. See that module for the full scheme.
//!
//! [GlueSQL]: gluesql_core
//! [SlateDB]: slatedb
//! [`bluedb-storage`]: https://docs.rs/bluedb-storage
//! [`bluedb-fts`]: https://docs.rs/bluedb-fts

mod cdc;
mod coerce;
mod colcat;
mod compositepk;
mod connection;
mod cte;
mod error;
mod guardrail;
mod jsoncat;
mod keyspace;
mod lakehouse;
mod nullorder;
mod pkcodec;
mod precheck;
mod projection;
mod pushdown;
mod registry;
mod rewrite;
mod schema_rules;
mod setops;
mod storage;

pub use cdc::{collapse_lww, CdcConfig, CdcEntry, CollapsedChanges};
pub use guardrail::GUARDRAIL_REJECT_PREFIX;
pub use compositepk::{prepare as prepare_composite_pk, PkCatalog, PK_COL};
pub use connection::Database;
pub use jsoncat::JsonCatalog;
pub use cte::{inline_ctes, inline_views, parse_create_view, parse_drop_view};
pub use error::SqlError;
pub use keyspace::{prefix_upper_bound, Keyspace, DEFAULT_TENANT, TAG_CDC, TAG_EXTERNAL_BASE};
pub use lakehouse::{parse_lakehouse_pragma, LhPragma};
pub use nullorder::{parse_default_null_order, rewrite_null_order};
pub use precheck::unsupported_reason;
pub use projection::{ProjColumn, ProjValue, ProjectedTable};
pub use registry::SchemaRegistry;
pub use rewrite::rewrite_multitable;
pub use setops::rewrite_set_ops;
pub use storage::{CommitObserver, RowChange, SlateDbStorage, WriteLease};
