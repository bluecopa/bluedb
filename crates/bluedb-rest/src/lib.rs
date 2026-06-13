//! `bluedb-rest` — PostgREST-style query DSL → SQL translation.
//!
//! This crate is bluedb's **REST query surface**: it gives input-table-v2 API
//! parity by turning PostgREST-shaped requests (a table plus
//! `select`/filter/`order`/`limit`/`offset` parameters, or insert/update/delete
//! payloads) into a single SQL string that bluedb's SQL engine
//! (`bluedb-sql`, GlueSQL dialect) can execute.
//!
//! It is deliberately **self-contained**: it does *pure* DSL → SQL *string*
//! translation and takes **no dependency on `bluedb-sql` or `gluesql-core`**.
//! That decoupling lets the SQL engine evolve independently, and lets this
//! crate be tested purely by asserting the generated SQL.
//!
//! # Two entry points, one output
//!
//! There are two ways to describe a request, and both render via `to_sql()`:
//!
//! * a **typed builder** — construct [`RestQuery`], [`InsertRequest`],
//!   [`UpdateRequest`], or [`DeleteRequest`] (with [`Filter`]s and
//!   [`OrderKey`]s) directly;
//! * a **query-string parser** — [`parse_query`] turns
//!   `name=eq.foo&age=gt.20&order=age.desc&limit=10` into a [`RestQuery`];
//!   [`parse_filters`] parses just the `WHERE` half for reuse by the
//!   update/delete builders.
//!
//! ```
//! use bluedb_rest::{parse_query, RestQuery, Filter, Operator};
//!
//! // Built two ways, identical SQL:
//! let parsed = parse_query("users", "select=id,name&age=gt.20&order=name.asc&limit=5")
//!     .unwrap()
//!     .to_sql()
//!     .unwrap();
//! assert_eq!(
//!     parsed,
//!     "SELECT id, name FROM users WHERE age > 20 ORDER BY name ASC LIMIT 5;"
//! );
//!
//! let built = RestQuery {
//!     table: "users".into(),
//!     select: vec!["id".into(), "name".into()],
//!     filters: vec![Filter::new("age", Operator::Gt, "20")],
//!     order: vec![],
//!     limit: Some(5),
//!     offset: None,
//! }
//! .to_sql()
//! .unwrap();
//! assert_eq!(built, "SELECT id, name FROM users WHERE age > 20 LIMIT 5;");
//! ```
//!
//! # Safety
//!
//! The rendered SQL is executed downstream, so two guards keep it safe:
//!
//! * **Identifiers** (table + column names) must match
//!   `^[A-Za-z_][A-Za-z0-9_]*$` — see [`validate_ident`]. Anything else is
//!   rejected with [`RestError::InvalidIdentifier`], blocking injection through
//!   the structural parts of the query.
//! * **Values** are typed and escaped by [`render_value`]: `null` → `NULL`,
//!   `true`/`false` → boolean, numeric-looking text → numeric literal, and
//!   everything else → a single-quoted string with embedded quotes doubled.
//!
//! See [`model`] for the full operator mapping and value-typing rule.

#![forbid(unsafe_code)]

mod error;
mod parse;
mod render;

pub mod model;

pub use error::RestError;
pub use model::{
    render_value, validate_ident, DeleteRequest, Direction, Filter, InsertRequest, Operator,
    OrderKey, RestQuery, UpdateRequest,
};
pub use parse::{parse_filters, parse_query};
