//! `bluedb-rest` — PostgREST-style query DSL → SQL translation.
//!
//! This crate is bluedb's **REST query surface**: it gives input-table-v2 API
//! parity by turning PostgREST-shaped requests (a table plus
//! `select`/filter/`order`/`limit`/`offset` parameters, or insert/update/delete
//! payloads) into a SQL string + `Vec<Param>` that bluedb's SQL engine
//! (`bluedb-sql`, GlueSQL dialect) can execute with bound parameters.
//!
//! It is deliberately **self-contained**: it does *pure* DSL → SQL *string*
//! translation and takes **no dependency on `bluedb-sql` or `gluesql-core`**.
//! That decoupling lets the SQL engine evolve independently, and lets this
//! crate be tested purely by asserting the generated SQL.
//!
//! # Two entry points, one output
//!
//! There are two ways to describe a request, and both render via
//! `to_sql_with_params()`:
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
//! use bluedb_rest::{parse_query, RestQuery, Filter, Operator, Param};
//!
//! // Built two ways, identical SQL + params:
//! let (sql, params) = parse_query("users", "select=id,name&age=gt.20&order=name.asc&limit=5")
//!     .unwrap()
//!     .to_sql_with_params()
//!     .unwrap();
//! assert_eq!(
//!     sql,
//!     "SELECT id, name FROM users WHERE age > $1 ORDER BY name ASC LIMIT 5;"
//! );
//! assert_eq!(params, vec![Param::Int(20)]);
//!
//! let (sql, params) = RestQuery {
//!     table: "users".into(),
//!     select: vec!["id".into(), "name".into()],
//!     filters: vec![Filter::new("age", Operator::Gt, "20")],
//!     order: vec![],
//!     limit: Some(5),
//!     offset: None,
//! }
//! .to_sql_with_params()
//! .unwrap();
//! assert_eq!(sql, "SELECT id, name FROM users WHERE age > $1 LIMIT 5;");
//! assert_eq!(params, vec![Param::Int(20)]);
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
//! * **Values** are carried as typed [`Param`] variants and bound as `$N`
//!   parameters by the engine — they never appear in the SQL text, so
//!   SQL-injection via filter values is structurally impossible.
//!
//! See [`model`] for the full operator mapping and value-typing rule.

#![forbid(unsafe_code)]

mod error;
mod parse;
mod render;

pub mod model;

pub use error::RestError;
pub use model::{
    validate_ident, DeleteRequest, Direction, Filter, InsertRequest, Operator, Param,
    OrderKey, RestQuery, UpdateRequest,
};
pub use parse::{parse_filters, parse_query};
