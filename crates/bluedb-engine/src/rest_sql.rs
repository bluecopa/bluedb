//! REST → SQL execution — the wire between [`bluedb_rest`] (PostgREST-style DSL
//! → SQL *string*) and [`bluedb_sql`] (the SQL engine that runs it).
//!
//! `bluedb-rest` is deliberately decoupled — it only renders SQL text and never
//! depends on the SQL engine. This module is the one place that closes the loop:
//! translate a REST request to SQL, hand it to a [`Glue`], and return the
//! resulting [`Payload`]s. A `RestError` (bad identifier, unfiltered mutation,
//! ...) and a SQL error are kept distinct in [`EngineError`].
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use slatedb::{Db, object_store::memory::InMemory};
//! # use gluesql_core::prelude::Glue;
//! # use bluedb_sql::SlateDbStorage;
//! # async fn run() -> bluedb_engine::Result<()> {
//! # let db = Arc::new(Db::open("x", Arc::new(InMemory::new())).await.unwrap());
//! let mut glue = Glue::new(SlateDbStorage::new(db));
//! # glue.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);").await.unwrap();
//! // GET /users?age=gt.20&order=name.asc&limit=10
//! let payloads = bluedb_engine::rest_sql::execute_query_str(
//!     &mut glue, "users", "age=gt.20&order=name.asc&limit=10",
//! ).await?;
//! # let _ = payloads;
//! # Ok(())
//! # }
//! ```

use bluedb_rest::{parse_query, DeleteRequest, InsertRequest, RestQuery, UpdateRequest};
use bluedb_sql::SlateDbStorage;
use gluesql_core::prelude::{Glue, Payload};

use crate::error::Result;

/// Execute a typed [`RestQuery`] (a `SELECT`) against `glue`.
pub async fn execute_query(glue: &mut Glue<SlateDbStorage>, query: &RestQuery) -> Result<Vec<Payload>> {
    run(glue, &query.to_sql()?).await
}

/// Parse a PostgREST query string for `table` and execute it.
///
/// `query_string` is the part after `?`, e.g.
/// `select=id,name&age=gt.20&order=name.asc&limit=10`.
pub async fn execute_query_str(
    glue: &mut Glue<SlateDbStorage>,
    table: &str,
    query_string: &str,
) -> Result<Vec<Payload>> {
    let query = parse_query(table, query_string)?;
    execute_query(glue, &query).await
}

/// Execute an [`InsertRequest`] (one or more rows).
pub async fn execute_insert(glue: &mut Glue<SlateDbStorage>, req: &InsertRequest) -> Result<Vec<Payload>> {
    run(glue, &req.to_sql()?).await
}

/// Execute an [`UpdateRequest`] (`SET ... WHERE ...`).
pub async fn execute_update(glue: &mut Glue<SlateDbStorage>, req: &UpdateRequest) -> Result<Vec<Payload>> {
    run(glue, &req.to_sql()?).await
}

/// Execute a [`DeleteRequest`] (`DELETE ... WHERE ...`).
pub async fn execute_delete(glue: &mut Glue<SlateDbStorage>, req: &DeleteRequest) -> Result<Vec<Payload>> {
    run(glue, &req.to_sql()?).await
}

async fn run(glue: &mut Glue<SlateDbStorage>, sql: &str) -> Result<Vec<Payload>> {
    Ok(glue.execute(sql).await?)
}
