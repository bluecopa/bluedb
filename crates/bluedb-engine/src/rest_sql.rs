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

use bluedb_rest::{parse_query, DeleteRequest, InsertRequest, Param, RestQuery, UpdateRequest};
use bluedb_sql::SlateDbStorage;
use gluesql_core::parse_sql::parse;
use gluesql_core::prelude::{Glue, Payload};
use gluesql_core::translate::{IntoParamLiteral, ParamLiteral};

use crate::error::{EngineError, Result};

/// Map a `bluedb-rest` [`Param`] to a gluesql [`ParamLiteral`]. Keeps `bluedb-rest`
/// free of any gluesql dependency — the type bridge lives here.
fn param_to_literal(p: &Param) -> ParamLiteral {
    match p {
        Param::Null => ParamLiteral::null(),
        Param::Bool(b) => (*b).into_param_literal(),
        Param::Int(i) => (*i).into_param_literal(),
        Param::Float(f) => (*f).into_param_literal(),
        Param::Str(s) => s.clone().into_param_literal(),
    }
}

fn literals(params: &[Param]) -> Vec<ParamLiteral> {
    params.iter().map(param_to_literal).collect()
}

/// Execute a typed [`RestQuery`] (`SELECT`) with bound parameters.
pub async fn execute_query(glue: &mut Glue<SlateDbStorage>, query: &RestQuery) -> Result<Vec<Payload>> {
    let (sql, params) = query.to_sql_with_params()?;
    Ok(glue.execute_with_params(&sql, literals(&params)).await?)
}

/// Parse a PostgREST query string for `table` and execute it.
pub async fn execute_query_str(
    glue: &mut Glue<SlateDbStorage>,
    table: &str,
    query_string: &str,
) -> Result<Vec<Payload>> {
    let query = parse_query(table, query_string)?;
    execute_query(glue, &query).await
}

/// Execute each row of `req` as an independent autocommit statement (no
/// transaction). A multi-row `req` is therefore **not atomic**. The server
/// uses this only for single-object POSTs (one row); array bodies go through
/// [`execute_insert_batch`] for atomicity.
pub async fn execute_insert(glue: &mut Glue<SlateDbStorage>, req: &InsertRequest) -> Result<Vec<Payload>> {
    let (stmts, params) = req.row_statements_with_params()?;
    let sql = format!("{};", stmts.join("; "));
    Ok(glue.execute_with_params(&sql, literals(&params)).await?)
}

/// Execute a multi-row [`InsertRequest`] atomically: the server-side
/// `BEGIN; <single-row INSERT…>; …; COMMIT;` batch — one `WriteBatch`, one flush.
/// Caller supplies the **serialized** connection (the txn holds the write lease).
pub async fn execute_insert_batch(
    glue: &mut Glue<SlateDbStorage>,
    req: &InsertRequest,
) -> Result<Vec<Payload>> {
    let (stmts, params) = req.row_statements_with_params()?;
    let sql = format!("BEGIN; {}; COMMIT;", stmts.join("; "));
    Ok(glue.execute_with_params(&sql, literals(&params)).await?)
}

/// Execute an [`UpdateRequest`] with bound parameters.
pub async fn execute_update(glue: &mut Glue<SlateDbStorage>, req: &UpdateRequest) -> Result<Vec<Payload>> {
    let (sql, params) = req.to_sql_with_params()?;
    Ok(glue.execute_with_params(&sql, literals(&params)).await?)
}

/// Execute a [`DeleteRequest`] with bound parameters.
pub async fn execute_delete(glue: &mut Glue<SlateDbStorage>, req: &DeleteRequest) -> Result<Vec<Payload>> {
    let (sql, params) = req.to_sql_with_params()?;
    Ok(glue.execute_with_params(&sql, literals(&params)).await?)
}

/// Execute a `{sql, params}` request. Values bind as `$N` (never interpolated).
///
/// When `allow_arbitrary` is false (the `/sql` surface) the SQL must be exactly
/// ONE `SELECT`/`INSERT`/`UPDATE`/`DELETE` statement — DDL, transactions, and
/// multi-statement are rejected (`EngineError::Rejected`). When true (the
/// `/admin/sql` surface) anything goes.
pub async fn execute_sql(
    glue: &mut Glue<SlateDbStorage>,
    sql: &str,
    params: &[Param],
    allow_arbitrary: bool,
) -> Result<Vec<Payload>> {
    if !allow_arbitrary {
        let parsed = parse(sql).map_err(|e: gluesql_core::error::Error| EngineError::Rejected(e.to_string()))?;
        if parsed.len() != 1 {
            return Err(EngineError::Rejected(format!(
                "exactly one statement required, got {}",
                parsed.len()
            )));
        }
        // Classify using sqlparser's AST directly — avoids parameter resolution in translate().
        let is_dml = matches!(
            &parsed[0],
            gluesql_core::sqlparser::ast::Statement::Query(_)
                | gluesql_core::sqlparser::ast::Statement::Insert(_)
                | gluesql_core::sqlparser::ast::Statement::Update { .. }
                | gluesql_core::sqlparser::ast::Statement::Delete(_)
        );
        if !is_dml {
            return Err(EngineError::Rejected(
                "only SELECT/INSERT/UPDATE/DELETE allowed on /sql; use /admin/sql for DDL".to_string(),
            ));
        }
    }
    Ok(glue.execute_with_params(sql, literals(params)).await?)
}
