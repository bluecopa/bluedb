//! Integration tests: `DECIMAL(p,s)` / `NUMERIC(p,s)` column types.
//!
//! bluedb-sql intercepts and normalises parameterised decimal types before
//! handing DDL to gluesql, whose translate layer only accepts bare `DECIMAL`.
//! These tests exercise the full path:
//!
//!   raw SQL → `prepare_composite_pk` (normalisation chokepoint)
//!             → `Glue::execute` (gluesql translate + executor)
//!
//! That matches the production path via `bluedb-engine::rest_sql::execute_sql`.

use std::sync::Arc;

use bluedb_sql::{Database, SlateDbStorage};
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn new_glue() -> Glue<SlateDbStorage> {
    let db = Arc::new(
        Db::open("decimal-ps-test", Arc::new(InMemory::new()))
            .await
            .unwrap(),
    );
    Glue::new(Database::new(db).connection_serialized())
}

/// Apply the composite-PK / data-type normalisation rewrite, then execute.
async fn exec(glue: &mut Glue<SlateDbStorage>, sql: &str) -> Result<Payload, String> {
    let prepared = bluedb_sql::prepare_composite_pk(&mut glue.storage, sql, &[])
        .await
        .map_err(|e| e.to_string())?;
    let mut payloads = glue.execute(&prepared).await.map_err(|e| e.to_string())?;
    Ok(payloads.pop().unwrap())
}

fn rows(payload: Payload) -> Vec<Vec<Value>> {
    match payload {
        Payload::Select { rows, .. } => rows,
        other => panic!("expected Select payload, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Core: DECIMAL(p,s) and NUMERIC(p,s) are accepted and values round-trip
// ---------------------------------------------------------------------------

/// `DECIMAL(12,2)` and `NUMERIC(8,3)` columns are accepted in `CREATE TABLE`.
/// The declared precision/scale is coerced to gluesql's internal (38,18) —
/// enforcement of the declared p,s is intentionally out of scope (noted in the
/// shim comments).
#[tokio::test]
async fn create_table_decimal_ps_succeeds() {
    let mut glue = new_glue().await;
    let result = exec(
        &mut glue,
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount DECIMAL(12,2), qty NUMERIC(8,3))",
    )
    .await;
    assert!(
        result.is_ok(),
        "DECIMAL(12,2) / NUMERIC(8,3) CREATE TABLE should succeed: {:?}",
        result
    );
}

/// INSERT and SELECT round-trip for `DECIMAL(12,2)` columns.
#[tokio::test]
async fn decimal_ps_insert_select_roundtrip() {
    let mut glue = new_glue().await;
    exec(
        &mut glue,
        "CREATE TABLE prices (id INTEGER PRIMARY KEY, amount DECIMAL(12,2))",
    )
    .await
    .unwrap();

    exec(&mut glue, "INSERT INTO prices VALUES (1, 9.99)")
        .await
        .unwrap();
    exec(&mut glue, "INSERT INTO prices VALUES (2, 123.45)")
        .await
        .unwrap();
    exec(&mut glue, "INSERT INTO prices VALUES (3, 0.01)")
        .await
        .unwrap();

    let got = rows(
        exec(&mut glue, "SELECT id, amount FROM prices ORDER BY id")
            .await
            .unwrap(),
    );
    assert_eq!(got.len(), 3, "expected 3 rows");
    // Confirm ids survive.
    assert_eq!(got[0][0], Value::I64(1));
    assert_eq!(got[1][0], Value::I64(2));
    assert_eq!(got[2][0], Value::I64(3));
    // Decimal values must come back as Decimal variants (not null / error).
    for row in &got {
        assert!(
            matches!(row[1], Value::Decimal(_)),
            "amount should be Decimal, got {:?}",
            row[1]
        );
    }
}

/// `NUMERIC(p,s)` is treated identically to `DECIMAL(p,s)`.
#[tokio::test]
async fn numeric_ps_insert_select_roundtrip() {
    let mut glue = new_glue().await;
    exec(
        &mut glue,
        "CREATE TABLE measurements (id INTEGER PRIMARY KEY, reading NUMERIC(10,4))",
    )
    .await
    .unwrap();

    exec(&mut glue, "INSERT INTO measurements VALUES (1, 3.1416)")
        .await
        .unwrap();

    let got = rows(
        exec(&mut glue, "SELECT reading FROM measurements WHERE id = 1")
            .await
            .unwrap(),
    );
    assert_eq!(got.len(), 1);
    assert!(
        matches!(got[0][0], Value::Decimal(_)),
        "reading should be Decimal, got {:?}",
        got[0][0]
    );
}

// ---------------------------------------------------------------------------
// Regression: bare DECIMAL (no params) still works
// ---------------------------------------------------------------------------

/// Bare `DECIMAL` (no precision/scale) must continue to work — it was already
/// supported by gluesql and must not be broken by the shim.
#[tokio::test]
async fn bare_decimal_still_works() {
    let mut glue = new_glue().await;
    exec(
        &mut glue,
        "CREATE TABLE bare (id INTEGER PRIMARY KEY, val DECIMAL)",
    )
    .await
    .unwrap();
    exec(&mut glue, "INSERT INTO bare VALUES (1, 42.0)")
        .await
        .unwrap();
    let got = rows(
        exec(&mut glue, "SELECT val FROM bare WHERE id = 1")
            .await
            .unwrap(),
    );
    assert_eq!(got.len(), 1);
    assert!(
        matches!(got[0][0], Value::Decimal(_)),
        "val should be Decimal, got {:?}",
        got[0][0]
    );
}

// ---------------------------------------------------------------------------
// Non-regression: other types unaffected
// ---------------------------------------------------------------------------

/// Other column types (TEXT, INTEGER, BOOLEAN, FLOAT) are unaffected by the shim.
#[tokio::test]
async fn other_types_unaffected() {
    let mut glue = new_glue().await;
    exec(
        &mut glue,
        "CREATE TABLE mixed (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN, score FLOAT)",
    )
    .await
    .unwrap();
    exec(
        &mut glue,
        "INSERT INTO mixed VALUES (1, 'alice', TRUE, 9.5)",
    )
    .await
    .unwrap();
    let got = rows(
        exec(
            &mut glue,
            "SELECT id, name, active, score FROM mixed WHERE id = 1",
        )
        .await
        .unwrap(),
    );
    assert_eq!(got.len(), 1);
    assert_eq!(got[0][0], Value::I64(1));
    assert!(matches!(got[0][1], Value::Str(_)), "name should be Str");
    assert!(matches!(got[0][2], Value::Bool(_)), "active should be Bool");
    assert!(matches!(got[0][3], Value::F64(_)), "score should be F64");
}

// ---------------------------------------------------------------------------
// Multi-column table with mixed DECIMAL(p,s) and composite PK
// ---------------------------------------------------------------------------

/// `DECIMAL(p,s)` works alongside a composite primary key — the two rewrites
/// compose cleanly.
#[tokio::test]
async fn decimal_ps_with_composite_pk() {
    let mut glue = new_glue().await;
    exec(
        &mut glue,
        "CREATE TABLE orders (tenant TEXT, order_id INTEGER, amount DECIMAL(12,2), \
         PRIMARY KEY (tenant, order_id))",
    )
    .await
    .unwrap();

    exec(
        &mut glue,
        "INSERT INTO orders (tenant, order_id, amount) VALUES ('acme', 1, 100.00)",
    )
    .await
    .unwrap();
    exec(
        &mut glue,
        "INSERT INTO orders (tenant, order_id, amount) VALUES ('acme', 2, 50.50)",
    )
    .await
    .unwrap();

    let got = rows(
        exec(
            &mut glue,
            "SELECT order_id, amount FROM orders WHERE tenant = 'acme' ORDER BY order_id",
        )
        .await
        .unwrap(),
    );
    assert_eq!(got.len(), 2);
    assert_eq!(got[0][0], Value::I64(1));
    assert!(matches!(got[0][1], Value::Decimal(_)));
    assert_eq!(got[1][0], Value::I64(2));
    assert!(matches!(got[1][1], Value::Decimal(_)));
}
