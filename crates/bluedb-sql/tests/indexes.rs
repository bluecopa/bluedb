//! Secondary-index integration tests for [`bluedb_sql::SlateDbStorage`].
//!
//! These drive `CREATE INDEX` / mutations through `Glue::execute`, then call
//! [`Index::scan_indexed_data`] directly via the public `Glue::storage` handle —
//! the unambiguous proof that the index entry keyspace is built, maintained, and
//! scanned correctly. The string-column ordering tests specifically guard the
//! order-preserving value encoding (a length-prefixed value would sort by length
//! before content, mis-ordering different-length strings).

use std::sync::Arc;

use bluedb_sql::SlateDbStorage;
use futures::stream::StreamExt;
use gluesql_core::ast::IndexOperator;
use gluesql_core::data::Value;
use gluesql_core::prelude::Glue;
use gluesql_core::store::{DataRow, Index};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn new_glue() -> Glue<SlateDbStorage> {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("bluedb-sql-index-test", object_store)
        .await
        .expect("open slatedb");
    Glue::new(SlateDbStorage::new(Arc::new(db)))
}

async fn exec(glue: &mut Glue<SlateDbStorage>, sql: &str) {
    glue.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("execute `{sql}`: {e}"));
}

/// Scan `index` directly and return the `name` column (col 1) of each row, in
/// the order the index produced them.
async fn index_names(
    glue: &Glue<SlateDbStorage>,
    table: &str,
    index: &str,
    asc: Option<bool>,
    cmp: Option<(IndexOperator, Value)>,
) -> Vec<String> {
    let cmp_ref = cmp.as_ref().map(|(op, v)| (op, v.clone()));
    let iter = glue
        .storage
        .scan_indexed_data(table, index, asc, cmp_ref)
        .await
        .expect("scan_indexed_data");
    let rows: Vec<_> = iter.collect().await;
    rows.into_iter()
        .map(|r| match r.expect("row").1 {
            DataRow::Vec(vals) => match &vals[1] {
                Value::Str(s) => s.clone(),
                other => panic!("expected Str name, got {other:?}"),
            },
            other => panic!("expected Vec row, got {other:?}"),
        })
        .collect()
}

/// CREATE TABLE + INSERT names of deliberately *different lengths*, then index.
async fn glue_with_string_index() -> Glue<SlateDbStorage> {
    let mut glue = new_glue().await;
    exec(
        &mut glue,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT);",
    )
    .await;
    // 'a' < 'aa' < 'b' < 'z' < 'zzz' lexically — but 'a'(len1),'aa'(len2),'zzz'(len3)
    // would mis-sort under a length-prefixed value encoding.
    exec(
        &mut glue,
        "INSERT INTO t VALUES (1, 'b'), (2, 'aa'), (3, 'z'), (4, 'a'), (5, 'zzz');",
    )
    .await;
    exec(&mut glue, "CREATE INDEX idx_name ON t (name);").await;
    glue
}

#[tokio::test]
async fn index_scan_orders_strings_across_lengths() {
    let glue = glue_with_string_index().await;

    // Ascending: must be lexical order regardless of encoded length.
    let asc = index_names(&glue, "t", "idx_name", Some(true), None).await;
    assert_eq!(
        asc,
        vec!["a", "aa", "b", "z", "zzz"],
        "order-preserving across lengths"
    );

    // None defaults to ascending.
    let none = index_names(&glue, "t", "idx_name", None, None).await;
    assert_eq!(none, vec!["a", "aa", "b", "z", "zzz"]);

    // Descending reverses.
    let desc = index_names(&glue, "t", "idx_name", Some(false), None).await;
    assert_eq!(desc, vec!["zzz", "z", "b", "aa", "a"]);
}

#[tokio::test]
async fn index_scan_range_and_eq_bounds() {
    let glue = glue_with_string_index().await;
    let s = |x: &str| Value::Str(x.to_owned());

    let gt = index_names(
        &glue,
        "t",
        "idx_name",
        Some(true),
        Some((IndexOperator::Gt, s("b"))),
    )
    .await;
    assert_eq!(gt, vec!["z", "zzz"], "name > 'b'");

    let gte = index_names(
        &glue,
        "t",
        "idx_name",
        Some(true),
        Some((IndexOperator::GtEq, s("b"))),
    )
    .await;
    assert_eq!(gte, vec!["b", "z", "zzz"], "name >= 'b'");

    let lt = index_names(
        &glue,
        "t",
        "idx_name",
        Some(true),
        Some((IndexOperator::Lt, s("b"))),
    )
    .await;
    assert_eq!(lt, vec!["a", "aa"], "name < 'b'");

    let lte = index_names(
        &glue,
        "t",
        "idx_name",
        Some(true),
        Some((IndexOperator::LtEq, s("aa"))),
    )
    .await;
    assert_eq!(lte, vec!["a", "aa"], "name <= 'aa'");

    let eq = index_names(
        &glue,
        "t",
        "idx_name",
        Some(true),
        Some((IndexOperator::Eq, s("aa"))),
    )
    .await;
    assert_eq!(
        eq,
        vec!["aa"],
        "name = 'aa' (and not 'a', which is a byte-prefix)"
    );
}

#[tokio::test]
async fn index_on_integer_column_scans_in_numeric_order() {
    let mut glue = new_glue().await;
    exec(
        &mut glue,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);",
    )
    .await;
    exec(
        &mut glue,
        "INSERT INTO t VALUES (1,'a',40),(2,'b',-5),(3,'c',0),(4,'d',7);",
    )
    .await;
    exec(&mut glue, "CREATE INDEX idx_age ON t (age);").await;

    // Drain the index ordered by age and read back the names in age order,
    // crossing the sign boundary: -5, 0, 7, 40 -> b, c, d, a.
    let names = index_names(&glue, "t", "idx_age", Some(true), None).await;
    assert_eq!(names, vec!["b", "c", "d", "a"]);
}

#[tokio::test]
async fn index_stays_consistent_across_update_and_delete() {
    let mut glue = glue_with_string_index().await; // names: 1=b,2=aa,3=z,4=a,5=zzz

    exec(&mut glue, "UPDATE t SET name = 'zzzz' WHERE id = 4;").await; // 'a' -> 'zzzz'
    exec(&mut glue, "DELETE FROM t WHERE id = 3;").await; // remove 'z'

    // Index must reflect both: old 'a' entry gone, 'zzzz' added, 'z' removed.
    let names = index_names(&glue, "t", "idx_name", Some(true), None).await;
    assert_eq!(names, vec!["aa", "b", "zzz", "zzzz"]);
}

#[tokio::test]
async fn index_changes_roll_back_with_the_transaction() {
    let mut glue = glue_with_string_index().await;

    // Insert inside a transaction, then roll back: the index entry must vanish.
    exec(&mut glue, "BEGIN;").await;
    exec(&mut glue, "INSERT INTO t VALUES (6, 'mmm');").await;
    exec(&mut glue, "ROLLBACK;").await;

    let after_rollback = index_names(&glue, "t", "idx_name", Some(true), None).await;
    assert_eq!(
        after_rollback,
        vec!["a", "aa", "b", "z", "zzz"],
        "rolled-back insert leaves no index entry"
    );

    // Now commit one and confirm it lands in the index.
    exec(&mut glue, "BEGIN;").await;
    exec(&mut glue, "INSERT INTO t VALUES (6, 'mmm');").await;
    exec(&mut glue, "COMMIT;").await;

    let after_commit = index_names(&glue, "t", "idx_name", Some(true), None).await;
    assert_eq!(after_commit, vec!["a", "aa", "b", "mmm", "z", "zzz"]);
}

#[tokio::test]
async fn drop_index_removes_all_entries() {
    let mut glue = glue_with_string_index().await;

    // Sanity: entries exist before the drop.
    assert_eq!(
        index_names(&glue, "t", "idx_name", Some(true), None)
            .await
            .len(),
        5
    );

    exec(&mut glue, "DROP INDEX t.idx_name;").await;

    // After DROP, the index entry keyspace is empty.
    let names = index_names(&glue, "t", "idx_name", Some(true), None).await;
    assert!(
        names.is_empty(),
        "DROP INDEX removed every entry, got {names:?}"
    );

    // The table itself is unaffected (full scan still returns all rows).
    let payload = glue.execute("SELECT id FROM t;").await.expect("select");
    let n = match &payload[0] {
        gluesql_core::prelude::Payload::Select { rows, .. } => rows.len(),
        other => panic!("expected Select, got {other:?}"),
    };
    assert_eq!(n, 5, "rows survive DROP INDEX");
}

/// Exercise the planner path: with an index present, a `WHERE`-filtered query
/// returns correct results (whether or not the planner routes through the index,
/// the answer must be right). Pairs with the direct `scan_indexed_data` proofs.
#[tokio::test]
async fn indexed_where_query_returns_correct_rows() {
    let glue_holder = glue_with_string_index().await;
    let mut glue = glue_holder;
    let payload = glue
        .execute("SELECT id FROM t WHERE name = 'aa';")
        .await
        .expect("select");
    let ids: Vec<i64> = match &payload[0] {
        gluesql_core::prelude::Payload::Select { rows, .. } => rows
            .iter()
            .map(|r| match &r[0] {
                Value::I64(n) => *n,
                other => panic!("expected I64, got {other:?}"),
            })
            .collect(),
        other => panic!("expected Select, got {other:?}"),
    };
    assert_eq!(ids, vec![2], "only id=2 has name 'aa'");
}
