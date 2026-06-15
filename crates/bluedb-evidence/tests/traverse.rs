//! Graph traversal integration tests (reachable + widest_path).

use std::sync::Arc;

use bluedb_evidence::{EdgeUpsert, Graph, Merge};
use bluedb_sql::Database;
use slatedb::{object_store::memory::InMemory, Db};

async fn db() -> Database {
    let d = Db::open("trav-test", Arc::new(InMemory::new())).await.unwrap();
    Database::new(Arc::new(d))
}

async fn build(database: &Database) {
    let g = Graph::new(database, "_");
    let e = |s: &str, d: &str, w: i64| EdgeUpsert { src: s.into(), dst: d.into(), weight: w, etype: String::new() };
    g.upsert("g", &[e("A", "B", 5), e("B", "C", 3), e("A", "C", 1), e("C", "D", 10)], Merge::Set)
        .await
        .unwrap();
}

#[tokio::test]
async fn reachable_directed_includes_seed_and_all_downstream() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let r = g.reachable("g", &["A".into()], i64::MIN, true).await.unwrap();
    assert_eq!(r, vec!["A", "B", "C", "D"]);
}

#[tokio::test]
async fn reachable_floor_prunes_low_weight_edges() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let r = g.reachable("g", &["A".into()], 4, true).await.unwrap();
    assert_eq!(r, vec!["A", "B"]);
}

#[tokio::test]
async fn reachable_undirected_follows_in_edges() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let r = g.reachable("g", &["D".into()], i64::MIN, false).await.unwrap();
    assert_eq!(r, vec!["A", "B", "C", "D"]);
}

#[tokio::test]
async fn reachable_directed_from_sink_is_just_itself() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let r = g.reachable("g", &["D".into()], i64::MIN, true).await.unwrap();
    assert_eq!(r, vec!["D"]);
}

#[tokio::test]
async fn reachable_multi_seed_dedups() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let r = g.reachable("g", &["A".into(), "C".into()], i64::MIN, true).await.unwrap();
    assert_eq!(r, vec!["A", "B", "C", "D"]);
}

#[tokio::test]
async fn reachable_unknown_seed_returns_itself() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let r = g.reachable("g", &["Z".into()], i64::MIN, true).await.unwrap();
    assert_eq!(r, vec!["Z"]);
}
