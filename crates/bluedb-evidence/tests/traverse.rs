//! Graph traversal integration tests (reachable + widest_path).

use std::sync::Arc;

use bluedb_evidence::{EdgeUpsert, Graph, Merge, WidestPath};
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

#[tokio::test]
async fn widest_path_picks_max_bottleneck() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    // A->B->C->D: min(5,3,10)=3 ; A->C->D: min(1,10)=1 ; widest = 3.
    let wp = g.widest_path("g", "A", "D", true).await.unwrap();
    assert_eq!(wp, WidestPath { connected: true, bottleneck: Some(3) });
}

#[tokio::test]
async fn widest_path_unreachable_directed() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let wp = g.widest_path("g", "D", "A", true).await.unwrap();
    assert_eq!(wp, WidestPath { connected: false, bottleneck: None });
}

#[tokio::test]
async fn widest_path_undirected_uses_reverse_edges() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    // Undirected D..A: D-C(10)-B(3)-A(5) => 3 ; D-C(10)-A(1) => 1 ; widest = 3.
    let wp = g.widest_path("g", "D", "A", false).await.unwrap();
    assert_eq!(wp, WidestPath { connected: true, bottleneck: Some(3) });
}

#[tokio::test]
async fn widest_path_from_equals_to() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let wp = g.widest_path("g", "A", "A", true).await.unwrap();
    assert_eq!(wp, WidestPath { connected: true, bottleneck: None });
}

#[tokio::test]
async fn widest_path_to_unknown_node() {
    let database = db().await;
    build(&database).await;
    let g = Graph::new(&database, "_");
    let wp = g.widest_path("g", "A", "Z", true).await.unwrap();
    assert_eq!(wp, WidestPath { connected: false, bottleneck: None });
}
