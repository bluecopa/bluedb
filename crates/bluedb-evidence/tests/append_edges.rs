//! Append-with-edges: an evidence entry's `edges[]` materialize into the graph
//! store atomically with the entry, on both verified and plain chains.

use std::sync::Arc;

use bluedb_evidence::{EdgeDelta, EdgeOp, EdgeRef, Evidence, EntryInput, Graph, Merge};
use bluedb_sql::{prefix_upper_bound, Database, Keyspace};
use slatedb::{object_store::memory::InMemory, Db};

async fn db() -> Database {
    let d = Db::open("ae-test", Arc::new(InMemory::new())).await.unwrap();
    Database::new(Arc::new(d))
}

fn upsert_edge(graph: &str, src: &str, dst: &str, w: i64) -> EdgeDelta {
    EdgeDelta { graph: graph.into(), src: src.into(), dst: dst.into(), weight: w, etype: String::new(), op: EdgeOp::Upsert { merge: Merge::Set } }
}

/// `(edge, out, in)` key counts for tenant `"_"`, scanning each graph tag's
/// exact external range via the public `bluedb_sql::Keyspace` API.
async fn count_graph_keys(database: &Database) -> (usize, usize, usize) {
    async fn count_tag(database: &Database, tag: u8) -> usize {
        let ks = Keyspace::new("_");
        let prefix = ks.external_prefix(tag);
        let end = prefix_upper_bound(&prefix);
        let mut it = database.substrate().scan_range(&prefix, end.as_deref()).await.unwrap();
        let mut n = 0;
        while it.next().await.unwrap().is_some() {
            n += 1;
        }
        n
    }
    (count_tag(database, 0x1C).await, count_tag(database, 0x1D).await, count_tag(database, 0x1E).await)
}

#[tokio::test]
async fn append_materializes_edges_on_verified_chain() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    let entry = EntryInput { etype: "lineage".into(), payload: b"p".to_vec(), at: String::new(), edges: vec![upsert_edge("lin", "D1", "D2", 5)] };
    ev.append("c", vec![entry], None).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
}

#[tokio::test]
async fn append_materializes_edges_on_plain_chain() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    ev.create_chain("c", false).await.unwrap();
    let entry = EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![upsert_edge("lin", "A", "B", 1)] };
    ev.append("c", vec![entry], None).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
}

#[tokio::test]
async fn multi_entry_batch_same_identity_composes() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    let e1 = EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![upsert_edge("lin", "A", "B", 1)] };
    let e2 = EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![upsert_edge("lin", "A", "B", 9)] };
    ev.append("c", vec![e1, e2], None).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
}

#[tokio::test]
async fn append_with_no_edges_writes_no_graph_keys() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    let entry = EntryInput { etype: "e".into(), payload: b"x".to_vec(), at: String::new(), edges: vec![] };
    ev.append("c", vec![entry], None).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (0, 0, 0));
}

#[tokio::test]
async fn idempotent_replay_does_not_double_apply_edges() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    let entry = EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![upsert_edge("lin", "A", "B", 3)] };
    ev.append("c", vec![entry.clone()], Some("k1")).await.unwrap();
    ev.append("c", vec![entry], Some("k1")).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
}

#[tokio::test]
async fn standalone_delete_removes_append_created_edge() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    let entry = EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![upsert_edge("lin", "A", "B", 5)] };
    ev.append("c", vec![entry], None).await.unwrap();
    let g = Graph::new(&database, "_");
    g.delete("lin", &[EdgeRef { src: "A".into(), dst: "B".into(), etype: String::new() }]).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (0, 0, 0));
}

#[tokio::test]
async fn hard_delete_retracts_edges_by_default() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    ev.create_chain("c", false).await.unwrap(); // plain chain (deletable)
    let entry = EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![upsert_edge("lin", "A", "B", 5)] };
    let r = ev.append("c", vec![entry], None).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
    ev.hard_delete("c", r.seqs[0], true).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (0, 0, 0));
}

#[tokio::test]
async fn hard_delete_keeps_edges_when_retract_false() {
    let database = db().await;
    let ev = Evidence::new(&database, "_");
    ev.create_chain("c", false).await.unwrap();
    let entry = EntryInput { etype: "e".into(), payload: vec![], at: String::new(), edges: vec![upsert_edge("lin", "A", "B", 5)] };
    let r = ev.append("c", vec![entry], None).await.unwrap();
    ev.hard_delete("c", r.seqs[0], false).await.unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
}
