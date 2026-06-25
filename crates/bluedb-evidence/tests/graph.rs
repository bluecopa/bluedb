//! Integration tests for the native graph store (edge maintenance).
//!
//! The test re-derives keys independently via the PUBLIC `bluedb_sql::Keyspace`
//! API (`external_prefix`/`external_key` + `prefix_upper_bound`) — an independent
//! verifier of the crate's encoding — and scans exact per-tag ranges. No byte
//! grubbing, no heuristics.

use std::sync::Arc;

use bluedb_evidence::{EdgeRef, EdgeUpsert, Graph, Merge};
use bluedb_sql::{prefix_upper_bound, Database, Keyspace};
use slatedb::{object_store::memory::InMemory, Db};

const TAG_EDGE: u8 = 0x1C;
const TAG_OUT: u8 = 0x1D;
const TAG_IN: u8 = 0x1E;

async fn db() -> Database {
    let d = Db::open("g-test", Arc::new(InMemory::new())).await.unwrap();
    Database::new(Arc::new(d))
}

/// Count keys in one external tag's range for tenant `"_"`.
async fn count_tag(database: &Database, tag: u8) -> usize {
    let ks = Keyspace::new("_");
    let prefix = ks.external_prefix(tag);
    let end = prefix_upper_bound(&prefix);
    let mut it = database
        .substrate()
        .scan_range(&prefix, end.as_deref())
        .await
        .unwrap();
    let mut n = 0;
    while it.next().await.unwrap().is_some() {
        n += 1;
    }
    n
}

/// `(edge, out, in)` key counts for tenant `"_"`.
async fn count_graph_keys(database: &Database) -> (usize, usize, usize) {
    (
        count_tag(database, TAG_EDGE).await,
        count_tag(database, TAG_OUT).await,
        count_tag(database, TAG_IN).await,
    )
}

/// Read the retained canonical weight of one edge (re-deriving the key exactly
/// as `keyspace.rs` does: u32-be length-prefixed components, value = obe weight).
async fn canonical_weight(
    database: &Database,
    graph: &str,
    src: &str,
    dst: &str,
    etype: &str,
) -> Option<i64> {
    let ks = Keyspace::new("_");
    let mut suffix = Vec::new();
    for comp in [graph, src, dst, etype] {
        suffix.extend_from_slice(&(comp.len() as u32).to_be_bytes());
        suffix.extend_from_slice(comp.as_bytes());
    }
    let v = database
        .substrate()
        .get(&ks.external_key(TAG_EDGE, &suffix))
        .await
        .unwrap()?;
    let arr: [u8; 8] = v.as_ref().try_into().unwrap();
    Some((u64::from_be_bytes(arr) ^ (i64::MIN as u64)) as i64)
}

#[tokio::test]
async fn upsert_then_delete_roundtrip() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    g.upsert(
        "lineage",
        &[EdgeUpsert {
            src: "A".into(),
            dst: "B".into(),
            weight: 5,
            etype: String::new(),
        }],
        Merge::Set,
    )
    .await
    .unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
    assert_eq!(
        canonical_weight(&database, "lineage", "A", "B", "").await,
        Some(5)
    );

    g.delete(
        "lineage",
        &[EdgeRef {
            src: "A".into(),
            dst: "B".into(),
            etype: String::new(),
        }],
    )
    .await
    .unwrap();
    assert_eq!(count_graph_keys(&database).await, (0, 0, 0));
    assert_eq!(
        canonical_weight(&database, "lineage", "A", "B", "").await,
        None
    );
}

#[tokio::test]
async fn set_overwrites_stale_out_in_no_orphans() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    g.upsert(
        "g",
        &[EdgeUpsert {
            src: "A".into(),
            dst: "B".into(),
            weight: 1,
            etype: String::new(),
        }],
        Merge::Set,
    )
    .await
    .unwrap();
    g.upsert(
        "g",
        &[EdgeUpsert {
            src: "A".into(),
            dst: "B".into(),
            weight: 9,
            etype: String::new(),
        }],
        Merge::Set,
    )
    .await
    .unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
    assert_eq!(
        canonical_weight(&database, "g", "A", "B", "").await,
        Some(9)
    );
}

#[tokio::test]
async fn max_keeps_larger_weight() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    g.upsert(
        "g",
        &[EdgeUpsert {
            src: "A".into(),
            dst: "B".into(),
            weight: 9,
            etype: String::new(),
        }],
        Merge::Max,
    )
    .await
    .unwrap();
    g.upsert(
        "g",
        &[EdgeUpsert {
            src: "A".into(),
            dst: "B".into(),
            weight: 2,
            etype: String::new(),
        }],
        Merge::Max,
    )
    .await
    .unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
    assert_eq!(
        canonical_weight(&database, "g", "A", "B", "").await,
        Some(9)
    );
}

#[tokio::test]
async fn within_one_request_two_deltas_same_identity_compose() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    g.upsert(
        "g",
        &[
            EdgeUpsert {
                src: "A".into(),
                dst: "B".into(),
                weight: 1,
                etype: String::new(),
            },
            EdgeUpsert {
                src: "A".into(),
                dst: "B".into(),
                weight: 7,
                etype: String::new(),
            },
        ],
        Merge::Set,
    )
    .await
    .unwrap();
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
    assert_eq!(
        canonical_weight(&database, "g", "A", "B", "").await,
        Some(7)
    );
}

#[tokio::test]
async fn parallel_edges_distinguished_by_type() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    g.upsert(
        "g",
        &[
            EdgeUpsert {
                src: "A".into(),
                dst: "B".into(),
                weight: 1,
                etype: "knows".into(),
            },
            EdgeUpsert {
                src: "A".into(),
                dst: "B".into(),
                weight: 2,
                etype: "likes".into(),
            },
        ],
        Merge::Set,
    )
    .await
    .unwrap();
    assert_eq!(count_graph_keys(&database).await, (2, 2, 2));
    assert_eq!(
        canonical_weight(&database, "g", "A", "B", "knows").await,
        Some(1)
    );
    assert_eq!(
        canonical_weight(&database, "g", "A", "B", "likes").await,
        Some(2)
    );
}

#[tokio::test]
async fn delete_nonexistent_is_noop() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    g.delete(
        "g",
        &[EdgeRef {
            src: "X".into(),
            dst: "Y".into(),
            etype: String::new(),
        }],
    )
    .await
    .unwrap();
    assert_eq!(count_graph_keys(&database).await, (0, 0, 0));
}

#[tokio::test]
async fn drop_graph_removes_only_that_graph() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    // graph g1: two edges; graph g2: one edge.
    g.upsert(
        "g1",
        &[
            EdgeUpsert {
                src: "A".into(),
                dst: "B".into(),
                weight: 5,
                etype: String::new(),
            },
            EdgeUpsert {
                src: "B".into(),
                dst: "C".into(),
                weight: 3,
                etype: String::new(),
            },
        ],
        bluedb_evidence::Merge::Set,
    )
    .await
    .unwrap();
    g.upsert(
        "g2",
        &[EdgeUpsert {
            src: "X".into(),
            dst: "Y".into(),
            weight: 1,
            etype: String::new(),
        }],
        bluedb_evidence::Merge::Set,
    )
    .await
    .unwrap();
    assert_eq!(count_graph_keys(&database).await, (3, 3, 3)); // 2 + 1 edges

    let dropped = g.drop_graph("g1").await.unwrap();
    assert_eq!(dropped, 2);
    // Only g2's single edge remains.
    assert_eq!(count_graph_keys(&database).await, (1, 1, 1));
}

#[tokio::test]
async fn drop_empty_graph_is_noop() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    assert_eq!(g.drop_graph("nope").await.unwrap(), 0);
    assert_eq!(count_graph_keys(&database).await, (0, 0, 0));
}
