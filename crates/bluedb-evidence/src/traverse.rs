//! Read-only graph traversal over the `out`/`in` adjacency indexes (Plan 3).
//! A node's edges are a bounded prefix range scan with the weight folded
//! order-preserving into the key. No write lease — works on a read replica.
//!
//! Every traversal reads through a single pinned [`ReadView`] (see
//! [`bluedb_storage::Substrate::read_view`]). On the writer that is a true MVCC
//! snapshot, so all of a traversal's scans — including the concurrent
//! per-node scans of one BFS level — observe one consistent cut: an edge
//! written (as an atomic 3-key batch) after the view is pinned is invisible,
//! and one written before is fully visible. On a replica the view is the live
//! reader (consistent within a scan, may advance between scans). Output is
//! sorted (`reachable`) / maximin-unique (`widest_path`), so it is
//! deterministic for a fixed cut.

use std::collections::{BinaryHeap, HashMap, HashSet};

use bluedb_storage::ReadView;

use crate::error::EvidenceError;
use crate::keyspace::{weight_from_obe, EvidenceKeyspace};

/// Read `(neighbor, weight, type)` for every edge under `prefix`, keeping only
/// edges with `weight >= floor`. `prefix` is an out/in adjacency scan prefix;
/// the bytes after it are `weight_obe(8) ‖ neighbor_lp ‖ type_lp`.
async fn scan_adjacency(
    view: &ReadView,
    prefix: &[u8],
    floor: i64,
) -> Result<Vec<(String, i64, String)>, EvidenceError> {
    let mut start = prefix.to_vec();
    start.extend_from_slice(&crate::keyspace::weight_obe(floor));
    let end = bluedb_sql::prefix_upper_bound(prefix);
    let mut iter = view.scan_range(&start, end.as_deref()).await?;
    let mut out = Vec::new();
    let plen = prefix.len();
    while let Some(kv) = iter.next().await.map_err(|e| EvidenceError::Storage(anyhow::anyhow!("{e}")))? {
        let key = kv.key.as_ref();
        let tail = &key[plen..];
        let w = {
            let arr: [u8; 8] = tail.get(0..8).ok_or_else(|| {
                EvidenceError::Storage(anyhow::anyhow!("adjacency key too short for weight"))
            })?.try_into().map_err(|_| {
                EvidenceError::Storage(anyhow::anyhow!("adjacency key too short for weight"))
            })?;
            weight_from_obe(&arr)
        };
        let mut p = 8usize;
        let neighbor = read_lp(tail, &mut p)?;
        let etype = read_lp(tail, &mut p)?;
        out.push((neighbor, w, etype));
    }
    Ok(out)
}

/// Read a `<u32-be len> <utf8 bytes>` segment from `buf` at `*pos`, advancing it.
fn read_lp(buf: &[u8], pos: &mut usize) -> Result<String, EvidenceError> {
    let err = || EvidenceError::Storage(anyhow::anyhow!("malformed adjacency key segment"));
    let len = {
        let arr: [u8; 4] = buf.get(*pos..*pos + 4).ok_or_else(err)?.try_into().map_err(|_| err())?;
        u32::from_be_bytes(arr) as usize
    };
    *pos += 4;
    let bytes = buf.get(*pos..*pos + len).ok_or_else(err)?;
    *pos += len;
    String::from_utf8(bytes.to_vec()).map_err(|_| err())
}

/// Out-neighbors `(dst, weight, type)` of `src` with `weight >= floor`, ascending.
pub(crate) async fn out_neighbors(
    view: &ReadView,
    ks: &EvidenceKeyspace,
    graph: &str,
    src: &str,
    floor: i64,
) -> Result<Vec<(String, i64, String)>, EvidenceError> {
    scan_adjacency(view, &ks.graph_out_prefix(graph, src), floor).await
}

/// In-neighbors `(src, weight, type)` of `dst` with `weight >= floor`, ascending.
pub(crate) async fn in_neighbors(
    view: &ReadView,
    ks: &EvidenceKeyspace,
    graph: &str,
    dst: &str,
    floor: i64,
) -> Result<Vec<(String, i64, String)>, EvidenceError> {
    scan_adjacency(view, &ks.graph_in_prefix(graph, dst), floor).await
}

/// Concurrency cap for level expansion (in-flight neighbor scans).
const REACHABLE_FANOUT: usize = 16;

/// Scan every node in `nodes` for its out- (and in-, when undirected) neighbors
/// concurrently, bounded to `REACHABLE_FANOUT` in-flight scans. Returns all
/// discovered neighbor ids (with duplicates; the caller dedups). Order is
/// unspecified — the caller sorts, so the final result is deterministic.
async fn expand_level(
    view: &ReadView,
    ks: &EvidenceKeyspace,
    graph: &str,
    nodes: &[String],
    floor: i64,
    directed: bool,
) -> Result<Vec<String>, EvidenceError> {
    let mut set: tokio::task::JoinSet<Result<Vec<String>, EvidenceError>> = tokio::task::JoinSet::new();
    let mut iter = nodes.iter();

    let spawn_one = |set: &mut tokio::task::JoinSet<Result<Vec<String>, EvidenceError>>, node: &str| {
        // Each task clones the pinned view (an Arc bump); every clone shares the
        // same snapshot seq, so the whole level reads one consistent cut.
        let view = view.clone();
        let ks = ks.clone();
        let g = graph.to_string();
        let n = node.to_string();
        set.spawn(async move {
            let mut ns: Vec<String> = out_neighbors(&view, &ks, &g, &n, floor)
                .await?
                .into_iter()
                .map(|(v, _, _)| v)
                .collect();
            if !directed {
                ns.extend(in_neighbors(&view, &ks, &g, &n, floor).await?.into_iter().map(|(v, _, _)| v));
            }
            Ok(ns)
        });
    };

    for _ in 0..REACHABLE_FANOUT {
        match iter.next() {
            Some(n) => spawn_one(&mut set, n),
            None => break,
        }
    }

    let mut out: Vec<String> = Vec::new();
    while let Some(joined) = set.join_next().await {
        let ns = joined
            .map_err(|e| EvidenceError::Storage(anyhow::anyhow!("traversal join: {e}")))??;
        out.extend(ns);
        if let Some(n) = iter.next() {
            spawn_one(&mut set, n);
        }
    }
    Ok(out)
}

/// Nodes reachable from any of `from`, traversing only edges with weight ≥
/// `floor`. Seeds are included. `directed=false` also follows `in` edges.
/// Output is sorted (order-independent). Each BFS level is expanded
/// concurrently (bounded fan-out); the `visited` set keeps the result
/// identical and deterministic regardless of completion order.
pub(crate) async fn reachable(
    view: &ReadView,
    ks: &EvidenceKeyspace,
    graph: &str,
    from: &[String],
    floor: i64,
    directed: bool,
) -> Result<Vec<String>, EvidenceError> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut frontier: Vec<String> = Vec::new();
    for n in from {
        if visited.insert(n.clone()) {
            frontier.push(n.clone());
        }
    }
    while !frontier.is_empty() {
        let neighbors = expand_level(view, ks, graph, &frontier, floor, directed).await?;
        let mut next: Vec<String> = Vec::new();
        for v in neighbors {
            if visited.insert(v.clone()) {
                next.push(v);
            }
        }
        frontier = next;
    }
    let mut out: Vec<String> = visited.into_iter().collect();
    out.sort();
    Ok(out)
}

/// Result of [`widest_path`]: whether `to` is reachable and, if so, the maximum
/// bottleneck (the widest path's minimum edge weight). `from == to` → connected
/// with `bottleneck: None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WidestPath {
    pub connected: bool,
    pub bottleneck: Option<i64>,
}

/// Max-bottleneck path from `from` to `to` (maximin Dijkstra with a max-heap).
pub(crate) async fn widest_path(
    view: &ReadView,
    ks: &EvidenceKeyspace,
    graph: &str,
    from: &str,
    to: &str,
    directed: bool,
) -> Result<WidestPath, EvidenceError> {
    if from == to {
        return Ok(WidestPath { connected: true, bottleneck: None });
    }
    // best[node] = best-known bottleneck to reach `node`. Source has +inf.
    let mut best: HashMap<String, i64> = HashMap::new();
    best.insert(from.to_string(), i64::MAX);
    // Max-heap by bottleneck; ties broken by node id (deterministic).
    let mut heap: BinaryHeap<(i64, String)> = BinaryHeap::new();
    heap.push((i64::MAX, from.to_string()));

    while let Some((bw, u)) = heap.pop() {
        if best.get(&u).copied() != Some(bw) {
            continue; // stale heap entry
        }
        if u == to {
            return Ok(WidestPath { connected: true, bottleneck: Some(bw) });
        }
        let mut edges = out_neighbors(view, ks, graph, &u, i64::MIN).await?;
        if !directed {
            edges.extend(in_neighbors(view, ks, graph, &u, i64::MIN).await?);
        }
        for (v, w, _t) in edges {
            let nb = bw.min(w);
            if nb > best.get(&v).copied().unwrap_or(i64::MIN) {
                best.insert(v.clone(), nb);
                heap.push((nb, v));
            }
        }
    }
    Ok(WidestPath { connected: false, bottleneck: None })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use bluedb_sql::Database;
    use slatedb::{object_store::memory::InMemory, Db};
    use crate::graph::{Graph, EdgeRef, EdgeUpsert};
    use crate::model::Merge;

    async fn db() -> Database {
        let d = Db::open("trav-unit", Arc::new(InMemory::new())).await.unwrap();
        Database::new(Arc::new(d))
    }

    #[tokio::test]
    async fn out_neighbors_ascending_and_floor_filter() {
        let database = db().await;
        let g = Graph::new(&database, "_");
        g.upsert("g", &[
            EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 2, etype: "x".into() },
            EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 9, etype: "y".into() },
            EdgeUpsert { src: "A".into(), dst: "C".into(), weight: 5, etype: String::new() },
        ], Merge::Set).await.unwrap();

        let view = database.substrate().read_view().await.unwrap();
        let ks = EvidenceKeyspace::new("_");
        let all = out_neighbors(&view, &ks, "g", "A", i64::MIN).await.unwrap();
        assert_eq!(all, vec![
            ("B".to_string(), 2, "x".to_string()),
            ("C".to_string(), 5, "".to_string()),
            ("B".to_string(), 9, "y".to_string()),
        ]);
        let hi = out_neighbors(&view, &ks, "g", "A", 5).await.unwrap();
        assert_eq!(hi, vec![
            ("C".to_string(), 5, "".to_string()),
            ("B".to_string(), 9, "y".to_string()),
        ]);
        let inb = in_neighbors(&view, &ks, "g", "B", i64::MIN).await.unwrap();
        assert_eq!(inb, vec![
            ("A".to_string(), 2, "x".to_string()),
            ("A".to_string(), 9, "y".to_string()),
        ]);
    }

    /// A `ReadView` pinned before a write must not observe that write across any
    /// of the traversal's scans — the snapshot-isolation guarantee Phase 2 adds.
    /// We pin the view, then extend the graph (A→B already exists; add B→C), and
    /// run `reachable` through the *pinned* view: it must see {A, B} (the cut at
    /// pin time), never C. A fresh view taken after the write sees {A, B, C}.
    #[tokio::test]
    async fn pinned_view_is_isolated_from_later_writes() {
        let database = db().await;
        let g = Graph::new(&database, "_");
        let ks = EvidenceKeyspace::new("_");
        g.upsert("g", &[
            EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 1, etype: String::new() },
        ], Merge::Set).await.unwrap();

        // Pin the cut: {A→B}.
        let pinned = database.substrate().read_view().await.unwrap();
        assert!(pinned.snapshot_seq().is_some(), "writer view must be a true snapshot");

        // Mutate after pinning.
        g.upsert("g", &[
            EdgeUpsert { src: "B".into(), dst: "C".into(), weight: 1, etype: String::new() },
        ], Merge::Set).await.unwrap();

        // The pinned view never sees B→C, so C is unreachable through it.
        let seen = reachable(&pinned, &ks, "g", &["A".to_string()], i64::MIN, true).await.unwrap();
        assert_eq!(seen, vec!["A".to_string(), "B".to_string()]);

        // A fresh view (taken now) sees the new edge.
        let fresh = database.substrate().read_view().await.unwrap();
        let seen_now = reachable(&fresh, &ks, "g", &["A".to_string()], i64::MIN, true).await.unwrap();
        assert_eq!(seen_now, vec!["A".to_string(), "B".to_string(), "C".to_string()]);
    }

    /// The Jepsen graph-swap invariant, in miniature. Config A = {R→A, A→Z};
    /// the writer atomically rewires to config B = {R→B, B→Z} (delete the A
    /// pair + add the B pair in one batch). Under a pinned snapshot every
    /// traversal sees exactly one config — the sink Z is *always* reachable and
    /// the result is always size 3 — never the torn `{R,A}` (Z dropped) a
    /// non-snapshot read could produce when the swap lands between its scan of
    /// R and its scan of the bridge.
    #[tokio::test]
    async fn atomic_swap_never_drops_the_sink() {
        let database = db().await;
        let g = Graph::new(&database, "_");
        let ks = EvidenceKeyspace::new("_");
        let e = |s: &str, d: &str| EdgeUpsert { src: s.into(), dst: d.into(), weight: 1, etype: String::new() };
        let r = |s: &str, d: &str| EdgeRef { src: s.into(), dst: d.into(), etype: String::new() };

        // Seed config A.
        g.mutate("g", &[e("R", "A"), e("A", "Z")], &[], Merge::Set).await.unwrap();

        // Pin a view on config A, then atomically swap A → B.
        let pinned = database.substrate().read_view().await.unwrap();
        g.mutate("g", &[e("R", "B"), e("B", "Z")], &[r("R", "A"), r("A", "Z")], Merge::Set).await.unwrap();

        // The pinned snapshot still sees config A in full — Z reachable.
        let from_r = vec!["R".to_string()];
        let pre = reachable(&pinned, &ks, "g", &from_r, i64::MIN, true).await.unwrap();
        assert_eq!(pre, vec!["A".to_string(), "R".to_string(), "Z".to_string()]);
        assert!(pre.contains(&"Z".to_string()), "sink must stay reachable on the pinned cut");

        // A fresh view sees config B in full (the swap was all-or-nothing) — Z
        // still reachable, never the torn {R, A}.
        let fresh = database.substrate().read_view().await.unwrap();
        let post = reachable(&fresh, &ks, "g", &from_r, i64::MIN, true).await.unwrap();
        assert_eq!(post, vec!["B".to_string(), "R".to_string(), "Z".to_string()]);
    }
}
