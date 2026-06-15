//! Read-only graph traversal over the `out`/`in` adjacency indexes (Plan 3).
//! A node's edges are a bounded prefix range scan with the weight folded
//! order-preserving into the key. No write lease — works on a read replica.
//! v1 caveat: there is no cross-scan snapshot, so a traversal sees read-committed
//! state across its scans; output is sorted (`reachable`) / maximin-unique
//! (`widest_path`), so it is deterministic for a fixed graph.

use bluedb_storage::Substrate;

use crate::error::EvidenceError;
use crate::keyspace::{weight_from_obe, EvidenceKeyspace};

/// Read `(neighbor, weight, type)` for every edge under `prefix`, keeping only
/// edges with `weight >= floor`. `prefix` is an out/in adjacency scan prefix;
/// the bytes after it are `weight_obe(8) ‖ neighbor_lp ‖ type_lp`.
async fn scan_adjacency(
    substrate: &Substrate,
    prefix: &[u8],
    floor: i64,
) -> Result<Vec<(String, i64, String)>, EvidenceError> {
    let mut start = prefix.to_vec();
    start.extend_from_slice(&crate::keyspace::weight_obe(floor));
    let end = bluedb_sql::prefix_upper_bound(prefix);
    let mut iter = substrate.scan_range(&start, end.as_deref()).await?;
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
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    graph: &str,
    src: &str,
    floor: i64,
) -> Result<Vec<(String, i64, String)>, EvidenceError> {
    scan_adjacency(substrate, &ks.graph_out_prefix(graph, src), floor).await
}

/// In-neighbors `(src, weight, type)` of `dst` with `weight >= floor`, ascending.
pub(crate) async fn in_neighbors(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    graph: &str,
    dst: &str,
    floor: i64,
) -> Result<Vec<(String, i64, String)>, EvidenceError> {
    scan_adjacency(substrate, &ks.graph_in_prefix(graph, dst), floor).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use bluedb_sql::Database;
    use slatedb::{object_store::memory::InMemory, Db};
    use crate::graph::{Graph, EdgeUpsert};
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

        let substrate = database.substrate();
        let ks = EvidenceKeyspace::new("_");
        let all = out_neighbors(&substrate, &ks, "g", "A", i64::MIN).await.unwrap();
        assert_eq!(all, vec![
            ("B".to_string(), 2, "x".to_string()),
            ("C".to_string(), 5, "".to_string()),
            ("B".to_string(), 9, "y".to_string()),
        ]);
        let hi = out_neighbors(&substrate, &ks, "g", "A", 5).await.unwrap();
        assert_eq!(hi, vec![
            ("C".to_string(), 5, "".to_string()),
            ("B".to_string(), 9, "y".to_string()),
        ]);
        let inb = in_neighbors(&substrate, &ks, "g", "B", i64::MIN).await.unwrap();
        assert_eq!(inb, vec![
            ("A".to_string(), 2, "x".to_string()),
            ("A".to_string(), 9, "y".to_string()),
        ]);
    }
}
