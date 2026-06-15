//! Native directed-weighted-typed graph store. Edge identity is
//! `(graph, src, dst, type)`; `out`/`in` adjacency indexes fold the weight into
//! the key so traversal (Plan 4) is a bounded prefix range scan. Mutations go
//! through [`apply_edge_delta`], which maintains canonical+out+in atomically and
//! provides read-your-own-writes within one batch (SlateDB `WriteBatch` alone
//! does not). The graph is a reproducible projection of the evidence chain: the
//! hot path is append-with-edges (`chain.rs`); this standalone handle is the
//! bulk / rebuild / scratch path for edges with no originating event.

use std::collections::HashMap;

use bluedb_sql::{Database, WriteLease};
use bluedb_storage::Substrate;
use slatedb::config::WriteOptions;
use slatedb::WriteBatch;

use crate::error::EvidenceError;
use crate::keyspace::{weight_obe, EvidenceKeyspace};
use crate::model::{EdgeDelta, EdgeOp, Merge};
use crate::store;

/// Presence-only marker value for `out`/`in` index keys (never read).
const MARK: [u8; 1] = [1u8];

/// One edge to upsert via the standalone API.
#[derive(Debug, Clone)]
pub struct EdgeUpsert {
    pub src: String,
    pub dst: String,
    pub weight: i64,
    pub etype: String,
}

/// One edge identity to delete via the standalone API (weight not needed).
#[derive(Debug, Clone)]
pub struct EdgeRef {
    pub src: String,
    pub dst: String,
    pub etype: String,
}

/// Apply one [`EdgeDelta`] into `batch`, maintaining canonical + out + in and
/// the in-batch `overlay` (`canonical_key_bytes -> Some(weight) | None`).
///
/// - `Upsert { Set }`: new weight = delta weight. `Upsert { Max }`: new weight =
///   `max(existing, delta)` (existing from overlay, else committed store).
/// - On any weight change (or first insert) the stale out/in keys (old weight)
///   are deleted before the new canonical+out+in are written.
/// - `Delete`: removes canonical+out+in if the edge exists (no-op otherwise).
pub(crate) async fn apply_edge_delta(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    batch: &mut WriteBatch,
    overlay: &mut HashMap<Vec<u8>, Option<i64>>,
    d: &EdgeDelta,
) -> Result<(), EvidenceError> {
    let canon = ks.graph_edge_key(&d.graph, &d.src, &d.dst, &d.etype);
    let current: Option<i64> = match overlay.get(&canon) {
        Some(v) => *v,
        None => store::get_edge_weight(substrate, ks, &d.graph, &d.src, &d.dst, &d.etype).await?,
    };

    match &d.op {
        EdgeOp::Delete => {
            if let Some(w0) = current {
                batch.delete(canon.clone());
                batch.delete(ks.graph_out_key(&d.graph, &d.src, w0, &d.dst, &d.etype));
                batch.delete(ks.graph_in_key(&d.graph, &d.dst, w0, &d.src, &d.etype));
                overlay.insert(canon, None);
            }
        }
        EdgeOp::Upsert { merge } => {
            let new_w = match (current, merge) {
                (Some(w0), Merge::Max) => w0.max(d.weight),
                _ => d.weight,
            };
            if let Some(w0) = current {
                if w0 != new_w {
                    batch.delete(ks.graph_out_key(&d.graph, &d.src, w0, &d.dst, &d.etype));
                    batch.delete(ks.graph_in_key(&d.graph, &d.dst, w0, &d.src, &d.etype));
                }
            }
            batch.put(canon.clone(), weight_obe(new_w));
            batch.put(ks.graph_out_key(&d.graph, &d.src, new_w, &d.dst, &d.etype), MARK);
            batch.put(ks.graph_in_key(&d.graph, &d.dst, new_w, &d.src, &d.etype), MARK);
            overlay.insert(canon, Some(new_w));
        }
    }
    Ok(())
}

/// Standalone handle to the graph store in one bluedb database / tenant.
/// Mirrors [`crate::Evidence`]: writes need the active writer.
pub struct Graph {
    substrate: Substrate,
    write_lease: WriteLease,
    keyspace: EvidenceKeyspace,
}

impl Graph {
    pub fn new(database: &Database, tenant: &str) -> Self {
        Self {
            substrate: database.substrate(),
            write_lease: database.write_lease(),
            keyspace: EvidenceKeyspace::new(tenant),
        }
    }

    fn storage_err(e: impl std::fmt::Display) -> EvidenceError {
        EvidenceError::Storage(anyhow::anyhow!("{e}"))
    }

    /// Upsert `edges` into `graph` with the given `merge` mode. Atomic + durable.
    pub async fn upsert(
        &self,
        graph: &str,
        edges: &[EdgeUpsert],
        merge: Merge,
    ) -> Result<(), EvidenceError> {
        if edges.is_empty() {
            return Ok(());
        }
        let _lease = self.write_lease.lock().await;
        let writer = self.substrate.require_writer().map_err(|_| EvidenceError::NotWriter)?;
        let mut batch = WriteBatch::new();
        let mut overlay = HashMap::new();
        for e in edges {
            let d = EdgeDelta {
                graph: graph.to_string(),
                src: e.src.clone(),
                dst: e.dst.clone(),
                weight: e.weight,
                etype: e.etype.clone(),
                op: EdgeOp::Upsert { merge },
            };
            apply_edge_delta(&self.substrate, &self.keyspace, &mut batch, &mut overlay, &d).await?;
        }
        if batch.is_empty() {
            return Ok(());
        }
        writer
            .write_with_options(batch, &WriteOptions { await_durable: false, ..Default::default() })
            .await
            .map_err(Self::storage_err)?;
        drop(_lease);
        writer.flush().await.map_err(Self::storage_err)?;
        Ok(())
    }

    /// Delete `edges` (by identity) from `graph`. Atomic + durable. Deleting a
    /// non-existent edge is a no-op.
    pub async fn delete(&self, graph: &str, edges: &[EdgeRef]) -> Result<(), EvidenceError> {
        if edges.is_empty() {
            return Ok(());
        }
        let _lease = self.write_lease.lock().await;
        let writer = self.substrate.require_writer().map_err(|_| EvidenceError::NotWriter)?;
        let mut batch = WriteBatch::new();
        let mut overlay = HashMap::new();
        for e in edges {
            let d = EdgeDelta {
                graph: graph.to_string(),
                src: e.src.clone(),
                dst: e.dst.clone(),
                weight: 0,
                etype: e.etype.clone(),
                op: EdgeOp::Delete,
            };
            apply_edge_delta(&self.substrate, &self.keyspace, &mut batch, &mut overlay, &d).await?;
        }
        if batch.is_empty() {
            return Ok(());
        }
        writer
            .write_with_options(batch, &WriteOptions { await_durable: false, ..Default::default() })
            .await
            .map_err(Self::storage_err)?;
        drop(_lease);
        writer.flush().await.map_err(Self::storage_err)?;
        Ok(())
    }
}
