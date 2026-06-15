use serde::{Deserialize, Serialize};

/// One event in an evidence chain. `etype`/`payload`/`at` are opaque to bluedb.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntryRecord {
    pub etype: String,
    pub payload: Vec<u8>,
    pub at: String,
    pub edges: Vec<EdgeDelta>,
    pub leaf_hash: Option<[u8; 32]>,
    pub redacted: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeDelta {
    pub graph: String,
    pub src: String,
    pub dst: String,
    pub weight: i64,
    pub etype: String,
    pub op: EdgeOp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EdgeOp {
    Upsert { merge: Merge },
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Merge {
    Set,
    Max,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdemRecord {
    pub base_seq: i64,
    pub seqs: Vec<i64>,
    pub fingerprint: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainMeta {
    pub verified: bool,
}

/// RFC 6962 incremental Merkle frontier: the ≤ log N perfect-subtree roots
/// ("peaks") covering `size` leaves, ordered left→right (largest subtree first).
/// Persisted per verified chain and advanced in the same WriteBatch as entries.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frontier {
    pub size: i64,
    pub peaks: Vec<[u8; 32]>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frontier_roundtrips_through_postcard() {
        let f = Frontier { size: 3, peaks: vec![[1u8; 32], [2u8; 32]] };
        let bytes = postcard::to_allocvec(&f).unwrap();
        let back: Frontier = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(f, back);
    }

    #[test]
    fn entry_record_roundtrips_through_postcard() {
        let rec = EntryRecord {
            etype: "attestation".into(),
            payload: vec![0u8, 159, 146, 150],
            at: "2026-06-15T10:00:00Z".into(),
            edges: vec![EdgeDelta {
                graph: "lineage".into(),
                src: "D1".into(),
                dst: "D2".into(),
                weight: 5,
                etype: String::new(),
                op: EdgeOp::Upsert { merge: Merge::Set },
            }],
            leaf_hash: None,
            redacted: false,
        };
        let bytes = postcard::to_allocvec(&rec).unwrap();
        let back: EntryRecord = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(rec, back);
    }
}
