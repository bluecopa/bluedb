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

#[cfg(test)]
mod tests {
    use super::*;

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
