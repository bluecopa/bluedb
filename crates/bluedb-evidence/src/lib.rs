//! bluedb-evidence: append-only, verifiable evidence chains + a native graph
//! store, for an external event-sourced evidence-graph service. Mirrors the
//! bluedb-ledger subsystem shape (native postcard records + atomic WriteBatch
//! inside the serialized writer).

pub mod chain;
mod error;
mod graph;
mod keyspace;
mod merkle;
mod model;
mod proof;
mod sth;
mod store;
mod traverse;

pub use chain::{Appended, ConsistencyProof, Digest, EntryInput, Evidence, InclusionProof};
pub use error::EvidenceError;
pub use graph::{EdgeRef, EdgeUpsert, Graph};
pub use model::{ChainMeta, EdgeDelta, EdgeOp, EntryRecord, IdemRecord, Merge};
pub use sth::sth_payload;
pub use traverse::WidestPath;
