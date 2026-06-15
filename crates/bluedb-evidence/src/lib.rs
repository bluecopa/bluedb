//! bluedb-evidence: append-only, verifiable evidence chains + a native graph
//! store, for an external event-sourced evidence-graph service. Mirrors the
//! bluedb-ledger subsystem shape (native postcard records + atomic WriteBatch
//! inside the serialized writer).

mod error;
mod graph;
mod keyspace;
mod merkle;
mod model;
mod store;
mod traverse;
pub mod chain;

pub use chain::{Appended, ConsistencyProof, Digest, EntryInput, Evidence, InclusionProof};
pub use error::EvidenceError;
pub use graph::{EdgeRef, EdgeUpsert, Graph};
pub use model::{ChainMeta, EdgeDelta, EdgeOp, EntryRecord, IdemRecord, Merge};
pub use traverse::WidestPath;
