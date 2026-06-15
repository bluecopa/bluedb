//! bluedb-evidence: append-only, verifiable evidence chains + a native graph
//! store, for an external event-sourced evidence-graph service. Mirrors the
//! bluedb-ledger subsystem shape (native postcard records + atomic WriteBatch
//! inside the serialized writer).

mod error;
mod keyspace;
mod model;
mod store;
pub mod chain;

pub use chain::{Appended, EntryInput, Evidence};
pub use error::EvidenceError;
pub use model::{ChainMeta, EdgeDelta, EdgeOp, EntryRecord, IdemRecord, Merge};
