//! `bluedb-search` — an Elasticsearch-shaped search DSL translated onto tantivy.
//!
//! Pure translation only (no I/O): ES request/response models, the ES mapping →
//! tantivy schema mapping, the ES Query-DSL → tantivy query lowering, and the ES
//! hits-envelope assembly. The bluedb-server wiring layer owns all I/O.
pub mod error;
pub mod mapping;
pub mod model;
pub use error::{Result, SearchError};
