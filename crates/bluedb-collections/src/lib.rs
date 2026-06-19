//! MongoDB-style document API translation for bluedb: MQL shapes <=> bluedb SQL,
//! DataFusion `Expr`, and `DataFrame`. Pure translation -- no I/O.

pub mod error;
pub mod filter;
pub mod id;
pub mod index;
pub mod model;
pub mod pipeline;
pub mod project;
pub mod update;

pub use error::MqlError;
