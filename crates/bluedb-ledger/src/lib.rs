//! `bluedb-ledger` — a TigerBeetle-style double-entry ledger over bluedb.
//!
//! Typed [`Account`]/[`Transfer`] records (u128 amounts) are applied inside
//! bluedb's serialized writer and committed as one atomic SlateDB
//! [`WriteBatch`](slatedb::WriteBatch), reusing the lease + epoch fencing +
//! durable-before-ack that the rest of bluedb already provides. See
//! `docs/superpowers/specs/2026-06-14-bluedb-ledger-design.md`.

mod keyspace;
mod ledger;
mod model;
mod store;

pub use ledger::Ledger;
pub use model::{
    Account, AccountFlags, CreateAccountResult, CreateTransferResult, Transfer, TransferFlags,
};
