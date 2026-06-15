//! The durable CDC log that backs the lakehouse mirror: a global monotonic
//! sequence, change capture into the same `WriteBatch` as the data, and
//! scan/gc over the log.

use std::sync::Arc;

use bluedb_sql::Database;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

#[tokio::test]
async fn cdc_seq_is_monotonic_and_starts_at_one() {
    let db = Arc::new(Db::open("cdc", Arc::new(InMemory::new())).await.unwrap());
    let database = Database::new(db);
    assert_eq!(database.next_cdc_seq().await.unwrap(), 1);
    assert_eq!(database.next_cdc_seq().await.unwrap(), 2);
}
