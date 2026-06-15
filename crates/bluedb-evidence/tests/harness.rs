use std::sync::Arc;

use bluedb_sql::Database;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

/// Open a brand-new in-memory writer database for a test.
pub async fn memory_db() -> Database {
    let db = Db::open("evidence-test", Arc::new(InMemory::new()))
        .await
        .expect("open in-memory db");
    Database::new(Arc::new(db))
}
