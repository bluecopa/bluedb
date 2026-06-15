//! GlueSQL's official custom-storage conformance suite, run against
//! [`SlateDbStorage`].
//!
//! Unlike the sqllogictest corpus (which measures SQL-surface breadth through
//! gluesql + our string rewrites), this battery validates that our storage-trait
//! impls — `Store`/`StoreMut`/`Index`/`Transaction`/`AlterTable`/`Metadata` — and
//! our `Planner` override obey the contract gluesql expects. It is the same suite
//! every gluesql backend (memory, sled, redb, …) runs.
//!
//! Each test gets a fresh in-memory SlateDB. The suite parses + translates with
//! gluesql's own front end and then calls `storage.plan(...)`, so our `Planner`
//! override (pushdown / cross-product rejection / text↔number coercion) is
//! exercised — failures here are either real storage bugs or our *intentional*
//! deviations from gluesql-native semantics.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bluedb_sql::SlateDbStorage;
use gluesql_core::prelude::Glue;
use gluesql_test_suite::*;
use slatedb::config::Settings;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

struct SlateDbTester {
    glue: Glue<SlateDbStorage>,
}

#[async_trait(?Send)]
impl Tester<SlateDbStorage> for SlateDbTester {
    async fn new(_namespace: &str) -> Self {
        // Ephemeral in-memory store; 1ms flush keeps the per-test durable writes
        // fast (the suite runs many small statements).
        let settings = Settings {
            flush_interval: Some(Duration::from_millis(1)),
            ..Default::default()
        };
        let db = Db::builder("bluedb-suite", Arc::new(InMemory::new()))
            .with_settings(settings)
            .build()
            .await
            .expect("open in-memory slatedb");
        SlateDbTester {
            glue: Glue::new(SlateDbStorage::new(Arc::new(db))),
        }
    }

    fn get_glue(&mut self) -> &mut Glue<SlateDbStorage> {
        &mut self.glue
    }
}

generate_store_tests!(tokio::test, SlateDbTester);
generate_alter_table_tests!(tokio::test, SlateDbTester);
generate_index_tests!(tokio::test, SlateDbTester);
generate_transaction_tests!(tokio::test, SlateDbTester);
generate_metadata_table_tests!(tokio::test, SlateDbTester);
generate_metadata_index_tests!(tokio::test, SlateDbTester);

// NOT run: `generate_custom_function_tests!` — user-defined functions are
// unsupported by design (CustomFunction(Mut) are the empty defaults).
