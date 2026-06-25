//! Schema-as-data registry tests for [`bluedb_sql::SchemaRegistry`].
//!
//! The registry is a typed facade over the same tenant-namespaced schema
//! keyspace the SQL engine uses: register/list/get schemas without DDL, and
//! validate a row against a registered schema before it lands.

use std::sync::Arc;

use bluedb_sql::{SchemaRegistry, SlateDbStorage};
use gluesql_core::ast::{ColumnDef, DataType};
use gluesql_core::data::{Schema, Value};
use gluesql_core::store::DataRow;
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

async fn new_storage() -> SlateDbStorage {
    let object_store = Arc::new(InMemory::new());
    let db = Db::open("bluedb-sql-registry-test", object_store)
        .await
        .expect("open slatedb");
    SlateDbStorage::new(Arc::new(db))
}

fn col(name: &str, data_type: DataType, nullable: bool) -> ColumnDef {
    ColumnDef {
        name: name.to_owned(),
        data_type,
        nullable,
        default: None,
        unique: None,
        comment: None,
    }
}

/// `users(id INT NOT NULL, name TEXT NOT NULL)`.
fn users_schema() -> Schema {
    Schema {
        table_name: "users".to_owned(),
        column_defs: Some(vec![
            col("id", DataType::Int, false),
            col("name", DataType::Text, false),
        ]),
        indexes: vec![],
        engine: None,
        foreign_keys: vec![],
        comment: None,
    }
}

#[tokio::test]
async fn register_then_get_and_list() {
    let mut storage = new_storage().await;
    let mut registry = SchemaRegistry::new(&mut storage);

    assert!(
        registry.get("users").await.unwrap().is_none(),
        "absent before register"
    );
    assert!(registry.list().await.unwrap().is_empty());

    registry.register(&users_schema()).await.expect("register");

    let got = registry.get("users").await.unwrap().expect("present");
    assert_eq!(got.table_name, "users");
    assert_eq!(got.column_defs.as_ref().unwrap().len(), 2);

    let all = registry.list().await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].table_name, "users");
}

#[tokio::test]
async fn validate_row_accepts_a_conforming_row() {
    let mut storage = new_storage().await;
    let mut registry = SchemaRegistry::new(&mut storage);
    registry.register(&users_schema()).await.unwrap();

    let ok = DataRow::Vec(vec![Value::I64(1), Value::Str("alice".to_owned())]);
    registry
        .validate_row("users", &ok)
        .await
        .expect("conforming row validates");
}

#[tokio::test]
async fn validate_row_rejects_column_count_mismatch() {
    let mut storage = new_storage().await;
    let mut registry = SchemaRegistry::new(&mut storage);
    registry.register(&users_schema()).await.unwrap();

    let too_few = DataRow::Vec(vec![Value::I64(1)]);
    assert!(
        registry.validate_row("users", &too_few).await.is_err(),
        "1 value, 2 columns"
    );

    let too_many = DataRow::Vec(vec![
        Value::I64(1),
        Value::Str("a".to_owned()),
        Value::Str("extra".to_owned()),
    ]);
    assert!(
        registry.validate_row("users", &too_many).await.is_err(),
        "3 values, 2 columns"
    );
}

#[tokio::test]
async fn validate_row_rejects_type_mismatch() {
    let mut storage = new_storage().await;
    let mut registry = SchemaRegistry::new(&mut storage);
    registry.register(&users_schema()).await.unwrap();

    // id column is INT; a string there is a type error.
    let bad = DataRow::Vec(vec![
        Value::Str("not-an-int".to_owned()),
        Value::Str("a".to_owned()),
    ]);
    assert!(registry.validate_row("users", &bad).await.is_err());
}

#[tokio::test]
async fn validate_row_rejects_null_in_non_nullable_column() {
    let mut storage = new_storage().await;
    let mut registry = SchemaRegistry::new(&mut storage);
    registry.register(&users_schema()).await.unwrap();

    let bad = DataRow::Vec(vec![Value::Null, Value::Str("a".to_owned())]);
    assert!(
        registry.validate_row("users", &bad).await.is_err(),
        "NULL into NOT NULL id"
    );
}

#[tokio::test]
async fn validate_row_rejects_unregistered_table() {
    let mut storage = new_storage().await;
    let registry = SchemaRegistry::new(&mut storage);

    let row = DataRow::Vec(vec![Value::I64(1)]);
    assert!(
        registry.validate_row("ghost", &row).await.is_err(),
        "unregistered table"
    );
}

#[tokio::test]
async fn schemaless_table_accepts_any_row() {
    let mut storage = new_storage().await;
    let mut registry = SchemaRegistry::new(&mut storage);

    // A schema with no column_defs is schemaless: validation is a no-op.
    let schemaless = Schema {
        table_name: "docs".to_owned(),
        column_defs: None,
        indexes: vec![],
        engine: None,
        foreign_keys: vec![],
        comment: None,
    };
    registry.register(&schemaless).await.unwrap();

    let anything = DataRow::Vec(vec![Value::I64(1), Value::Str("whatever".to_owned())]);
    registry
        .validate_row("docs", &anything)
        .await
        .expect("schemaless accepts any row");
}

#[tokio::test]
async fn register_replaces_existing_schema() {
    let mut storage = new_storage().await;
    let mut registry = SchemaRegistry::new(&mut storage);
    registry.register(&users_schema()).await.unwrap();

    // Re-register the same table with a different column set; it replaces.
    let mut replacement = users_schema();
    replacement
        .column_defs
        .as_mut()
        .unwrap()
        .push(col("age", DataType::Int, true));
    registry.register(&replacement).await.unwrap();

    let got = registry.get("users").await.unwrap().unwrap();
    assert_eq!(
        got.column_defs.unwrap().len(),
        3,
        "replaced with the 3-column schema"
    );
    assert_eq!(
        registry.list().await.unwrap().len(),
        1,
        "still one table named users"
    );
}
