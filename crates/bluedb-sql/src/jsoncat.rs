//! JSON-column catalog: remembers which columns of a table were declared
//! `JSON`/`JSONB` so the read path can re-inflate their stored `TEXT` to real
//! JSON.
//!
//! GlueSQL has no JSON type, so [`crate::rewrite::normalize_data_type`] maps
//! `JSON`/`JSONB` → `TEXT` before the engine sees the DDL — which erases the
//! JSON-ness. This catalog (persisted per table, like
//! [`crate::compositepk::PkCatalog`]) is the out-of-band record of it: captured
//! at `CREATE TABLE`, consulted by the server's `/tables` row serializer to emit
//! a JSON column's text as a real JSON object/array instead of an escaped string.

use serde::{Deserialize, Serialize};
use sqlparser::ast::{CreateTable, DataType};

/// The columns of a table declared `JSON`/`JSONB` (stored as `TEXT`). Absent for
/// tables with no JSON column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonCatalog {
    /// The JSON column names, in declaration order.
    pub columns: Vec<String>,
}

impl JsonCatalog {
    /// True if `column` is a JSON column of this table.
    pub fn contains(&self, column: &str) -> bool {
        self.columns.iter().any(|c| c == column)
    }
}

/// The `JSON`/`JSONB` columns of a `CREATE TABLE`, by the rendered base type word
/// (so it works *before* `normalize_data_type` rewrites `JSON` → `TEXT`). Empty
/// if the table declares no JSON column.
pub fn json_columns(create: &CreateTable) -> Vec<String> {
    create
        .columns
        .iter()
        .filter(|c| is_json_type(&c.data_type))
        .map(|c| c.name.value.clone())
        .collect()
}

/// True if `data_type` is `JSON` or `JSONB` (compared by rendered base word, the
/// same way `normalize_data_type` classifies types).
fn is_json_type(data_type: &DataType) -> bool {
    let rendered = data_type.to_string().to_ascii_uppercase();
    let base = rendered.split(['(', ' ']).next().unwrap_or("");
    base == "JSON" || base == "JSONB"
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    fn create_of(sql: &str) -> CreateTable {
        let stmts = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        match stmts.into_iter().next().unwrap() {
            sqlparser::ast::Statement::CreateTable(c) => c,
            other => panic!("expected CREATE TABLE, got {other:?}"),
        }
    }

    #[test]
    fn detects_json_and_jsonb_columns() {
        let c =
            create_of("CREATE TABLE t (id INTEGER PRIMARY KEY, data JSON, meta JSONB, name TEXT)");
        assert_eq!(
            json_columns(&c),
            vec!["data".to_string(), "meta".to_string()]
        );
    }

    #[test]
    fn no_json_columns_yields_empty() {
        let c = create_of("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        assert!(json_columns(&c).is_empty());
    }

    #[test]
    fn catalog_contains_lookup() {
        let cat = JsonCatalog {
            columns: vec!["data".into(), "meta".into()],
        };
        assert!(cat.contains("data"));
        assert!(!cat.contains("name"));
    }
}
