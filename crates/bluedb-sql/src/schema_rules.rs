//! bluedb's schema regime, enforced at the user surface (a "strict" connection).
//!
//! bluedb is a schema'd OLTP store: schemaless tables are gone, and every table
//! must declare a `PRIMARY KEY` (it is the physical row key and the only
//! guaranteed point/range access path — and the Iceberg identity column for the
//! lakehouse mirror). These rules are enforced on `insert_schema` for *strict*
//! connections (the user-facing surface). The raw engine stays a faithful
//! GlueSQL storage backend — so the conformance suite, which legitimately
//! exercises schemaless and PK-less tables, keeps passing against an unguarded
//! connection.

use gluesql_core::data::Schema;
use gluesql_core::error::{Error, Result as GlueResult};

/// Reject a schema that bluedb's regime disallows: a schemaless table
/// (`column_defs == None`) or a table without a `PRIMARY KEY`.
pub fn enforce(schema: &Schema) -> GlueResult<()> {
    let Some(columns) = &schema.column_defs else {
        return Err(Error::StorageMsg(format!(
            "rejected: schemaless tables are not supported — declare columns for \
             `{}` (e.g. CREATE TABLE {0} (id INTEGER PRIMARY KEY, ...))",
            schema.table_name
        )));
    };
    let has_pk = columns
        .iter()
        .any(|c| c.unique.as_ref().is_some_and(|u| u.is_primary));
    if !has_pk {
        return Err(Error::StorageMsg(format!(
            "rejected: table `{}` must declare a PRIMARY KEY (it is the row key \
             and the only guaranteed access path)",
            schema.table_name
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gluesql_core::ast::{ColumnDef, ColumnUniqueOption, DataType};

    fn col(name: &str, pk: bool) -> ColumnDef {
        ColumnDef {
            name: name.to_owned(),
            data_type: DataType::Int,
            nullable: !pk,
            default: None,
            unique: pk.then_some(ColumnUniqueOption { is_primary: true }),
            comment: None,
        }
    }

    fn schema(column_defs: Option<Vec<ColumnDef>>) -> Schema {
        Schema {
            table_name: "t".to_owned(),
            column_defs,
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            comment: None,
        }
    }

    #[test]
    fn rejects_schemaless_table() {
        assert!(enforce(&schema(None)).is_err());
    }

    #[test]
    fn rejects_table_without_primary_key() {
        let s = schema(Some(vec![col("id", false), col("name", false)]));
        assert!(enforce(&s).is_err());
    }

    #[test]
    fn allows_table_with_primary_key() {
        let s = schema(Some(vec![col("id", true), col("name", false)]));
        assert!(enforce(&s).is_ok());
    }
}
