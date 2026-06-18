//! Plan-time guardrail: keep every read **bounded**, so no client can trip an
//! unbounded full-table scan or an in-memory sort over a bluedb table.
//!
//! bluedb is an indexed point/range OLTP store — a read should be served by the
//! primary key or a secondary index. Anything that wants to scan or sort over
//! non-indexed data belongs in the warehouse, reached through the Iceberg mirror.
//! This pass runs inside [`crate::storage`]'s `Planner::plan` override (so it sees
//! the schema map: primary key + secondary indexes) and, for a single-table
//! `SELECT`:
//!
//! * **Index/PK-served `WHERE`** → allowed, any size. You bound it yourself with
//!   the predicate.
//! * **No `WHERE` at all** → *not* rejected. The query is auto-bounded to
//!   [`UNFILTERED_SCAN_CAP`] rows by injecting/capping a `LIMIT`. Rows stream out
//!   of the store in primary-key order, so a bare `SELECT * FROM t` becomes
//!   "the first 100 rows by primary key" — a bounded prefix scan, no sort. This
//!   is the safe-browse default.
//! * **`WHERE` on only non-indexed columns** → rejected. A `LIMIT` would *not*
//!   bound it: the filter runs after the scan, so it can read the whole table to
//!   find even a handful of matches.
//! * **`ORDER BY` on a non-indexed column** → rejected. That is a real in-memory
//!   sort; a `LIMIT` does not make it cheap (you must scan everything to find the
//!   top N), and silently re-ordering by the primary key would return the wrong
//!   rows.
//!
//! It is **not overridable** — there is deliberately no client/PRAGMA/scope
//! bypass, so nobody can ask for an unbounded scan by accident or injection.
//! Engine internals never reach it because they go through the `Store` traits
//! directly, not `Glue::execute`. Conservative by construction — multi-table
//! queries are left to [`crate::pushdown::reject_cross_products`] + hash joins,
//! and anything it can't confidently classify passes through.

use std::collections::HashMap;
use std::collections::HashSet;

use bigdecimal::BigDecimal;
use gluesql_core::ast::{BinaryOperator, Expr, Literal, SetExpr, Statement, TableFactor};
use gluesql_core::data::Schema;
use gluesql_core::error::{Error, Result as GlueResult};

type SchemaMap = HashMap<String, Schema>;

/// Stable sentinel prefix that every guardrail-reject error message begins with.
///
/// Detection in the server (`bluedb-server::is_guardrail_reject`) checks for
/// this single prefix rather than multiple prose substrings, so it stays correct
/// even if the human-readable part of the message is later rephrased.
///
/// # Why a prefix rather than a Rust type?
///
/// `bound_or_reject` runs inside gluesql's `Planner::plan()` method, which is
/// part of the `gluesql_core::store::Planner` trait.  That trait method **must**
/// return `gluesql_core::error::Error`; there is no escape hatch for a
/// bluedb-defined variant above this boundary.  All bluedb storage errors
/// (including guardrail rejects) therefore reach the server as
/// `EngineError::Sql(Error::StorageMsg(String))`.  A sentinel prefix in that
/// `String` is the only reliable, non-fragile discriminant available.
pub const GUARDRAIL_REJECT_PREFIX: &str = "BLUEDB_GUARDRAIL_REJECT:";

/// The row ceiling an **unfiltered** single-table `SELECT` is bounded to. A bare
/// `SELECT * FROM t` reads at most this many rows, in primary-key order. To read
/// more, page with a primary-key predicate (`WHERE pk > cursor`), which is
/// index-served and uncapped.
pub const UNFILTERED_SCAN_CAP: u64 = 100;

/// Comparison operators a B-tree (PK or secondary index) can serve directly.
fn is_sargable_op(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Eq
            | BinaryOperator::Lt
            | BinaryOperator::Gt
            | BinaryOperator::LtEq
            | BinaryOperator::GtEq
    )
}

/// Bound or reject a query so it cannot full-scan or sort in memory, returning
/// the (possibly rewritten) statement.
///
/// An unfiltered single-table `SELECT` is returned with its `LIMIT` capped to
/// [`UNFILTERED_SCAN_CAP`]; everything index-served (or not something we guard —
/// a multi-table join, DDL, a query over an unknown/derived table) is returned
/// unchanged. A non-indexed `WHERE` or `ORDER BY` is an `Err`.
pub fn bound_or_reject(schema_map: &SchemaMap, mut statement: Statement) -> GlueResult<Statement> {
    match classify(schema_map, &statement)? {
        Decision::PassThrough => {}
        Decision::CapUnfiltered => cap_unfiltered_limit(&mut statement),
    }
    Ok(statement)
}

/// What [`bound_or_reject`] should do with a statement (or an `Err` to reject).
enum Decision {
    /// Index-served, or not a shape we guard — leave it alone.
    PassThrough,
    /// Unfiltered single-table SELECT — cap its `LIMIT` to the scan ceiling.
    CapUnfiltered,
}

fn classify(schema_map: &SchemaMap, statement: &Statement) -> GlueResult<Decision> {
    let Statement::Query(query) = statement else {
        return Ok(Decision::PassThrough);
    };
    let SetExpr::Select(select) = &query.body else {
        return Ok(Decision::PassThrough);
    };
    // Multi-table queries are governed by reject_cross_products + plan_join.
    if !select.from.joins.is_empty() {
        return Ok(Decision::PassThrough);
    }
    let TableFactor::Table { name, .. } = &select.from.relation else {
        return Ok(Decision::PassThrough);
    };
    let Some(schema) = schema_map.get(name) else {
        // Unknown table (e.g. a CTE/derived name) — let gluesql handle it.
        return Ok(Decision::PassThrough);
    };

    let indexable = indexable_columns(schema);

    // ORDER BY must be served by PK order or an index, with or without a WHERE,
    // else it is a real in-memory sort that a LIMIT would not make cheap.
    for ob in &query.order_by {
        let col = column_of(&ob.expr);
        if col.as_deref().is_none_or(|c| !indexable.contains(c)) {
            return Err(sort_err(name, col.as_deref()));
        }
    }

    match &select.selection {
        // No WHERE → bound it to the first N rows in PK order (safe browse).
        None => Ok(Decision::CapUnfiltered),
        // A WHERE that touches the PK or an index → bounded by the caller; allow
        // it at any size. A WHERE on only non-indexed columns is NOT bounded by a
        // LIMIT (the filter runs post-scan), so it is rejected.
        Some(expr) => {
            if where_hits_index(expr, &indexable) {
                Ok(Decision::PassThrough)
            } else {
                // Name the non-indexed columns the filter uses, so the error can
                // tell the user exactly which index to create.
                let mut cols = Vec::new();
                predicate_columns(expr, &mut cols);
                cols.retain(|c| !indexable.contains(c));
                Err(scan_err(name, &cols))
            }
        }
    }
}

/// Cap an unfiltered SELECT's `LIMIT` at [`UNFILTERED_SCAN_CAP`]: keep a smaller
/// explicit limit, but clamp a larger or absent one. A non-literal limit (e.g. a
/// parameter) is clamped to the ceiling too, since we can't prove it is smaller.
fn cap_unfiltered_limit(statement: &mut Statement) {
    let Statement::Query(query) = statement else {
        return;
    };
    let capped = match query.limit.as_ref().and_then(literal_u64) {
        Some(n) => n.min(UNFILTERED_SCAN_CAP),
        None => UNFILTERED_SCAN_CAP,
    };
    query.limit = Some(Expr::Literal(Literal::Number(BigDecimal::from(capped))));
}

/// The `u64` value of a `LIMIT` literal, if it is a non-negative integer literal.
fn literal_u64(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Literal(Literal::Number(bd)) => bd.to_string().parse::<u64>().ok(),
        _ => None,
    }
}

/// The set of columns that can be searched/ordered without a scan: the primary
/// key column(s) and every secondary-indexed column.
fn indexable_columns(schema: &Schema) -> HashSet<String> {
    let mut set = HashSet::new();
    if let Some(defs) = &schema.column_defs {
        for col in defs {
            if col.unique.as_ref().is_some_and(|u| u.is_primary) {
                set.insert(col.name.clone());
            }
        }
    }
    for index in &schema.indexes {
        if let Some(col) = column_of(&index.expr) {
            set.insert(col);
        }
    }
    set
}

/// True if some top-level `AND` conjunct is a sargable predicate against an
/// indexable column. A disjunction (`OR`) is treated conservatively — not
/// index-served unless we can prove otherwise (we don't try), so it fails here.
fn where_hits_index(expr: &Expr, indexable: &HashSet<String>) -> bool {
    for conjunct in split_and(expr) {
        if conjunct_hits_index(conjunct, indexable) {
            return true;
        }
    }
    false
}

fn conjunct_hits_index(expr: &Expr, indexable: &HashSet<String>) -> bool {
    match expr {
        Expr::Nested(inner) => conjunct_hits_index(inner, indexable),
        Expr::BinaryOp { left, op, right } if is_sargable_op(op) => {
            (is_indexable(left, indexable) && column_of(right).is_none())
                || (is_indexable(right, indexable) && column_of(left).is_none())
        }
        Expr::Between { expr, negated: false, .. } => is_indexable(expr, indexable),
        Expr::InList { expr, negated: false, .. } => is_indexable(expr, indexable),
        _ => false,
    }
}

fn is_indexable(expr: &Expr, indexable: &HashSet<String>) -> bool {
    column_of(expr).is_some_and(|c| indexable.contains(&c))
}

/// The column name an expression refers to, if it is a (possibly qualified or
/// parenthesized) column reference.
fn column_of(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(name) => Some(name.clone()),
        Expr::CompoundIdentifier { ident, .. } => Some(ident.clone()),
        Expr::Nested(inner) => column_of(inner),
        _ => None,
    }
}

/// Collect the column names that appear in a sargable position anywhere in
/// `expr` (recursing through `AND`/`OR` and nesting), so a rejection can name the
/// columns the user should index. Best-effort — only the shapes the planner could
/// have served from an index.
fn predicate_columns(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Nested(inner) => predicate_columns(inner, out),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                predicate_columns(left, out);
                predicate_columns(right, out);
            }
            _ if is_sargable_op(op) => {
                if let Some(c) = column_of(left).or_else(|| column_of(right)) {
                    out.push(c);
                }
            }
            _ => {}
        },
        Expr::Between { expr, .. } => {
            if let Some(c) = column_of(expr) {
                out.push(c);
            }
        }
        Expr::InList { expr, .. } => {
            if let Some(c) = column_of(expr) {
                out.push(c);
            }
        }
        _ => {}
    }
}

/// The exact `CREATE INDEX` statement that would make a query on `column` of
/// `table` index-served — handed back in the rejection so the user can copy it,
/// the way Firestore surfaces the index its query needs.
fn create_index_ddl(table: &str, column: &str) -> String {
    format!("CREATE INDEX {table}_{column} ON {table} ({column});")
}

/// Split a boolean expression on its top-level `AND`s.
fn split_and(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
            let mut out = split_and(left);
            out.extend(split_and(right));
            out
        }
        Expr::Nested(inner) => split_and(inner),
        other => vec![other],
    }
}

fn scan_err(table: &str, unindexed: &[String]) -> Error {
    // Order-preserving de-dup of the offending columns.
    let mut cols: Vec<&String> = Vec::new();
    for c in unindexed {
        if !cols.contains(&c) {
            cols.push(c);
        }
    }
    match cols.first() {
        Some(first) => {
            let list = cols.iter().map(|c| format!("`{c}`")).collect::<Vec<_>>().join(", ");
            Error::StorageMsg(format!(
                "{pfx} query on `{table}` filters only non-indexed column(s) {list} — \
                 this would scan the whole table (a LIMIT can't bound a post-scan filter). \
                 Create an index and retry, e.g.\n    {ddl}\nthen filter on that column \
                 (one index is enough). Or filter on the primary key, or run the scan in \
                 the warehouse via the Iceberg mirror.",
                pfx = GUARDRAIL_REJECT_PREFIX,
                ddl = create_index_ddl(table, first),
            ))
        }
        // No nameable column (e.g. a `LIKE`-only predicate) — generic guidance.
        None => Error::StorageMsg(format!(
            "{pfx} query on `{table}` would scan the whole table — it filters no \
             indexed column. Filter on the primary key or an indexed column (declare a \
             trigram index to accelerate `LIKE`), or run the scan in the warehouse via \
             the Iceberg mirror.",
            pfx = GUARDRAIL_REJECT_PREFIX,
        )),
    }
}

fn sort_err(table: &str, column: Option<&str>) -> Error {
    match column {
        Some(col) => Error::StorageMsg(format!(
            "{pfx} ORDER BY `{col}` on `{table}` is an in-memory sort — `{col}` is \
             neither the primary key nor indexed. Create an index and retry, e.g.\n    {ddl}\n\
             then order by `{col}`. Or order by the primary key, or sort in the warehouse \
             via the Iceberg mirror.",
            pfx = GUARDRAIL_REJECT_PREFIX,
            ddl = create_index_ddl(table, col),
        )),
        None => Error::StorageMsg(format!(
            "{pfx} in-memory sort on `{table}` — ORDER BY must use the primary key or \
             an indexed column, or be sorted in the warehouse via the Iceberg mirror.",
            pfx = GUARDRAIL_REJECT_PREFIX,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gluesql_core::ast::{ColumnDef, ColumnUniqueOption, DataType, Expr};
    use gluesql_core::data::{SchemaIndex, SchemaIndexOrd};

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

    /// `cols`: (name, is_primary_key). `indexed`: secondary-indexed column names.
    fn schema(table: &str, cols: &[(&str, bool)], indexed: &[&str]) -> Schema {
        let created = chrono::DateTime::from_timestamp(0, 0).unwrap().naive_utc();
        Schema {
            table_name: table.to_owned(),
            column_defs: Some(cols.iter().map(|(n, pk)| col(n, *pk)).collect()),
            indexes: indexed
                .iter()
                .map(|c| SchemaIndex {
                    name: format!("idx_{c}"),
                    expr: Expr::Identifier((*c).to_owned()),
                    order: SchemaIndexOrd::Both,
                    created,
                })
                .collect(),
            engine: None,
            foreign_keys: Vec::new(),
            comment: None,
        }
    }

    fn map(schemas: Vec<Schema>) -> SchemaMap {
        schemas.into_iter().map(|s| (s.table_name.clone(), s)).collect()
    }

    /// Run the guardrail and return the (possibly rewritten) statement.
    fn run(m: &SchemaMap, sql: &str) -> GlueResult<Statement> {
        let parsed = gluesql_core::parse_sql::parse(sql).unwrap();
        let stmt = gluesql_core::translate::translate(&parsed[0]).unwrap();
        bound_or_reject(m, stmt)
    }

    /// The `LIMIT` of a (rewritten) single SELECT, if any.
    fn limit_of(stmt: &Statement) -> Option<u64> {
        let Statement::Query(q) = stmt else { return None };
        q.limit.as_ref().and_then(super::literal_u64)
    }

    #[test]
    fn bare_select_is_capped_not_rejected() {
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        let stmt = run(&m, "SELECT * FROM t").expect("bare scan is bounded, not rejected");
        assert_eq!(limit_of(&stmt), Some(UNFILTERED_SCAN_CAP));
    }

    #[test]
    fn bare_select_clamps_a_larger_explicit_limit() {
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        let stmt = run(&m, "SELECT * FROM t LIMIT 500").unwrap();
        assert_eq!(limit_of(&stmt), Some(UNFILTERED_SCAN_CAP));
    }

    #[test]
    fn bare_select_keeps_a_smaller_explicit_limit() {
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        let stmt = run(&m, "SELECT * FROM t LIMIT 10").unwrap();
        assert_eq!(limit_of(&stmt), Some(10));
    }

    #[test]
    fn bare_select_ordered_by_pk_is_capped() {
        // No WHERE but ORDER BY the primary key: a bounded PK-prefix scan once
        // capped, so it is allowed (it used to be rejected outright).
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        let stmt = run(&m, "SELECT * FROM t ORDER BY id").unwrap();
        assert_eq!(limit_of(&stmt), Some(UNFILTERED_SCAN_CAP));
    }

    #[test]
    fn bare_select_ordered_by_unindexed_is_rejected() {
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        assert!(run(&m, "SELECT * FROM t ORDER BY name").is_err());
    }

    #[test]
    fn point_lookup_on_primary_key_is_unbounded() {
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        let stmt = run(&m, "SELECT * FROM t WHERE id = 5").unwrap();
        assert_eq!(limit_of(&stmt), None, "index-served reads are not capped");
    }

    #[test]
    fn range_on_primary_key_is_unbounded() {
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        let stmt = run(&m, "SELECT * FROM t WHERE id > 5").unwrap();
        assert_eq!(limit_of(&stmt), None);
    }

    #[test]
    fn predicate_on_unindexed_column_is_rejected() {
        // A LIMIT cannot bound this — the filter runs after the scan.
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        assert!(run(&m, "SELECT * FROM t WHERE name = 'x'").is_err());
    }

    #[test]
    fn predicate_on_secondary_index_is_allowed() {
        let m = map(vec![schema("t", &[("id", true), ("email", false)], &["email"])]);
        assert!(run(&m, "SELECT * FROM t WHERE email = 'a@b.c'").is_ok());
    }

    #[test]
    fn indexed_predicate_anded_with_unindexed_is_allowed() {
        // The index narrows; gluesql filters the rest. One sargable conjunct is enough.
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        assert!(run(&m, "SELECT * FROM t WHERE id = 5 AND name = 'x'").is_ok());
    }

    #[test]
    fn order_by_unindexed_column_is_rejected() {
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        assert!(run(&m, "SELECT * FROM t WHERE id > 1 ORDER BY name").is_err());
    }

    #[test]
    fn order_by_primary_key_is_allowed() {
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        assert!(run(&m, "SELECT * FROM t WHERE id >= 1 ORDER BY id").is_ok());
    }

    #[test]
    fn or_across_unindexed_is_rejected() {
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        assert!(run(&m, "SELECT * FROM t WHERE id = 1 OR name = 'x'").is_err());
    }

    #[test]
    fn unindexed_filter_error_suggests_the_exact_index() {
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        let err = run(&m, "SELECT * FROM t WHERE name = 'x'").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("`name`"), "names the offending column: {msg}");
        assert!(
            msg.contains("CREATE INDEX t_name ON t (name);"),
            "hands back the exact DDL to create: {msg}"
        );
    }

    #[test]
    fn unindexed_order_by_error_suggests_the_exact_index() {
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        let err = run(&m, "SELECT * FROM t WHERE id > 1 ORDER BY name").unwrap_err();
        assert!(
            err.to_string().contains("CREATE INDEX t_name ON t (name);"),
            "hands back the exact DDL for the sort column: {err}"
        );
    }
}
