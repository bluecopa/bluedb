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
    let pk_cols = primary_key_columns(schema);

    // Is the WHERE clause a **point set** on the PK (equality / IN-list)? When it
    // is, an in-memory ORDER BY is O(k log k) over the bounded match set, not a
    // scan-sort — so e.g. a full-text `ORDER BY ts_rank(...)` (rewritten to a
    // `pk IN (...)` predicate plus a CASE-on-pk ordering) is allowed. A range or
    // BETWEEN on an indexed column still bounds the *scan* but can match many
    // rows, so an unindexed ORDER BY over a range is still rejected.
    let where_point_bounded = match &select.selection {
        None => false,
        Some(expr) => where_is_point_bounded(&pk_cols, expr),
    };

    if !where_point_bounded {
        // ORDER BY must be served by PK order or an index, else it is a real
        // in-memory sort that a LIMIT would not make cheap.
        for ob in &query.order_by {
            let col = column_of(&ob.expr);
            if col.as_deref().is_none_or(|c| !indexable.contains(c)) {
                return Err(sort_err(name, col.as_deref()));
            }
        }
    }

    match &select.selection {
        // No WHERE → bound it to the first N rows in PK order (safe browse).
        None => Ok(Decision::CapUnfiltered),
        // A WHERE that touches the PK or an index → bounded by the caller; allow
        // it at any size. A WHERE on only non-indexed columns is NOT bounded by a
        // LIMIT (the filter runs post-scan), so it is rejected.
        Some(expr) => {
            if where_hits_index(schema_map, &pk_cols, expr, &indexable) {
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

/// The set of primary-key column names of a schema. Used to recognise the
/// `<pk> IN (SELECT …)` shape: an `InSubquery` is only index-served when its left
/// side is the table's own primary key (PK-membership), never an arbitrary column.
fn primary_key_columns(schema: &Schema) -> HashSet<String> {
    let mut set = HashSet::new();
    if let Some(defs) = &schema.column_defs {
        for col in defs {
            if col.unique.as_ref().is_some_and(|u| u.is_primary) {
                set.insert(col.name.clone());
            }
        }
    }
    set
}

/// True if some top-level `AND` conjunct is a sargable predicate against an
/// indexable column. A disjunction (`OR`) is treated conservatively — not
/// index-served unless we can prove otherwise (we don't try), so it fails here.
///
/// `pk_cols` is the *current* table's primary-key column set (for recognising a
/// `<pk> IN (SELECT …)` membership); `schema_map` is threaded so a PK-IN-subquery
/// can resolve the subquery table's own indexes (which it must hit for the whole
/// shape to be index-served). See [`in_subquery_hits_index`].
fn where_hits_index(
    schema_map: &SchemaMap,
    pk_cols: &HashSet<String>,
    expr: &Expr,
    indexable: &HashSet<String>,
) -> bool {
    for conjunct in split_and(expr) {
        if conjunct_hits_index(schema_map, pk_cols, conjunct, indexable) {
            return true;
        }
    }
    false
}

/// True if some top-level `AND` conjunct bounds the result to a **point set**
/// on the primary key — a PK equality (`pk = lit`) or a PK `IN (lit, …)` list.
/// Unlike [`where_hits_index`] (which also accepts ranges and `BETWEEN`), this
/// recognizes only shapes whose matched-row count is a known small set (e.g. a
/// full-text `pk IN (...)` match set), so an in-memory `ORDER BY` over that set
/// is genuinely O(k log k) and safe to allow even on a non-indexed column.
fn where_is_point_bounded(pk_cols: &HashSet<String>, expr: &Expr) -> bool {
    for conjunct in split_and(expr) {
        match conjunct {
            Expr::Nested(inner) => {
                if where_is_point_bounded(pk_cols, inner) {
                    return true;
                }
            }
            // pk = literal  (either side)
            Expr::BinaryOp { left, op: BinaryOperator::Eq, right }
                if is_pk(left, pk_cols) && column_of(right).is_none() =>
            {
                return true
            }
            Expr::BinaryOp { left, op: BinaryOperator::Eq, right }
                if is_pk(right, pk_cols) && column_of(left).is_none() =>
            {
                return true
            }
            // pk IN (literal, …)
            Expr::InList { expr, negated: false, .. } if is_pk(expr, pk_cols) => return true,
            _ => {}
        }
    }
    false
}

/// Is `expr` a bare reference to a primary-key column?
fn is_pk(expr: &Expr, pk_cols: &HashSet<String>) -> bool {
    column_of(expr).as_deref().is_some_and(|c| pk_cols.contains(c))
}

fn conjunct_hits_index(
    schema_map: &SchemaMap,
    pk_cols: &HashSet<String>,
    expr: &Expr,
    indexable: &HashSet<String>,
) -> bool {
    match expr {
        Expr::Nested(inner) => conjunct_hits_index(schema_map, pk_cols, inner, indexable),
        Expr::BinaryOp { left, op, right } if is_sargable_op(op) => {
            (is_indexable(left, indexable) && column_of(right).is_none())
                || (is_indexable(right, indexable) && column_of(left).is_none())
        }
        Expr::Between { expr, negated: false, .. } => is_indexable(expr, indexable),
        Expr::InList { expr, negated: false, .. } => is_indexable(expr, indexable),
        // `<pk> IN (SELECT <one col> FROM <one table> WHERE <indexed conjunct>)`.
        // PK membership is itself an index lookup, and the subquery must be
        // index-served on its *own* table — so the whole shape is bounded. Any
        // looser shape (negated, non-PK left, complex/joined subquery, or a
        // subquery WHERE that scans a non-indexed column) is rejected. See
        // [`in_subquery_hits_index`].
        Expr::InSubquery { expr, subquery, negated: false } => {
            in_subquery_hits_index(schema_map, pk_cols, expr, subquery)
        }
        _ => false,
    }
}

/// Decide whether a `<pk> IN (SELECT …)` conjunct is genuinely index-served, and
/// therefore safe for the guardrail to accept (rather than route to a full scan).
///
/// **Deliberately narrow.** It accepts *only* this shape, and rejects everything
/// else by returning `false`:
///
/// * `left` is a bare reference to the current table's primary-key column
///   (`pk_cols`) — membership in a PK set is a point/seek lookup, not a scan.
/// * the subquery is a single `SELECT` (not `VALUES`/`UNION`) with **no joins**,
///   **no `GROUP BY`**, **no `HAVING`**, **no `LIMIT`/`OFFSET`**, and exactly one
///   projected column (the membership column).
/// * the subquery reads exactly **one base table** (a named `TableFactor::Table`,
///   not a derived/sub-subquery) whose schema we can resolve from `schema_map`
///   (gluesql's `fetch_schema_map` already walks `InSubquery`, so the side
///   table's `Schema` — including its secondary indexes — is present).
/// * the subquery has a `WHERE` whose conjunct hits an index **on that subquery
///   table** — checked by recursing with the subquery table's own PK + indexable
///   sets, so a subquery filtering a non-indexed column is *not* accepted.
///
/// The recursion bottoms out quickly in practice: the only nested `InSubquery`
/// this would chase is another `<pk> IN (SELECT …)`, and the outer-table set it
/// is checked against is the subquery table's, so it cannot loop on the same
/// table set.
fn in_subquery_hits_index(
    schema_map: &SchemaMap,
    pk_cols: &HashSet<String>,
    left: &Expr,
    subquery: &gluesql_core::ast::Query,
) -> bool {
    // Left side must be the *current* table's primary key.
    if !column_of(left).is_some_and(|c| pk_cols.contains(&c)) {
        return false;
    }

    // No ORDER BY / LIMIT / OFFSET on the subquery (keep it a plain membership set).
    if !subquery.order_by.is_empty() || subquery.limit.is_some() || subquery.offset.is_some() {
        return false;
    }

    let SetExpr::Select(select) = &subquery.body else {
        return false; // VALUES (or any non-Select set expression) — reject.
    };

    // Plain single-table SELECT: no joins, no GROUP BY, no HAVING.
    if !select.from.joins.is_empty()
        || !select.group_by.is_empty()
        || select.having.is_some()
    {
        return false;
    }

    // Exactly one projected column (the membership column). Aggregates would be a
    // `SelectItem::Expr` carrying a function call — still a single item, but the
    // GROUP BY / HAVING guards above already exclude the grouped-aggregate shape,
    // and a bare aggregate without GROUP BY collapses to one row (membership in a
    // single value), which we conservatively decline by requiring a plain column.
    if select.projection.len() != 1 {
        return false;
    }

    // The subquery must read exactly one *named base table* we can resolve.
    let TableFactor::Table { name: sub_table, .. } = &select.from.relation else {
        return false; // derived subquery / series / dictionary — reject.
    };
    let Some(sub_schema) = schema_map.get(sub_table) else {
        return false; // can't resolve its schema → can't prove it's indexed → reject.
    };

    // The subquery MUST have a WHERE that hits an index on its own table.
    let Some(sub_where) = &select.selection else {
        return false; // unfiltered subquery = full scan of the side table — reject.
    };
    let sub_indexable = indexable_columns(sub_schema);
    let sub_pk = primary_key_columns(sub_schema);
    where_hits_index(schema_map, &sub_pk, sub_where, &sub_indexable)
}

fn is_indexable(expr: &Expr, indexable: &HashSet<String>) -> bool {
    column_of(expr).is_some_and(|c| indexable.contains(&c))
}

/// The column name an expression refers to, if it is a (possibly qualified,
/// parenthesized, or null-checked) column reference. `IS NULL` / `IS NOT NULL`
/// are recognized so the null-order rewrite's synthetic `IsNull(col)` ORDER BY
/// term (injected by `rewrite_null_order`) is treated as a reference to `col`,
/// not rejected as an opaque expression.
fn column_of(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(name) => Some(name.clone()),
        Expr::CompoundIdentifier { ident, .. } => Some(ident.clone()),
        Expr::Nested(inner) => column_of(inner),
        Expr::IsNull(inner) => column_of(inner),
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
    fn order_by_unindexed_over_pk_point_set_is_allowed() {
        // A PK IN-list bounds the result to a known set (e.g. a full-text match
        // set), so an in-memory ORDER BY on a non-indexed column is O(k log k)
        // and allowed. This is the FTS-rank (`ORDER BY ts_rank(...)`) case.
        let m = map(vec![schema("t", &[("id", true), ("name", false)], &[])]);
        assert!(run(&m, "SELECT * FROM t WHERE id IN (1, 2, 3) ORDER BY name").is_ok());
        // PK equality is point-bounded too.
        assert!(run(&m, "SELECT * FROM t WHERE id = 1 ORDER BY name").is_ok());
        // A range is NOT point-bounded → still rejected.
        assert!(run(&m, "SELECT * FROM t WHERE id >= 1 ORDER BY name").is_err());
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

    // -----------------------------------------------------------------------
    // PK-IN-subquery (multikey side-table membership) — the conservative arm.
    // -----------------------------------------------------------------------
    //
    // The collections layer rewrites a multikey `find {tags:"x"}` to
    //   SELECT doc FROM t WHERE _id IN (SELECT _id FROM t__mk_tags WHERE val = 'x')
    // This is index-served (PK membership + an indexed subquery scan) and so must
    // be ACCEPTED on the fast path — but only when the subquery's filter column is
    // actually indexed. The negative cases below are the conservative guarantee.

    /// Outer collection `t(_id PK, doc)` plus its multikey side table
    /// `t__mk_tags(rid PK, _id, val)`. `side_val_indexed` controls whether the
    /// side table carries the secondary index on `val` that makes the membership
    /// subquery index-served.
    fn collection_with_side_table(side_val_indexed: bool) -> SchemaMap {
        let side_idx: &[&str] = if side_val_indexed { &["val"] } else { &[] };
        map(vec![
            schema("t", &[("_id", true), ("doc", false)], &[]),
            schema("t__mk_tags", &[("rid", true), ("_id", false), ("val", false)], side_idx),
        ])
    }

    #[test]
    fn pk_in_indexed_subquery_is_accepted() {
        // `_id` (the outer PK) IN (SELECT _id FROM side WHERE val = 'x'), and the
        // side table HAS an index on `val` → genuinely index-served → not capped,
        // not rejected (stays on the GlueSQL fast path).
        let m = collection_with_side_table(true);
        let stmt = run(&m, "SELECT doc FROM t WHERE _id IN (SELECT _id FROM t__mk_tags WHERE val = 'x')")
            .expect("PK-IN-(indexed subquery) is index-served, must be accepted");
        assert_eq!(limit_of(&stmt), None, "index-served reads are not capped");
    }

    #[test]
    fn pk_in_unindexed_subquery_is_rejected() {
        // SAME shape, but the side table has NO index on `val`: the subquery would
        // full-scan the side table, so the guardrail must STILL reject it. This is
        // the conservative guarantee — acceptance hinges on the subquery being
        // index-served on its own table.
        let m = collection_with_side_table(false);
        assert!(
            run(&m, "SELECT doc FROM t WHERE _id IN (SELECT _id FROM t__mk_tags WHERE val = 'x')").is_err(),
            "PK-IN-(non-indexed subquery) is a side-table scan — must be rejected"
        );
    }

    #[test]
    fn pk_not_in_subquery_is_rejected() {
        // Negation is never index-served (anti-join over the whole table), even
        // when the subquery itself is indexed.
        let m = collection_with_side_table(true);
        assert!(
            run(&m, "SELECT doc FROM t WHERE _id NOT IN (SELECT _id FROM t__mk_tags WHERE val = 'x')").is_err(),
            "NOT IN (SELECT …) must still be rejected"
        );
    }

    #[test]
    fn non_pk_in_indexed_subquery_is_rejected() {
        // Left side is a non-PK, non-indexed column of `t` (`doc`), not the PK.
        // Membership of an arbitrary column is a post-scan filter, so reject even
        // though the subquery is indexed.
        let m = collection_with_side_table(true);
        assert!(
            run(&m, "SELECT doc FROM t WHERE doc IN (SELECT _id FROM t__mk_tags WHERE val = 'x')").is_err(),
            "<non-pk> IN (SELECT …) must be rejected"
        );
    }

    #[test]
    fn pk_in_unfiltered_subquery_is_rejected() {
        // No WHERE in the subquery → it scans the entire side table → reject, even
        // though the side table is indexed on `val`.
        let m = collection_with_side_table(true);
        assert!(
            run(&m, "SELECT doc FROM t WHERE _id IN (SELECT _id FROM t__mk_tags)").is_err(),
            "PK-IN-(unfiltered subquery) is a full side-table scan — must be rejected"
        );
    }

    #[test]
    fn pk_in_subquery_with_unknown_table_is_rejected() {
        // The subquery table's schema is not resolvable (we can't prove it is
        // indexed) → reject. Here only the outer table is in the map.
        let m = map(vec![schema("t", &[("_id", true), ("doc", false)], &[])]);
        assert!(
            run(&m, "SELECT doc FROM t WHERE _id IN (SELECT _id FROM mystery WHERE val = 'x')").is_err(),
            "PK-IN-(unresolvable subquery table) must be rejected"
        );
    }

    #[test]
    fn pk_in_indexed_subquery_anded_with_unindexed_is_accepted() {
        // A PK-IN-(indexed subquery) conjunct is sargable; gluesql filters the
        // rest. One index-served conjunct is enough — mirrors the plain
        // `indexed AND unindexed` rule.
        let m = collection_with_side_table(true);
        assert!(
            run(&m, "SELECT doc FROM t WHERE _id IN (SELECT _id FROM t__mk_tags WHERE val = 'x') AND doc = 'y'").is_ok(),
            "an index-served IN-subquery conjunct ANDed with an unindexed one is accepted"
        );
    }
}
