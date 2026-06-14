//! Comma-join rewrite shim.
//!
//! GlueSQL rejects multiple table factors in a `FROM` clause (`FROM a, b`) and
//! also rejects `CROSS JOIN`, but it executes `INNER JOIN ... ON <expr>`
//! correctly (including chained N-way joins). A comma join is semantically
//! `a CROSS JOIN b`, i.e. `a INNER JOIN b ON TRUE` — the `WHERE` clause does the
//! real filtering. So we rewrite
//!
//! ```sql
//! SELECT ... FROM a, b, c WHERE ...
//! -- becomes
//! SELECT ... FROM a JOIN b ON TRUE JOIN c ON TRUE WHERE ...
//! ```
//!
//! before handing the SQL to GlueSQL, reusing its working inner-join executor.
//!
//! The rewrite is **conservative**: if the SQL doesn't parse here, or contains
//! no multi-table `FROM`, the original string is returned unchanged — the shim
//! never makes a query worse than it was.

use sqlparser::ast::{
    CreateTable, Cte, DataType, ExactNumberInfo, Expr, Join, JoinConstraint, JoinOperator, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, TimezoneInfo, Value, With,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Rewrite comma-joins into `INNER JOIN ... ON TRUE`. Returns the original SQL
/// unchanged if it can't be parsed or needs no rewrite.
pub fn rewrite_multitable(sql: &str) -> String {
    let dialect = GenericDialect {};
    let Ok(mut statements) = Parser::parse_sql(&dialect, sql) else {
        return sql.to_string();
    };

    let mut changed = false;
    for stmt in &mut statements {
        match stmt {
            Statement::Query(query) => rewrite_query(query, &mut changed),
            Statement::CreateTable(create) => rewrite_create_table(create, &mut changed),
            _ => {}
        }
    }

    if !changed {
        return sql.to_string();
    }
    statements
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join("; ")
}

fn rewrite_query(query: &mut Query, changed: &mut bool) {
    if let Some(with) = &mut query.with {
        rewrite_with(with, changed);
    }
    rewrite_set_expr(&mut query.body, changed);
}

fn rewrite_with(with: &mut With, changed: &mut bool) {
    for cte in &mut with.cte_tables {
        rewrite_cte(cte, changed);
    }
}

fn rewrite_cte(cte: &mut Cte, changed: &mut bool) {
    rewrite_query(&mut cte.query, changed);
}

fn rewrite_set_expr(set_expr: &mut SetExpr, changed: &mut bool) {
    match set_expr {
        SetExpr::Select(select) => rewrite_select(select, changed),
        SetExpr::Query(query) => rewrite_query(query, changed),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr(left, changed);
            rewrite_set_expr(right, changed);
        }
        _ => {}
    }
}

fn rewrite_select(select: &mut Select, changed: &mut bool) {
    fold_from(&mut select.from, changed);
    // Recurse into derived tables (subqueries that appear in FROM).
    for twj in &mut select.from {
        rewrite_table_factor(&mut twj.relation, changed);
        for join in &mut twj.joins {
            rewrite_table_factor(&mut join.relation, changed);
        }
    }
    // Recurse into subqueries in the projection / WHERE / HAVING — e.g. the
    // `IN (SELECT ... FROM a, b ...)` subqueries that the set-op rewrite
    // produces, whose branches may themselves contain comma-joins.
    for item in &mut select.projection {
        if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
            fold_expr_subqueries(expr, changed);
        }
    }
    if let Some(selection) = &mut select.selection {
        fold_expr_subqueries(selection, changed);
    }
    if let Some(having) = &mut select.having {
        fold_expr_subqueries(having, changed);
    }
}

fn rewrite_table_factor(factor: &mut TableFactor, changed: &mut bool) {
    if let TableFactor::Derived { subquery, .. } = factor {
        rewrite_query(subquery, changed);
    }
}

/// Walk an expression and fold comma-joins inside any subquery it contains
/// (`IN (SELECT ...)`, `EXISTS (...)`, scalar `(SELECT ...)`).
fn fold_expr_subqueries(expr: &mut Expr, changed: &mut bool) {
    match expr {
        Expr::Subquery(query)
        | Expr::InSubquery { subquery: query, .. }
        | Expr::Exists { subquery: query, .. } => rewrite_query(query, changed),
        Expr::Cast { expr, data_type, .. } => {
            if normalize_data_type(data_type) {
                *changed = true;
            }
            fold_expr_subqueries(expr, changed);
        }
        Expr::BinaryOp { left, right, .. } => {
            fold_expr_subqueries(left, changed);
            fold_expr_subqueries(right, changed);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => fold_expr_subqueries(expr, changed),
        Expr::InList { expr, list, .. } => {
            fold_expr_subqueries(expr, changed);
            for item in list {
                fold_expr_subqueries(item, changed);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            fold_expr_subqueries(expr, changed);
            fold_expr_subqueries(low, changed);
            fold_expr_subqueries(high, changed);
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => {
            if let Some(op) = operand {
                fold_expr_subqueries(op, changed);
            }
            for e in conditions.iter_mut().chain(results.iter_mut()) {
                fold_expr_subqueries(e, changed);
            }
            if let Some(e) = else_result {
                fold_expr_subqueries(e, changed);
            }
        }
        _ => {}
    }
}

/// Cap on how many tables we fold into an `ON TRUE` join chain. GlueSQL's
/// executor materializes the full cartesian product *before* applying `WHERE`,
/// so folding an N-table comma-join means an N-way cross product (~rows^N) that
/// explodes for large N. We only rewrite small comma-joins; larger ones are
/// left for GlueSQL to reject (fast) rather than hang. Real many-table support
/// needs predicate pushdown / join optimization in the engine — a SQL shim
/// cannot provide it.
const MAX_COMMA_JOIN_TABLES: usize = 10;

/// Fold a small comma-join `FROM a, b` into `FROM a JOIN b ON TRUE`.
fn fold_from(from: &mut Vec<TableWithJoins>, changed: &mut bool) {
    if from.len() <= 1 || from.len() > MAX_COMMA_JOIN_TABLES {
        return;
    }
    let extras = from.split_off(1);
    let first = &mut from[0];
    for extra in extras {
        first.joins.push(Join {
            relation: extra.relation,
            global: false,
            join_operator: JoinOperator::Inner(JoinConstraint::On(Expr::Value(Value::Boolean(
                true,
            )))),
        });
        // Preserve any joins the extra table factor itself carried.
        first.joins.extend(extra.joins);
    }
    *changed = true;
}

/// Normalize column data types GlueSQL rejects (see [`normalize_data_type`]). A
/// `CREATE TABLE` using an unsupported type fails outright and then cascades
/// into "table not found" for every query against that table.
fn rewrite_create_table(create: &mut CreateTable, changed: &mut bool) {
    for column in &mut create.columns {
        if normalize_data_type(&mut column.data_type) {
            *changed = true;
        }
    }
}

/// Map a data type GlueSQL rejects to the nearest type it accepts:
/// parameterized/extra string types -> `TEXT`, any decimal/numeric -> bare
/// `DECIMAL`, and every integer width (`INT(n)`, `BIGINT`, `SMALLINT`, ...) ->
/// `INTEGER`. GlueSQL only accepts a small set of bare types, so width/precision
/// parameters and unsupported integer widths otherwise fail at translate.
///
/// Compares by the rendered base word to avoid enumerating sqlparser's ~30
/// numeric/string variants. Returns whether it changed the type.
fn normalize_data_type(data_type: &mut DataType) -> bool {
    let rendered = data_type.to_string().to_ascii_uppercase();
    let base = rendered.split(['(', ' ']).next().unwrap_or("");
    let has_param = rendered.contains('(');

    let replacement = match base {
        "VARCHAR" | "CHAR" | "CHARACTER" | "NVARCHAR" | "NCHAR" | "CLOB" | "STRING" => {
            Some(DataType::Text)
        }
        "DECIMAL" | "NUMERIC" | "DEC" | "BIGDECIMAL" | "BIGNUMERIC" | "BIGNUM" => {
            Some(DataType::Decimal(ExactNumberInfo::None))
        }
        // Floating point: GlueSQL has a single `FLOAT` (f64). Map the whole
        // family (and strip any precision) onto it.
        "DOUBLE" | "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "BINARY_FLOAT" | "BINARY_DOUBLE" => {
            Some(DataType::Float(None))
        }
        // Integer family, including unsigned and wide variants (widened to i64;
        // lossy for out-of-range values but lets the DDL/query run).
        "BIGINT" | "SMALLINT" | "TINYINT" | "MEDIUMINT" | "INT2" | "INT4" | "INT8" | "INT16"
        | "INT32" | "INT64" | "INT128" | "INT256" | "HUGEINT" | "UHUGEINT" | "UBIGINT" | "UINT"
        | "UINTEGER" | "USMALLINT" | "UTINYINT" | "UMEDIUMINT" | "UINT8" | "UINT16" | "UINT32"
        | "UINT64" | "UINT128" => Some(DataType::Integer(None)),
        "INT" | "INTEGER" if has_param => Some(DataType::Integer(None)),
        // Temporal: strip sub-second precision / timezone GlueSQL doesn't model
        // (bare forms map to themselves and are skipped by the `!=` guard below).
        "TIMESTAMP" | "TIMESTAMPTZ" | "DATETIME" => {
            Some(DataType::Timestamp(None, TimezoneInfo::None))
        }
        "TIME" | "TIMETZ" => Some(DataType::Time(None, TimezoneInfo::None)),
        _ => None,
    };

    match replacement {
        Some(rep) if *data_type != rep => {
            *data_type = rep;
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::rewrite_multitable;

    #[test]
    fn folds_two_table_comma_join() {
        let out = rewrite_multitable("SELECT a.x FROM a, b WHERE a.id = b.id");
        assert!(out.contains("JOIN b"), "got: {out}");
        assert!(out.to_uppercase().contains("ON TRUE") || out.contains("ON true"), "got: {out}");
        assert!(!out.contains("FROM a, b"), "comma join should be gone: {out}");
    }

    #[test]
    fn folds_three_table_comma_join() {
        let out = rewrite_multitable("SELECT * FROM a, b, c");
        assert_eq!(out.matches("JOIN").count(), 2, "expected two joins: {out}");
    }

    #[test]
    fn leaves_single_table_untouched() {
        let sql = "SELECT x FROM a WHERE x > 1";
        assert_eq!(rewrite_multitable(sql), sql);
    }

    #[test]
    fn leaves_explicit_join_untouched() {
        let sql = "SELECT a.x FROM a JOIN b ON a.id = b.id";
        assert_eq!(rewrite_multitable(sql), sql);
    }

    #[test]
    fn unparseable_sql_passes_through() {
        let sql = "CREATE TABLE t;";
        assert_eq!(rewrite_multitable(sql), sql);
    }

    #[test]
    fn varchar_n_becomes_text() {
        let out = rewrite_multitable("CREATE TABLE t (a INTEGER, x VARCHAR(30))");
        assert!(out.to_uppercase().contains("TEXT"), "got: {out}");
        assert!(!out.to_uppercase().contains("VARCHAR"), "got: {out}");
    }

    #[test]
    fn float_family_becomes_float() {
        for ddl in [
            "CREATE TABLE t (x DOUBLE)",
            "CREATE TABLE t (x REAL)",
            "CREATE TABLE t (x DOUBLE PRECISION)",
            "CREATE TABLE t (x FLOAT(10))",
        ] {
            let out = rewrite_multitable(ddl).to_uppercase();
            assert!(out.contains("FLOAT"), "{ddl} -> {out}");
            assert!(!out.contains("DOUBLE") && !out.contains("REAL"), "{ddl} -> {out}");
        }
    }

    #[test]
    fn wide_and_unsigned_ints_become_integer() {
        for ddl in ["CREATE TABLE t (x UHUGEINT)", "CREATE TABLE t (x UBIGINT)"] {
            let out = rewrite_multitable(ddl).to_uppercase();
            assert!(out.contains("INT"), "{ddl} -> {out}");
            assert!(!out.contains("HUGE") && !out.contains("UBIG"), "{ddl} -> {out}");
        }
    }

    #[test]
    fn timestamp_precision_is_stripped() {
        let out = rewrite_multitable("CREATE TABLE t (x TIMESTAMP(6))").to_uppercase();
        assert!(out.contains("TIMESTAMP"), "got: {out}");
        assert!(!out.contains("TIMESTAMP(6)") && !out.contains("(6)"), "got: {out}");
    }

    #[test]
    fn cast_type_normalized_inside_case() {
        let out = rewrite_multitable(
            "SELECT CASE WHEN x THEN CAST(y AS DOUBLE) ELSE NULL END FROM t",
        )
        .to_uppercase();
        assert!(out.contains("FLOAT"), "got: {out}");
        assert!(!out.contains("DOUBLE"), "got: {out}");
    }

    #[test]
    fn folds_comma_join_inside_where_subquery() {
        // The set-op rewrite emits `... IN (SELECT ... FROM a, b ...)`; the inner
        // comma-join must fold too, or it reaches gluesql unfolded.
        let out = rewrite_multitable("SELECT a FROM t1 WHERE a IN (SELECT b FROM t2, t3 WHERE x = y)");
        assert!(out.contains("JOIN t3"), "inner comma-join should fold: {out}");
    }

    #[test]
    fn normalizes_parameterized_types_in_create() {
        let out =
            rewrite_multitable("CREATE TABLE t (a DECIMAL(10,2), b INT(11), c BIGINT, d VARCHAR(20))");
        let up = out.to_uppercase();
        assert!(!up.contains("(10, 2)") && !up.contains("(11)") && !up.contains("(20)"), "params not stripped: {out}");
        assert!(up.contains("DECIMAL") && up.contains("INTEGER") && up.contains("TEXT"), "got: {out}");
        assert!(!up.contains("BIGINT"), "BIGINT should become INTEGER: {out}");
    }

    #[test]
    fn normalizes_cast_target_type() {
        let out = rewrite_multitable("SELECT CAST(x AS DECIMAL(10,2)) FROM t");
        assert!(!out.contains("10, 2") && !out.contains("10,2"), "cast type param not stripped: {out}");
    }

    #[test]
    fn leaves_supported_bare_types_untouched() {
        let sql = "CREATE TABLE t (a INTEGER, b TEXT, c BOOLEAN)";
        assert_eq!(rewrite_multitable(sql), sql);
    }
}
