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
    CreateTable, Cte, DataType, Expr, Join, JoinConstraint, JoinOperator, Query, Select, SetExpr,
    Statement, TableFactor, TableWithJoins, Value, With,
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
}

fn rewrite_table_factor(factor: &mut TableFactor, changed: &mut bool) {
    if let TableFactor::Derived { subquery, .. } = factor {
        rewrite_query(subquery, changed);
    }
}

/// Cap on how many tables we fold into an `ON TRUE` join chain. GlueSQL's
/// executor materializes the full cartesian product *before* applying `WHERE`,
/// so folding an N-table comma-join means an N-way cross product (~rows^N) that
/// explodes for large N. We only rewrite small comma-joins; larger ones are
/// left for GlueSQL to reject (fast) rather than hang. Real many-table support
/// needs predicate pushdown / join optimization in the engine — a SQL shim
/// cannot provide it.
const MAX_COMMA_JOIN_TABLES: usize = 2;

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

/// Normalize parameterized string column types (`VARCHAR(n)`, `CHAR(n)`, ...) to
/// `TEXT`. GlueSQL rejects the length-parameterized forms, so a `CREATE TABLE`
/// using them fails outright — which then cascades into "table not found" for
/// every query against that table. `TEXT` is the type GlueSQL supports.
fn rewrite_create_table(create: &mut CreateTable, changed: &mut bool) {
    for column in &mut create.columns {
        if is_parameterized_string(&column.data_type) {
            column.data_type = DataType::Text;
            *changed = true;
        }
    }
}

fn is_parameterized_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Varchar(_)
            | DataType::Char(_)
            | DataType::CharVarying(_)
            | DataType::Nvarchar(_)
            | DataType::Clob(_)
    )
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
    fn large_comma_join_left_unfolded() {
        // Beyond MAX_COMMA_JOIN_TABLES we don't fold (avoids a cartesian-product
        // blowup on GlueSQL's executor); the engine rejects it fast instead.
        let sql = "SELECT * FROM a, b, c";
        assert_eq!(rewrite_multitable(sql), sql);
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
}
