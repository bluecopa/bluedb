//! Pre-execution checks for SQL that GlueSQL *accepts but mis-executes*.
//!
//! GlueSQL does not implement window functions: `SUM(x) OVER (...)`,
//! `ROW_NUMBER() OVER (...)`, etc. run but return **wrong results instead of an
//! error**. That silent-wrong behavior is the most dangerous gap in the engine,
//! so we detect window functions before execution and reject them with a clear
//! "unsupported" error — an honest failure the caller can see.
//!
//! Conservative: if the SQL can't be parsed here, we don't reject (GlueSQL will
//! handle/parse it), so this never turns a working query into a failure.

use sqlparser::ast::{Expr, Query, Select, SelectItem, SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Returns a rejection reason if the SQL uses a feature GlueSQL silently
/// mis-executes (currently: window functions). `None` means safe to run.
pub fn unsupported_reason(sql: &str) -> Option<String> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).ok()?;
    for stmt in &statements {
        if let Statement::Query(query) = stmt {
            if query_has_window(query) {
                return Some(
                    "unsupported: window functions (OVER) are not supported \
                     (GlueSQL mis-executes them silently)"
                        .to_string(),
                );
            }
        }
    }
    None
}

fn query_has_window(query: &Query) -> bool {
    set_expr_has_window(&query.body)
}

fn set_expr_has_window(body: &SetExpr) -> bool {
    match body {
        SetExpr::Select(select) => select_has_window(select),
        SetExpr::Query(query) => query_has_window(query),
        SetExpr::SetOperation { left, right, .. } => {
            set_expr_has_window(left) || set_expr_has_window(right)
        }
        _ => false,
    }
}

fn select_has_window(select: &Select) -> bool {
    let in_projection = select.projection.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_has_window(expr)
        }
        _ => false,
    });
    in_projection
        || select.selection.as_ref().is_some_and(expr_has_window)
        || select.having.as_ref().is_some_and(expr_has_window)
        || select.from.iter().any(|twj| {
            table_factor_has_window(&twj.relation)
                || twj.joins.iter().any(|j| table_factor_has_window(&j.relation))
        })
}

fn table_factor_has_window(factor: &TableFactor) -> bool {
    matches!(factor, TableFactor::Derived { subquery, .. } if query_has_window(subquery))
}

fn expr_has_window(expr: &Expr) -> bool {
    match expr {
        Expr::Function(func) => func.over.is_some(),
        Expr::BinaryOp { left, right, .. } => expr_has_window(left) || expr_has_window(right),
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) | Expr::Cast { expr, .. } => {
            expr_has_window(expr)
        }
        Expr::Subquery(query)
        | Expr::InSubquery { subquery: query, .. }
        | Expr::Exists { subquery: query, .. } => query_has_window(query),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::unsupported_reason;

    #[test]
    fn rejects_window_function() {
        assert!(unsupported_reason("SELECT SUM(x) OVER (ORDER BY id) FROM t").is_some());
        assert!(unsupported_reason("SELECT ROW_NUMBER() OVER (PARTITION BY a) FROM t").is_some());
    }

    #[test]
    fn rejects_window_in_subquery() {
        let sql = "SELECT * FROM (SELECT rank() OVER (ORDER BY a) AS r FROM t) s WHERE r = 1";
        assert!(unsupported_reason(sql).is_some());
    }

    #[test]
    fn allows_plain_aggregate() {
        assert!(unsupported_reason("SELECT SUM(x) FROM t GROUP BY y").is_none());
        assert!(unsupported_reason("SELECT a, COUNT(*) FROM t GROUP BY a").is_none());
    }

    #[test]
    fn unparseable_passes_through() {
        // Don't reject what we can't parse — GlueSQL handles it.
        assert!(unsupported_reason("definitely not sql").is_none());
    }
}
