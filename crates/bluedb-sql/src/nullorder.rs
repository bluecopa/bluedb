//! `SET default_null_order` support — NULL placement in `ORDER BY`.
//!
//! GlueSQL's [`OrderByExpr`](sqlparser::ast::OrderByExpr) has no
//! `NULLS FIRST`/`NULLS LAST`, and its sort treats `NULL` as the largest value
//! (so `ORDER BY x` puts NULLs last when ascending, first when descending).
//! DuckDB/PostgreSQL let a session choose with `SET default_null_order`.
//!
//! We honor it without an engine change: for each sort key `e`, prepend the key
//! `(e IS NULL)`. `e IS NULL` is `false` (0) for non-null and `true` (1) for
//! null, and GlueSQL sorts booleans natively (proven: `ORDER BY x IS NULL, x`
//! runs), so:
//! - **NULLS LAST**  → `(e IS NULL) ASC` (non-nulls first), then `e`.
//! - **NULLS FIRST** → `(e IS NULL) DESC` (nulls first), then `e`.
//!
//! The placement key's direction depends only on the chosen policy, not on the
//! original key's `ASC`/`DESC` — matching SQL, where `NULLS FIRST/LAST` is
//! orthogonal to the value sort direction.

use sqlparser::ast::{Expr, OrderByExpr, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Rewrite each top-level `ORDER BY` so NULLs sort first (`nulls_first = true`)
/// or last (`false`). Returns the SQL unchanged if it doesn't parse or has no
/// `ORDER BY` to adjust.
pub fn rewrite_null_order(sql: &str, nulls_first: bool) -> String {
    let dialect = GenericDialect {};
    let Ok(mut statements) = Parser::parse_sql(&dialect, sql) else {
        return sql.to_string();
    };
    let mut changed = false;
    for stmt in &mut statements {
        if let Statement::Query(query) = stmt {
            if let Some(order_by) = &mut query.order_by {
                if !order_by.exprs.is_empty() {
                    order_by.exprs = expand(&order_by.exprs, nulls_first);
                    changed = true;
                }
            }
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

/// Detect `SET default_null_order = '…'` / `PRAGMA default_null_order = '…'` and
/// return the requested placement: `Some(true)` for nulls-first, `Some(false)`
/// for nulls-last, `None` if this isn't a recognizable null-order setting (an
/// unknown value falls through to the engine, which errors — matching the
/// corpus's negative tests).
pub fn parse_default_null_order(sql: &str) -> Option<bool> {
    let lower = sql.trim().to_ascii_lowercase();
    let body = lower.strip_suffix(';').unwrap_or(&lower).trim();
    let is_setting = body.starts_with("set ") || body.starts_with("pragma ");
    if !is_setting || !body.contains("default_null_order") {
        return None;
    }
    if body.contains("first") {
        Some(true)
    } else if body.contains("last") {
        Some(false)
    } else {
        None
    }
}

fn expand(exprs: &[OrderByExpr], nulls_first: bool) -> Vec<OrderByExpr> {
    let mut out = Vec::with_capacity(exprs.len() * 2);
    for e in exprs {
        out.push(OrderByExpr {
            expr: Expr::IsNull(Box::new(e.expr.clone())),
            // ASC => non-null (false) first => NULLS LAST; DESC => nulls first.
            asc: Some(!nulls_first),
            nulls_first: None,
            with_fill: None,
        });
        out.push(e.clone());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{parse_default_null_order, rewrite_null_order};

    #[test]
    fn nulls_first_prepends_is_null_desc() {
        let out = rewrite_null_order("SELECT s FROM t ORDER BY s", true);
        assert!(out.contains("IS NULL"), "got: {out}");
        assert!(out.to_uppercase().contains("IS NULL DESC"), "got: {out}");
    }

    #[test]
    fn nulls_last_prepends_is_null_asc() {
        let out = rewrite_null_order("SELECT s FROM t ORDER BY s", false);
        // ASC is the default direction, so it renders without an explicit ASC.
        assert!(out.contains("IS NULL"), "got: {out}");
        assert!(!out.to_uppercase().contains("IS NULL DESC"), "got: {out}");
    }

    #[test]
    fn expands_every_key() {
        let out = rewrite_null_order("SELECT a, b FROM t ORDER BY a, b DESC", true);
        assert_eq!(out.matches("IS NULL").count(), 2, "got: {out}");
    }

    #[test]
    fn no_order_by_is_untouched() {
        let sql = "SELECT s FROM t";
        assert_eq!(rewrite_null_order(sql, true), sql);
    }

    #[test]
    fn unparseable_is_untouched() {
        let sql = "definitely not sql";
        assert_eq!(rewrite_null_order(sql, true), sql);
    }

    #[test]
    fn detects_set_statement() {
        assert_eq!(parse_default_null_order("SET default_null_order='nulls_first';"), Some(true));
        assert_eq!(parse_default_null_order("SET default_null_order='nulls_last'"), Some(false));
        assert_eq!(parse_default_null_order("PRAGMA default_null_order='NULLS FIRST'"), Some(true));
        assert_eq!(parse_default_null_order("SET default_null_order='UNKNOWN'"), None);
        assert_eq!(parse_default_null_order("SELECT 1"), None);
        assert_eq!(parse_default_null_order("SET foo='bar'"), None);
    }
}
