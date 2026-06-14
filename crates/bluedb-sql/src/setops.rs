//! Set-operation rewrite shim (UNION / UNION ALL / INTERSECT / EXCEPT).
//!
//! GlueSQL's AST has no set-operation node (`SetExpr` is only `Select`/`Values`)
//! and no row-concatenation primitive, so it rejects every set operation at
//! translate time. But each one rewrites into constructs GlueSQL *does* execute
//! (all verified runnable):
//!
//! ```text
//! A INTERSECT B  ->  SELECT DISTINCT c FROM (A) _l WHERE c IN (B)        (semi-join)
//! A EXCEPT B     ->  SELECT DISTINCT c FROM (A) _l WHERE c NOT IN (B)    (anti-join)
//! A UNION ALL B  ->  SELECT CASE WHEN _d.N=1 THEN _l.c ELSE _r.c END AS c
//!                    FROM SERIES(2) _d LEFT JOIN (A) _l ON _d.N=1
//!                                      LEFT JOIN (B) _r ON _d.N=2        (driver concat)
//! A UNION B      ->  SELECT DISTINCT c FROM (A UNION ALL B) _u
//! ```
//!
//! Nested/mixed chains are handled by recursing into each branch first. Limited
//! to **single-column** branches (what the corpus uses); a branch whose output
//! column can't be named (wildcard, bare expression, multi-column) makes the
//! rewrite bail and the original is left for GlueSQL to reject. Conservative:
//! unparseable or set-op-free SQL is returned verbatim.

use sqlparser::ast::{Expr, SelectItem, SetExpr, SetOperator, SetQuantifier, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Rewrite UNION/INTERSECT/EXCEPT into GlueSQL-executable SQL. Returns the
/// original string unchanged if it can't be parsed or has no set operation.
pub fn rewrite_set_ops(sql: &str) -> String {
    let dialect = GenericDialect {};
    let Ok(statements) = Parser::parse_sql(&dialect, sql) else {
        return sql.to_string();
    };

    let mut changed = false;
    let mut out: Vec<String> = Vec::with_capacity(statements.len());
    for stmt in &statements {
        match rewrite_statement(stmt, &dialect) {
            Some(rewritten) => {
                changed = true;
                out.push(rewritten);
            }
            None => out.push(stmt.to_string()),
        }
    }
    if changed {
        out.join("; ")
    } else {
        sql.to_string()
    }
}

fn rewrite_statement(stmt: &Statement, dialect: &GenericDialect) -> Option<String> {
    let Statement::Query(query) = stmt else {
        return None;
    };
    if !matches!(query.body.as_ref(), SetExpr::SetOperation { .. }) {
        return None;
    }
    let mut counter = 0u32;
    let body_sql = setexpr_to_sql(&query.body, &mut counter)?;

    // Re-parse the rewritten body and splice it back into the original query so
    // ORDER BY / LIMIT / WITH are preserved.
    let parsed = Parser::parse_sql(dialect, &body_sql).ok()?;
    let [Statement::Query(rewritten)] = parsed.as_slice() else {
        return None;
    };
    let mut new_query = query.clone();
    new_query.body = rewritten.body.clone();
    Some(Statement::Query(new_query).to_string())
}

/// Recursively turn a `SetExpr` into GlueSQL-executable SQL. `counter` hands out
/// globally-unique alias ids so nested rewrites never shadow each other.
fn setexpr_to_sql(body: &SetExpr, counter: &mut u32) -> Option<String> {
    match body {
        SetExpr::Select(_) | SetExpr::Values(_) => Some(body.to_string()),
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let id = *counter;
            *counter += 1;
            let lsql = setexpr_to_sql(left, counter)?;
            let rsql = setexpr_to_sql(right, counter)?;
            let keep_all = matches!(set_quantifier, SetQuantifier::All | SetQuantifier::AllByName);
            let distinct = if keep_all { "" } else { "DISTINCT " };
            // Unique aliases for this node so nested set ops don't collide.
            let (l, r, d, u) = (format!("_l{id}"), format!("_r{id}"), format!("_d{id}"), format!("_u{id}"));

            let sql = match op {
                // INTERSECT/EXCEPT use an IN/NOT IN subquery, which GlueSQL only
                // supports for a single column; multi-column would need
                // NULL-aware row matching, so it stays unsupported.
                SetOperator::Intersect => {
                    let lcol = single_column(left)?;
                    format!("SELECT {distinct}{l}.{lcol} FROM ({lsql}) AS {l} WHERE {l}.{lcol} IN ({rsql})")
                }
                SetOperator::Except => {
                    let lcol = single_column(left)?;
                    format!(
                        "SELECT {distinct}{l}.{lcol} FROM ({lsql}) AS {l} WHERE {l}.{lcol} NOT IN ({rsql})"
                    )
                }
                // UNION concatenates, so it generalizes to N columns: a 2-row
                // driver + gated LEFT JOINs, with one CASE per output column.
                SetOperator::Union => {
                    let lcols = columns(left)?;
                    let rcols = columns(right)?;
                    if lcols.len() != rcols.len() {
                        return None;
                    }
                    let projection = lcols
                        .iter()
                        .zip(&rcols)
                        .map(|(lc, rc)| {
                            format!("CASE WHEN {d}.N = 1 THEN {l}.{lc} ELSE {r}.{rc} END AS {lc}")
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    let union_all = format!(
                        "SELECT {projection} \
                         FROM SERIES(2) AS {d} \
                         LEFT JOIN ({lsql}) AS {l} ON {d}.N = 1 \
                         LEFT JOIN ({rsql}) AS {r} ON {d}.N = 2"
                    );
                    if keep_all {
                        union_all
                    } else {
                        let ucols = lcols
                            .iter()
                            .map(|c| format!("{u}.{c}"))
                            .collect::<Vec<_>>()
                            .join(", ");
                        format!("SELECT DISTINCT {ucols} FROM ({union_all}) AS {u}")
                    }
                }
            };
            Some(sql)
        }
        _ => None,
    }
}

/// The single output column name of a branch (descending into nested set ops via
/// the left branch). `None` if the branch isn't a single named column.
fn single_column(body: &SetExpr) -> Option<String> {
    match body {
        SetExpr::Select(select) => {
            if select.projection.len() != 1 {
                return None;
            }
            match &select.projection[0] {
                SelectItem::UnnamedExpr(Expr::Identifier(id)) => Some(id.value.clone()),
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                    parts.last().map(|ident| ident.value.clone())
                }
                SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                _ => None,
            }
        }
        SetExpr::SetOperation { left, .. } => single_column(left),
        _ => None,
    }
}

/// Output column names of a branch (descending nested set ops via the left
/// branch). `None` if any projection item isn't a simply-named column or alias
/// (e.g. a wildcard), since then we can't name the columns to build the CASEs.
fn columns(body: &SetExpr) -> Option<Vec<String>> {
    match body {
        SetExpr::Select(select) => {
            if select.projection.is_empty() {
                return None;
            }
            select
                .projection
                .iter()
                .map(|item| match item {
                    SelectItem::UnnamedExpr(Expr::Identifier(id)) => Some(id.value.clone()),
                    SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                        parts.last().map(|ident| ident.value.clone())
                    }
                    SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.clone()),
                    _ => None,
                })
                .collect()
        }
        SetExpr::SetOperation { left, .. } => columns(left),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::rewrite_set_ops;

    #[test]
    fn intersect_becomes_in_subquery() {
        let out = rewrite_set_ops("SELECT a FROM x INTERSECT SELECT b FROM y");
        assert!(out.to_uppercase().contains(" IN ("), "got: {out}");
        assert!(!out.to_uppercase().contains("INTERSECT"), "got: {out}");
    }

    #[test]
    fn except_becomes_not_in_subquery() {
        let out = rewrite_set_ops("SELECT a FROM x EXCEPT SELECT b FROM y");
        assert!(out.to_uppercase().contains("NOT IN ("), "got: {out}");
        assert!(!out.to_uppercase().contains("EXCEPT"), "got: {out}");
    }

    #[test]
    fn union_all_becomes_driver_join() {
        let out = rewrite_set_ops("SELECT a FROM x UNION ALL SELECT b FROM y");
        assert!(out.contains("SERIES(2)"), "got: {out}");
        assert!(out.to_uppercase().contains("LEFT JOIN"), "got: {out}");
        assert!(!out.to_uppercase().contains("UNION"), "got: {out}");
    }

    #[test]
    fn multi_column_union_builds_case_per_column() {
        let out = rewrite_set_ops("SELECT a, b FROM x UNION ALL SELECT c, d FROM y");
        assert!(!out.to_uppercase().contains("UNION"), "got: {out}");
        assert!(out.contains("SERIES(2)"), "got: {out}");
        // One CASE per output column.
        assert_eq!(out.to_uppercase().matches("CASE WHEN").count(), 2, "got: {out}");
    }

    #[test]
    fn multi_column_union_distinct_wraps() {
        let out = rewrite_set_ops("SELECT a, b FROM x UNION SELECT c, d FROM y");
        assert!(out.to_uppercase().contains("DISTINCT"), "got: {out}");
        assert!(!out.to_uppercase().contains("UNION"), "got: {out}");
    }

    #[test]
    fn multi_column_intersect_stays_unsupported() {
        // Multi-column INTERSECT can't be rewritten (single-column only); left
        // intact for GlueSQL to reject.
        let out = rewrite_set_ops("SELECT a, b FROM x INTERSECT SELECT c, d FROM y");
        assert!(out.to_uppercase().contains("INTERSECT"), "got: {out}");
    }

    #[test]
    fn union_distinct_wraps_with_distinct() {
        let out = rewrite_set_ops("SELECT a FROM x UNION SELECT b FROM y");
        assert!(out.to_uppercase().contains("DISTINCT"), "got: {out}");
        assert!(out.contains("SERIES(2)"), "got: {out}");
    }

    #[test]
    fn no_set_op_passes_through() {
        let sql = "SELECT a FROM x WHERE a > 1";
        assert_eq!(rewrite_set_ops(sql), sql);
    }

    #[test]
    fn multi_column_union_is_now_rewritten() {
        // Multi-column UNION used to be left unchanged; it is now rewritten via
        // the driver-join (one CASE per column).
        let out = rewrite_set_ops("SELECT a, b FROM x UNION SELECT a, b FROM y");
        assert!(!out.to_uppercase().contains("UNION"), "got: {out}");
        assert!(out.contains("SERIES(2)"), "got: {out}");
    }

    #[test]
    fn wildcard_branch_left_unchanged() {
        // `SELECT *` has no nameable columns, so we can't build the CASEs — bail.
        let sql = "SELECT * FROM x UNION SELECT * FROM y";
        assert_eq!(rewrite_set_ops(sql), sql);
    }
}
