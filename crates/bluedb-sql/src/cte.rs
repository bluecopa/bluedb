//! CTE (`WITH`) inlining shim.
//!
//! GlueSQL has no `WITH` clause. A non-recursive CTE is pure sugar:
//! `WITH c AS (q) SELECT ... FROM c` is equivalent to `SELECT ... FROM (q) AS c`.
//! So we drop the `WITH` and replace every reference to a CTE name with the
//! CTE's query wrapped as a derived table. The scope accumulates outer CTEs so a
//! later CTE (or a nested subquery) can reference an earlier one.
//!
//! `WITH RECURSIVE` needs iteration and can't be inlined — left for GlueSQL to
//! reject. Conservative: unparseable or WITH-free SQL is returned unchanged.

use std::collections::HashMap;

use sqlparser::ast::{
    Expr, Ident, ObjectName, ObjectType, Query, Select, SelectItem, SetExpr, Statement, TableAlias,
    TableFactor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

type Scope = HashMap<String, Box<Query>>;

fn last_name(name: &ObjectName) -> String {
    name.0.last().map(|i| i.value.clone()).unwrap_or_default()
}

/// If `sql` is `CREATE [OR REPLACE] VIEW name AS <query>`, return
/// `(name, query-SQL)`. GlueSQL has no views, so the harness stores the
/// definition and inlines references (see [`inline_views`]).
pub fn parse_create_view(sql: &str) -> Option<(String, String)> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).ok()?;
    match stmts.into_iter().next()? {
        Statement::CreateView { name, query, .. } => Some((last_name(&name), query.to_string())),
        _ => None,
    }
}

/// If `sql` is `DROP VIEW …`, return the view names dropped.
pub fn parse_drop_view(sql: &str) -> Option<Vec<String>> {
    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql).ok()?;
    match stmts.into_iter().next()? {
        Statement::Drop {
            object_type: ObjectType::View,
            names,
            ..
        } => Some(names.iter().map(last_name).collect()),
        _ => None,
    }
}

/// Inline references to stored views (name → view-body SQL) as derived tables,
/// reusing the CTE substitution. View bodies should already be CTE-inlined when
/// stored. One level of reference is resolved (a view referencing another view
/// is not recursively expanded).
pub fn inline_views(sql: &str, views: &HashMap<String, String>) -> String {
    if views.is_empty() {
        return sql.to_string();
    }
    let dialect = GenericDialect {};
    let mut scope = Scope::new();
    for (name, body) in views {
        if let Ok(stmts) = Parser::parse_sql(&dialect, body) {
            if let Some(Statement::Query(query)) = stmts.into_iter().next() {
                scope.insert(name.clone(), query);
            }
        }
    }
    if scope.is_empty() {
        return sql.to_string();
    }
    let Ok(mut statements) = Parser::parse_sql(&dialect, sql) else {
        return sql.to_string();
    };
    let mut changed = false;
    for stmt in &mut statements {
        let inner = match stmt {
            Statement::Query(query) => Some(query.as_mut()),
            Statement::CreateTable(create) => create.query.as_deref_mut(),
            Statement::Insert(insert) => insert.source.as_deref_mut(),
            _ => None,
        };
        if let Some(query) = inner {
            inline_query(query, &scope, &mut changed);
        }
    }
    if changed {
        statements
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    } else {
        sql.to_string()
    }
}

/// Inline non-recursive CTEs. Returns the original SQL unchanged if it can't be
/// parsed or has no inlinable `WITH`.
pub fn inline_ctes(sql: &str) -> String {
    let dialect = GenericDialect {};
    let Ok(mut statements) = Parser::parse_sql(&dialect, sql) else {
        return sql.to_string();
    };
    let mut changed = false;
    let empty = Scope::new();
    for stmt in &mut statements {
        // `WITH` can head a bare query, a `CREATE TABLE … AS <query>`, or an
        // `INSERT … <query>`; inline it in all three.
        let inner = match stmt {
            Statement::Query(query) => Some(query.as_mut()),
            Statement::CreateTable(create) => create.query.as_deref_mut(),
            Statement::Insert(insert) => insert.source.as_deref_mut(),
            _ => None,
        };
        if let Some(query) = inner {
            inline_query(query, &empty, &mut changed);
        }
    }
    if changed {
        statements
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    } else {
        sql.to_string()
    }
}

fn inline_query(query: &mut Query, outer: &Scope, changed: &mut bool) {
    let mut scope = outer.clone();
    if let Some(with) = query.with.take() {
        if with.recursive {
            query.with = Some(with); // recursive CTEs can't be inlined
        } else {
            *changed = true;
            for cte in with.cte_tables {
                // Resolve each CTE body against the CTEs defined before it, then
                // add it to scope so later definitions / the body can use it.
                let mut body = cte.query;
                inline_query(&mut body, &scope, changed);
                scope.insert(cte.alias.name.value.clone(), body);
            }
        }
    }
    sub_setexpr(&mut query.body, &scope, changed);
}

fn sub_setexpr(body: &mut SetExpr, scope: &Scope, changed: &mut bool) {
    match body {
        SetExpr::Select(select) => sub_select(select, scope, changed),
        SetExpr::Query(query) => inline_query(query, scope, changed),
        SetExpr::SetOperation { left, right, .. } => {
            sub_setexpr(left, scope, changed);
            sub_setexpr(right, scope, changed);
        }
        _ => {}
    }
}

fn sub_select(select: &mut Select, scope: &Scope, changed: &mut bool) {
    for twj in &mut select.from {
        sub_table_factor(&mut twj.relation, scope, changed);
        for join in &mut twj.joins {
            sub_table_factor(&mut join.relation, scope, changed);
        }
    }
    for item in &mut select.projection {
        if let SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } = item {
            sub_expr(expr, scope, changed);
        }
    }
    if let Some(selection) = &mut select.selection {
        sub_expr(selection, scope, changed);
    }
    if let Some(having) = &mut select.having {
        sub_expr(having, scope, changed);
    }
}

fn sub_table_factor(factor: &mut TableFactor, scope: &Scope, changed: &mut bool) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            if let Some(cte_body) = scope.get(&name.to_string()) {
                // The CTE body is already resolved against earlier CTEs.
                let table_alias = alias.clone().unwrap_or(TableAlias {
                    name: Ident::new(name.to_string()),
                    columns: Vec::new(),
                });
                *factor = TableFactor::Derived {
                    lateral: false,
                    subquery: cte_body.clone(),
                    alias: Some(table_alias),
                };
                *changed = true;
            }
        }
        TableFactor::Derived { subquery, .. } => inline_query(subquery, scope, changed),
        _ => {}
    }
}

fn sub_expr(expr: &mut Expr, scope: &Scope, changed: &mut bool) {
    match expr {
        Expr::Subquery(query)
        | Expr::InSubquery {
            subquery: query, ..
        }
        | Expr::Exists {
            subquery: query, ..
        } => inline_query(query, scope, changed),
        Expr::BinaryOp { left, right, .. } => {
            sub_expr(left, scope, changed);
            sub_expr(right, scope, changed);
        }
        Expr::UnaryOp { expr, .. } | Expr::Nested(expr) => sub_expr(expr, scope, changed),
        Expr::InList { expr, list, .. } => {
            sub_expr(expr, scope, changed);
            for item in list {
                sub_expr(item, scope, changed);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::inline_ctes;

    #[test]
    fn inlines_simple_cte() {
        let out = inline_ctes("WITH c AS (SELECT a FROM t WHERE a > 1) SELECT a FROM c");
        let up = out.to_uppercase();
        assert!(!up.contains("WITH "), "WITH should be gone: {out}");
        assert!(
            up.contains("AS C"),
            "CTE should become a derived table aliased C: {out}"
        );
    }

    #[test]
    fn inlines_two_ctes() {
        let out =
            inline_ctes("WITH a AS (SELECT x FROM t1), b AS (SELECT y FROM t2) SELECT x FROM a, b");
        assert!(!out.to_uppercase().contains("WITH "), "got: {out}");
        assert_eq!(
            out.matches("SELECT").count() >= 3,
            true,
            "both CTEs inlined: {out}"
        );
    }

    #[test]
    fn inlines_cte_in_create_table_as() {
        let out = inline_ctes("CREATE TABLE u AS WITH c AS (SELECT a FROM t) SELECT a FROM c");
        assert!(
            !out.to_uppercase().contains("WITH "),
            "WITH should be gone: {out}"
        );
    }

    #[test]
    fn inlines_cte_in_insert() {
        let out = inline_ctes("INSERT INTO u WITH c AS (SELECT a FROM t) SELECT a FROM c");
        assert!(
            !out.to_uppercase().contains("WITH "),
            "WITH should be gone: {out}"
        );
    }

    #[test]
    fn leaves_recursive_cte() {
        let sql = "WITH RECURSIVE c AS (SELECT 1 AS n UNION SELECT n + 1 FROM c) SELECT n FROM c";
        assert!(
            inline_ctes(sql).to_uppercase().contains("RECURSIVE"),
            "recursive CTE must not be inlined"
        );
    }

    #[test]
    fn no_with_passes_through() {
        let sql = "SELECT a FROM t WHERE a > 1";
        assert_eq!(inline_ctes(sql), sql);
    }
}
