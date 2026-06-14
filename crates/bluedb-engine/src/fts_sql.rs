//! SQL-integrated full-text search: the pre-parse rewrite that turns the
//! PostgreSQL FTS surface (`to_tsvector(cfg,col) @@ *_tsquery(q)`, `ts_rank`)
//! into a tantivy-backed `pk IN (...)` query gluesql can execute. gluesql's
//! `translate` rejects `@@`, so this MUST run before gluesql parses.

use bigdecimal::BigDecimal;
use gluesql_core::parse_sql::parse;
use gluesql_core::sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName,
    SetExpr, Statement, TableFactor, TableWithJoins, Value,
};

use crate::error::{EngineError, Result};

// ---------------------------------------------------------------------------
// Public seam types
// ---------------------------------------------------------------------------

/// Which tsquery function was used on the right-hand side of `@@`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TsQueryKind {
    ToTsQuery,
    Plain,
    Websearch,
}

/// The FTS predicate extracted from a PostgreSQL-style
/// `to_tsvector(config, col) @@ *_tsquery(query)` expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtsPredicate {
    pub table: String,
    pub column: String,
    pub config: String,
    pub query: String,
    pub kind: TsQueryKind,
}

/// A single FTS hit returned by a searcher.
#[derive(Debug, Clone, PartialEq)]
pub struct FtsHit {
    pub pk: i64,
    pub score: f32,
}

/// The async seam that backs the FTS rewrite. In production this wraps a real
/// tantivy index (later increment); in tests a `FakeSearcher` suffices.
#[async_trait::async_trait]
pub trait FtsSearcher {
    async fn search(&self, predicate: &FtsPredicate) -> Result<Vec<FtsHit>>;
}

// ---------------------------------------------------------------------------
// Internal parse helpers
// ---------------------------------------------------------------------------

/// Pull the last segment of a dotted function name, lowercased.
fn fn_last_name(name: &ObjectName) -> String {
    name.0
        .last()
        .map(|i| i.value.to_lowercase())
        .unwrap_or_default()
}

/// Extract the string value from `Expr::Value(Value::SingleQuotedString(..))`.
fn as_string_literal(expr: &Expr) -> Option<&str> {
    if let Expr::Value(Value::SingleQuotedString(s)) = expr {
        Some(s.as_str())
    } else {
        None
    }
}

/// Extract an identifier value from `Expr::Identifier` or the last segment of
/// `Expr::CompoundIdentifier`.
fn as_identifier(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(Ident { value, .. }) => Some(value.as_str()),
        Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.as_str()),
        _ => None,
    }
}

/// Collect the plain (unnamed) expr args from a function's `FunctionArguments`.
fn fn_args(args: &FunctionArguments) -> Vec<&Expr> {
    match args {
        FunctionArguments::List(list) => list
            .args
            .iter()
            .filter_map(|a| match a {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => Some(e),
                _ => None,
            })
            .collect(),
        _ => vec![],
    }
}

/// Try to parse `to_tsvector(config, col)` from an `Expr::Function`.
/// Returns `(config_str, column_str)` on success.
fn parse_tsvector(expr: &Expr) -> Option<(String, String)> {
    if let Expr::Function(func) = expr {
        if fn_last_name(&func.name) == "to_tsvector" {
            let args = fn_args(&func.args);
            if args.len() == 2 {
                let config = as_string_literal(args[0])?.to_owned();
                let column = as_identifier(args[1])?.to_owned();
                return Some((config, column));
            }
        }
    }
    None
}

/// Try to parse `*_tsquery(query)` from an `Expr::Function`.
/// Returns `(kind, query_str)` on success.
fn parse_tsquery(expr: &Expr) -> Option<(TsQueryKind, String)> {
    if let Expr::Function(func) = expr {
        let kind = match fn_last_name(&func.name).as_str() {
            "to_tsquery" => TsQueryKind::ToTsQuery,
            "plainto_tsquery" => TsQueryKind::Plain,
            "websearch_to_tsquery" => TsQueryKind::Websearch,
            _ => return None,
        };
        let args = fn_args(&func.args);
        if args.len() == 1 {
            let query = as_string_literal(args[0])?.to_owned();
            return Some((kind, query));
        }
    }
    None
}

/// Recursively search an Expr tree for exactly one `@@ ` binary-op node.
/// Returns `Some((left, right))` for the first one found, or `None`.
/// Does NOT recurse into sub-selects (we only care about the WHERE clause).
fn find_at_at(expr: &Expr) -> Option<(&Expr, &Expr)> {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if *op == BinaryOperator::AtAt {
                return Some((left, right));
            }
            // Recurse through AND / OR / other binary ops
            find_at_at(left).or_else(|| find_at_at(right))
        }
        Expr::Nested(inner) => find_at_at(inner),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Task 2: extract_fts_predicate
// ---------------------------------------------------------------------------

/// Parse `sql` and extract any PostgreSQL FTS predicate
/// (`to_tsvector(cfg,col) @@ *_tsquery(q)`) from the WHERE clause.
///
/// Returns `Ok(None)` if no `@@` is present, `Ok(Some(...))` when a valid FTS
/// predicate is found, or `Err` for an unsupported shape (multi-table FROM,
/// wrong argument types, etc.).
pub(crate) fn extract_fts_predicate(sql: &str) -> Result<Option<FtsPredicate>> {
    let mut stmts =
        parse(sql).map_err(|e| EngineError::Rejected(format!("parse error: {e}")))?;
    if stmts.len() != 1 {
        return Err(EngineError::Rejected(format!(
            "expected exactly 1 statement, got {}",
            stmts.len()
        )));
    }

    let stmt = stmts.remove(0);
    let query = match stmt {
        Statement::Query(q) => q,
        _ => return Ok(None),
    };

    let select = match *query.body {
        SetExpr::Select(s) => s,
        _ => return Ok(None),
    };

    // Check for the @@ anywhere in the WHERE before we validate the FROM shape,
    // so we can return an error for unsupported-shape queries that DO contain @@.
    let has_at_at = select
        .selection
        .as_ref()
        .map(|w| find_at_at(w).is_some())
        .unwrap_or(false);

    if !has_at_at {
        return Ok(None);
    }

    // Validate FROM: exactly one table, no joins.
    if select.from.len() != 1 {
        return Err(EngineError::Rejected(format!(
            "unsupported FTS query shape: expected exactly 1 FROM table, got {}",
            select.from.len()
        )));
    }
    let twj: &TableWithJoins = &select.from[0];
    if !twj.joins.is_empty() {
        return Err(EngineError::Rejected(
            "unsupported FTS query shape: JOINs not supported with @@".into(),
        ));
    }
    let table = match &twj.relation {
        TableFactor::Table { name, .. } => name
            .0
            .last()
            .map(|i| i.value.clone())
            .unwrap_or_default(),
        _ => {
            return Err(EngineError::Rejected(
                "unsupported FTS query shape: FROM must be a plain table".into(),
            ))
        }
    };

    // Extract the @@ operands.
    let selection = select.selection.as_ref().unwrap();
    let (lhs, rhs) = find_at_at(selection).unwrap(); // safe: has_at_at==true

    let (config, column) = parse_tsvector(lhs).ok_or_else(|| {
        EngineError::Rejected(
            "unsupported FTS query shape: @@ LHS must be to_tsvector(config, col)".into(),
        )
    })?;

    let (kind, query_str) = parse_tsquery(rhs).ok_or_else(|| {
        EngineError::Rejected(
            "unsupported FTS query shape: @@ RHS must be to_tsquery/plainto_tsquery/websearch_to_tsquery(q)".into(),
        )
    })?;

    Ok(Some(FtsPredicate {
        table,
        column,
        config,
        query: query_str,
        kind,
    }))
}

// ---------------------------------------------------------------------------
// Task 3: rewrite_fts_query
// ---------------------------------------------------------------------------

/// Helper: build an integer literal `Expr`.
fn int_literal(n: i64) -> Expr {
    Expr::Value(Value::Number(BigDecimal::from(n), false))
}

/// Replace the first `@@` expr found in `expr` (in-place) with `replacement`.
fn replace_at_at(expr: &mut Expr, replacement: Expr) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if *op == BinaryOperator::AtAt {
                *expr = replacement;
                return true;
            }
            if replace_at_at(left, replacement.clone()) {
                return true;
            }
            replace_at_at(right, replacement)
        }
        Expr::Nested(inner) => replace_at_at(inner, replacement),
        _ => false,
    }
}

/// Returns true if `expr` is a call to `ts_rank(...)`.
fn is_ts_rank(expr: &Expr) -> bool {
    if let Expr::Function(func) = expr {
        return fn_last_name(&func.name) == "ts_rank";
    }
    false
}

/// Rewrite a PostgreSQL FTS query by:
///
/// 1. Calling `searcher` to get the matching primary-key values.
/// 2. Replacing `to_tsvector(...) @@ *_tsquery(...)` with `pk_column IN (...)`.
///    Empty result set → `1 = 0` (never-match, safe with gluesql).
/// 3. Replacing any `ORDER BY ts_rank(...)` with a `CASE pk WHEN v THEN pos …
///    ELSE len END` expression that preserves BM25 rank order.
///
/// Returns `Ok(None)` when the query contains no `@@`.
pub async fn rewrite_fts_query(
    sql: &str,
    pk_column: &str,
    searcher: &impl FtsSearcher,
) -> Result<Option<String>> {
    // Fast path: no @@ present.
    let predicate = match extract_fts_predicate(sql)? {
        None => return Ok(None),
        Some(p) => p,
    };

    let hits = searcher.search(&predicate).await?;

    // Re-parse to get a mutable Statement we can rewrite.
    let mut stmts =
        parse(sql).map_err(|e| EngineError::Rejected(format!("parse error: {e}")))?;
    let stmt = stmts.remove(0);
    let mut query = match stmt {
        Statement::Query(q) => q,
        _ => return Ok(None),
    };

    let select = match query.body.as_mut() {
        SetExpr::Select(s) => s,
        _ => return Ok(None),
    };

    // Build the replacement for @@: pk IN (...) or 1 = 0.
    let pk_ident = Expr::Identifier(Ident::new(pk_column));
    let replacement: Expr = if hits.is_empty() {
        // 1 = 0 — universally safe never-match.
        Expr::BinaryOp {
            left: Box::new(int_literal(1)),
            op: BinaryOperator::Eq,
            right: Box::new(int_literal(0)),
        }
    } else {
        Expr::InList {
            expr: Box::new(pk_ident.clone()),
            list: hits.iter().map(|h| int_literal(h.pk)).collect(),
            negated: false,
        }
    };

    // Mutate the WHERE clause in-place.
    if let Some(selection) = select.selection.as_mut() {
        replace_at_at(selection, replacement);
    }

    // Rewrite ORDER BY ts_rank(...) → CASE pk WHEN pk_val THEN pos … ELSE len END.
    if let Some(order_by) = query.order_by.as_mut() {
        let n = hits.len();
        for ob_expr in order_by.exprs.iter_mut() {
            if is_ts_rank(&ob_expr.expr) {
                // Build CASE id WHEN 2 THEN 0 WHEN 5 THEN 1 ELSE len END
                let conditions: Vec<Expr> = hits
                    .iter()
                    .map(|h| int_literal(h.pk))
                    .collect();
                let results: Vec<Expr> = (0..n as i64).map(int_literal).collect();
                let case_expr = Expr::Case {
                    operand: Some(Box::new(pk_ident.clone())),
                    conditions,
                    results,
                    else_result: Some(Box::new(int_literal(n as i64))),
                };
                ob_expr.expr = case_expr;
                ob_expr.asc = Some(true); // ascending → best-rank (pos 0) first
            }
        }
    }

    Ok(Some(Statement::Query(query).to_string()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- Task 2: extract_fts_predicate ---

    #[test]
    fn extract_plain_tsquery() {
        let sql = "SELECT id, title FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')";
        let pred = extract_fts_predicate(sql).unwrap().unwrap();
        assert_eq!(pred.table, "docs");
        assert_eq!(pred.column, "body");
        assert_eq!(pred.config, "english");
        assert_eq!(pred.query, "invoice overdue");
        assert_eq!(pred.kind, TsQueryKind::Plain);
    }

    #[test]
    fn extract_to_tsquery() {
        let sql = "SELECT id FROM docs WHERE to_tsvector('english', body) @@ to_tsquery('a & b')";
        let pred = extract_fts_predicate(sql).unwrap().unwrap();
        assert_eq!(pred.kind, TsQueryKind::ToTsQuery);
        assert_eq!(pred.query, "a & b");
    }

    #[test]
    fn extract_websearch_tsquery() {
        let sql = "SELECT id FROM docs WHERE to_tsvector('english', body) @@ websearch_to_tsquery('x')";
        let pred = extract_fts_predicate(sql).unwrap().unwrap();
        assert_eq!(pred.kind, TsQueryKind::Websearch);
        assert_eq!(pred.query, "x");
    }

    #[test]
    fn no_at_at_returns_none() {
        let sql = "SELECT * FROM docs WHERE status = 'open'";
        assert!(extract_fts_predicate(sql).unwrap().is_none());
    }

    #[test]
    fn multi_table_with_at_at_errors() {
        let sql = "SELECT * FROM a, b WHERE to_tsvector('english', x) @@ plainto_tsquery('q')";
        assert!(extract_fts_predicate(sql).is_err());
    }

    // --- Task 3: rewrite_fts_query ---

    struct FakeSearcher;

    #[async_trait::async_trait]
    impl FtsSearcher for FakeSearcher {
        async fn search(&self, _pred: &FtsPredicate) -> Result<Vec<FtsHit>> {
            Ok(vec![
                FtsHit { pk: 2, score: 0.9 },
                FtsHit { pk: 5, score: 0.4 },
            ])
        }
    }

    struct EmptySearcher;

    #[async_trait::async_trait]
    impl FtsSearcher for EmptySearcher {
        async fn search(&self, _pred: &FtsPredicate) -> Result<Vec<FtsHit>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn rewrite_basic_at_at() {
        let sql = "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('q')";
        let out = rewrite_fts_query(sql, "id", &FakeSearcher).await.unwrap().unwrap();
        assert!(out.contains("id IN (2, 5)"), "expected IN clause, got: {out}");
        assert!(!out.contains("@@"), "@@  should be gone: {out}");
        assert!(!out.contains("to_tsvector"), "to_tsvector should be gone: {out}");
    }

    #[tokio::test]
    async fn rewrite_at_at_with_extra_condition() {
        let sql = "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('q') AND status = 'open'";
        let out = rewrite_fts_query(sql, "id", &FakeSearcher).await.unwrap().unwrap();
        assert!(out.contains("id IN (2, 5)"), "expected IN clause, got: {out}");
        assert!(out.contains("status"), "status condition should be kept: {out}");
        assert!(!out.contains("@@"), "@@ should be gone: {out}");
    }

    #[tokio::test]
    async fn rewrite_empty_hits_never_match() {
        let sql = "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('q')";
        let out = rewrite_fts_query(sql, "id", &EmptySearcher).await.unwrap().unwrap();
        let low = out.to_lowercase();
        // Must be a never-match: either "1 = 0" or "false"
        assert!(
            low.contains("1 = 0") || low.contains("false"),
            "expected never-match expression, got: {out}"
        );
        assert!(!out.contains("@@"), "@@ should be gone: {out}");
    }

    #[tokio::test]
    async fn rewrite_ts_rank_replaced_by_case() {
        let sql = "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('q') ORDER BY ts_rank(to_tsvector('english', body), plainto_tsquery('q')) DESC";
        let out = rewrite_fts_query(sql, "id", &FakeSearcher).await.unwrap().unwrap();
        let low = out.to_lowercase();
        assert!(!low.contains("ts_rank"), "ts_rank should be gone: {out}");
        assert!(out.contains("CASE") || out.contains("case"), "CASE ordering should be present: {out}");
    }

    #[tokio::test]
    async fn rewrite_no_at_at_returns_none() {
        let sql = "SELECT * FROM docs WHERE status = 'open'";
        let out = rewrite_fts_query(sql, "id", &FakeSearcher).await.unwrap();
        assert!(out.is_none());
    }
}
