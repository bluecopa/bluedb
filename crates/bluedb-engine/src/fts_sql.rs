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

/// The query text from a `*_tsquery(..)` argument: a single-quoted string
/// literal, or a `$N` placeholder resolved against `params` — so a parameterized
/// `plainto_tsquery($1)` works (the docs advertise this form). The resolved text
/// only feeds the index searcher; it is never re-injected into SQL, so there is
/// no injection surface. A non-string param, an out-of-range index, or a `?` /
/// named placeholder yields `None`.
fn as_query_text(expr: &Expr, params: &[serde_json::Value]) -> Option<String> {
    match expr {
        Expr::Value(Value::SingleQuotedString(s)) => Some(s.clone()),
        Expr::Value(Value::Placeholder(p)) => {
            let n: usize = p.strip_prefix('$')?.parse().ok()?;
            params.get(n.checked_sub(1)?)?.as_str().map(str::to_string)
        }
        _ => None,
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
fn parse_tsquery(expr: &Expr, params: &[serde_json::Value]) -> Option<(TsQueryKind, String)> {
    if let Expr::Function(func) = expr {
        let kind = match fn_last_name(&func.name).as_str() {
            "to_tsquery" => TsQueryKind::ToTsQuery,
            "plainto_tsquery" => TsQueryKind::Plain,
            "websearch_to_tsquery" => TsQueryKind::Websearch,
            _ => return None,
        };
        let args = fn_args(&func.args);
        if args.len() == 1 {
            let query = as_query_text(args[0], params)?;
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
pub(crate) fn extract_fts_predicate(
    sql: &str,
    params: &[serde_json::Value],
) -> Result<Option<FtsPredicate>> {
    let mut stmts = parse(sql).map_err(|e| EngineError::Rejected(format!("parse error: {e}")))?;
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
        TableFactor::Table { name, .. } => {
            name.0.last().map(|i| i.value.clone()).unwrap_or_default()
        }
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

    let (kind, query_str) = parse_tsquery(rhs, params).ok_or_else(|| {
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
// B3: trigram-accelerated LIKE '%lit%'
// ---------------------------------------------------------------------------

/// A `col LIKE '%lit%'` predicate whose literal is a **clean infix** safe to
/// accelerate with a trigram prefilter: `column` is a bare identifier, `literal`
/// is the infix core with its (one optional) leading/trailing `%` stripped and
/// contains no remaining `LIKE` wildcards (`%`/`_`) and is ≥ 3 chars.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LikePredicate {
    pub table: String,
    pub column: String,
    /// The clean infix literal (no surrounding `%`, no internal wildcards, len ≥ 3).
    pub literal: String,
}

/// Recursively search an `Expr` tree for the first non-negated `Expr::Like` whose
/// `expr` is a bare column and `pattern` is a string literal. Returns the
/// `(column, raw_pattern)`. Recurses through AND/OR/Nested like [`find_at_at`],
/// but never into a negated LIKE (a trigram prefilter on a negation would be a
/// false-negative trap) and never into sub-selects.
fn find_like(expr: &Expr) -> Option<(&str, &str)> {
    match expr {
        Expr::Like {
            negated: false,
            any: false,
            expr: col,
            pattern,
            ..
        } => {
            let column = as_identifier(col)?;
            let pat = as_string_literal(pattern)?;
            Some((column, pat))
        }
        Expr::BinaryOp { left, right, .. } => find_like(left).or_else(|| find_like(right)),
        Expr::Nested(inner) => find_like(inner),
        _ => None,
    }
}

/// Strip one optional leading and one optional trailing `%` from `pattern`,
/// returning the infix core — but only if the core is a CLEAN literal: ≥ 3 chars
/// and containing no remaining `%` or `_` (a `LIKE` wildcard inside the core makes
/// the literal's trigrams an unsound prefilter). Returns `None` otherwise (so the
/// caller passes the SQL through to gluesql's exact scan).
fn clean_infix_core(pattern: &str) -> Option<String> {
    // Strip ONE optional leading `%`, then ONE optional trailing `%`.
    let after_prefix = pattern.strip_prefix('%').unwrap_or(pattern);
    let core = after_prefix.strip_suffix('%').unwrap_or(after_prefix);
    if core.chars().count() < 3 {
        return None;
    }
    // Any remaining wildcard inside the core makes the literal's trigrams an
    // unsound prefilter (a `_`/`%` would match rows the trigrams don't cover).
    if core.contains('%') || core.contains('_') {
        return None;
    }
    Some(core.to_string())
}

/// Parse `sql` and extract a trigram-accelerable `col LIKE '%lit%'` predicate from
/// the WHERE clause (single table, no joins — mirrors [`extract_fts_predicate`]).
///
/// Returns `Ok(Some(..))` only for a clean ≥3-char infix LIKE; any other shape
/// (no LIKE, negated, internal wildcard, <3-char core, prefix-only `foo%` whose
/// core fails the clean test, multi-table) → `Ok(None)` — a non-prunable LIKE is
/// valid SQL gluesql runs, NOT an error. (An unparseable SQL is still an `Err`.)
pub(crate) fn extract_like_predicate(sql: &str) -> Result<Option<LikePredicate>> {
    let mut stmts = parse(sql).map_err(|e| EngineError::Rejected(format!("parse error: {e}")))?;
    if stmts.len() != 1 {
        return Ok(None);
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
    let Some(selection) = select.selection.as_ref() else {
        return Ok(None);
    };
    // Single table, no joins (multi-table → unsupported shape → pass-through).
    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Ok(None);
    }
    let table = match &select.from[0].relation {
        TableFactor::Table { name, .. } => {
            name.0.last().map(|i| i.value.clone()).unwrap_or_default()
        }
        _ => return Ok(None),
    };

    let Some((column, pattern)) = find_like(selection) else {
        return Ok(None);
    };
    let Some(literal) = clean_infix_core(pattern) else {
        return Ok(None);
    };
    Ok(Some(LikePredicate {
        table,
        column: column.to_string(),
        literal,
    }))
}

/// Add a `pk IN (candidates)` conjunct to `sql`'s WHERE clause, keeping the
/// original `Expr::Like` intact (gluesql's authoritative exact verify). Empty
/// candidates → an index-served `pk IN (NULL)` never-match conjunct (reusing the
/// `@@` empty-hit approach). Re-emits via Display.
///
/// The caller guarantees `sql` matched [`extract_like_predicate`], so the WHERE
/// clause is present; if the shape changed unexpectedly, returns `Ok(None)`.
pub(crate) fn rewrite_like_query(
    sql: &str,
    pk_column: &str,
    candidates: &[i64],
) -> Result<Option<String>> {
    let mut stmts = parse(sql).map_err(|e| EngineError::Rejected(format!("parse error: {e}")))?;
    let stmt = stmts.remove(0);
    let mut query = match stmt {
        Statement::Query(q) => q,
        _ => return Ok(None),
    };
    let select = match query.body.as_mut() {
        SetExpr::Select(s) => s,
        _ => return Ok(None),
    };
    let Some(selection) = select.selection.take() else {
        return Ok(None);
    };

    let prefilter: Expr = if candidates.is_empty() {
        // Index-served never-match (see `never_match`) — keeps the empty case
        // within the scan guardrail, unlike a constant `1 = 0`.
        never_match(pk_column)
    } else {
        Expr::InList {
            expr: Box::new(Expr::Identifier(Ident::new(pk_column))),
            list: candidates.iter().map(|pk| int_literal(*pk)).collect(),
            negated: false,
        }
    };

    // `prefilter AND (original WHERE)` — the original LIKE stays as gluesql's verify.
    select.selection = Some(Expr::BinaryOp {
        left: Box::new(prefilter),
        op: BinaryOperator::And,
        right: Box::new(Expr::Nested(Box::new(selection))),
    });

    Ok(Some(Statement::Query(query).to_string()))
}

// ---------------------------------------------------------------------------
// Task 3: rewrite_fts_query
// ---------------------------------------------------------------------------

/// Helper: build an integer literal `Expr`.
fn int_literal(n: i64) -> Expr {
    Expr::Value(Value::Number(BigDecimal::from(n), false))
}

/// An **index-served** never-match prefilter: `<pk> IN (NULL)`. It matches no
/// row (a `NULL` membership test is never true) and is planned off the
/// primary-key index — no table scan. We use this instead of a constant `1 = 0`
/// so an empty FTS / trigram hit set still satisfies the scan guardrail
/// (`bluedb_sql::guardrail`), which rejects a WHERE that touches no indexed
/// column. Both are never-matches; only this one is bounded.
fn never_match(pk_column: &str) -> Expr {
    Expr::InList {
        expr: Box::new(Expr::Identifier(Ident::new(pk_column))),
        list: vec![Expr::Value(Value::Null)],
        negated: false,
    }
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
///    Empty result set → an index-served `pk_column IN (NULL)` never-match.
/// 3. Replacing any `ORDER BY ts_rank(...)` with a `CASE pk WHEN v THEN pos …
///    ELSE len END` expression that preserves BM25 rank order.
///
/// Returns `Ok(None)` when the query contains no `@@`.
pub async fn rewrite_fts_query(
    sql: &str,
    pk_column: &str,
    searcher: &impl FtsSearcher,
    params: &[serde_json::Value],
) -> Result<Option<String>> {
    // Fast path: no @@ present.
    let predicate = match extract_fts_predicate(sql, params)? {
        None => return Ok(None),
        Some(p) => p,
    };

    let hits = searcher.search(&predicate).await?;

    // Re-parse to get a mutable Statement we can rewrite.
    let mut stmts = parse(sql).map_err(|e| EngineError::Rejected(format!("parse error: {e}")))?;
    let stmt = stmts.remove(0);
    let mut query = match stmt {
        Statement::Query(q) => q,
        _ => return Ok(None),
    };

    let select = match query.body.as_mut() {
        SetExpr::Select(s) => s,
        _ => return Ok(None),
    };

    // Build the replacement for @@: pk IN (hits), or an index-served never-match
    // (pk IN (NULL)) when there are no hits.
    let pk_ident = Expr::Identifier(Ident::new(pk_column));
    let replacement: Expr = if hits.is_empty() {
        never_match(pk_column)
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
                let conditions: Vec<Expr> = hits.iter().map(|h| int_literal(h.pk)).collect();
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

    // --- B3: extract_like_predicate (gating) ---

    #[test]
    fn extract_like_clean_infix() {
        let p = extract_like_predicate("SELECT id FROM docs WHERE body LIKE '%overdue%'")
            .unwrap()
            .unwrap();
        assert_eq!(p.table, "docs");
        assert_eq!(p.column, "body");
        assert_eq!(
            p.literal, "overdue",
            "one leading + one trailing % stripped"
        );
    }

    #[test]
    fn extract_like_prefix_and_suffix_cores() {
        // Suffix anchor `%lit`: strip leading %, no trailing % → core 'overdue'.
        let p = extract_like_predicate("SELECT id FROM docs WHERE body LIKE '%overdue'")
            .unwrap()
            .unwrap();
        assert_eq!(p.literal, "overdue");
        // Prefix anchor `lit%`: no leading %, strip trailing % → core 'overdue'.
        let p = extract_like_predicate("SELECT id FROM docs WHERE body LIKE 'overdue%'")
            .unwrap()
            .unwrap();
        assert_eq!(p.literal, "overdue");
        // Bare literal (no % at all) ≥ 3 chars is also a clean infix core.
        let p = extract_like_predicate("SELECT id FROM docs WHERE body LIKE 'overdue'")
            .unwrap()
            .unwrap();
        assert_eq!(p.literal, "overdue");
    }

    #[test]
    fn extract_like_gated_cases_pass_through() {
        // < 3-char core → None.
        assert!(
            extract_like_predicate("SELECT id FROM docs WHERE body LIKE '%ab%'")
                .unwrap()
                .is_none()
        );
        // Internal wildcard in the core → None.
        assert!(
            extract_like_predicate("SELECT id FROM docs WHERE body LIKE '%ov_rdue%'")
                .unwrap()
                .is_none()
        );
        assert!(
            extract_like_predicate("SELECT id FROM docs WHERE body LIKE '%over%due%'")
                .unwrap()
                .is_none()
        );
        // Negated LIKE → None.
        assert!(
            extract_like_predicate("SELECT id FROM docs WHERE body NOT LIKE '%overdue%'")
                .unwrap()
                .is_none()
        );
        // No LIKE at all → None.
        assert!(
            extract_like_predicate("SELECT id FROM docs WHERE status = 'open'")
                .unwrap()
                .is_none()
        );
        // Multi-table → None (pass-through, not error).
        assert!(
            extract_like_predicate("SELECT id FROM a, b WHERE x LIKE '%overdue%'")
                .unwrap()
                .is_none()
        );
    }

    // --- B3: rewrite_like_query ---

    #[test]
    fn rewrite_like_adds_pk_in_prefilter_keeping_like() {
        let sql = "SELECT id FROM docs WHERE body LIKE '%overdue%'";
        let out = rewrite_like_query(sql, "id", &[1, 3]).unwrap().unwrap();
        assert_eq!(
            out,
            "SELECT id FROM docs WHERE id IN (1, 3) AND (body LIKE '%overdue%')"
        );
    }

    #[test]
    fn rewrite_like_empty_candidates_never_match() {
        let sql = "SELECT id FROM docs WHERE body LIKE '%overdue%'";
        let out = rewrite_like_query(sql, "id", &[]).unwrap().unwrap();
        assert_eq!(
            out,
            "SELECT id FROM docs WHERE id IN (NULL) AND (body LIKE '%overdue%')"
        );
    }

    #[test]
    fn rewrite_like_preserves_extra_conditions() {
        let sql = "SELECT id FROM docs WHERE body LIKE '%overdue%' AND status = 'open'";
        let out = rewrite_like_query(sql, "id", &[7]).unwrap().unwrap();
        // The original WHERE (LIKE AND status) is nested under the prefilter,
        // both the LIKE and the status condition preserved for gluesql.
        assert!(out.contains("id IN (7)"), "prefilter present: {out}");
        assert!(out.contains("body LIKE '%overdue%'"), "LIKE kept: {out}");
        assert!(
            out.contains("status = 'open'"),
            "extra condition kept: {out}"
        );
    }

    // --- Task 2: extract_fts_predicate ---

    #[test]
    fn extract_plain_tsquery() {
        let sql = "SELECT id, title FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')";
        let pred = extract_fts_predicate(sql, &[]).unwrap().unwrap();
        assert_eq!(pred.table, "docs");
        assert_eq!(pred.column, "body");
        assert_eq!(pred.config, "english");
        assert_eq!(pred.query, "invoice overdue");
        assert_eq!(pred.kind, TsQueryKind::Plain);
    }

    #[test]
    fn extract_plain_tsquery_with_param_placeholder() {
        // Parameterized RHS: `plainto_tsquery($1)` resolves the query text from
        // params (the documented form) instead of requiring a string literal.
        let sql = "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery($1)";
        let params = vec![serde_json::json!("invoice overdue")];
        let pred = extract_fts_predicate(sql, &params).unwrap().unwrap();
        assert_eq!(pred.query, "invoice overdue");
        assert_eq!(pred.kind, TsQueryKind::Plain);
    }

    #[test]
    fn param_placeholder_without_params_is_rejected() {
        // No bound params → the placeholder can't resolve → unsupported shape
        // (rather than silently searching for the literal "$1").
        let sql = "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery($1)";
        assert!(extract_fts_predicate(sql, &[]).is_err());
    }

    #[test]
    fn extract_to_tsquery() {
        let sql = "SELECT id FROM docs WHERE to_tsvector('english', body) @@ to_tsquery('a & b')";
        let pred = extract_fts_predicate(sql, &[]).unwrap().unwrap();
        assert_eq!(pred.kind, TsQueryKind::ToTsQuery);
        assert_eq!(pred.query, "a & b");
    }

    #[test]
    fn extract_websearch_tsquery() {
        let sql =
            "SELECT id FROM docs WHERE to_tsvector('english', body) @@ websearch_to_tsquery('x')";
        let pred = extract_fts_predicate(sql, &[]).unwrap().unwrap();
        assert_eq!(pred.kind, TsQueryKind::Websearch);
        assert_eq!(pred.query, "x");
    }

    #[test]
    fn no_at_at_returns_none() {
        let sql = "SELECT * FROM docs WHERE status = 'open'";
        assert!(extract_fts_predicate(sql, &[]).unwrap().is_none());
    }

    #[test]
    fn multi_table_with_at_at_errors() {
        let sql = "SELECT * FROM a, b WHERE to_tsvector('english', x) @@ plainto_tsquery('q')";
        assert!(extract_fts_predicate(sql, &[]).is_err());
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
        let out = rewrite_fts_query(sql, "id", &FakeSearcher, &[])
            .await
            .unwrap()
            .unwrap();
        assert!(
            out.contains("id IN (2, 5)"),
            "expected IN clause, got: {out}"
        );
        assert!(!out.contains("@@"), "@@  should be gone: {out}");
        assert!(
            !out.contains("to_tsvector"),
            "to_tsvector should be gone: {out}"
        );
    }

    #[tokio::test]
    async fn rewrite_at_at_with_extra_condition() {
        let sql = "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('q') AND status = 'open'";
        let out = rewrite_fts_query(sql, "id", &FakeSearcher, &[])
            .await
            .unwrap()
            .unwrap();
        assert!(
            out.contains("id IN (2, 5)"),
            "expected IN clause, got: {out}"
        );
        assert!(
            out.contains("status"),
            "status condition should be kept: {out}"
        );
        assert!(!out.contains("@@"), "@@ should be gone: {out}");
    }

    #[tokio::test]
    async fn rewrite_empty_hits_never_match() {
        let sql = "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('q')";
        let out = rewrite_fts_query(sql, "id", &EmptySearcher, &[])
            .await
            .unwrap()
            .unwrap();
        let low = out.to_lowercase();
        // Must be an index-served never-match (`pk IN (NULL)`), so the empty case
        // still satisfies the scan guardrail.
        assert!(
            low.contains("in (null)"),
            "expected index-served never-match, got: {out}"
        );
        assert!(!out.contains("@@"), "@@ should be gone: {out}");
    }

    #[tokio::test]
    async fn rewrite_ts_rank_replaced_by_case() {
        let sql = "SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('q') ORDER BY ts_rank(to_tsvector('english', body), plainto_tsquery('q')) DESC";
        let out = rewrite_fts_query(sql, "id", &FakeSearcher, &[])
            .await
            .unwrap()
            .unwrap();
        let low = out.to_lowercase();
        assert!(!low.contains("ts_rank"), "ts_rank should be gone: {out}");
        assert!(
            out.contains("CASE") || out.contains("case"),
            "CASE ordering should be present: {out}"
        );
    }

    #[tokio::test]
    async fn rewrite_no_at_at_returns_none() {
        let sql = "SELECT * FROM docs WHERE status = 'open'";
        let out = rewrite_fts_query(sql, "id", &FakeSearcher, &[])
            .await
            .unwrap();
        assert!(out.is_none());
    }
}
