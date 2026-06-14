# bluedb Spec B — increment B1: FtsSearcher seam + `@@` pre-parse rewrite

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** The crux of SQL-integrated FTS: a pre-parse pass that recognizes the PostgreSQL FTS surface (`to_tsvector(cfg,col) @@ <*_tsquery>(q)`, `ts_rank`) and rewrites it into a tantivy-search-backed `pk IN (...)` query that gluesql can run. This is mandatory because gluesql's `translate` rejects `BinaryOperator::AtAt` before any planner hook (verified in Spec B). B1 builds the seam + the rewrite as a pure, unit-tested module; B2 wires a real searcher + index maintenance.

**Architecture:** A new module `crates/bluedb-engine/src/fts_sql.rs`:
- `trait FtsSearcher` (async): given a search request, returns matching `(pk, score)` ranked by BM25.
- `async fn rewrite_fts_query(sql, pk_column, searcher) -> Result<Option<String>>`: parse with `gluesql_core::sqlparser`; if the statement is a single `SELECT` containing a `to_tsvector(cfg,col) @@ <*_tsquery>(lit)` predicate, extract `(col, cfg, query, kind)`, call the searcher, rewrite the `@@` node → `pk_column IN (<pks>)` (or a never-match when empty), rewrite `ORDER BY ts_rank(...)` → a `CASE pk WHEN .. THEN ..` ordering that preserves BM25 rank, re-emit via sqlparser `Display`. Returns `Ok(None)` when there's no `@@` (caller runs the SQL unchanged). Unsupported shapes containing `@@` → `Err`.

**Tech Stack:** Rust, `gluesql_core::sqlparser` (0.52, re-exported), async-trait or native async-fn-in-trait.

**Scope (B1):** the rewrite + seam, PURE and unit-tested against a fake searcher. NOT in B1: a real tantivy-backed searcher, `CREATE FULLTEXT INDEX`, write-path maintenance, RYW/live-segment, durable seal, failover, trigram — those are later increments (B2+).

---

## Task 1: `FtsSearcher` trait + search request/hit types

**Files:** new `crates/bluedb-engine/src/fts_sql.rs`; `mod fts_sql;` in `lib.rs`.

- [ ] Define (TDD: write the module + a trivial type test first):
```rust
//! SQL-integrated full-text search: the pre-parse rewrite that turns the
//! PostgreSQL FTS surface (`to_tsvector(cfg,col) @@ *_tsquery(q)`, `ts_rank`)
//! into a tantivy-backed `pk IN (...)` query gluesql can execute. gluesql's
//! `translate` rejects `@@`, so this MUST run before gluesql parses.
use crate::error::Result;

/// Which `*_tsquery` parser produced the query string (selects tantivy query parsing).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TsQueryKind {
    /// `to_tsquery` — boolean/operator syntax.
    ToTsQuery,
    /// `plainto_tsquery` — AND of terms.
    Plain,
    /// `websearch_to_tsquery` — web-search syntax.
    Websearch,
}

/// A parsed FTS predicate extracted from a SQL statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtsPredicate {
    pub table: String,
    pub column: String,
    pub config: String,    // e.g. "english" (the analyzer/text-search config)
    pub query: String,     // the user query string
    pub kind: TsQueryKind,
}

/// One ranked match: the row's primary key and its BM25 score.
#[derive(Debug, Clone, PartialEq)]
pub struct FtsHit {
    pub pk: i64,
    pub score: f32,
}

/// Resolves an `FtsPredicate` to ranked `(pk, score)` hits via the FTS index.
#[async_trait::async_trait]
pub trait FtsSearcher {
    async fn search(&self, predicate: &FtsPredicate) -> Result<Vec<FtsHit>>;
}
```
(Use `async_trait` — it's already used in `bluedb-sql`; add it to `bluedb-engine`'s deps if not present. Or use native `async fn` in trait if the toolchain/edition supports it cleanly — pick what compiles without `dyn` headaches, since B2 will use `&impl FtsSearcher`.)
- [ ] `mod fts_sql;` (or `pub mod fts_sql;`) in `lib.rs`; re-export the public types. Build clean. Commit:
```bash
git commit -am "feat(engine): FtsSearcher seam + FTS predicate/hit types"
```

## Task 2: extract the `@@` predicate from a SELECT (pure parser pass)

**Files:** `fts_sql.rs` (+ tests).

- [ ] **Failing test** — add `#[cfg(test)] mod tests` asserting `extract_fts_predicate(sql) -> Result<Option<(FtsPredicate, ParsedShape)>>` (you choose the exact return that Task 3 needs) on:
  - `SELECT id, title FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('invoice overdue')` → `Some` with `FtsPredicate{ table:"docs", column:"body", config:"english", query:"invoice overdue", kind:Plain }`.
  - `to_tsquery('a & b')` → `kind: ToTsQuery`; `websearch_to_tsquery('x')` → `Websearch`.
  - `SELECT * FROM docs WHERE status = 'open'` (no `@@`) → `Ok(None)`.
  - `SELECT * FROM a, b WHERE to_tsvector('english', a.x) @@ plainto_tsquery('q')` (multi-table) → `Err` (unsupported shape with `@@`).
- [ ] **Implement** using `gluesql_core::sqlparser::parser`/`ast`: `gluesql_core::parse(sql)` → one `Statement::Query`; descend `query.body` → `SetExpr::Select(select)`; require exactly one `select.from` table (`TableFactor::Table { name, .. }`) → `table`; walk `select.selection` (the WHERE `Expr`) to find a `Expr::BinaryOp { left, op: BinaryOperator::AtAt, right }` where `left` is `Expr::Function` named `to_tsvector` with 2 args (a string-literal `config` + an identifier `column`) and `right` is `Expr::Function` named `to_tsquery`/`plainto_tsquery`/`websearch_to_tsquery` with one string-literal arg (`query`). Extract those. If `@@` appears but the shape doesn't match (multi-table, wrong arg shapes), return `Err`. If no `@@` anywhere, `Ok(None)`.
  - sqlparser 0.52 notes: function args are `FunctionArguments::List(FunctionArgumentList{ args, .. })`, each `FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr))`. String literals are `Expr::Value(Value::SingleQuotedString(s))`. Identifiers are `Expr::Identifier(Ident)` or `Expr::CompoundIdentifier`. Function name is `func.name.0` (a `Vec<Ident>`); take the last segment, lowercase-compare.
- [ ] Run tests (pass), commit:
```bash
git commit -am "feat(engine): extract PostgreSQL @@ FTS predicate from a SELECT"
```

## Task 3: rewrite `@@` → `pk IN (...)` (+ ts_rank ordering), re-emit

**Files:** `fts_sql.rs` (+ tests).

- [ ] **Failing test** — a `FakeSearcher` returning, for any predicate, `[FtsHit{pk:2,score:0.9}, FtsHit{pk:5,score:0.4}]`. Assert `rewrite_fts_query(sql, "id", &fake).await`:
  - `SELECT id FROM docs WHERE to_tsvector('english', body) @@ plainto_tsquery('q')`
    → `Ok(Some("SELECT id FROM docs WHERE id IN (2, 5)"))` (exact string may vary with sqlparser's `Display`; assert it parses back and contains `id IN (2, 5)` and no `@@`/`to_tsvector`).
  - With an extra conjunct: `... WHERE to_tsvector('english',body) @@ plainto_tsquery('q') AND status = 'open'`
    → the rewrite keeps `AND status = 'open'` and replaces only the `@@` term with `id IN (2, 5)`.
  - Empty hits (`FakeSearcher` returns `[]`) → the `@@` term becomes a never-match (e.g. `id IN ()` is invalid SQL — use `1 = 0` / `FALSE` instead). Assert the result yields zero rows semantically (contains `1 = 0` or `FALSE`).
  - `ORDER BY ts_rank(to_tsvector('english',body), plainto_tsquery('q')) DESC`
    → replaced by an ordering that preserves BM25 rank: emit `ORDER BY CASE id WHEN 2 THEN 0 WHEN 5 THEN 1 ELSE 2 END` (ascending = best-first; the searcher already returns best-first). Assert the rewritten SQL orders by that CASE and the `ts_rank(...)` call is gone.
  - No `@@` → `Ok(None)`.
- [ ] **Implement**: build on Task 2's extraction. Mutate the parsed `Statement` AST in place:
  - Replace the `@@` `Expr` with `Expr::InList { expr: Box::new(Expr::Identifier(pk_column)), list: pks.map(int_literal), negated: false }`; when `pks` is empty, replace with a `FALSE`/`1=0` `Expr` instead.
  - If `select`/`query.order_by` contains an `OrderByExpr` whose `expr` is a `ts_rank(...)` `Function`, replace that `expr` with a `CASE pk WHEN p_i THEN i ... ELSE n END` `Expr` (and set the order to ascending) so rows come back in BM25 order. (If implementing the CASE proves disproportionately fiddly, a simpler acceptable B1 behavior: drop the `ts_rank` ORDER BY term and document that result order follows the `IN` list — but prefer the CASE.)
  - Re-emit via the AST's `Display`/`to_string()` (sqlparser `Statement: Display`). Return `Ok(Some(string))`.
  - `Ok(None)` when Task 2 found no `@@`.
- [ ] Run tests (pass), `cargo test -p bluedb-engine`, `cargo build --workspace 2>&1 | grep -i warn` (clean). Commit:
```bash
git commit -am "feat(engine): rewrite @@/ts_rank to pk IN (...) + CASE ordering (fork-free, pre-parse)"
```

## Self-Review
- Spec B coverage (B1 slice): Postgres `@@`/`*_tsquery`/`ts_rank` surface recognized ✓; pre-parse rewrite (not planner hook — gluesql rejects `@@`) ✓; fork-free ✓; `FtsSearcher` seam for B2 ✓; empty-result safety ✓; rank-preserving ordering ✓.
- Deferred to B2+: real tantivy searcher, `CREATE FULLTEXT INDEX`, write-path maintenance, RYW/live-segment, seal, failover, trigram LIKE/regex — documented as such.
- Tests are pure (fake searcher), so B1 is fully verifiable without the FTS engine or write path.
