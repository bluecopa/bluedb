# bluedb Spec B — increment B2a: in-memory `LiveSegment` (real `FtsSearcher`)

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** Make the B1 rewrite seam *real*. B1 left `FtsSearcher` backed only by a test fake. B2a adds `LiveSegment` — an in-memory tantivy index (BM25) with a tombstone set — that implements `FtsSearcher`. After B2a, `rewrite_fts_query` can run against actual relevance search. This is **pure and in-memory**: no write-path coupling, no DDL, no SlateDB — fully unit-testable. (Wiring it to the SQL commit path + DDL is B2b/B2c.)

**Architecture:** new module `crates/bluedb-engine/src/live_segment.rs`:
- `LiveSegment` wraps a tantivy **RAM-directory** index built from `bluedb_fts::mapping::IndexMapping` with two fields: an `I64` `pk` field (`stored + fast`, the SQL primary key) and a `Text` `body` field carrying the chosen `Analyzer`. Plus a tombstone `HashSet<i64>`.
- Interior mutability from the start (production shape — B2c mutates via the commit tap while reads run): `Mutex<IndexWriter>`, an `IndexReader` (manual reload), `Mutex<HashSet<i64>>`, `AtomicBool dirty`.
- `index(&self, pk, text)` buffers an add (sets `dirty`); `tombstone(&self, pk)` records a delete; `search` lazily commits+reloads when `dirty` (NRT read-your-writes), runs BM25, returns `(pk, score)` minus tombstones.
- Implements `FtsSearcher` (the B1 trait): maps `FtsPredicate.query`/`.kind` → a tantivy query string, searches, drops tombstoned pks.

**Tech stack:** Rust, the workspace tantivy git pin (quickwit-oss fork, rev 6270552 — `default-features=false`, features lz4/mmap/quickwit/zstd). `bluedb-engine` already depends on `tantivy`, `bluedb-fts`, `async-trait`. **Fork note:** `TopDocs::with_limit(n)` is a `Collector` only after `.order_by_score()` (see `bluedb-fts/src/search.rs:127`). RAM index: `tantivy::Index::create_in_ram(schema)`.

**Scope (B2a):** the in-memory segment + real `FtsSearcher` + analyzer-from-config + tsquery-kind→tantivy-query translation, unit-tested against a real tantivy index. **NOT in B2a:** durable splits, the `bluedb-fts` `FtsIndex` union, commit tap, DDL, seal, RYW through SQL, trigram. Those are B2b+.

---

## Task 1: `LiveSegment` skeleton — build a RAM index, index + search by pk

**Files:** new `crates/bluedb-engine/src/live_segment.rs`; `mod live_segment;` + re-export in `lib.rs`.

- [ ] **Failing test first** (`#[cfg(test)] mod tests` in the module). Build a segment with the `english` config, index three rows, search, assert pks/order:
```rust
#[tokio::test]
async fn indexes_and_searches_by_pk() {
    let seg = LiveSegment::new("english").unwrap();
    seg.index(1, "the quarterly invoice is overdue").unwrap();
    seg.index(2, "weather report sunny skies").unwrap();
    seg.index(3, "overdue invoice reminder please pay").unwrap();
    let hits = seg.search("invoice overdue", TsQueryKind::Plain, 10).unwrap();
    let pks: Vec<i64> = hits.iter().map(|h| h.pk).collect();
    assert!(pks.contains(&1) && pks.contains(&3));
    assert!(!pks.contains(&2), "non-matching row must not appear");
    // BM25: doc 3 (both terms, shorter) should rank at or above doc 1
    assert_eq!(hits[0].pk, 3);
    assert!(hits[0].score > 0.0);
}
```
- [ ] **Implement.** Struct + constructor:
```rust
use std::collections::HashSet;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use bluedb_fts::mapping::{Analyzer, FieldMapping, IndexMapping};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Value};
use tantivy::{Index, IndexReader, IndexWriter, TantivyDocument, doc};

use crate::error::{EngineError, Result};
use crate::fts_sql::{FtsHit, FtsPredicate, FtsSearcher, TsQueryKind};

const WRITER_HEAP: usize = 15_000_000;

pub struct LiveSegment {
    index: Index,
    pk_field: Field,
    body_field: Field,
    writer: Mutex<IndexWriter>,
    reader: IndexReader,
    tombstones: Mutex<HashSet<i64>>,
    dirty: AtomicBool,
}
```
  - `pub fn new(config: &str) -> Result<Self>`: `let analyzer = analyzer_for_config(config);` build `IndexMapping::new().field(FieldMapping{ name:"pk", kind: FieldKind::I64{stored:true, fast:true} }).text("body", analyzer)`. (Use whatever public constructor `FieldMapping`/`IndexMapping` expose — there is `IndexMapping::text(name, analyzer)` and `FieldMapping::text/keyword`; for the I64 pk field add a `FieldMapping{ name, kind: FieldKind::I64{..} }` directly since `FieldKind` is public, or use a field helper if one exists.) Then `let schema = mapping.build_schema(); let index = Index::create_in_ram(schema); mapping.register_tokenizers(&index);` Look up `pk_field`/`body_field` via `index.schema().get_field("pk"/"body").map_err(...)`. Build `writer = index.writer(WRITER_HEAP)?` and `reader = index.reader_builder().reload_policy(ReloadPolicy::Manual).try_into()?`. Map every tantivy error through `EngineError` (add a `Fts(String)` variant if no suitable one exists — check `error.rs` first; reuse if there's a generic one).
  - `analyzer_for_config(config: &str) -> Analyzer`: `"english"|"en"|"english_stem"` → `EnStem`; `"simple"|"default"` → `Default`; `"whitespace"|"ws"` → `Whitespace`; `"raw"|"keyword"|"exact"` → `Raw`; anything else → `Default` (a permissive, documented fallback — log nothing, just default).
  - `pub fn index(&self, pk: i64, text: &str) -> Result<()>`: lock writer, `w.add_document(doc!(self.pk_field => pk, self.body_field => text.to_string()))?;` set `dirty=true`. (Do NOT commit here — defer to `ensure_fresh`.) Also: a re-index of an existing pk should supersede the old text → record `pk` for delete-then-add. Simplest correct approach for B2a: on `index`, first add `pk` to tombstones-of-old by deleting via the pk term *and* relying on search-time tombstone filtering being for explicit deletes only. **Cleaner:** use tantivy's `writer.delete_term(Term::from_field_i64(pk_field, pk))` before re-adding, so an updated row doesn't double-match. Do that in `index` (delete the pk term, then add) — that gives correct update semantics without bloating the explicit tombstone set. Keep the explicit `tombstones` set for `tombstone(pk)` (hard delete) only.
  - `pub fn tombstone(&self, pk: i64) -> Result<()>`: lock writer, `w.delete_term(Term::from_field_i64(self.pk_field, pk))`; set `dirty`; also insert into the `tombstones` set so a search filters it even before the next commit. (Belt-and-suspenders: delete_term removes it from the index at next commit; the set guards the window.)
  - `fn ensure_fresh(&self) -> Result<()>`: `if self.dirty.swap(false, Ordering::SeqCst) { self.writer.lock().commit()?; self.reader.reload()?; }`
  - `pub fn search(&self, query: &str, kind: TsQueryKind, limit: usize) -> Result<Vec<FtsHit>>`: `ensure_fresh()?;` `let searcher = self.reader.searcher();` build the query string via `translate_query(query, kind)` (Task 2), `let parser = QueryParser::for_index(&self.index, vec![self.body_field]);` for `Plain`/`ToTsQuery` set conjunction-by-default (see Task 2). `let top = searcher.search(&parsed, &TopDocs::with_limit(limit + tombs.len()).order_by_score())?;` For each `(score, addr)`: `let d: TantivyDocument = searcher.doc(addr)?;` read the i64 pk from `self.pk_field` (`d.get_first(self.pk_field)` → `Value::I64` via the fork's value API — check how `search.rs`/`IdField` extracts stored values and mirror it). Skip if pk ∈ tombstones. Push `FtsHit{pk, score}`. Truncate to `limit`.
- [ ] Run `cargo test -p bluedb-engine --lib live_segment` (pass), `cargo build -p bluedb-engine` (clean). Commit:
```bash
git commit -am "feat(engine): in-memory LiveSegment — RAM tantivy index, index/search by pk"
```

## Task 2: tsquery-kind → tantivy query translation

**Files:** `live_segment.rs` (+ tests).

- [ ] **Failing tests** for `translate_query(query: &str, kind) -> String` and conjunction behavior:
  - `Plain` "invoice overdue" → both terms required (AND). Assert a row with only "invoice" does NOT match when another row has both, *and* that plain terms with no operators work. (Test via `search`: index a row with only one term, assert it's excluded when the query has two terms under `Plain`.)
  - `ToTsQuery` "invoice & overdue" → AND; "invoice | report" → OR; "invoice & !weather" → invoice AND NOT weather. (Translate Postgres `&`/`|`/`!` and strip `:*` weight/prefix markers.)
  - `Websearch` `"\"quarterly invoice\""` (quoted) → phrase; `foo -bar` → foo AND NOT bar; `foo or bar` → OR. (tantivy's `QueryParser` already handles quotes, `-`, and `OR`; pass through with light normalization.)
- [ ] **Implement** `translate_query`:
  - `Plain`: escape any tantivy query-syntax metacharacters in the user string (so `plainto_tsquery` treats them as literal terms), then return the cleaned terms; rely on **conjunction-by-default** (set on the parser) so space-separated terms AND. Simplest: strip tantivy operators, return the lowercased term string; set `parser.set_conjunction_by_default()`.
  - `ToTsQuery`: translate Postgres operators to tantivy: `&`→` AND `, `|`→` OR `, `!`→` NOT `, remove `:*`/`:A`-style weights, collapse whitespace. (Parser conjunction default doesn't matter — operators are explicit; still safe to set AND default.)
  - `Websearch`: pass the string through largely as-is (tantivy handles `"phrase"`, `-term`, `OR`); lowercase a bare `or`→`OR` so it's treated as the operator; do NOT set conjunction-by-default (websearch is OR-ish by default in PG, but tantivy default is OR — leave parser default OR for this kind).
  - Decide conjunction per kind inside `search` (build the parser, call `set_conjunction_by_default()` for `Plain` and `ToTsQuery` only).
  - Be defensive: if the translated query fails to parse, return `EngineError::Rejected`/`Fts` rather than panicking.
- [ ] Run tests (pass), commit:
```bash
git commit -am "feat(engine): tsquery-kind → tantivy query translation (plain/to_tsquery/websearch)"
```

## Task 3: implement `FtsSearcher` + tombstone/update semantics

**Files:** `live_segment.rs` (+ tests).

- [ ] **Failing tests:**
  - `FtsSearcher`: `let hits = (&seg as &dyn ...).search(&predicate).await` — actually assert via the trait method: build an `FtsPredicate{ table:"docs", column:"body", config:"english", query:"invoice", kind:Plain }`, call `LiveSegment::search` *through the trait*, get the same pks as the inherent search. (The trait `search` ignores `table`/`column` for B2a — single-column segment — and uses `query`/`kind`; document that.)
  - Update: `seg.index(1, "alpha")` then `seg.index(1, "beta")`; searching "alpha" returns nothing, "beta" returns pk 1 (delete_term-before-add gives clean update).
  - Hard delete: `seg.tombstone(3)` → a query that matched 3 no longer returns it, immediately (same connection, no explicit flush) — proves NRT read-your-writes within the segment.
  - Over-fetch correctness: with several tombstoned matches, `search(.., limit=2)` still returns 2 live hits (not 2-minus-tombstones). (This is why Task 1 fetches `limit + tombs.len()`.)
- [ ] **Implement** the trait:
```rust
#[async_trait::async_trait]
impl FtsSearcher for LiveSegment {
    async fn search(&self, predicate: &FtsPredicate) -> Result<Vec<FtsHit>> {
        // single-column segment: column/table are informational in B2a
        LiveSegment::search(self, &predicate.query, predicate.kind, DEFAULT_LIMIT)
    }
}
```
  - Pick a `DEFAULT_LIMIT` (e.g. 100) for the trait path; document that the over-fetch window sizing (Spec B §9) is refined in a later increment.
  - Verify update/tombstone semantics hold under `ensure_fresh` (commit makes `delete_term` effective; the `tombstones` set guards the pre-commit window — confirm a `tombstone` then immediate `search` excludes the pk even though commit may not have run yet, because the set is consulted).
- [ ] Run `cargo test -p bluedb-engine` (ALL pass), `cargo build --workspace 2>&1 | grep -i warn` (clean). Commit:
```bash
git commit -am "feat(engine): LiveSegment implements FtsSearcher + update/tombstone NRT semantics"
```

## Self-Review
- Spec B coverage (B2a slice): real BM25 over an in-memory tantivy segment ✓; analyzer selected from the `to_tsvector` config ✓; `plainto_tsquery`/`to_tsquery`/`websearch_to_tsquery` kinds mapped ✓; tombstone + update semantics (NRT within the segment) ✓; implements the B1 `FtsSearcher` seam ✓ — so `rewrite_fts_query(.., &live_segment)` now runs against real search.
- Deferred to B2b/B2c/B4: durable-split union (`bluedb-fts::FtsIndex`), `CREATE FULLTEXT INDEX` DDL + registry, the SQL commit tap that feeds `index()/tombstone()`, RYW through the SQL execution path, the background seal, trigram. Documented.
- Tests use a **real** tantivy RAM index (not a fake), so B2a proves the searcher end-to-end in isolation.
- Reuses `bluedb_fts::mapping` (Analyzer/IndexMapping/build_schema/register_tokenizers) so the live segment's schema + tokenization match the durable splits — the B4 seal (live → split) stays consistent.
