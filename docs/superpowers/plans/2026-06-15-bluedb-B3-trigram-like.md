# bluedb Spec B — increment B3 (part 1): trigram index + `LIKE '%…%'` acceleration

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** The `pg_trgm`-analog (Spec B §4.6) for substring search. Add a **trigram index** kind that reuses the entire B2/B4 machinery (live segment, durable tier, seal, registry, union, commit tap) and use it to accelerate `col LIKE '%lit%'` — rewrite to `… WHERE pk IN (<trigram candidates>) AND col LIKE '%lit%'`, letting **gluesql do the exact `LIKE` verify** (so correctness never depends on the index being precise). Regex `~` (the heavier, value-fetch path) is **part 2 (B3-2)**.

**Key design — no custom tantivy tokenizer:** a custom "trigram" tokenizer would not be in tantivy's default manager, so opened durable splits couldn't tokenize it. Instead the **engine pre-trigramizes** text (`"overdue"` → `"ove erd rdu due"`... i.e. the 3-grams joined by spaces) and indexes it through the **built-in `whitespace` analyzer**. Both write (`on_commit`) and query (the rewrite) trigramize in Rust; the segment just stores/matches whitespace tokens. So the live segment, durable `FtsIndex`, seal, and registry all work **unchanged** — only a `kind` discriminant + the trigramize-at-the-boundary logic is new.

**Correctness (critical):** `pk IN (candidates) AND col LIKE '%lit%'` is correct **only if `candidates ⊇ every matching row`** (a false negative would silently drop a real match). Trigram pruning guarantees this only when the search literal is **≥ 3 chars and contains no `LIKE` wildcards** (`%`/`_`) inside it — then every matching row contains all the literal's trigrams. So the rewrite adds the `pk IN` prefilter **only** for a clean `%lit%`/`%lit`/`lit%`-infix literal of length ≥ 3 **and** when a trigram index exists on that column; otherwise it passes the SQL through unchanged (gluesql full-scans — exact, just slower). gluesql's `LIKE` stays in the query as the authoritative verify in all cases.

**Scope (B3 part 1):** `Analyzer`-free engine trigramization + a `Trigram` index kind (create/maintain/durable/registry/seal reuse) + the `LIKE` rewrite (gated) + a `/schema/.../trigram-indexes` DDL endpoint, tested for **correctness against a full-scan baseline**. **NOT here:** regex `~`/`~*`/`!~` (B3-2 — needs in-rewrite candidate-value fetch + Rust `regex` verify); prefix `LIKE 'foo%'` → B-tree range (separate, the B-tree already serves it — leave to gluesql). Same `INTEGER PRIMARY KEY` restriction.

**Tech stack:** Rust; the B2/B4 `FtsEngine`/`LiveSegment`/`FtsIndex` (analyzer = `whitespace`); `gluesql_core::sqlparser` `Expr::Like`.

---

## Task 1: engine trigram support — `trigramize`, `IndexKind`, maintain + durable + registry

**Files:** `crates/bluedb-engine/src/fts_engine.rs`; small helper maybe in `live_segment.rs` (reuse). Tests in `fts_engine.rs`/a new engine test.

- [ ] **Failing test:** over a real `Database`, create `docs(id INTEGER PRIMARY KEY, body TEXT)`, `fts.create_trigram_index(&conn, "docs", "body", "id")`, insert rows via an observed conn, then assert (via a direct engine helper or the union search with a trigramized query) that the trigram segment matches a substring: e.g. searching the trigram index for the trigrams of `"verd"` returns the pk of `"overdue"`. Also assert it survives a `seal()` (durable trigram tier) and a `reopen()` (registry round-trips the `Trigram` kind).
- [ ] **Implement:**
  - `pub(crate) fn trigramize(s: &str) -> String`: lowercase, produce all contiguous 3-grams of the string, joined by spaces. `< 3` chars → empty string (no trigrams; documented — such values aren't trigram-searchable and fall back to scan). (Optionally pad/sentinel like pg_trgm, but plain 3-grams are sufficient and simpler; keep it plain.)
  - `enum IndexKind { Fulltext, Trigram }` (serde, `#[serde(default)]`-friendly — default `Fulltext` so old registry blobs deserialize). Add `kind: IndexKind` to `IndexDef` and `PersistedDef`.
  - Trigram index uses analyzer `whitespace` and a distinct stable durable `index_id = format!("trgm/{table}/{column}")` (vs `fts/...`) so a column can carry BOTH a BM25 and a trigram index without split collision. Factor the durable-index builder to take the analyzer + id prefix by kind.
  - `pub async fn create_trigram_index(&self, storage, table, text_column, pk_column, analyzer_ignored?) ...` — resolve the column ordinal like `create_fulltext_index`, build the def with `kind: Trigram`, analyzer `whitespace`, persist to the registry. (Add a `create_trigram_index_auto` that resolves the PK, mirroring `create_fulltext_index_auto`, for the server endpoint.)
  - `on_commit`: for a `Trigram`-kind def, index `trigramize(text)` (not the raw text); for `Fulltext`, raw text (unchanged). Same for the value fed on update; tombstones unchanged (pk-based).
  - `reopen`/`persist_registry`: round-trip `kind`. `build_index_def` keyed by kind (analyzer + id prefix).
- [ ] Run `cargo test -p bluedb-engine` (ALL pass) + build clean. Commit:
```bash
git commit -am "feat(engine): trigram index kind (pre-trigramized, whitespace analyzer; reuses live/durable/seal/registry)"
```

## Task 2: `LIKE '%lit%'` rewrite (gated, correctness-safe)

**Files:** `crates/bluedb-engine/src/fts_sql.rs` (extract + rewrite helpers), `crates/bluedb-engine/src/fts_engine.rs` (`rewrite_for` tries `@@` then `LIKE`). Test `crates/bluedb-engine/tests/trigram_like.rs`.

- [ ] **Failing end-to-end test** (real `Database`, correctness vs scan):
  - create table + trigram index on `body`; insert rows e.g. `(1,'quarterly invoice overdue')`,`(2,'weather sunny')`,`(3,'overdue notice')`.
  - `SELECT id FROM docs WHERE body LIKE '%overdue%'` via `execute_fts` → `{1,3}` (SAME as a plain scan). Assert the rewritten SQL (capture via `rewrite_for`) contains both `id IN (` and `LIKE '%overdue%'` (prefilter + verify).
  - A literal that the trigram prefilter must NOT prune unsafely: `LIKE '%ab%'` (2 chars) → passes through unchanged (no `pk IN`), still returns the scan-correct rows.
  - `LIKE '%xyznotpresent%'` → empty (candidates empty → never-match), matching the scan.
  - A column with no trigram index → `LIKE` passes through unchanged (gluesql scans).
- [ ] **Implement:**
  - In `fts_sql.rs`: `pub(crate) struct LikePredicate { table, column, literal }` and `extract_like_predicate(sql) -> Result<Option<LikePredicate>>`: parse, single table no joins (mirror `extract_fts_predicate`), find an `Expr::Like { negated: false, expr: Identifier(col), pattern: SingleQuotedString(p), .. }` where `p` is a **clean infix** — strip one optional leading and trailing `%`, and the remaining core has **no `%` or `_`** and **len ≥ 3**; return `(table, col, core)`. Any other LIKE shape (negated, internal wildcards, < 3, prefix-only `foo%` which is a B-tree job) → `Ok(None)` (pass-through). No `@@`-style hard error — a non-prunable LIKE is valid SQL gluesql runs.
  - A rewrite that, given candidate pks, **adds** `pk IN (candidates)` as a conjunct to the WHERE (keeping the original `Expr::Like` intact for gluesql to verify); empty candidates → a never-match (`1 = 0`) conjunct (reuse the `@@` empty-hit approach). Re-emit via Display.
  - In `fts_engine.rs::rewrite_for`: after the `@@` path returns `Ok(None)`, try `extract_like_predicate`; if `Some` AND a **Trigram**-kind def exists for `(table, column)`: trigram-search via the union searcher with the query `trigramize(literal)` (AND-of-trigrams — pass through the same union path, trigram def), collect candidate pks, apply the LIKE rewrite. No trigram def → `Ok(None)` (pass-through). The union searcher already masks stale durable hits via the live `covered()` set, so candidates stay a correct superset across seals.
  - Note: the union search for a trigram def must search the **trigram** def's segment/durable (not a BM25 def). Pick the def by `(table, column, kind == Trigram)`.
- [ ] Run `cargo test -p bluedb-engine` (ALL pass) + build clean. Commit:
```bash
git commit -am "feat(engine): trigram-accelerated LIKE '%lit%' (pk IN prefilter + gluesql LIKE verify; gated >=3-char clean infix)"
```

## Task 3: `/schema/.../trigram-indexes` DDL endpoint

**Files:** `crates/bluedb-server/src/schema.rs` (handler), `crates/bluedb-server/src/lib.rs` (route), `main.rs` (doc), test in `crates/bluedb-server/tests/fts.rs` (or new `trigram.rs`).

- [ ] **Failing HTTP test:** `POST /schema/tables` (docs, id PK + body TEXT) → `POST /schema/tables/docs/trigram-indexes {"column":"body"}` → 2xx → insert rows via `/sql` → `POST /sql {"sql":"SELECT id FROM docs WHERE body LIKE '%overdue%'"}` returns the matching ids (RYW + trigram-accelerated over HTTP), and the same with no trigram index declared still works (pass-through).
- [ ] **Implement:** `schema::create_trigram_index` mirroring `create_fulltext_index` (authz `SchemaAdmin`, `require_active`, validate idents) → `state.fts().await.create_trigram_index_auto(&conn, table, column).await?`. Route `POST /schema/tables/{table}/trigram-indexes`. Doc note in `main.rs`.
- [ ] Run `cargo test -p bluedb-server` (ALL pass) + build clean. Commit:
```bash
git commit -am "feat(server): CREATE TRIGRAM INDEX endpoint + LIKE acceleration over HTTP"
```

## Self-Review
- Spec B coverage (B3 part 1): trigram index (the GIN/`pg_trgm` analog, §4.5/§4.6) reusing the BM25 machinery via engine-side trigramization + the built-in whitespace analyzer ✓; `LIKE '%lit%'` accelerated with a correctness-safe `pk IN` prefilter + gluesql verify ✓; durable + sealed + restart-survivable (registry round-trips the kind) ✓; over HTTP ✓.
- Correctness: the prefilter is added ONLY for ≥3-char clean infix literals with a trigram index present; every other LIKE passes through to gluesql's exact scan. gluesql's `LIKE` remains in the query as the authoritative verify, so a candidate-set bug can only over-include (slower), never wrong results. Proven against a scan baseline.
- Deferred to B3 part 2 (B3-2): regex `~`/`~*`/`!~` — gluesql rejects `~` and has no regex engine, so the shim must extract the regex's mandatory trigrams, candidate-search, **fetch candidate column values and verify with the Rust `regex` crate**, then emit `pk IN (final)`. That needs in-rewrite candidate execution (a new pattern) — its own increment. Prefix `LIKE 'foo%'` stays a gluesql/B-tree scan.
