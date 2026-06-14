//! In-memory `LiveSegment` — a RAM-directory tantivy index (BM25) with a
//! tombstone set, implementing the B1 [`FtsSearcher`] seam.
//!
//! This is the "live" tier of the FTS index: writes land here first (buffered,
//! committed lazily for near-real-time read-your-writes), and reads run BM25
//! against it minus an explicit tombstone set. It is **pure and in-memory** —
//! no SlateDB, no DDL, no SQL-commit coupling. Wiring it to the SQL commit tap
//! and the durable-split union (`bluedb_fts::FtsIndex`) is a later increment
//! (B2b/B2c).
//!
//! The schema (an `I64` `pk` + a `Text` `body`) and tokenizers are built from
//! [`bluedb_fts::mapping::IndexMapping`], the same source of truth the durable
//! splits use, so a future seal (live → split) stays schema-consistent.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use bluedb_fts::mapping::{Analyzer, FieldKind, FieldMapping, IndexMapping};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Value};
use tantivy::{doc, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyDocument, Term};

use crate::error::{EngineError, Result};
use crate::fts_sql::{FtsHit, FtsPredicate, FtsSearcher, TsQueryKind};

/// Heap budget for the in-memory index writer (15 MB — tantivy's documented
/// floor is 3 MB per thread; this gives comfortable headroom for the live tier).
const WRITER_HEAP: usize = 15_000_000;

/// Default result cap for the [`FtsSearcher`] trait path. The over-fetch window
/// sizing (Spec B §9) is refined in a later increment.
const DEFAULT_LIMIT: usize = 100;

/// An in-memory tantivy segment: a RAM-directory BM25 index over a single
/// `(pk, body)` schema, plus an explicit tombstone set for hard deletes.
///
/// Interior mutability throughout (production shape): writes mutate through a
/// `Mutex<IndexWriter>` and the tombstone `Mutex<HashSet>` while reads run
/// concurrently. A `dirty` flag drives lazy commit+reload so a read sees its own
/// prior writes (near-real-time) without committing on every `index`/`tombstone`.
pub struct LiveSegment {
    index: Index,
    pk_field: Field,
    body_field: Field,
    writer: Mutex<IndexWriter>,
    reader: IndexReader,
    tombstones: Mutex<HashSet<i64>>,
    /// Every pk this live segment has indexed or tombstoned since construction
    /// (or the last [`Self::drain_for_seal`]). The union searcher uses it to mask
    /// any durable-split hit whose pk the live tier already covers (live wins).
    covered: Mutex<HashSet<i64>>,
    dirty: AtomicBool,
}

/// Map a PostgreSQL `to_tsvector` config string onto a tantivy [`Analyzer`].
///
/// Unknown configs fall back to [`Analyzer::Default`] — a permissive default so
/// an unrecognized config still tokenizes sensibly rather than erroring.
pub(crate) fn analyzer_for_config(config: &str) -> Analyzer {
    match config.to_lowercase().as_str() {
        "english" | "en" | "english_stem" => Analyzer::EnStem,
        "simple" | "default" => Analyzer::Default,
        "whitespace" | "ws" => Analyzer::Whitespace,
        "raw" | "keyword" | "exact" => Analyzer::Raw,
        _ => Analyzer::Default,
    }
}

impl LiveSegment {
    /// Build a fresh in-memory segment whose `body` field is analyzed per
    /// `config` (a `to_tsvector` config string; see [`analyzer_for_config`]).
    pub fn new(config: &str) -> Result<Self> {
        let analyzer = analyzer_for_config(config);
        let mapping = IndexMapping::new()
            .field(FieldMapping {
                name: "pk".into(),
                kind: FieldKind::I64 {
                    stored: true,
                    fast: true,
                },
            })
            .text("body", analyzer);

        let schema = mapping.build_schema();
        let index = Index::create_in_ram(schema);
        mapping.register_tokenizers(&index);

        let pk_field = index
            .schema()
            .get_field("pk")
            .map_err(|e| EngineError::Other(e.into()))?;
        let body_field = index
            .schema()
            .get_field("body")
            .map_err(|e| EngineError::Other(e.into()))?;

        let writer: IndexWriter = index
            .writer(WRITER_HEAP)
            .map_err(|e| EngineError::Other(e.into()))?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e: tantivy::TantivyError| EngineError::Other(e.into()))?;

        Ok(Self {
            index,
            pk_field,
            body_field,
            writer: Mutex::new(writer),
            reader,
            tombstones: Mutex::new(HashSet::new()),
            covered: Mutex::new(HashSet::new()),
            dirty: AtomicBool::new(false),
        })
    }

    /// Buffer an indexing of `(pk, text)`. Re-indexing an existing `pk`
    /// supersedes the old document: we delete the pk term before re-adding, so
    /// an updated row never double-matches. Does not commit (deferred to
    /// [`Self::ensure_fresh`]).
    pub fn index(&self, pk: i64, text: &str) -> Result<()> {
        let w = self.writer.lock().expect("writer mutex poisoned");
        w.delete_term(Term::from_field_i64(self.pk_field, pk));
        w.add_document(doc!(self.pk_field => pk, self.body_field => text.to_string()))
            .map_err(|e| EngineError::Other(e.into()))?;
        drop(w);
        self.covered
            .lock()
            .expect("covered mutex poisoned")
            .insert(pk);
        self.dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Hard-delete `pk`: delete the pk term (effective at next commit) and
    /// record it in the explicit tombstone set so a search filters it out even
    /// in the pre-commit window (belt-and-suspenders NRT delete).
    pub fn tombstone(&self, pk: i64) -> Result<()> {
        let w = self.writer.lock().expect("writer mutex poisoned");
        w.delete_term(Term::from_field_i64(self.pk_field, pk));
        drop(w);
        self.tombstones
            .lock()
            .expect("tombstones mutex poisoned")
            .insert(pk);
        self.covered
            .lock()
            .expect("covered mutex poisoned")
            .insert(pk);
        self.dirty.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Lazily commit buffered writes and reload the reader if anything changed,
    /// giving near-real-time read-your-writes.
    fn ensure_fresh(&self) -> Result<()> {
        if self.dirty.swap(false, Ordering::SeqCst) {
            self.writer
                .lock()
                .expect("writer mutex poisoned")
                .commit()
                .map_err(|e| EngineError::Other(e.into()))?;
            self.reader
                .reload()
                .map_err(|e| EngineError::Other(e.into()))?;
        }
        Ok(())
    }

    /// Run a BM25 search for `query` (interpreted per `kind`), returning up to
    /// `limit` live hits sorted by descending score, with tombstoned pks
    /// removed.
    pub fn search(&self, query: &str, kind: TsQueryKind, limit: usize) -> Result<Vec<FtsHit>> {
        self.ensure_fresh()?;

        let tombs = self
            .tombstones
            .lock()
            .expect("tombstones mutex poisoned")
            .clone();

        let searcher = self.reader.searcher();
        let query_str = translate_query(query, kind);

        // `translate_query` emits explicit AND/OR operators for every kind, so
        // the parser's default conjunction is irrelevant — no
        // `set_conjunction_by_default` needed (and the durable tier, which can't
        // set it, parses the same string identically).
        let parser = QueryParser::for_index(&self.index, vec![self.body_field]);

        let parsed = parser
            .parse_query(&query_str)
            .map_err(|e| EngineError::Rejected(format!("fts query parse: {e}")))?;

        // Over-fetch by the tombstone count so dropped hits don't starve the
        // top-`limit` window.
        let fetch = limit.saturating_add(tombs.len());
        let raw = searcher
            .search(&parsed, &TopDocs::with_limit(fetch).order_by_score())
            .map_err(|e| EngineError::Other(e.into()))?;

        let mut hits = Vec::with_capacity(raw.len());
        for (score, addr) in raw {
            let d: TantivyDocument = searcher
                .doc(addr)
                .map_err(|e| EngineError::Other(e.into()))?;
            let pk = d
                .get_first(self.pk_field)
                .and_then(|v| v.as_i64())
                .ok_or_else(|| {
                    EngineError::Other(anyhow::anyhow!(
                        "live segment hit at {addr:?} missing stored i64 pk"
                    ))
                })?;
            if tombs.contains(&pk) {
                continue;
            }
            hits.push(FtsHit { pk, score });
        }
        hits.truncate(limit);
        Ok(hits)
    }

    /// Every pk this segment has indexed or tombstoned since construction (or the
    /// last [`Self::drain_for_seal`]). The union searcher masks any durable hit
    /// whose pk is in this set — the live tier holds the authoritative (latest or
    /// deleted) version of those pks, so a stale durable copy must not surface.
    pub fn covered(&self) -> HashSet<i64> {
        self.covered
            .lock()
            .expect("covered mutex poisoned")
            .clone()
    }

    /// Drain the live segment for a seal: return `(live docs as (pk, body),
    /// tombstone set)`, then **reset** the segment (clear its index, tombstones,
    /// and covered set) so subsequent writes start fresh.
    ///
    /// The caller (the engine's `seal`) folds the returned docs/tombstones into
    /// the durable tier (re-add the live docs, superseding any prior durable copy;
    /// tombstone the deleted pks). After this returns, a `search` yields nothing
    /// and `covered()` is empty until new writes land.
    pub fn drain_for_seal(&self) -> Result<(Vec<(i64, String)>, HashSet<i64>)> {
        use tantivy::collector::DocSetCollector;
        use tantivy::query::AllQuery;

        self.ensure_fresh()?;

        let tombs = self
            .tombstones
            .lock()
            .expect("tombstones mutex poisoned")
            .clone();

        // Read every live doc (all addresses, not just a top-K window).
        let searcher = self.reader.searcher();
        let addrs = searcher
            .search(&AllQuery, &DocSetCollector)
            .map_err(|e| EngineError::Other(e.into()))?;

        let mut docs: Vec<(i64, String)> = Vec::with_capacity(addrs.len());
        for addr in addrs {
            let d: TantivyDocument = searcher
                .doc(addr)
                .map_err(|e| EngineError::Other(e.into()))?;
            let pk = d
                .get_first(self.pk_field)
                .and_then(|v| v.as_i64())
                .ok_or_else(|| {
                    EngineError::Other(anyhow::anyhow!(
                        "live segment doc at {addr:?} missing stored i64 pk"
                    ))
                })?;
            // Defensive: a tombstoned pk should already be gone from the index
            // (its term was deleted), but skip it explicitly so it never leaks
            // into the docs vec — it belongs in the tombstone set.
            if tombs.contains(&pk) {
                continue;
            }
            let body = d
                .get_first(self.body_field)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            docs.push((pk, body));
        }

        // Reset: clear the index (delete all docs + commit + reload), then the
        // tombstone and covered sets.
        {
            let mut w = self.writer.lock().expect("writer mutex poisoned");
            w.delete_all_documents()
                .map_err(|e| EngineError::Other(e.into()))?;
            w.commit().map_err(|e| EngineError::Other(e.into()))?;
        }
        self.reader
            .reload()
            .map_err(|e| EngineError::Other(e.into()))?;
        self.tombstones
            .lock()
            .expect("tombstones mutex poisoned")
            .clear();
        self.covered
            .lock()
            .expect("covered mutex poisoned")
            .clear();
        self.dirty.store(false, Ordering::SeqCst);

        Ok((docs, tombs))
    }
}

/// The B1 [`FtsSearcher`] seam: drive [`LiveSegment::search`] from an extracted
/// [`FtsPredicate`].
///
/// For B2a this is a **single-column** segment, so `predicate.table` and
/// `predicate.column` are informational only — the live segment indexes one
/// `body` field and answers from it; routing a predicate to the right segment by
/// table/column is a later increment (B2c, the DDL registry). The match uses
/// `predicate.query` and `predicate.kind`, capped at [`DEFAULT_LIMIT`] (the
/// over-fetch window sizing per Spec B §9 is refined later).
#[async_trait::async_trait]
impl FtsSearcher for LiveSegment {
    async fn search(&self, predicate: &FtsPredicate) -> Result<Vec<FtsHit>> {
        LiveSegment::search(self, &predicate.query, predicate.kind, DEFAULT_LIMIT)
    }
}

/// Strip a Postgres tsquery lexeme weight/prefix marker (`:*`, `:A`, `:AB`,
/// `:*A`, ...) from the end of a token, leaving the bare lexeme.
fn strip_weight(token: &str) -> &str {
    match token.split_once(':') {
        Some((lexeme, _marker)) => lexeme,
        None => token,
    }
}

/// Translate a `query` string into a tantivy [`QueryParser`] query string per
/// the tsquery `kind`.
///
/// - `Plain` (`plainto_tsquery`): the input is a bag of literal terms. Strip any
///   tantivy/Postgres metacharacters, lowercase, and join the terms with an
///   explicit `AND` — matching `plainto_tsquery`'s all-terms-required semantics.
///   Emitting an explicit operator (rather than relying on a parser's
///   default-conjunction setting) lets BOTH FTS tiers — the live segment and the
///   durable [`bluedb_fts::FtsIndex`] splits — parse the same translated string
///   identically (the durable search path can't call `set_conjunction_by_default`).
/// - `ToTsQuery` (`to_tsquery`): translate Postgres boolean operators —
///   `&`→`AND`, `|`→`OR` — and negation `!X` to tantivy's `-X` (a `MustNot`
///   prefix). (Tantivy's `NOT` keyword wraps the negated leaf in an all-negative
///   inner boolean that matches nothing under `Must`, so the `-` prefix is the
///   correct mapping.) `:*`/`:A`-style lexeme weight markers are dropped.
/// - `Websearch` (`websearch_to_tsquery`): pass through largely as-is — tantivy's
///   `QueryParser` already handles `"phrase"`, `-term`, and `OR`; only normalize a
///   bare `or` to the `OR` operator.
pub(crate) fn translate_query(query: &str, kind: TsQueryKind) -> String {
    match kind {
        TsQueryKind::Plain => {
            // Replace tantivy/Postgres metacharacters with spaces, lowercase,
            // and collapse whitespace into space-separated literal terms.
            let cleaned: String = query
                .chars()
                .map(|c| match c {
                    '&' | '|' | '!' | '(' | ')' | ':' | '*' | '"' | '+' | '-' | '^' | '~' => ' ',
                    other => other,
                })
                .collect();
            cleaned
                .split_whitespace()
                .map(|t| t.to_lowercase())
                .collect::<Vec<_>>()
                .join(" AND ")
        }
        TsQueryKind::ToTsQuery => {
            // Token-wise: map `&`/`|` to AND/OR, fold `!` into a `-` prefix on
            // the following lexeme (tantivy MustNot), strip weight markers.
            // Pad `&`/`|` so they tokenize independently; keep `!` glued so it
            // attaches to its operand.
            let spaced = query.replace('&', " & ").replace('|', " | ");
            let mut out: Vec<String> = Vec::new();
            for tok in spaced.split_whitespace() {
                match tok {
                    "&" => out.push("AND".into()),
                    "|" => out.push("OR".into()),
                    other => {
                        // `!lexeme` (one or more leading `!`) → `-lexeme`.
                        let trimmed = other.trim_start_matches('!');
                        let negated = trimmed.len() != other.len();
                        let lexeme = strip_weight(trimmed);
                        if lexeme.is_empty() {
                            continue;
                        }
                        if negated {
                            out.push(format!("-{lexeme}"));
                        } else {
                            out.push(lexeme.to_string());
                        }
                    }
                }
            }
            out.join(" ")
        }
        TsQueryKind::Websearch => {
            // tantivy handles quotes / `-term` / OR; just normalize a bare `or`.
            query
                .split_whitespace()
                .map(|t| if t.eq_ignore_ascii_case("or") { "OR" } else { t })
                .collect::<Vec<_>>()
                .join(" ")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn indexes_and_searches_by_pk() {
        let seg = LiveSegment::new("english").unwrap();
        seg.index(
            1,
            "the quarterly invoice is overdue and the customer has been notified twice already",
        )
        .unwrap();
        seg.index(2, "weather report sunny skies").unwrap();
        seg.index(3, "overdue invoice").unwrap();
        let hits = seg.search("invoice overdue", TsQueryKind::Plain, 10).unwrap();
        let pks: Vec<i64> = hits.iter().map(|h| h.pk).collect();
        assert!(pks.contains(&1) && pks.contains(&3));
        assert!(!pks.contains(&2), "non-matching row must not appear");
        // BM25: doc 3 (both terms, shorter) should rank at or above doc 1
        assert_eq!(hits[0].pk, 3);
        assert!(hits[0].score > 0.0);
    }

    // --- Task 2: tsquery-kind → tantivy query translation ---

    #[test]
    fn translate_to_tsquery_operators() {
        assert_eq!(translate_query("invoice & overdue", TsQueryKind::ToTsQuery), "invoice AND overdue");
        assert_eq!(translate_query("invoice | report", TsQueryKind::ToTsQuery), "invoice OR report");
        // `!X` → tantivy's `-X` MustNot prefix (the `NOT` keyword nests into an
        // all-negative inner boolean that matches nothing; see translate_query docs).
        assert_eq!(translate_query("invoice & !weather", TsQueryKind::ToTsQuery), "invoice AND -weather");
        // `:*` prefix / weight markers are stripped.
        assert_eq!(translate_query("invoic:* & overdue:A", TsQueryKind::ToTsQuery), "invoic AND overdue");
    }

    #[test]
    fn translate_plain_strips_operators() {
        // Plain treats the input as literal terms — Postgres operators are not
        // honored; metacharacters are removed and the bare terms joined with an
        // explicit `AND` (so both FTS tiers parse identically regardless of any
        // per-parser default-conjunction setting).
        assert_eq!(translate_query("invoice & overdue", TsQueryKind::Plain), "invoice AND overdue");
        assert_eq!(translate_query("INVOICE Overdue", TsQueryKind::Plain), "invoice AND overdue");
    }

    #[test]
    fn translate_websearch_normalizes_or() {
        // tantivy already handles "phrase", -term; a bare `or` becomes the OR operator.
        assert_eq!(translate_query("foo or bar", TsQueryKind::Websearch), "foo OR bar");
        assert_eq!(translate_query("\"quarterly invoice\"", TsQueryKind::Websearch), "\"quarterly invoice\"");
        assert_eq!(translate_query("foo -bar", TsQueryKind::Websearch), "foo -bar");
    }

    #[test]
    fn plain_terms_conjoin_excluding_single_term_rows() {
        let seg = LiveSegment::new("english").unwrap();
        seg.index(1, "invoice overdue payment").unwrap();
        seg.index(2, "invoice only here").unwrap();
        let hits = seg.search("invoice overdue", TsQueryKind::Plain, 10).unwrap();
        let pks: Vec<i64> = hits.iter().map(|h| h.pk).collect();
        assert_eq!(pks, vec![1], "Plain conjoins terms: row with only 'invoice' is excluded");
    }

    #[test]
    fn to_tsquery_and_or_not() {
        let seg = LiveSegment::new("english").unwrap();
        seg.index(1, "invoice overdue").unwrap();
        seg.index(2, "weather report").unwrap();
        seg.index(3, "invoice weather").unwrap();

        // invoice & overdue → AND
        let a: Vec<i64> = seg
            .search("invoice & overdue", TsQueryKind::ToTsQuery, 10)
            .unwrap()
            .iter()
            .map(|h| h.pk)
            .collect();
        assert_eq!(a, vec![1]);

        // invoice | report → OR
        let mut o: Vec<i64> = seg
            .search("invoice | report", TsQueryKind::ToTsQuery, 10)
            .unwrap()
            .iter()
            .map(|h| h.pk)
            .collect();
        o.sort();
        assert_eq!(o, vec![1, 2, 3]);

        // invoice & !weather → invoice AND NOT weather
        let n: Vec<i64> = seg
            .search("invoice & !weather", TsQueryKind::ToTsQuery, 10)
            .unwrap()
            .iter()
            .map(|h| h.pk)
            .collect();
        assert_eq!(n, vec![1]);
    }

    #[test]
    fn websearch_phrase_and_negation() {
        let seg = LiveSegment::new("english").unwrap();
        seg.index(1, "the quarterly invoice arrived").unwrap();
        seg.index(2, "invoice quarterly mismatch order").unwrap();
        seg.index(3, "weather report only").unwrap();

        // Phrase: only doc 1 has the adjacent "quarterly invoice".
        let p: Vec<i64> = seg
            .search("\"quarterly invoice\"", TsQueryKind::Websearch, 10)
            .unwrap()
            .iter()
            .map(|h| h.pk)
            .collect();
        assert_eq!(p, vec![1]);

        // `or` becomes OR.
        let mut o: Vec<i64> = seg
            .search("invoice or weather", TsQueryKind::Websearch, 10)
            .unwrap()
            .iter()
            .map(|h| h.pk)
            .collect();
        o.sort();
        assert_eq!(o, vec![1, 2, 3]);
    }

    // --- Task 3: FtsSearcher trait + update/tombstone NRT semantics ---

    #[tokio::test]
    async fn fts_searcher_trait_matches_inherent_search() {
        let seg = LiveSegment::new("english").unwrap();
        seg.index(1, "invoice overdue").unwrap();
        seg.index(2, "weather report").unwrap();

        let predicate = FtsPredicate {
            table: "docs".into(),
            column: "body".into(),
            config: "english".into(),
            query: "invoice".into(),
            kind: TsQueryKind::Plain,
        };
        // Through the trait (table/column are informational for the single-column
        // B2a segment).
        let via_trait = FtsSearcher::search(&seg, &predicate).await.unwrap();
        let via_inherent = LiveSegment::search(&seg, "invoice", TsQueryKind::Plain, DEFAULT_LIMIT).unwrap();
        let tp: Vec<i64> = via_trait.iter().map(|h| h.pk).collect();
        let ip: Vec<i64> = via_inherent.iter().map(|h| h.pk).collect();
        assert_eq!(tp, ip);
        assert_eq!(tp, vec![1]);
    }

    #[tokio::test]
    async fn update_supersedes_old_text() {
        let seg = LiveSegment::new("english").unwrap();
        seg.index(1, "alpha").unwrap();
        seg.index(1, "beta").unwrap();

        let alpha = seg.search("alpha", TsQueryKind::Plain, 10).unwrap();
        assert!(alpha.is_empty(), "old text must not match after re-index");
        let beta: Vec<i64> = seg
            .search("beta", TsQueryKind::Plain, 10)
            .unwrap()
            .iter()
            .map(|h| h.pk)
            .collect();
        assert_eq!(beta, vec![1], "new text matches the single live row");
    }

    #[tokio::test]
    async fn hard_delete_is_nrt() {
        let seg = LiveSegment::new("english").unwrap();
        seg.index(1, "invoice overdue").unwrap();
        seg.index(3, "overdue invoice reminder").unwrap();
        // Both match before delete.
        let before: Vec<i64> = seg
            .search("invoice", TsQueryKind::Plain, 10)
            .unwrap()
            .iter()
            .map(|h| h.pk)
            .collect();
        assert!(before.contains(&1) && before.contains(&3));

        // Tombstone 3, then search immediately (no explicit flush) — NRT.
        seg.tombstone(3).unwrap();
        let after: Vec<i64> = seg
            .search("invoice", TsQueryKind::Plain, 10)
            .unwrap()
            .iter()
            .map(|h| h.pk)
            .collect();
        assert!(after.contains(&1));
        assert!(!after.contains(&3), "tombstoned pk must not appear immediately");
    }

    // --- Task 3: seal-support — covered set + drain_for_seal ---

    #[tokio::test]
    async fn covered_tracks_every_indexed_and_tombstoned_pk() {
        let seg = LiveSegment::new("english").unwrap();
        seg.index(1, "alpha").unwrap();
        seg.index(2, "beta").unwrap();
        seg.tombstone(3).unwrap();
        // Re-indexing an existing pk doesn't duplicate it in covered.
        seg.index(1, "alpha updated").unwrap();

        let mut covered: Vec<i64> = seg.covered().into_iter().collect();
        covered.sort_unstable();
        assert_eq!(covered, vec![1, 2, 3], "covered = every pk index/tombstone touched");
    }

    #[tokio::test]
    async fn drain_for_seal_returns_live_docs_plus_tombstones_then_resets() {
        let seg = LiveSegment::new("english").unwrap();
        seg.index(1, "first version").unwrap();
        seg.index(2, "second doc").unwrap();
        seg.index(1, "first updated").unwrap(); // update pk 1's text
        seg.tombstone(2).unwrap(); // delete pk 2

        let (docs, tombs) = seg.drain_for_seal().unwrap();

        // Live docs: only pk 1, carrying its LATEST text. pk 2 is tombstoned, so
        // it's in the tombstone set, NOT the docs vec.
        assert_eq!(docs.len(), 1, "only the single live doc (pk 1)");
        assert_eq!(docs[0].0, 1, "the live pk");
        assert_eq!(docs[0].1, "first updated", "latest text per pk (post-update)");
        assert!(tombs.contains(&2), "tombstoned pk 2 is in the tombstone set");
        assert!(!docs.iter().any(|(pk, _)| *pk == 2), "tombstoned pk not in docs vec");

        // Reset: a subsequent search returns nothing, covered() is empty,
        // tombstones cleared.
        let after = seg.search("first", TsQueryKind::Plain, 10).unwrap();
        assert!(after.is_empty(), "drained segment has no searchable docs");
        assert!(seg.covered().is_empty(), "covered cleared after drain");

        // Indexing fresh after a drain works and is the only thing covered now.
        seg.index(9, "ninth").unwrap();
        let again = seg.search("ninth", TsQueryKind::Plain, 10).unwrap();
        assert_eq!(again.iter().map(|h| h.pk).collect::<Vec<_>>(), vec![9]);
        assert_eq!(seg.covered().into_iter().collect::<Vec<_>>(), vec![9]);
    }

    #[tokio::test]
    async fn over_fetch_fills_limit_past_tombstones() {
        let seg = LiveSegment::new("english").unwrap();
        // 5 matching rows; tombstone 3 of them, then ask for limit=2 live hits.
        for pk in 1..=5 {
            seg.index(pk, "invoice overdue").unwrap();
        }
        seg.tombstone(1).unwrap();
        seg.tombstone(2).unwrap();
        seg.tombstone(3).unwrap();
        let hits = seg.search("invoice", TsQueryKind::Plain, 2).unwrap();
        assert_eq!(hits.len(), 2, "limit=2 must yield 2 LIVE hits, not 2-minus-tombstones");
        for h in &hits {
            assert!(h.pk == 4 || h.pk == 5, "only live pks 4/5 may appear, got {}", h.pk);
        }
    }
}
