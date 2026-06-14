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
// `FtsPredicate`/`FtsSearcher` and `DEFAULT_LIMIT` are consumed by the
// `FtsSearcher` trait impl in Task 3; allow until then so each commit is clean.
#[allow(unused_imports)]
use crate::fts_sql::{FtsHit, FtsPredicate, FtsSearcher, TsQueryKind};

/// Heap budget for the in-memory index writer (15 MB — tantivy's documented
/// floor is 3 MB per thread; this gives comfortable headroom for the live tier).
const WRITER_HEAP: usize = 15_000_000;

/// Default result cap for the [`FtsSearcher`] trait path. The over-fetch window
/// sizing (Spec B §9) is refined in a later increment.
#[allow(dead_code)]
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
    dirty: AtomicBool,
}

/// Map a PostgreSQL `to_tsvector` config string onto a tantivy [`Analyzer`].
///
/// Unknown configs fall back to [`Analyzer::Default`] — a permissive default so
/// an unrecognized config still tokenizes sensibly rather than erroring.
fn analyzer_for_config(config: &str) -> Analyzer {
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

        let mut parser = QueryParser::for_index(&self.index, vec![self.body_field]);
        // Plain/to_tsquery space-separated terms AND together (PG semantics);
        // websearch leaves tantivy's default OR.
        if matches!(kind, TsQueryKind::Plain | TsQueryKind::ToTsQuery) {
            parser.set_conjunction_by_default();
        }

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
///   tantivy/Postgres metacharacters, lowercase, and return the space-separated
///   terms — conjunction-by-default (set on the parser in [`LiveSegment::search`])
///   ANDs them, matching `plainto_tsquery`'s all-terms-required semantics.
/// - `ToTsQuery` (`to_tsquery`): translate Postgres boolean operators —
///   `&`→`AND`, `|`→`OR` — and negation `!X` to tantivy's `-X` (a `MustNot`
///   prefix). (Tantivy's `NOT` keyword wraps the negated leaf in an all-negative
///   inner boolean that matches nothing under `Must`, so the `-` prefix is the
///   correct mapping.) `:*`/`:A`-style lexeme weight markers are dropped.
/// - `Websearch` (`websearch_to_tsquery`): pass through largely as-is — tantivy's
///   `QueryParser` already handles `"phrase"`, `-term`, and `OR`; only normalize a
///   bare `or` to the `OR` operator.
fn translate_query(query: &str, kind: TsQueryKind) -> String {
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
                .join(" ")
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
        // honored; metacharacters are removed and the terms left to AND.
        assert_eq!(translate_query("invoice & overdue", TsQueryKind::Plain), "invoice overdue");
        assert_eq!(translate_query("INVOICE Overdue", TsQueryKind::Plain), "invoice overdue");
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
}
