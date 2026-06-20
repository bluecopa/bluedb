# Collections Search (Elasticsearch-style) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an Elasticsearch-shaped search API over `/collections` documents — an ES Query-DSL subset (`match`/`match_phrase`/`term`/`range`/`bool`/`exists`), BM25 scoring, and an ES-style hits envelope — by translating ES query shapes into real tantivy `Query` objects and executing them on bluedb's existing `bluedb-fts` engine, keyed by each document's `_id`.

**Architecture:** A new **pure** crate `bluedb-search` (ES-DSL ⇄ tantivy translation: `model`/`mapping`/`query`/`hits`, no I/O, unit-testable). `bluedb-fts` gains three small **additive** `Query`-object search functions (the existing ones take query *strings*; we need to run pre-built `Query` objects). `bluedb-engine::FtsIndex` (the general per-index orchestrator — `new(index_id, blob, schema, id_field, policy)`) gains matching `search_query_*` methods. `bluedb-server` gets thin `/collections/{c}/search` + `/collections/{c}/searchIndex` handlers, a per-tenant search-mapping registry (mirrors the existing TTL registry), write-path hooks that maintain the tantivy index on insert/update/delete, and a `SearchEngine` (swapped on promote/demote like `FtsEngine`) that owns the per-`(tenant,collection)` `FtsIndex` write handles. Search indexes are tenant-isolated by prefixing the `index_id` as `search/{tenant}/{collection}` (one shared SlateDB `Db`, tenant-prefixed keys).

**Tech Stack:** Rust, axum 0.8, tantivy (quickwit fork, rev `6270552`, features incl. `quickwit`+`stemmer`), `bluedb-fts`, `bluedb-engine::FtsIndex`, `bluedb-collections`, SlateDB object-store blobs.

**v1 scope notes (honored by the compat matrix in Task 16):**
- Mapping field types: **text** (analyzed), **keyword** (raw/exact), **integer** (i64, stored+fast). Boolean/float field types are out of v1 (documented).
- Query types: `match`, `match_phrase`, `term`, `range`, `bool`, `exists`.
- Sort: `_score` desc (default) fully; **integer** field sort (asc/desc) via fast fields. Sort by a text/keyword field → clear error.
- Highlights: best-effort term highlighting over the fetched `_source` (wraps matched analyzed terms in `<em>`), not tantivy fragment snippets. v1-lite, as the spec states.
- Search reads run on any node with a bound DB (writer = read-your-writes; reader = eventually consistent). `searchIndex` + write-path maintenance are writer-only.

---

## File Structure

**New crate `crates/bluedb-search/`** (pure translation, no I/O):
- `Cargo.toml` — deps: `tantivy` (+`stemmer`), `bluedb-fts`, `serde`, `serde_json`, `thiserror`, `anyhow`.
- `src/lib.rs` — module decls + crate doc + re-exports.
- `src/error.rs` — `SearchError` (+ ES-shaped `status()`/`to_es_json()` helpers).
- `src/model.rs` — ES request/response structs (`SearchRequest`, `SourceSpec`, `SortClause`, `MappingSpec`/`FieldSpec`, `Hit`, `HitsTotal`, `HitsBlock`, `SearchResponse`).
- `src/mapping.rs` — `MappingSpec` → `bluedb_fts::mapping::IndexMapping` + a resolved `SearchSchema` (schema, field table, id field).
- `src/query.rs` — ES Query-DSL JSON → `CompiledQuery { query: Box<dyn tantivy::query::Query>, terms_by_field }`.
- `src/hits.rs` — assemble the ES hits envelope (source trimming, term highlighting, totals, max_score).

**Modified `crates/bluedb-fts/src/search.rs`** — add `multi_split_search_query_filtered_ids`, `multi_split_count_query_filtered`, `multi_split_search_query_sorted_ids` (additive; existing fns untouched).

**Modified `crates/bluedb-engine/src/fts.rs`** — add `FtsIndex::search_query_ids`, `FtsIndex::count_query`, `FtsIndex::search_query_sorted_ids`. Ensure `FtsIndex` + `IdField` are reachable from `bluedb-server`.

**New `crates/bluedb-server/src/search.rs`** — `SearchEngine` (per-`(tenant,coll)` `FtsIndex` cache + writer blob), `build_fts_index`/`index_id`/`search_blob` helpers, the search-mapping registry (`__bluedb_search_config` + `__bluedb_search_tenants`), the `searchIndex`/`search` handlers, the write-path maintenance helpers, the compaction sweep.

**Modified `crates/bluedb-server/src/lib.rs`** — `mod search;`, `AppState.inner.search` field, promote/demote wiring + compaction-sweep task, route registration.

**Modified `crates/bluedb-server/src/collections.rs`** — call the search write-path maintenance from `insert`/`update`/`delete`.

**New `crates/bluedb-server/tests/search.rs`** — embedded-server integration tests.

**Docs:** `docs/collections/search.md` (compat matrix) + `mkdocs.yml` nav + `docs/api/rest.md` + `docs/index.md`.

---

## Task 1: Scaffold `bluedb-search` crate + workspace wiring

**Files:**
- Create: `crates/bluedb-search/Cargo.toml`
- Create: `crates/bluedb-search/src/lib.rs`
- Create: `crates/bluedb-search/src/error.rs`
- Modify: `Cargo.toml` (workspace root — `[workspace.dependencies]`)
- Modify: `crates/bluedb-server/Cargo.toml` (`[dependencies]`)

- [ ] **Step 1: Create the crate manifest**

`crates/bluedb-search/Cargo.toml`:
```toml
[package]
name = "bluedb-search"
version = "0.0.0"
edition.workspace = true
license.workspace = true
description = "Elasticsearch-shaped search DSL translated onto tantivy (bluedb-fts) for the /collections document API."

[dependencies]
bluedb-fts = { workspace = true }
tantivy    = { workspace = true, features = ["stemmer"] }
serde      = { workspace = true }
serde_json = { workspace = true }
thiserror  = { workspace = true }
anyhow     = { workspace = true }

[dev-dependencies]
```

- [ ] **Step 2: Register the crate in the workspace**

In the root `Cargo.toml`, under `[workspace.dependencies]` add (after the `bluedb-cache` line):
```toml
bluedb-search = { path = "crates/bluedb-search" }
```
(`members = ["crates/*"]` already picks up the new directory.)

- [ ] **Step 3: Add the dep to `bluedb-server`**

In `crates/bluedb-server/Cargo.toml` `[dependencies]`, add (near the other `bluedb-*` deps):
```toml
bluedb-search   = { workspace = true }
bluedb-fts      = { workspace = true }
tantivy         = { workspace = true, features = ["stemmer"] }
```
(`bluedb-fts` + `tantivy` are needed directly because the write-path hooks build `TantivyDocument`s and the search handler constructs `FtsIndex`es.)

- [ ] **Step 4: Write the error type**

`crates/bluedb-search/src/error.rs`:
```rust
//! Errors produced by ES-DSL translation, shaped for an ES-style HTTP response.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum SearchError {
    #[error("no field [{0}] in this search mapping")]
    UnmappedField(String),
    #[error("unsupported query type [{0}]")]
    UnsupportedQuery(String),
    #[error("unsupported analyzer [{0}]")]
    UnsupportedAnalyzer(String),
    #[error("unsupported field type [{0}]")]
    UnsupportedFieldType(String),
    #[error("malformed search request: {0}")]
    BadRequest(String),
    #[error("cannot sort by field [{0}]: only `_score` and integer fields are sortable")]
    UnsortableField(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl SearchError {
    /// ES `error.type` token for the JSON body.
    pub fn es_type(&self) -> &'static str {
        match self {
            SearchError::UnmappedField(_) => "query_shard_exception",
            SearchError::UnsupportedQuery(_) => "parsing_exception",
            SearchError::UnsupportedAnalyzer(_)
            | SearchError::UnsupportedFieldType(_)
            | SearchError::UnsortableField(_)
            | SearchError::BadRequest(_) => "illegal_argument_exception",
            SearchError::Other(_) => "internal_error",
        }
    }

    /// ES-shaped error body: `{"error": {"type", "reason"}, "status": N}`.
    pub fn to_es_json(&self, status: u16) -> serde_json::Value {
        serde_json::json!({
            "error": { "type": self.es_type(), "reason": self.to_string() },
            "status": status,
        })
    }
}

pub type Result<T> = std::result::Result<T, SearchError>;
```

- [ ] **Step 5: Write the crate root**

`crates/bluedb-search/src/lib.rs`:
```rust
//! `bluedb-search` — an Elasticsearch-shaped search DSL translated onto tantivy.
//!
//! Pure translation only (no I/O): ES request/response models ([`model`]), the
//! ES mapping → tantivy schema mapping ([`mapping`]), the ES Query-DSL → tantivy
//! [`tantivy::query::Query`] lowering ([`query`]), and the ES hits-envelope
//! assembly ([`hits`]). The bluedb-server wiring layer owns all I/O (storage,
//! tenancy, the `FtsIndex`).

pub mod error;
pub mod hits;
pub mod mapping;
pub mod model;
pub mod query;

pub use error::{Result, SearchError};
```

- [ ] **Step 6: Build**

Run: `cargo build -p bluedb-search 2>&1`
Expected: compiles clean (empty `model`/`mapping`/`query`/`hits` modules don't exist yet — add `pub mod` lines only as each module is created; for THIS step temporarily reduce `lib.rs` to just `pub mod error; pub use error::{Result, SearchError};` and restore the others in their tasks).

Adjust `lib.rs` for Step 6 to:
```rust
//! `bluedb-search` — an Elasticsearch-shaped search DSL translated onto tantivy.
pub mod error;
pub use error::{Result, SearchError};
```
Run: `cargo build -p bluedb-search 2>&1` → PASS.

- [ ] **Step 7: Commit**
```bash
git add crates/bluedb-search Cargo.toml crates/bluedb-server/Cargo.toml
git commit -F - <<'EOF'
feat(search): scaffold bluedb-search crate + workspace wiring

Pure ES-DSL <-> tantivy translation crate (error module only so far).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 2: `model.rs` — ES request/response types

**Files:**
- Create: `crates/bluedb-search/src/model.rs`
- Modify: `crates/bluedb-search/src/lib.rs` (add `pub mod model;`)

- [ ] **Step 1: Write the failing test**

Append to `crates/bluedb-search/src/model.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_search_request() {
        let v = serde_json::json!({
            "query": {"match": {"title": "hello"}},
            "from": 5, "size": 10,
            "sort": [{"year": "desc"}],
            "_source": ["title", "year"],
            "highlight": {"fields": {"body": {}}}
        });
        let req: SearchRequest = serde_json::from_value(v).unwrap();
        assert_eq!(req.from, 5);
        assert_eq!(req.size, 10);
        assert_eq!(req.sort.len(), 1);
        assert!(matches!(req.source, SourceSpec::Fields(_)));
        assert!(req.highlight_fields().contains(&"body".to_string()));
    }

    #[test]
    fn defaults_when_omitted() {
        let req: SearchRequest = serde_json::from_value(serde_json::json!({
            "query": {"match_all": {}}
        }))
        .unwrap();
        assert_eq!(req.from, 0);
        assert_eq!(req.size, 10);
        assert!(req.sort.is_empty());
        assert!(matches!(req.source, SourceSpec::Bool(true)));
    }

    #[test]
    fn parses_mapping_spec() {
        let m: MappingSpec = serde_json::from_value(serde_json::json!({
            "fields": {
                "title": {"analyzer": "english"},
                "tag": {"type": "keyword"},
                "year": {"type": "integer"}
            }
        }))
        .unwrap();
        assert_eq!(m.fields.len(), 3);
    }
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p bluedb-search model:: 2>&1`
Expected: FAIL (types not defined).

- [ ] **Step 3: Write the model types (above the test module)**

Prepend to `crates/bluedb-search/src/model.rs`:
```rust
//! ES request/response wire models. Deserialized from ES-shaped JSON; the
//! response side is serialized back to the client.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `_source` selector: `true`/`false`, or an explicit include list.
#[derive(Debug, Clone)]
pub enum SourceSpec {
    Bool(bool),
    Fields(Vec<String>),
}

impl Default for SourceSpec {
    fn default() -> Self {
        SourceSpec::Bool(true)
    }
}

impl<'de> Deserialize<'de> for SourceSpec {
    fn deserialize<D>(d: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match Value::deserialize(d)? {
            Value::Bool(b) => Ok(SourceSpec::Bool(b)),
            Value::Array(a) => Ok(SourceSpec::Fields(
                a.into_iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect(),
            )),
            Value::String(s) => Ok(SourceSpec::Fields(vec![s])),
            _ => Ok(SourceSpec::Bool(true)),
        }
    }
}

/// One sort clause: `{ "field": "asc"|"desc" }` or the bare string `"field"`.
#[derive(Debug, Clone)]
pub struct SortClause {
    pub field: String,
    pub descending: bool,
}

impl<'de> Deserialize<'de> for SortClause {
    fn deserialize<D>(d: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match Value::deserialize(d)? {
            Value::String(field) => Ok(SortClause { field, descending: true }),
            Value::Object(map) => {
                let (field, dir) = map.into_iter().next().ok_or_else(|| {
                    serde::de::Error::custom("empty sort clause")
                })?;
                let descending = match dir {
                    Value::String(s) => s.eq_ignore_ascii_case("desc"),
                    Value::Object(o) => o
                        .get("order")
                        .and_then(|v| v.as_str())
                        .map(|s| s.eq_ignore_ascii_case("desc"))
                        .unwrap_or(true),
                    _ => true,
                };
                Ok(SortClause { field, descending })
            }
            _ => Err(serde::de::Error::custom("invalid sort clause")),
        }
    }
}

fn default_size() -> usize {
    10
}

/// An ES `_search` request body (the subset bluedb supports).
#[derive(Debug, Clone, Deserialize)]
pub struct SearchRequest {
    /// The raw ES query object (lowered by `query.rs`).
    #[serde(default)]
    pub query: Value,
    #[serde(default)]
    pub from: usize,
    #[serde(default = "default_size")]
    pub size: usize,
    #[serde(default)]
    pub sort: Vec<SortClause>,
    #[serde(default, rename = "_source")]
    pub source: SourceSpec,
    #[serde(default)]
    pub highlight: Option<Highlight>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Highlight {
    #[serde(default)]
    pub fields: BTreeMap<String, Value>,
}

impl SearchRequest {
    /// Field names requested for highlighting (empty if none).
    pub fn highlight_fields(&self) -> Vec<String> {
        self.highlight
            .as_ref()
            .map(|h| h.fields.keys().cloned().collect())
            .unwrap_or_default()
    }
}

/// A declared search mapping: `{ "fields": { name: {analyzer|type} } }`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MappingSpec {
    pub fields: BTreeMap<String, FieldSpec>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FieldSpec {
    /// `text` (default), `keyword`, `integer`.
    #[serde(default)]
    pub r#type: Option<String>,
    /// For text fields: `english`/`standard`/`whitespace`.
    #[serde(default)]
    pub analyzer: Option<String>,
}

// ---- response side ----

#[derive(Debug, Clone, Serialize)]
pub struct HitsTotal {
    pub value: usize,
    pub relation: &'static str, // always "eq" in v1
}

#[derive(Debug, Clone, Serialize)]
pub struct Hit {
    pub _index: String,
    pub _id: String,
    pub _score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub _source: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub highlight: Option<BTreeMap<String, Vec<String>>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HitsBlock {
    pub total: HitsTotal,
    pub max_score: Option<f32>,
    pub hits: Vec<Hit>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchResponse {
    pub took: u64,
    pub timed_out: bool,
    pub hits: HitsBlock,
}
```

- [ ] **Step 4: Add the module + run tests**

In `crates/bluedb-search/src/lib.rs`, restore/add `pub mod model;` (place after `pub mod hits;` slot — final lib.rs from Task 1 Step 5 is the target; for now just ensure `pub mod model;` is present).

Run: `cargo test -p bluedb-search model:: 2>&1`
Expected: 3 tests PASS.

- [ ] **Step 5: Commit**
```bash
git add crates/bluedb-search/src/model.rs crates/bluedb-search/src/lib.rs
git commit -F - <<'EOF'
feat(search): ES request/response wire models

SearchRequest (query/from/size/sort/_source/highlight), MappingSpec/FieldSpec,
and the ES hits envelope types, with custom deserializers for _source and sort.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 3: `mapping.rs` — MappingSpec → IndexMapping + SearchSchema

**Files:**
- Create: `crates/bluedb-search/src/mapping.rs`
- Modify: `crates/bluedb-search/src/lib.rs` (add `pub mod mapping;`)

Background facts (from `bluedb-fts/src/mapping.rs`): `Analyzer::{Raw,Default,EnStem,Whitespace}`; `Analyzer::build_analyzer() -> tantivy::tokenizer::TextAnalyzer`; `IndexMapping::new()`, `.keyword(name)` (Raw, STORED), `.text(name, Analyzer)`, `FieldMapping::i64(name)` (stored+fast), `.field(FieldMapping)`, `.build_schema() -> tantivy::schema::Schema`. `bluedb_fts::IdField(pub tantivy::schema::Field)`.

- [ ] **Step 1: Write the failing test**

Append to `crates/bluedb-search/src/mapping.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::MappingSpec;

    fn spec(json: serde_json::Value) -> MappingSpec {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn compiles_text_keyword_integer() {
        let m = spec(serde_json::json!({"fields": {
            "title": {"analyzer": "english"},
            "tag": {"type": "keyword"},
            "year": {"type": "integer"}
        }}));
        let ss = compile(&m).unwrap();
        // _id is always present and is the id field.
        assert!(ss.field("_id").is_some());
        assert_eq!(ss.id_field.0, ss.field("_id").unwrap().field);
        // mapped fields resolve with the right kind.
        assert!(matches!(ss.field("title").unwrap().kind, FieldKindInfo::Text(_)));
        assert!(matches!(ss.field("tag").unwrap().kind, FieldKindInfo::Keyword));
        assert!(matches!(ss.field("year").unwrap().kind, FieldKindInfo::Integer));
    }

    #[test]
    fn rejects_unknown_analyzer_and_type() {
        let bad_an = spec(serde_json::json!({"fields": {"t": {"analyzer": "klingon"}}}));
        assert!(compile(&bad_an).is_err());
        let bad_ty = spec(serde_json::json!({"fields": {"t": {"type": "geo_point"}}}));
        assert!(compile(&bad_ty).is_err());
    }

    #[test]
    fn rejects_redefining_id() {
        let m = spec(serde_json::json!({"fields": {"_id": {"type": "keyword"}}}));
        assert!(compile(&m).is_err());
    }

    #[test]
    fn english_analyzer_tokenizes_with_stemming() {
        let m = spec(serde_json::json!({"fields": {"body": {"analyzer": "english"}}}));
        let ss = compile(&m).unwrap();
        let terms = ss.analyze("body", "running QUICKLY").unwrap();
        // en_stem lowercases + stems: "running" -> "run", "quickly" -> "quickli"
        assert!(terms.contains(&"run".to_string()));
    }
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p bluedb-search mapping:: 2>&1`
Expected: FAIL (not defined).

- [ ] **Step 3: Implement (prepend, above the tests)**

```rust
//! ES mapping → tantivy schema. Produces a [`SearchSchema`]: the built tantivy
//! [`Schema`], a per-field resolution table (field handle + kind + analyzer),
//! and the `_id` id field. Pure; no I/O.

use std::collections::HashMap;

use bluedb_fts::mapping::{Analyzer, FieldMapping, IndexMapping};
use bluedb_fts::IdField;
use tantivy::schema::{Field, Schema};

use crate::error::{Result, SearchError};
use crate::model::MappingSpec;

/// The reserved id field name (the collection primary key).
pub const ID_FIELD: &str = "_id";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKindInfo {
    /// Analyzed text (the analyzer is carried alongside).
    Text(TextAnalyzerKind),
    /// Exact/untokenized string.
    Keyword,
    /// 64-bit signed integer (stored + fast).
    Integer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextAnalyzerKind {
    English,
    Standard,
    Whitespace,
}

impl TextAnalyzerKind {
    fn to_fts(self) -> Analyzer {
        match self {
            TextAnalyzerKind::English => Analyzer::EnStem,
            TextAnalyzerKind::Standard => Analyzer::Default,
            TextAnalyzerKind::Whitespace => Analyzer::Whitespace,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ResolvedField {
    pub field: Field,
    pub kind: FieldKindInfo,
}

/// A compiled, ready-to-use view of a search mapping.
pub struct SearchSchema {
    pub schema: Schema,
    pub id_field: IdField,
    fields: HashMap<String, ResolvedField>,
}

impl SearchSchema {
    pub fn field(&self, name: &str) -> Option<&ResolvedField> {
        self.fields.get(name)
    }

    pub fn is_text_or_keyword(&self, name: &str) -> bool {
        matches!(
            self.fields.get(name).map(|r| r.kind),
            Some(FieldKindInfo::Text(_)) | Some(FieldKindInfo::Keyword)
        )
    }

    /// Tokenize `text` with the analyzer of field `name` (mirrors indexing).
    /// Keyword fields produce the single, whole, untouched value.
    pub fn analyze(&self, name: &str, text: &str) -> Result<Vec<String>> {
        let f = self
            .fields
            .get(name)
            .ok_or_else(|| SearchError::UnmappedField(name.to_string()))?;
        let analyzer = match f.kind {
            FieldKindInfo::Keyword => return Ok(vec![text.to_string()]),
            FieldKindInfo::Integer => {
                return Err(SearchError::BadRequest(format!(
                    "field [{name}] is numeric; cannot analyze as text"
                )))
            }
            FieldKindInfo::Text(a) => a.to_fts(),
        };
        let mut ta = analyzer.build_analyzer();
        let mut stream = ta.token_stream(text);
        let mut out = Vec::new();
        while let Some(tok) = stream.next() {
            out.push(tok.text.clone());
        }
        Ok(out)
    }
}

fn parse_field(name: &str, spec: &crate::model::FieldSpec) -> Result<FieldKindInfo> {
    // `type` wins; absent type + analyzer => text; absent both => text.
    match spec.r#type.as_deref() {
        Some("keyword") => Ok(FieldKindInfo::Keyword),
        Some("integer") | Some("long") => Ok(FieldKindInfo::Integer),
        Some("text") | None => {
            let a = match spec.analyzer.as_deref() {
                Some("english") => TextAnalyzerKind::English,
                Some("standard") | None => TextAnalyzerKind::Standard,
                Some("whitespace") => TextAnalyzerKind::Whitespace,
                Some(other) => return Err(SearchError::UnsupportedAnalyzer(other.to_string())),
            };
            Ok(FieldKindInfo::Text(a))
        }
        Some(other) => Err(SearchError::UnsupportedFieldType(format!("{other} (field {name})"))),
    }
}

/// Compile an ES mapping into a tantivy schema + resolution table.
pub fn compile(spec: &MappingSpec) -> Result<SearchSchema> {
    let mut im = IndexMapping::new().keyword(ID_FIELD); // _id: raw + STORED.
    let mut kinds: Vec<(String, FieldKindInfo)> = Vec::new();

    for (name, fspec) in &spec.fields {
        if name == ID_FIELD {
            return Err(SearchError::BadRequest(format!(
                "`{ID_FIELD}` is reserved and cannot be declared in a mapping"
            )));
        }
        let kind = parse_field(name, fspec)?;
        im = match kind {
            FieldKindInfo::Text(a) => im.text(name, a.to_fts()),
            FieldKindInfo::Keyword => im.keyword(name),
            FieldKindInfo::Integer => im.field(FieldMapping::i64(name)),
        };
        kinds.push((name.clone(), kind));
    }

    let schema = im.build_schema();
    let id = schema
        .get_field(ID_FIELD)
        .map_err(|e| SearchError::Other(e.into()))?;

    let mut fields = HashMap::new();
    fields.insert(
        ID_FIELD.to_string(),
        ResolvedField { field: id, kind: FieldKindInfo::Keyword },
    );
    for (name, kind) in kinds {
        let field = schema
            .get_field(&name)
            .map_err(|e| SearchError::Other(e.into()))?;
        fields.insert(name, ResolvedField { field, kind });
    }

    Ok(SearchSchema { schema, id_field: IdField(id), fields })
}
```

- [ ] **Step 4: Add module + run tests**

Add `pub mod mapping;` to `crates/bluedb-search/src/lib.rs`.
Run: `cargo test -p bluedb-search mapping:: 2>&1`
Expected: 4 tests PASS. (If `en_stem` stems "running"→something other than "run", adjust the assertion to the actual stem — run `cargo test -- --nocapture` to print `terms` first.)

- [ ] **Step 5: Commit**
```bash
git add crates/bluedb-search/src/mapping.rs crates/bluedb-search/src/lib.rs
git commit -F - <<'EOF'
feat(search): ES mapping -> tantivy schema (SearchSchema)

text/keyword/integer field types; english/standard/whitespace analyzers; always
adds the stored `_id` keyword id field; analyze() tokenizes query text with a
field's analyzer to mirror indexing.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 4: `query.rs` (part 1) — term / match / match_phrase / exists

**Files:**
- Create: `crates/bluedb-search/src/query.rs`
- Modify: `crates/bluedb-search/src/lib.rs` (add `pub mod query;`)

- [ ] **Step 1: Write the failing test**

Append to `crates/bluedb-search/src/query.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::compile;
    use crate::model::MappingSpec;

    fn schema() -> crate::mapping::SearchSchema {
        let m: MappingSpec = serde_json::from_value(serde_json::json!({"fields": {
            "title": {"analyzer": "english"},
            "tag": {"type": "keyword"},
            "year": {"type": "integer"}
        }})).unwrap();
        compile(&m).unwrap()
    }

    #[test]
    fn match_collects_analyzed_terms() {
        let ss = schema();
        let c = compile_query(&ss, &serde_json::json!({"match": {"title": "Running Dogs"}})).unwrap();
        let terms = c.terms_by_field.get("title").unwrap();
        assert!(terms.contains(&"dog".to_string()) || terms.contains(&"dogs".to_string()));
        // query is searchable (just assert it built)
        let _ = c.query;
    }

    #[test]
    fn term_is_exact() {
        let ss = schema();
        let c = compile_query(&ss, &serde_json::json!({"term": {"tag": "rust"}})).unwrap();
        assert_eq!(c.terms_by_field.get("tag").unwrap(), &vec!["rust".to_string()]);
    }

    #[test]
    fn match_phrase_builds() {
        let ss = schema();
        assert!(compile_query(&ss, &serde_json::json!({"match_phrase": {"title": "quick brown"}})).is_ok());
    }

    #[test]
    fn exists_builds() {
        let ss = schema();
        assert!(compile_query(&ss, &serde_json::json!({"exists": {"field": "title"}})).is_ok());
    }

    #[test]
    fn unmapped_field_errors() {
        let ss = schema();
        let e = compile_query(&ss, &serde_json::json!({"match": {"nope": "x"}})).unwrap_err();
        assert!(matches!(e, crate::SearchError::UnmappedField(_)));
    }

    #[test]
    fn match_all_builds() {
        let ss = schema();
        assert!(compile_query(&ss, &serde_json::json!({"match_all": {}})).is_ok());
    }
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p bluedb-search query:: 2>&1`
Expected: FAIL.

- [ ] **Step 3: Implement part 1 (prepend, above tests)**

```rust
//! ES Query-DSL JSON → a tantivy [`Query`]. Pure; needs only the compiled
//! [`SearchSchema`]. Also records the analyzed terms per field (for highlight).

use std::collections::HashMap;

use serde_json::Value;
use tantivy::query::{
    AllQuery, BooleanQuery, ExistsQuery, Occur, PhraseQuery, Query, TermQuery,
};
use tantivy::schema::{IndexRecordOption, Term};

use crate::error::{Result, SearchError};
use crate::mapping::{FieldKindInfo, SearchSchema};

/// A lowered query plus the analyzed terms per field (for highlighting).
pub struct CompiledQuery {
    pub query: Box<dyn Query>,
    pub terms_by_field: HashMap<String, Vec<String>>,
}

/// Lower an ES query object into a tantivy query.
pub fn compile_query(ss: &SearchSchema, q: &Value) -> Result<CompiledQuery> {
    let mut terms_by_field: HashMap<String, Vec<String>> = HashMap::new();
    let query = lower(ss, q, &mut terms_by_field)?;
    Ok(CompiledQuery { query, terms_by_field })
}

fn obj<'a>(q: &'a Value, key: &str) -> Result<&'a serde_json::Map<String, Value>> {
    q.get(key)
        .and_then(Value::as_object)
        .ok_or_else(|| SearchError::BadRequest(format!("`{key}` must be an object")))
}

/// Pull the `{field: <leaf>}` shape used by match/term/match_phrase.
fn single_field<'a>(m: &'a serde_json::Map<String, Value>) -> Result<(&'a String, &'a Value)> {
    let mut it = m.iter();
    let first = it
        .next()
        .ok_or_else(|| SearchError::BadRequest("empty query clause".into()))?;
    if it.next().is_some() {
        return Err(SearchError::BadRequest(
            "exactly one field per leaf query clause".into(),
        ));
    }
    Ok(first)
}

/// Extract the query text from either `{field: "text"}` or `{field: {"query": "text", ...}}`.
fn match_text(leaf: &Value) -> Result<(&str, bool)> {
    match leaf {
        Value::String(s) => Ok((s.as_str(), false)),
        Value::Object(o) => {
            let q = o
                .get("query")
                .and_then(Value::as_str)
                .ok_or_else(|| SearchError::BadRequest("match needs a `query` string".into()))?;
            let and = o
                .get("operator")
                .and_then(Value::as_str)
                .map(|s| s.eq_ignore_ascii_case("and"))
                .unwrap_or(false);
            Ok((q, and))
        }
        _ => Err(SearchError::BadRequest("match value must be a string or object".into())),
    }
}

fn lower(
    ss: &SearchSchema,
    q: &Value,
    terms: &mut HashMap<String, Vec<String>>,
) -> Result<Box<dyn Query>> {
    let m = q
        .as_object()
        .ok_or_else(|| SearchError::BadRequest("query must be an object".into()))?;
    let kind = m
        .keys()
        .next()
        .ok_or_else(|| SearchError::BadRequest("empty query".into()))?
        .as_str();

    match kind {
        "match_all" => Ok(Box::new(AllQuery)),
        "match" => lower_match(ss, obj(q, "match")?, terms),
        "match_phrase" => lower_phrase(ss, obj(q, "match_phrase")?, terms),
        "term" => lower_term(ss, obj(q, "term")?, terms),
        "exists" => lower_exists(ss, obj(q, "exists")?),
        "range" => crate::query::range::lower_range(ss, obj(q, "range")?),
        "bool" => crate::query::boolean::lower_bool(ss, obj(q, "bool")?, terms),
        other => Err(SearchError::UnsupportedQuery(other.to_string())),
    }
}

fn resolve_field<'a>(ss: &'a SearchSchema, name: &str) -> Result<&'a crate::mapping::ResolvedField> {
    ss.field(name)
        .ok_or_else(|| SearchError::UnmappedField(name.to_string()))
}

fn lower_match(
    ss: &SearchSchema,
    m: &serde_json::Map<String, Value>,
    terms: &mut HashMap<String, Vec<String>>,
) -> Result<Box<dyn Query>> {
    let (field_name, leaf) = single_field(m)?;
    let resolved = resolve_field(ss, field_name)?;
    let (text, want_and) = match_text(leaf)?;
    let analyzed = ss.analyze(field_name, text)?;
    terms.entry(field_name.clone()).or_default().extend(analyzed.clone());

    if analyzed.is_empty() {
        return Ok(Box::new(BooleanQuery::new(vec![]))); // matches nothing
    }
    let occur = if want_and { Occur::Must } else { Occur::Should };
    let clauses: Vec<(Occur, Box<dyn Query>)> = analyzed
        .into_iter()
        .map(|t| {
            let tq: Box<dyn Query> = Box::new(TermQuery::new(
                Term::from_field_text(resolved.field, &t),
                IndexRecordOption::WithFreqs,
            ));
            (occur, tq)
        })
        .collect();
    Ok(Box::new(BooleanQuery::new(clauses)))
}

fn lower_phrase(
    ss: &SearchSchema,
    m: &serde_json::Map<String, Value>,
    terms: &mut HashMap<String, Vec<String>>,
) -> Result<Box<dyn Query>> {
    let (field_name, leaf) = single_field(m)?;
    let resolved = resolve_field(ss, field_name)?;
    let (text, _) = match_text(leaf)?;
    let analyzed = ss.analyze(field_name, text)?;
    terms.entry(field_name.clone()).or_default().extend(analyzed.clone());

    if analyzed.len() < 2 {
        // A 0/1-term phrase degrades to a term query (PhraseQuery requires >=2).
        if let Some(t) = analyzed.into_iter().next() {
            return Ok(Box::new(TermQuery::new(
                Term::from_field_text(resolved.field, &t),
                IndexRecordOption::WithFreqs,
            )));
        }
        return Ok(Box::new(BooleanQuery::new(vec![])));
    }
    let tterms: Vec<Term> = analyzed
        .iter()
        .map(|t| Term::from_field_text(resolved.field, t))
        .collect();
    Ok(Box::new(PhraseQuery::new(tterms)))
}

fn lower_term(
    ss: &SearchSchema,
    m: &serde_json::Map<String, Value>,
    terms: &mut HashMap<String, Vec<String>>,
) -> Result<Box<dyn Query>> {
    let (field_name, leaf) = single_field(m)?;
    let resolved = resolve_field(ss, field_name)?;
    // `term` is exact: a string for text/keyword, an integer for numeric.
    let value = match leaf {
        Value::Object(o) => o.get("value").unwrap_or(&Value::Null),
        other => other,
    };
    match resolved.kind {
        FieldKindInfo::Integer => {
            let n = value
                .as_i64()
                .ok_or_else(|| SearchError::BadRequest(format!("term on numeric field [{field_name}] needs an integer")))?;
            Ok(Box::new(TermQuery::new(
                Term::from_field_i64(resolved.field, n),
                IndexRecordOption::Basic,
            )))
        }
        FieldKindInfo::Text(_) | FieldKindInfo::Keyword => {
            let s = value
                .as_str()
                .ok_or_else(|| SearchError::BadRequest(format!("term on field [{field_name}] needs a string")))?;
            terms.entry(field_name.clone()).or_default().push(s.to_string());
            Ok(Box::new(TermQuery::new(
                Term::from_field_text(resolved.field, s),
                IndexRecordOption::Basic,
            )))
        }
    }
}

fn lower_exists(ss: &SearchSchema, m: &serde_json::Map<String, Value>) -> Result<Box<dyn Query>> {
    let name = m
        .get("field")
        .and_then(Value::as_str)
        .ok_or_else(|| SearchError::BadRequest("exists needs a `field`".into()))?;
    let _ = resolve_field(ss, name)?;
    Ok(Box::new(ExistsQuery::new(name.to_string(), false)))
}
```

NOTE: `lower` references `crate::query::range::lower_range` and `crate::query::boolean::lower_bool`, added in Task 5. For THIS task, temporarily stub them so part-1 tests run — add at the bottom of `query.rs` (above the tests):
```rust
mod range {
    use super::*;
    pub fn lower_range(
        _ss: &SearchSchema,
        _m: &serde_json::Map<String, Value>,
    ) -> Result<Box<dyn Query>> {
        Err(SearchError::UnsupportedQuery("range".into()))
    }
}
mod boolean {
    use super::*;
    pub fn lower_bool(
        _ss: &SearchSchema,
        _m: &serde_json::Map<String, Value>,
        _t: &mut std::collections::HashMap<String, Vec<String>>,
    ) -> Result<Box<dyn Query>> {
        Err(SearchError::UnsupportedQuery("bool".into()))
    }
}
```
(Task 5 replaces these stubs with real implementations.)

- [ ] **Step 4: Add module + run tests**

Add `pub mod query;` to `crates/bluedb-search/src/lib.rs`.
Run: `cargo test -p bluedb-search query:: 2>&1`
Expected: part-1 tests PASS. (If `en_stem` stems "dogs"→"dog", the `match_collects_analyzed_terms` assertion's OR covers it.)

- [ ] **Step 5: Commit**
```bash
git add crates/bluedb-search/src/query.rs crates/bluedb-search/src/lib.rs
git commit -F - <<'EOF'
feat(search): lower match/match_phrase/term/exists/match_all to tantivy

ES leaf queries -> tantivy Query (BooleanQuery of analyzed TermQueries for match,
PhraseQuery for match_phrase, exact TermQuery for term, ExistsQuery for exists),
recording analyzed terms per field for highlighting. range/bool stubbed.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 5: `query.rs` (part 2) — range + bool

**Files:**
- Modify: `crates/bluedb-search/src/query.rs` (replace the `range`/`boolean` stub modules)

- [ ] **Step 1: Write the failing test**

Add to the `tests` module in `crates/bluedb-search/src/query.rs`:
```rust
    #[test]
    fn range_on_integer_builds() {
        let ss = schema();
        assert!(compile_query(&ss, &serde_json::json!({"range": {"year": {"gte": 2000, "lt": 2020}}})).is_ok());
    }

    #[test]
    fn range_on_keyword_builds() {
        let ss = schema();
        assert!(compile_query(&ss, &serde_json::json!({"range": {"tag": {"gte": "a", "lte": "m"}}})).is_ok());
    }

    #[test]
    fn bool_merges_clauses_and_terms() {
        let ss = schema();
        let c = compile_query(&ss, &serde_json::json!({"bool": {
            "must": [{"match": {"title": "dog"}}],
            "filter": [{"term": {"tag": "pets"}}],
            "must_not": [{"term": {"tag": "draft"}}],
            "should": [{"match": {"title": "park"}}]
        }})).unwrap();
        // must + should contribute highlight terms; must_not does not.
        assert!(c.terms_by_field.get("title").is_some());
    }

    #[test]
    fn range_on_text_field_errors() {
        let ss = schema();
        assert!(compile_query(&ss, &serde_json::json!({"range": {"title": {"gte": "a"}}})).is_err());
    }
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p bluedb-search query:: 2>&1`
Expected: FAIL (stubs return errors).

- [ ] **Step 3: Replace the `range` stub module**

```rust
mod range {
    use std::ops::Bound;

    use serde_json::Value;
    use tantivy::query::{Query, RangeQuery};
    use tantivy::schema::Term;

    use crate::error::{Result, SearchError};
    use crate::mapping::{FieldKindInfo, SearchSchema};

    fn bound_i64(m: &serde_json::Map<String, Value>, incl: &str, excl: &str, field: tantivy::schema::Field) -> Result<Bound<Term>> {
        if let Some(v) = m.get(incl) {
            let n = v.as_i64().ok_or_else(|| SearchError::BadRequest("range bound must be an integer".into()))?;
            Ok(Bound::Included(Term::from_field_i64(field, n)))
        } else if let Some(v) = m.get(excl) {
            let n = v.as_i64().ok_or_else(|| SearchError::BadRequest("range bound must be an integer".into()))?;
            Ok(Bound::Excluded(Term::from_field_i64(field, n)))
        } else {
            Ok(Bound::Unbounded)
        }
    }

    fn bound_text(m: &serde_json::Map<String, Value>, incl: &str, excl: &str, field: tantivy::schema::Field) -> Result<Bound<Term>> {
        if let Some(v) = m.get(incl) {
            let s = v.as_str().ok_or_else(|| SearchError::BadRequest("range bound must be a string".into()))?;
            Ok(Bound::Included(Term::from_field_text(field, s)))
        } else if let Some(v) = m.get(excl) {
            let s = v.as_str().ok_or_else(|| SearchError::BadRequest("range bound must be a string".into()))?;
            Ok(Bound::Excluded(Term::from_field_text(field, s)))
        } else {
            Ok(Bound::Unbounded)
        }
    }

    pub fn lower_range(
        ss: &SearchSchema,
        m: &serde_json::Map<String, Value>,
    ) -> Result<Box<dyn Query>> {
        let mut it = m.iter();
        let (field_name, body) = it
            .next()
            .ok_or_else(|| SearchError::BadRequest("empty range".into()))?;
        let body = body
            .as_object()
            .ok_or_else(|| SearchError::BadRequest("range body must be an object".into()))?;
        let resolved = ss
            .field(field_name)
            .ok_or_else(|| SearchError::UnmappedField(field_name.clone()))?;
        match resolved.kind {
            FieldKindInfo::Integer => {
                let lo = bound_i64(body, "gte", "gt", resolved.field)?;
                let hi = bound_i64(body, "lte", "lt", resolved.field)?;
                Ok(Box::new(RangeQuery::new(lo, hi)))
            }
            FieldKindInfo::Keyword => {
                let lo = bound_text(body, "gte", "gt", resolved.field)?;
                let hi = bound_text(body, "lte", "lt", resolved.field)?;
                Ok(Box::new(RangeQuery::new(lo, hi)))
            }
            FieldKindInfo::Text(_) => Err(SearchError::BadRequest(format!(
                "range is not supported on analyzed text field [{field_name}] (use keyword)"
            ))),
        }
    }
}
```

- [ ] **Step 4: Replace the `boolean` stub module**

```rust
mod boolean {
    use std::collections::HashMap;

    use serde_json::Value;
    use tantivy::query::{BooleanQuery, Occur, Query};

    use crate::error::{Result, SearchError};
    use crate::mapping::SearchSchema;

    fn clauses_for(
        ss: &SearchSchema,
        body: &serde_json::Map<String, Value>,
        key: &str,
        occur: Occur,
        terms: &mut HashMap<String, Vec<String>>,
        out: &mut Vec<(Occur, Box<dyn Query>)>,
    ) -> Result<()> {
        let Some(v) = body.get(key) else { return Ok(()) };
        // A clause may be a single object or an array of objects.
        let items: Vec<&Value> = match v {
            Value::Array(a) => a.iter().collect(),
            other => vec![other],
        };
        for item in items {
            // must_not must not pollute highlight terms: give it a throwaway map.
            let q = if matches!(occur, Occur::MustNot) {
                let mut scratch = HashMap::new();
                super::lower(ss, item, &mut scratch)?
            } else {
                super::lower(ss, item, terms)?
            };
            // `filter` is Must but should not score; tantivy scores Must clauses,
            // so we keep it as Must for matching (v1 treats filter == must for scoring).
            out.push((occur, q));
        }
        Ok(())
    }

    pub fn lower_bool(
        ss: &SearchSchema,
        body: &serde_json::Map<String, Value>,
        terms: &mut HashMap<String, Vec<String>>,
    ) -> Result<Box<dyn Query>> {
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        clauses_for(ss, body, "must", Occur::Must, terms, &mut clauses)?;
        clauses_for(ss, body, "filter", Occur::Must, terms, &mut clauses)?;
        clauses_for(ss, body, "should", Occur::Should, terms, &mut clauses)?;
        clauses_for(ss, body, "must_not", Occur::MustNot, terms, &mut clauses)?;
        if clauses.is_empty() {
            return Err(SearchError::BadRequest("empty bool query".into()));
        }
        Ok(Box::new(BooleanQuery::new(clauses)))
    }
}
```

NOTE: `super::lower` must be reachable from the `boolean` submodule — `lower` is a free fn in `query.rs`, so `super::lower` resolves. Same for `range`. Keep `lower`, `obj`, `resolve_field` etc. as module-level `fn` (not nested).

- [ ] **Step 5: Run tests**

Run: `cargo test -p bluedb-search query:: 2>&1`
Expected: all query tests PASS.

- [ ] **Step 6: Commit**
```bash
git add crates/bluedb-search/src/query.rs
git commit -F - <<'EOF'
feat(search): lower range + bool queries

range -> tantivy RangeQuery (integer + keyword fields; analyzed-text range
rejected); bool -> BooleanQuery with must/filter=Must, should=Should,
must_not=MustNot (must_not excluded from highlight terms).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 6: `hits.rs` — ES hits-envelope assembly

**Files:**
- Create: `crates/bluedb-search/src/hits.rs`
- Modify: `crates/bluedb-search/src/lib.rs` (add `pub mod hits;`)

- [ ] **Step 1: Write the failing test**

Append to `crates/bluedb-search/src/hits.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::SourceSpec;
    use std::collections::HashMap;

    fn src(id: &str, title: &str, body: &str) -> serde_json::Value {
        serde_json::json!({"_id": id, "title": title, "body": body})
    }

    #[test]
    fn assembles_envelope_in_rank_order() {
        let ranked = vec![("a".to_string(), 2.0f32), ("b".to_string(), 1.0f32)];
        let mut sources = HashMap::new();
        sources.insert("a".to_string(), src("a", "Dogs", "good dogs"));
        sources.insert("b".to_string(), src("b", "Cats", "ok cats"));
        let out = assemble(
            "pets", &ranked, 2, sources, &SourceSpec::Bool(true), &[], &HashMap::new(),
        );
        assert_eq!(out.total.value, 2);
        assert_eq!(out.max_score, Some(2.0));
        assert_eq!(out.hits[0]._id, "a");
        assert_eq!(out.hits[1]._id, "b");
        assert!(out.hits[0]._source.is_some());
    }

    #[test]
    fn source_false_omits_source() {
        let ranked = vec![("a".to_string(), 1.0f32)];
        let mut sources = HashMap::new();
        sources.insert("a".to_string(), src("a", "x", "y"));
        let out = assemble("c", &ranked, 1, sources, &SourceSpec::Bool(false), &[], &HashMap::new());
        assert!(out.hits[0]._source.is_none());
    }

    #[test]
    fn source_field_list_trims() {
        let ranked = vec![("a".to_string(), 1.0f32)];
        let mut sources = HashMap::new();
        sources.insert("a".to_string(), src("a", "x", "y"));
        let out = assemble(
            "c", &ranked, 1, sources,
            &SourceSpec::Fields(vec!["title".into()]), &[], &HashMap::new(),
        );
        let s = out.hits[0]._source.as_ref().unwrap();
        assert!(s.get("title").is_some());
        assert!(s.get("body").is_none());
    }

    #[test]
    fn highlights_matched_terms() {
        let ranked = vec![("a".to_string(), 1.0f32)];
        let mut sources = HashMap::new();
        sources.insert("a".to_string(), src("a", "x", "good dogs"));
        let mut terms = HashMap::new();
        terms.insert("body".to_string(), vec!["dogs".to_string()]);
        let out = assemble(
            "c", &ranked, 1, sources, &SourceSpec::Bool(true),
            &["body".to_string()], &terms,
        );
        let hl = out.hits[0].highlight.as_ref().unwrap();
        assert!(hl.get("body").unwrap()[0].contains("<em>dogs</em>"));
    }
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p bluedb-search hits:: 2>&1`
Expected: FAIL.

- [ ] **Step 3: Implement (prepend, above tests)**

```rust
//! Assemble the ES hits envelope from ranked `(id, score)` results plus the
//! re-inflated `_source` documents. Pure; no I/O.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value;

use crate::model::{Hit, HitsBlock, HitsTotal, SourceSpec};

fn trim_source(doc: &Value, spec: &SourceSpec) -> Option<Value> {
    match spec {
        SourceSpec::Bool(false) => None,
        SourceSpec::Bool(true) => Some(doc.clone()),
        SourceSpec::Fields(fields) => {
            let mut out = serde_json::Map::new();
            if let Some(obj) = doc.as_object() {
                for f in fields {
                    if let Some(v) = obj.get(f) {
                        out.insert(f.clone(), v.clone());
                    }
                }
            }
            Some(Value::Object(out))
        }
    }
}

/// Wrap whole-token, case-insensitive occurrences of any `terms` in `<em>...</em>`.
fn highlight_text(text: &str, terms: &[String]) -> Option<String> {
    if terms.is_empty() {
        return None;
    }
    let lset: std::collections::HashSet<String> =
        terms.iter().map(|t| t.to_lowercase()).collect();
    let mut out = String::with_capacity(text.len() + 16);
    let mut hit = false;
    // Tokenize on non-alphanumeric while preserving the original separators.
    let mut word = String::new();
    let flush = |word: &mut String, out: &mut String, hit: &mut bool, lset: &std::collections::HashSet<String>| {
        if word.is_empty() {
            return;
        }
        if lset.contains(&word.to_lowercase()) {
            out.push_str("<em>");
            out.push_str(word);
            out.push_str("</em>");
            *hit = true;
        } else {
            out.push_str(word);
        }
        word.clear();
    };
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            word.push(ch);
        } else {
            flush(&mut word, &mut out, &mut hit, &lset);
            out.push(ch);
        }
    }
    flush(&mut word, &mut out, &mut hit, &lset);
    if hit {
        Some(out)
    } else {
        None
    }
}

/// Build the hits envelope. `ranked` is the page slice (already `[from, from+size)`),
/// in display order; `total` is the full match count.
pub fn assemble(
    index: &str,
    ranked: &[(String, f32)],
    total: usize,
    mut sources: HashMap<String, Value>,
    source_spec: &SourceSpec,
    highlight_fields: &[String],
    terms_by_field: &HashMap<String, Vec<String>>,
) -> HitsBlock {
    let max_score = ranked.first().map(|(_, s)| *s);
    let mut hits = Vec::with_capacity(ranked.len());
    for (id, score) in ranked {
        let doc = sources.remove(id);
        let _source = doc.as_ref().and_then(|d| trim_source(d, source_spec));

        let highlight = if highlight_fields.is_empty() {
            None
        } else {
            let mut hl: BTreeMap<String, Vec<String>> = BTreeMap::new();
            if let Some(d) = doc.as_ref() {
                for f in highlight_fields {
                    let (Some(text), Some(terms)) =
                        (d.get(f).and_then(Value::as_str), terms_by_field.get(f))
                    else {
                        continue;
                    };
                    if let Some(snippet) = highlight_text(text, terms) {
                        hl.insert(f.clone(), vec![snippet]);
                    }
                }
            }
            if hl.is_empty() {
                None
            } else {
                Some(hl)
            }
        };

        hits.push(Hit {
            _index: index.to_string(),
            _id: id.clone(),
            _score: Some(*score),
            _source,
            highlight,
        });
    }
    HitsBlock {
        total: HitsTotal { value: total, relation: "eq" },
        max_score,
        hits,
    }
}
```

- [ ] **Step 4: Add module + run tests**

Add `pub mod hits;` to `crates/bluedb-search/src/lib.rs` (final lib.rs now matches Task 1 Step 5). Run: `cargo test -p bluedb-search 2>&1`
Expected: ALL bluedb-search tests PASS.

- [ ] **Step 5: Commit**
```bash
git add crates/bluedb-search/src/hits.rs crates/bluedb-search/src/lib.rs
git commit -F - <<'EOF'
feat(search): ES hits-envelope assembly (source trim + term highlight)

assemble() builds the {total,max_score,hits[]} block in rank order; _source
true/false/field-list trimming; best-effort whole-token <em> highlighting.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 7: `bluedb-fts` — `Query`-object search functions

**Files:**
- Modify: `crates/bluedb-fts/src/search.rs` (additive)
- Test: `crates/bluedb-fts/tests/query_object.rs` (new)

These mirror `multi_split_search_filtered_ids` exactly but take a pre-built `&dyn Query` (no `QueryParser`, no `fields`). Add a count variant and an integer-fast-field sorted variant.

- [ ] **Step 1: Write the failing test**

`crates/bluedb-fts/tests/query_object.rs`:
```rust
use std::ops::Bound;

use bluedb_fts::indexer::build_split;
use bluedb_fts::mapping::{Analyzer, IndexMapping};
use bluedb_fts::open::open_split_lazy; // if not pub, use the same opener the other tests use
use bluedb_fts::search::{
    multi_split_count_query_filtered, multi_split_search_query_filtered_ids, SplitHandle,
};
use bluedb_fts::tombstones::Tombstones;
use bluedb_fts::IdField;
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Term};
use tantivy::{Index, TantivyDocument};

// Build an in-RAM index with two docs and run a Query-object search.
#[test]
fn query_object_search_filtered_ids() {
    let mapping = IndexMapping::new().keyword("_id").text("body", Analyzer::Default);
    let schema = mapping.build_schema();
    let id_f = schema.get_field("_id").unwrap();
    let body_f = schema.get_field("body").unwrap();

    let index = Index::create_in_ram(schema.clone());
    mapping.register_tokenizers(&index);
    let mut writer = index.writer(15_000_000).unwrap();
    let mut d1 = TantivyDocument::default();
    d1.add_text(id_f, "a");
    d1.add_text(body_f, "good dogs");
    writer.add_document(d1).unwrap();
    let mut d2 = TantivyDocument::default();
    d2.add_text(id_f, "b");
    d2.add_text(body_f, "lazy cats");
    writer.add_document(d2).unwrap();
    writer.commit().unwrap();

    let handles = vec![SplitHandle::with_generation("s1", 0, &index)];
    let tombstones = Tombstones::new("idx");

    let query: Box<dyn Query> = Box::new(BooleanQuery::new(vec![(
        Occur::Should,
        Box::new(TermQuery::new(
            Term::from_field_text(body_f, "dogs"),
            IndexRecordOption::WithFreqs,
        )) as Box<dyn Query>,
    )]));

    let ids = multi_split_search_query_filtered_ids(
        &handles, query.as_ref(), 10, IdField(id_f), &tombstones,
    )
    .unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0].0, "a");

    let count = multi_split_count_query_filtered(&handles, query.as_ref(), IdField(id_f), &tombstones).unwrap();
    assert_eq!(count, 1);
    let _ = (build_split, open_split_lazy, Bound::<Term>::Unbounded); // silence unused if not used
}
```
(If `open_split_lazy`/`build_split` aren't `pub`, drop the unused imports + the final `let _ =` line. Match whatever the sibling tests under `crates/bluedb-fts/tests/` import.)

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p bluedb-fts --test query_object 2>&1`
Expected: FAIL (functions undefined).

- [ ] **Step 3: Implement (append to `crates/bluedb-fts/src/search.rs`)**

```rust
/// Like [`multi_split_search_filtered_ids`] but takes a pre-built tantivy
/// [`Query`] (no `QueryParser`). Returns `(id, bm25_score)` newest-wins-deduped,
/// tombstone-filtered, BM25-ordered, truncated to `limit`.
pub fn multi_split_search_query_filtered_ids(
    splits: &[SplitHandle<'_>],
    query: &dyn Query,
    limit: usize,
    id_field: IdField,
    tombstones: &Tombstones,
) -> anyhow::Result<Vec<(String, f32)>> {
    let per_split_limit = limit.saturating_add(tombstones.len()).max(limit);

    struct Candidate {
        hit: MultiSplitHit,
        id: String,
        generation: u64,
    }

    let mut all: Vec<Candidate> = Vec::new();
    for (split_ord, handle) in splits.iter().enumerate() {
        let index = handle.index;
        let reader = index.reader()?;
        let searcher = reader.searcher();
        let hits = searcher.search(query, &TopDocs::with_limit(per_split_limit).order_by_score())?;
        for (score, doc_address) in hits {
            let stored: TantivyDocument = searcher.doc(doc_address)?;
            let id = id_field.extract(&stored).ok_or_else(|| {
                anyhow::anyhow!(
                    "split {} hit at {doc_address:?} has no stored id-field value",
                    handle.split_id
                )
            })?;
            if tombstones.is_deleted_at(&id, handle.generation) {
                continue;
            }
            all.push(Candidate {
                hit: MultiSplitHit { score, split_ord, doc_address },
                id,
                generation: handle.generation,
            });
        }
    }

    use std::collections::HashMap;
    let mut best_by_id: HashMap<&str, usize> = HashMap::with_capacity(all.len());
    for (i, cand) in all.iter().enumerate() {
        match best_by_id.get(cand.id.as_str()) {
            Some(&j) => {
                let cur = &all[j];
                let wins = cand.generation > cur.generation
                    || (cand.generation == cur.generation
                        && (cand.hit.split_ord > cur.hit.split_ord
                            || (cand.hit.split_ord == cur.hit.split_ord
                                && cand.hit.score > cur.hit.score)));
                if wins {
                    best_by_id.insert(cand.id.as_str(), i);
                }
            }
            None => {
                best_by_id.insert(cand.id.as_str(), i);
            }
        }
    }
    let keep: std::collections::BTreeSet<usize> = best_by_id.values().copied().collect();
    let mut kept: Vec<&Candidate> = all
        .iter()
        .enumerate()
        .filter(|(i, _)| keep.contains(i))
        .map(|(_, c)| c)
        .collect();
    kept.sort_by(|a, b| cmp_hits(&a.hit, &b.hit));
    let mut out: Vec<(String, f32)> = kept.into_iter().map(|c| (c.id.clone(), c.hit.score)).collect();
    out.truncate(limit);
    Ok(out)
}

/// Total live matches for a pre-built [`Query`] (tombstone-filtered, deduped).
pub fn multi_split_count_query_filtered(
    splits: &[SplitHandle<'_>],
    query: &dyn Query,
    id_field: IdField,
    tombstones: &Tombstones,
) -> anyhow::Result<usize> {
    let mut live: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
    for handle in splits.iter() {
        let reader = handle.index.reader()?;
        let searcher = reader.searcher();
        // Collect all matching docs (bounded by a large cap) to dedup across splits.
        let hits = searcher.search(query, &TopDocs::with_limit(100_000).order_by_score())?;
        for (_score, doc_address) in hits {
            let stored: TantivyDocument = searcher.doc(doc_address)?;
            if let Some(id) = id_field.extract(&stored) {
                if tombstones.is_deleted_at(&id, handle.generation) {
                    continue;
                }
                let e = live.entry(id).or_insert(handle.generation);
                if handle.generation > *e {
                    *e = handle.generation;
                }
            }
        }
    }
    Ok(live.len())
}

/// Like [`multi_split_search_query_filtered_ids`] but ordered by an `i64`
/// fast field instead of BM25 score. Returns `(id, score=0.0)` pairs in the
/// requested order; docs missing the field sort last.
pub fn multi_split_search_query_sorted_ids(
    splits: &[SplitHandle<'_>],
    query: &dyn Query,
    sort_field_name: &str,
    descending: bool,
    limit: usize,
    id_field: IdField,
    tombstones: &Tombstones,
) -> anyhow::Result<Vec<(String, f32)>> {
    use tantivy::Order;
    let per_split_limit = limit.saturating_add(tombstones.len()).max(limit);
    let order = if descending { Order::Desc } else { Order::Asc };

    struct Cand {
        id: String,
        sort: Option<i64>,
        generation: u64,
    }
    let mut all: Vec<Cand> = Vec::new();
    for handle in splits.iter() {
        let reader = handle.index.reader()?;
        let searcher = reader.searcher();
        let collector = TopDocs::with_limit(per_split_limit)
            .order_by_fast_field::<i64>(sort_field_name, order.clone());
        let hits = searcher.search(query, &collector)?;
        for (sort_val, doc_address) in hits {
            let stored: TantivyDocument = searcher.doc(doc_address)?;
            if let Some(id) = id_field.extract(&stored) {
                if tombstones.is_deleted_at(&id, handle.generation) {
                    continue;
                }
                all.push(Cand { id, sort: sort_val, generation: handle.generation });
            }
        }
    }
    // newest-wins dedup by id.
    use std::collections::HashMap;
    let mut best: HashMap<String, usize> = HashMap::new();
    for (i, c) in all.iter().enumerate() {
        match best.get(&c.id) {
            Some(&j) if all[j].generation >= c.generation => {}
            _ => {
                best.insert(c.id.clone(), i);
            }
        }
    }
    let keep: std::collections::BTreeSet<usize> = best.values().copied().collect();
    let mut kept: Vec<&Cand> = all.iter().enumerate().filter(|(i, _)| keep.contains(i)).map(|(_, c)| c).collect();
    kept.sort_by(|a, b| {
        let cmp = match (a.sort, b.sort) {
            (Some(x), Some(y)) => x.cmp(&y),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        };
        if descending { cmp.reverse() } else { cmp }
    });
    let mut out: Vec<(String, f32)> = kept.into_iter().map(|c| (c.id.clone(), 0.0f32)).collect();
    out.truncate(limit);
    Ok(out)
}
```

NOTE: `cmp_hits`, `MultiSplitHit`, `SplitHandle`, `TopDocs`, `TantivyDocument`, `IdField` are already in scope in `search.rs` (see its existing imports). `tantivy::Order` is imported locally in the sorted fn. If `order.clone()` fails (Order not Clone in this fork), bind `order` once per call inside the loop instead.

- [ ] **Step 4: Run tests**

Run: `cargo test -p bluedb-fts --test query_object 2>&1`
Expected: PASS. Also run the existing fts tests to confirm no regression: `cargo test -p bluedb-fts 2>&1` → PASS.

- [ ] **Step 5: Commit**
```bash
git add crates/bluedb-fts/src/search.rs crates/bluedb-fts/tests/query_object.rs
git commit -F - <<'EOF'
feat(fts): Query-object search variants (additive)

multi_split_search_query_filtered_ids / _count_ / _sorted_ids accept a pre-built
tantivy Query (vs the existing query-string fns), reusing the same tombstone +
generation dedup. Needed by the ES-shaped search layer.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 8: `bluedb-engine::FtsIndex` — `search_query_*` methods + reachability

**Files:**
- Modify: `crates/bluedb-engine/src/fts.rs` (add methods)
- Modify: `crates/bluedb-engine/src/lib.rs` (ensure `pub use` of `FtsIndex`; verify `bluedb_fts` reachable)
- Test: `crates/bluedb-engine/tests/fts_query_object.rs` (new)

- [ ] **Step 1: Confirm reachability**

Check `crates/bluedb-engine/src/lib.rs` exports `FtsIndex`. Run:
```bash
grep -n "FtsIndex" crates/bluedb-engine/src/lib.rs 2>&1
```
If not re-exported, add (next to other `pub use`):
```rust
pub use fts::FtsIndex;
```
`bluedb-server` will reference `bluedb_engine::FtsIndex`, `bluedb_fts::IdField`, `bluedb_fts::mapping::*`, `bluedb_fts::policy::CompactionPolicy` directly (deps added in Task 1).

- [ ] **Step 2: Write the failing test**

`crates/bluedb-engine/tests/fts_query_object.rs`:
```rust
use std::sync::Arc;

use bluedb_engine::FtsIndex;
use bluedb_fts::mapping::{Analyzer, IndexMapping};
use bluedb_fts::policy::CompactionPolicy;
use bluedb_fts::IdField;
use bluedb_storage::SlateDbBlobStore;
use slatedb::Db;
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::{IndexRecordOption, Term};
use tantivy::TantivyDocument;

async fn in_mem_blob() -> Arc<SlateDbBlobStore> {
    // Mirror how other bluedb-engine tests build an in-memory SlateDB.
    let store = Arc::new(object_store::memory::InMemory::new());
    let db = Db::builder("test-fts", store).build().await.unwrap();
    Arc::new(SlateDbBlobStore::new(Arc::new(db)))
}

#[tokio::test]
async fn search_query_ids_roundtrip() {
    let mapping = IndexMapping::new().keyword("_id").text("body", Analyzer::Default);
    let schema = mapping.build_schema();
    let id_f = schema.get_field("_id").unwrap();
    let body_f = schema.get_field("body").unwrap();

    let blob = in_mem_blob().await;
    let idx = FtsIndex::new("search/_/docs", blob, schema, IdField(id_f), CompactionPolicy::default());

    let mut d = TantivyDocument::default();
    d.add_text(id_f, "x1");
    d.add_text(body_f, "hello search world");
    idx.append([d]).await.unwrap();

    let q: Box<dyn Query> = Box::new(BooleanQuery::new(vec![(
        Occur::Should,
        Box::new(TermQuery::new(Term::from_field_text(body_f, "search"), IndexRecordOption::WithFreqs)) as Box<dyn Query>,
    )]));
    let ids = idx.search_query_ids(q.as_ref(), 10).await.unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0].0, "x1");
    let n = idx.count_query(q.as_ref()).await.unwrap();
    assert_eq!(n, 1);
}
```
(Adjust `in_mem_blob` to match the exact pattern other `crates/bluedb-engine/tests/*.rs` use — check `crates/bluedb-engine/tests/fts.rs` for the canonical in-memory `Db` + `SlateDbBlobStore` construction and copy it verbatim.)

- [ ] **Step 3: Run it to confirm it fails**

Run: `cargo test -p bluedb-engine --test fts_query_object 2>&1`
Expected: FAIL.

- [ ] **Step 4: Add the methods to `impl FtsIndex` in `crates/bluedb-engine/src/fts.rs`**

First extend the imports at the top:
```rust
use bluedb_fts::search::{
    multi_split_count_query_filtered, multi_split_search_filtered, multi_split_search_filtered_ids,
    multi_split_search_query_filtered_ids, multi_split_search_query_sorted_ids, MultiSplitHit,
    SplitHandle,
};
use tantivy::query::Query;
```
(merge into the existing `use bluedb_fts::search::{...}` line; add `use tantivy::query::Query;`).

Then add inside `impl FtsIndex` (after `search_ids`):
```rust
    /// BM25 search with a pre-built tantivy [`Query`]; returns `(id, score)`.
    pub async fn search_query_ids(
        &self,
        query: &dyn Query,
        limit: usize,
    ) -> Result<Vec<(String, f32)>> {
        let manifest = self.load_manifest().await?;
        let tombstones = self.load_tombstones().await?;
        let opened = self.open_splits(manifest.splits.iter()).await?;
        let handles: Vec<SplitHandle> = opened
            .iter()
            .map(|(id, generation, index)| SplitHandle::with_generation(id.clone(), *generation, index))
            .collect();
        Ok(multi_split_search_query_filtered_ids(
            &handles,
            query,
            limit,
            self.id_field,
            &tombstones,
        )?)
    }

    /// Total live matches for a pre-built tantivy [`Query`].
    pub async fn count_query(&self, query: &dyn Query) -> Result<usize> {
        let manifest = self.load_manifest().await?;
        let tombstones = self.load_tombstones().await?;
        let opened = self.open_splits(manifest.splits.iter()).await?;
        let handles: Vec<SplitHandle> = opened
            .iter()
            .map(|(id, generation, index)| SplitHandle::with_generation(id.clone(), *generation, index))
            .collect();
        Ok(multi_split_count_query_filtered(
            &handles,
            query,
            self.id_field,
            &tombstones,
        )?)
    }

    /// Search with a pre-built [`Query`] ordered by an `i64` fast field.
    pub async fn search_query_sorted_ids(
        &self,
        query: &dyn Query,
        sort_field_name: &str,
        descending: bool,
        limit: usize,
    ) -> Result<Vec<(String, f32)>> {
        let manifest = self.load_manifest().await?;
        let tombstones = self.load_tombstones().await?;
        let opened = self.open_splits(manifest.splits.iter()).await?;
        let handles: Vec<SplitHandle> = opened
            .iter()
            .map(|(id, generation, index)| SplitHandle::with_generation(id.clone(), *generation, index))
            .collect();
        Ok(multi_split_search_query_sorted_ids(
            &handles,
            query,
            sort_field_name,
            descending,
            limit,
            self.id_field,
            &tombstones,
        )?)
    }
```

- [ ] **Step 5: Run tests**

Run: `cargo test -p bluedb-engine --test fts_query_object 2>&1` → PASS.
Run: `cargo test -p bluedb-engine 2>&1` → no regressions.

- [ ] **Step 6: Commit**
```bash
git add crates/bluedb-engine/src/fts.rs crates/bluedb-engine/src/lib.rs crates/bluedb-engine/tests/fts_query_object.rs
git commit -F - <<'EOF'
feat(engine): FtsIndex Query-object search methods

search_query_ids / count_query / search_query_sorted_ids drive the new
bluedb-fts Query-object functions over the same manifest/tombstone/split-open
orchestration as the string-based methods.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 9: `bluedb-server` — `search.rs` scaffold (SearchEngine + helpers + AppState wiring)

**Files:**
- Create: `crates/bluedb-server/src/search.rs`
- Modify: `crates/bluedb-server/src/lib.rs` (`mod search;`, `AppState.inner.search`, promote/demote)

Facts: `AppState.inner.db: RwLock<Option<Database>>`; `Database::substrate() -> Substrate`; `SlateDbBlobStore::from_substrate(substrate)`; promote builds `Database::new(...)` ~`lib.rs:506`, swaps fts ~`lib.rs:528`; demote resets fts ~`lib.rs:613`. `FtsEngine` field is `fts: RwLock<Arc<FtsEngine>>` ~`lib.rs:241`.

- [ ] **Step 1: Write `search.rs` scaffold**

`crates/bluedb-server/src/search.rs` (top section — types + helpers; handlers come in later tasks):
```rust
//! Elasticsearch-shaped search over `/collections` documents. Thin handlers +
//! per-(tenant,collection) tantivy index wiring; all ES-DSL translation lives in
//! the pure `bluedb-search` crate.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::Json;
use bluedb_engine::FtsIndex;
use bluedb_fts::policy::CompactionPolicy;
use bluedb_search::mapping::SearchSchema;
use bluedb_search::model::MappingSpec;
use bluedb_storage::{SlateDbBlobStore, Substrate};
use serde_json::Value;
use tokio::sync::RwLock;

use crate::{AppError, AppState};

/// Tenant-isolated index id. One shared SlateDB Db, tenant-prefixed blob keys.
pub(crate) fn index_id(tenant: &str, coll: &str) -> String {
    format!("search/{tenant}/{coll}")
}

/// Build a (read-or-write) `FtsIndex` for a compiled mapping over a blob store.
pub(crate) fn build_fts_index(
    blob: Arc<SlateDbBlobStore>,
    tenant: &str,
    coll: &str,
    ss: &SearchSchema,
) -> FtsIndex {
    FtsIndex::new(
        index_id(tenant, coll),
        blob,
        ss.schema.clone(),
        ss.id_field,
        CompactionPolicy::default(),
    )
}

/// A read-only blob store over the currently-bound substrate (writer OR reader).
/// Used by the search read path so it works on any node with a bound DB.
pub(crate) async fn search_blob(state: &AppState) -> Result<Arc<SlateDbBlobStore>, AppError> {
    let guard = state.db_read().await;
    let db = guard
        .as_ref()
        .ok_or_else(|| AppError::service_unavailable("no database bound on this node"))?;
    Ok(Arc::new(SlateDbBlobStore::from_substrate(db.substrate())))
}

/// Owns the writer-side, shared, write-serialized `FtsIndex` handles. Swapped on
/// promote/demote like `FtsEngine`. `blob == None` means "not the active writer".
pub(crate) struct SearchEngine {
    blob: Option<Arc<SlateDbBlobStore>>,
    indexes: RwLock<HashMap<(String, String), Arc<FtsIndex>>>,
}

impl SearchEngine {
    pub(crate) fn empty() -> Arc<Self> {
        Arc::new(Self { blob: None, indexes: RwLock::new(HashMap::new()) })
    }

    pub(crate) fn new_durable(substrate: Substrate) -> Arc<Self> {
        Arc::new(Self {
            blob: Some(Arc::new(SlateDbBlobStore::from_substrate(substrate))),
            indexes: RwLock::new(HashMap::new()),
        })
    }

    pub(crate) fn writer_blob(&self) -> Option<Arc<SlateDbBlobStore>> {
        self.blob.clone()
    }

    /// Get-or-create the shared, write-serialized index handle for (tenant, coll).
    pub(crate) async fn index_for(
        &self,
        tenant: &str,
        coll: &str,
        ss: &SearchSchema,
    ) -> Result<Arc<FtsIndex>, AppError> {
        let blob = self
            .blob
            .clone()
            .ok_or_else(|| AppError::service_unavailable("search writes require the active writer"))?;
        let key = (tenant.to_string(), coll.to_string());
        {
            let r = self.indexes.read().await;
            if let Some(idx) = r.get(&key) {
                return Ok(idx.clone());
            }
        }
        let mut w = self.indexes.write().await;
        let idx = w
            .entry(key)
            .or_insert_with(|| Arc::new(build_fts_index(blob, tenant, coll, ss)))
            .clone();
        Ok(idx)
    }

    /// Forget a cached handle (e.g. after the mapping is replaced).
    pub(crate) async fn forget(&self, tenant: &str, coll: &str) {
        self.indexes
            .write()
            .await
            .remove(&(tenant.to_string(), coll.to_string()));
    }

    /// Snapshot of all cached (tenant, coll) keys (for the compaction sweep).
    pub(crate) async fn cached_keys(&self) -> Vec<(String, String)> {
        self.indexes.read().await.keys().cloned().collect()
    }
}

// (handlers, registry, write-path maintenance, compaction sweep follow in later tasks)

/// Build an ES-shaped HTTP error response from a `bluedb_search::SearchError`.
pub(crate) fn search_err(e: bluedb_search::SearchError) -> AppError {
    use bluedb_search::SearchError::*;
    let status = match e {
        UnmappedField(_) | UnsupportedQuery(_) | UnsupportedAnalyzer(_)
        | UnsupportedFieldType(_) | BadRequest(_) | UnsortableField(_) => 400u16,
        Other(_) => 500,
    };
    AppError::new_with_status(status, e.to_string())
}
```

NOTE: this references `AppState::db_read()` and `AppError::new_with_status()`. If `AppState` has no `db_read()`, add a small accessor in `lib.rs`:
```rust
impl AppState {
    pub(crate) async fn db_read(&self) -> tokio::sync::RwLockReadGuard<'_, Option<Database>> {
        self.inner.db.read().await
    }
}
```
And if `AppError` has no `new_with_status`, use the existing constructors (`AppError::bad_request(msg)` for 400, `AppError::internal(msg)` for 500) in `search_err` instead — match what `lib.rs` actually exposes (Task explorer notes: `bad_request`, `internal`, `not_found`, `conflict`, `service_unavailable`, `not_implemented` exist).

Update `search_err` to:
```rust
pub(crate) fn search_err(e: bluedb_search::SearchError) -> AppError {
    use bluedb_search::SearchError::*;
    match e {
        Other(_) => AppError::internal(e.to_string()),
        _ => AppError::bad_request(e.to_string()),
    }
}
```

- [ ] **Step 2: Wire `AppState.inner.search` + promote/demote in `lib.rs`**

In the `inner` struct (near `fts: RwLock<Arc<FtsEngine>>`), add:
```rust
    search: tokio::sync::RwLock<Arc<search::SearchEngine>>,
```
Initialize it wherever `inner` is constructed (search for `fts: RwLock::new(FtsEngine::new())` — add alongside):
```rust
    search: tokio::sync::RwLock::new(search::SearchEngine::empty()),
```
Add `mod search;` near the other `mod` declarations.

In `promote()` (right after the `*self.inner.fts.write().await = fts;` line ~528):
```rust
    *self.inner.search.write().await = search::SearchEngine::new_durable(database.substrate());
```
In `demote()` (near `*self.inner.fts.write().await = FtsEngine::new();` ~613):
```rust
    *self.inner.search.write().await = search::SearchEngine::empty();
```
Add an accessor next to `fts()`:
```rust
    pub(crate) async fn search(&self) -> Arc<search::SearchEngine> {
        self.inner.search.read().await.clone()
    }
```

- [ ] **Step 3: Build**

Run: `cargo build -p bluedb-server 2>&1`
Expected: compiles (unused-warning on some helpers is fine for now).

- [ ] **Step 4: Commit**
```bash
git add crates/bluedb-server/src/search.rs crates/bluedb-server/src/lib.rs
git commit -F - <<'EOF'
feat(search): SearchEngine scaffold + AppState/promote-demote wiring

Per-(tenant,coll) FtsIndex cache swapped on promote/demote (writer-only writes),
index_id tenant-prefixing, a read-path blob accessor, and ES-error mapping.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 10: `bluedb-server` — search-mapping registry

**Files:**
- Modify: `crates/bluedb-server/src/search.rs` (registry fns)

Mirror the TTL registry pattern (`collections.rs`): per-tenant `__bluedb_search_config(collection TEXT PRIMARY KEY, mapping TEXT)` + global `__bluedb_search_tenants(tenant TEXT PRIMARY KEY)` in `DEFAULT_TENANT`. Reuse `collections::run_ddl`, `collections::run_write`, and `collections::run_read_routed_for_mutation` (make them `pub(crate)` if not already — check and adjust their visibility).

- [ ] **Step 1: Ensure shared helpers are reachable**

Run:
```bash
grep -n "async fn run_ddl\|async fn run_write\|async fn run_read_routed_for_mutation\|const DEFAULT_TENANT\|fn json_value_to_param" crates/bluedb-server/src/collections.rs crates/bluedb-server/src/lib.rs 2>&1
```
If `run_ddl`/`run_write`/`run_read_routed_for_mutation` are private (`async fn`), change to `pub(crate) async fn`. Confirm the `DEFAULT_TENANT` constant name/path (used by the TTL tenants registry).

- [ ] **Step 2: Write registry fns (append to `search.rs`)**

```rust
use bluedb_rest::Param;

const SEARCH_CONFIG_TABLE: &str = "__bluedb_search_config";
const SEARCH_TENANTS_TABLE: &str = "__bluedb_search_tenants";

async fn ensure_search_config_registry(state: &AppState, tenant: &str) -> Result<(), AppError> {
    crate::collections::run_ddl(
        state,
        tenant,
        &format!("CREATE TABLE IF NOT EXISTS {SEARCH_CONFIG_TABLE} (collection TEXT PRIMARY KEY, mapping TEXT);"),
    )
    .await
}

async fn ensure_search_tenants_registry(state: &AppState) -> Result<(), AppError> {
    crate::collections::run_ddl(
        state,
        crate::collections::DEFAULT_TENANT,
        &format!("CREATE TABLE IF NOT EXISTS {SEARCH_TENANTS_TABLE} (tenant TEXT PRIMARY KEY);"),
    )
    .await
}

/// Persist (replace) the mapping JSON for a collection.
pub(crate) async fn upsert_mapping(
    state: &AppState,
    tenant: &str,
    coll: &str,
    mapping: &MappingSpec,
) -> Result<(), AppError> {
    ensure_search_config_registry(state, tenant).await?;
    let json = serde_json::to_string(mapping)
        .map_err(|e| AppError::internal(format!("serialize mapping: {e}")))?;
    // DELETE-then-INSERT (GlueSQL has no UPSERT), mirroring upsert_ttl_config.
    crate::collections::run_write(
        state,
        tenant,
        &format!("DELETE FROM {SEARCH_CONFIG_TABLE} WHERE collection = $1;"),
        &[Param::Str(coll.to_string())],
    )
    .await?;
    crate::collections::run_write(
        state,
        tenant,
        &format!("INSERT INTO {SEARCH_CONFIG_TABLE} (collection, mapping) VALUES ($1, $2);"),
        &[Param::Str(coll.to_string()), Param::Str(json)],
    )
    .await?;
    register_search_tenant(state, tenant).await
}

async fn register_search_tenant(state: &AppState, tenant: &str) -> Result<(), AppError> {
    ensure_search_tenants_registry(state).await?;
    // Best-effort insert; ignore duplicate-key.
    let _ = crate::collections::run_write(
        state,
        crate::collections::DEFAULT_TENANT,
        &format!("INSERT INTO {SEARCH_TENANTS_TABLE} (tenant) VALUES ($1);"),
        &[Param::Str(tenant.to_string())],
    )
    .await;
    Ok(())
}

/// Load a collection's mapping spec, if any.
pub(crate) async fn get_mapping(
    state: &AppState,
    tenant: &str,
    coll: &str,
) -> Result<Option<MappingSpec>, AppError> {
    // Registry may not exist yet — treat "table not found" as "no mapping".
    let rows = match crate::collections::run_read_routed_for_mutation(
        state,
        tenant,
        &format!("SELECT mapping FROM {SEARCH_CONFIG_TABLE} WHERE collection = $1;"),
        &[Value::String(coll.to_string())],
    )
    .await
    {
        Ok(rows) => rows,
        Err(_) => return Ok(None),
    };
    let Some(row) = rows.into_iter().next() else { return Ok(None) };
    let json = row.get("mapping").and_then(Value::as_str).unwrap_or("");
    if json.is_empty() {
        return Ok(None);
    }
    let spec: MappingSpec = serde_json::from_str(json)
        .map_err(|e| AppError::internal(format!("parse stored mapping: {e}")))?;
    Ok(Some(spec))
}

/// All collections (per tenant) that have a search mapping (for the sweep).
pub(crate) async fn list_mapped_collections(
    state: &AppState,
    tenant: &str,
) -> Result<Vec<String>, AppError> {
    let rows = match crate::collections::run_read_routed_for_mutation(
        state,
        tenant,
        &format!("SELECT collection FROM {SEARCH_CONFIG_TABLE};"),
        &[],
    )
    .await
    {
        Ok(rows) => rows,
        Err(_) => return Ok(vec![]),
    };
    Ok(rows
        .into_iter()
        .filter_map(|r| r.get("collection").and_then(Value::as_str).map(str::to_string))
        .collect())
}

/// All tenants that have at least one search mapping.
pub(crate) async fn list_search_tenants(state: &AppState) -> Result<Vec<String>, AppError> {
    let rows = match crate::collections::run_read_routed_for_mutation(
        state,
        crate::collections::DEFAULT_TENANT,
        &format!("SELECT tenant FROM {SEARCH_TENANTS_TABLE};"),
        &[],
    )
    .await
    {
        Ok(rows) => rows,
        Err(_) => return Ok(vec![]),
    };
    Ok(rows
        .into_iter()
        .filter_map(|r| r.get("tenant").and_then(Value::as_str).map(str::to_string))
        .collect())
}
```

NOTE: confirm `run_read_routed_for_mutation` takes `&[serde_json::Value]` params (explorer: yes) and returns `Vec<serde_json::Value>` row objects (it does). Confirm `DEFAULT_TENANT` is accessible as `crate::collections::DEFAULT_TENANT` — if it lives in `lib.rs`, use `crate::DEFAULT_TENANT`. Adjust the path to wherever it is defined.

- [ ] **Step 3: Build**

Run: `cargo build -p bluedb-server 2>&1` → PASS.

- [ ] **Step 4: Commit**
```bash
git add crates/bluedb-server/src/search.rs crates/bluedb-server/src/collections.rs
git commit -F - <<'EOF'
feat(search): per-tenant search-mapping registry

__bluedb_search_config (per tenant) + __bluedb_search_tenants (global) with
upsert/get/list helpers, mirroring the TTL registry pattern.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 11: `bluedb-server` — `searchIndex` handlers + routes

**Files:**
- Modify: `crates/bluedb-server/src/search.rs` (handlers)
- Modify: `crates/bluedb-server/src/lib.rs` (routes)

- [ ] **Step 1: Write the `searchIndex` handlers (append to `search.rs`)**

```rust
use axum::http::HeaderMap;

use crate::authz::Scope;

/// Build a tantivy doc from a collection document for a compiled mapping.
/// Returns `None` if the doc has no `_id` (should not happen post-insert).
pub(crate) fn doc_to_tantivy(
    ss: &SearchSchema,
    spec: &MappingSpec,
    doc: &Value,
) -> Option<tantivy::TantivyDocument> {
    use bluedb_search::mapping::{FieldKindInfo, ID_FIELD};
    let id = doc.get(ID_FIELD).and_then(Value::as_str)?;
    let mut td = tantivy::TantivyDocument::default();
    let id_resolved = ss.field(ID_FIELD)?;
    td.add_text(id_resolved.field, id);
    for (name, _fspec) in &spec.fields {
        let Some(resolved) = ss.field(name) else { continue };
        let Some(v) = doc.get(name) else { continue };
        match resolved.kind {
            FieldKindInfo::Text(_) | FieldKindInfo::Keyword => {
                if let Some(s) = v.as_str() {
                    td.add_text(resolved.field, s);
                } else if !v.is_null() {
                    td.add_text(resolved.field, &v.to_string());
                }
            }
            FieldKindInfo::Integer => {
                if let Some(n) = v.as_i64() {
                    td.add_i64(resolved.field, n);
                }
            }
        }
    }
    Some(td)
}

/// `POST /collections/{c}/searchIndex` — declare/replace a search mapping and backfill.
pub(crate) async fn create_search_index(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(coll): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::SchemaAdmin)?;
    let tenant = state.tenant(&headers)?;
    state.require_active()?;

    let spec: MappingSpec =
        serde_json::from_value(body).map_err(|e| AppError::bad_request(format!("bad mapping: {e}")))?;
    let ss = bluedb_search::mapping::compile(&spec).map_err(search_err)?;

    // Persist mapping, drop any cached handle, (re)create + backfill the index.
    upsert_mapping(&state, &tenant, &coll, &spec).await?;
    let engine = state.search().await;
    engine.forget(&tenant, &coll).await;
    let idx = engine.index_for(&tenant, &coll, &ss).await?;

    // Backfill: read every existing document and (re)index it. Replace any prior
    // splits by deleting all current ids first is unnecessary on a fresh mapping;
    // for a re-declare we append the current docs (newest generation wins).
    let rows = crate::collections::run_read_routed_for_mutation(
        &state,
        &tenant,
        &format!("SELECT doc FROM \"{coll}\";"),
        &[],
    )
    .await
    .unwrap_or_default();
    let mut tdocs = Vec::with_capacity(rows.len());
    for row in &rows {
        if let Some(doc_str) = row.get("doc").and_then(Value::as_str) {
            if let Ok(doc) = serde_json::from_str::<Value>(doc_str) {
                if let Some(td) = doc_to_tantivy(&ss, &spec, &doc) {
                    tdocs.push(td);
                }
            }
        }
    }
    let backfilled = tdocs.len();
    if !tdocs.is_empty() {
        idx.append(tdocs)
            .await
            .map_err(|e| AppError::internal(format!("backfill index: {e}")))?;
    }

    Ok(Json(serde_json::json!({"acknowledged": true, "backfilled": backfilled})))
}

/// `GET /collections/{c}/searchIndex` — describe the mapping.
pub(crate) async fn get_search_index(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(coll): Path<String>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    match get_mapping(&state, &tenant, &coll).await? {
        Some(spec) => Ok(Json(serde_json::json!({coll: {"mappings": spec}}))),
        None => Err(AppError::not_found(format!("no search mapping for collection [{coll}]"))),
    }
}
```

NOTE: confirm `crate::authz::Scope` path + the variant names (`SchemaAdmin`, `DataRead`) — explorer confirmed these. Confirm `state.authorize`, `state.tenant`, `state.require_active` signatures (explorer confirmed). The doc-reinflation `row.get("doc").as_str()` matches how `collections::find` reads `doc` (explorer §2).

- [ ] **Step 2: Register routes in `lib.rs`**

In `build_app()`, in the collections routes block (~`lib.rs:920`), add:
```rust
        .route("/collections/{coll}/searchIndex", axum::routing::post(search::create_search_index).get(search::get_search_index))
```
(Place near the other `/collections/{coll}/...` routes. The `search` route is added in Task 13.)

- [ ] **Step 3: Build**

Run: `cargo build -p bluedb-server 2>&1` → PASS.

- [ ] **Step 4: Commit**
```bash
git add crates/bluedb-server/src/search.rs crates/bluedb-server/src/lib.rs
git commit -F - <<'EOF'
feat(search): searchIndex declare/describe handlers + route

POST /collections/{c}/searchIndex compiles + persists the mapping and backfills
existing docs into a tantivy index; GET describes it. Writer-gated, schema:admin.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 12: `bluedb-server` — write-path maintenance hooks

**Files:**
- Modify: `crates/bluedb-server/src/search.rs` (maintenance helpers)
- Modify: `crates/bluedb-server/src/collections.rs` (call from insert/update/delete)

- [ ] **Step 1: Write the maintenance helpers (append to `search.rs`)**

```rust
/// After an insert/update batch: (re)index the given documents if the collection
/// has a search mapping. `docs` carry their `_id`. No-op if no mapping / not writer.
pub(crate) async fn maintain_on_upsert(
    state: &AppState,
    tenant: &str,
    coll: &str,
    docs: &[Value],
) -> Result<(), AppError> {
    if docs.is_empty() {
        return Ok(());
    }
    let Some(spec) = get_mapping(state, tenant, coll).await? else { return Ok(()) };
    let ss = bluedb_search::mapping::compile(&spec).map_err(search_err)?;
    let engine = state.search().await;
    let idx = engine.index_for(tenant, coll, &ss).await?;

    use bluedb_search::mapping::ID_FIELD;
    let mut ids = Vec::with_capacity(docs.len());
    let mut tdocs = Vec::with_capacity(docs.len());
    for doc in docs {
        if let Some(td) = doc_to_tantivy(&ss, &spec, doc) {
            if let Some(id) = doc.get(ID_FIELD).and_then(Value::as_str) {
                ids.push(id.to_string());
            }
            tdocs.push(td);
        }
    }
    if tdocs.is_empty() {
        return Ok(());
    }
    // `update` tombstones old ids at the current generation and appends new docs
    // at generation+1 — correct for both first-insert (no prior id) and re-upsert.
    idx.update(ids, tdocs)
        .await
        .map_err(|e| AppError::internal(format!("index upsert: {e}")))?;
    Ok(())
}

/// After a delete: drop the given ids from the search index if a mapping exists.
pub(crate) async fn maintain_on_delete(
    state: &AppState,
    tenant: &str,
    coll: &str,
    ids: &[String],
) -> Result<(), AppError> {
    if ids.is_empty() {
        return Ok(());
    }
    let Some(spec) = get_mapping(state, tenant, coll).await? else { return Ok(()) };
    let ss = bluedb_search::mapping::compile(&spec).map_err(search_err)?;
    let engine = state.search().await;
    let idx = engine.index_for(tenant, coll, &ss).await?;
    idx.delete(ids.iter().cloned())
        .await
        .map_err(|e| AppError::internal(format!("index delete: {e}")))?;
    Ok(())
}
```

- [ ] **Step 2: Call from `collections::insert`**

In `collections.rs` `insert` (~line 675–722): the handler builds each doc (ensuring `_id`) and calls `write_full_doc` in a loop. Collect the finalized doc `Value`s into a `Vec<Value>` (the ones with `_id` set) and, after the write loop succeeds, call:
```rust
    crate::search::maintain_on_upsert(&state, &tenant, &coll, &inserted_docs).await?;
```
(Where `inserted_docs: Vec<serde_json::Value>` accumulates each `doc` after `ensure_id`. If the handler already has the docs in a `Vec`, reuse it; otherwise push `doc.clone()` inside the loop.)

- [ ] **Step 3: Call from `collections::update`**

In `update` (~1165–1241): after a matched doc is rewritten via `rewrite_doc_row` (and after the upsert `write_full_doc` branch), accumulate the new doc `Value`(s) into a `Vec<Value>` and after the loop call:
```rust
    crate::search::maintain_on_upsert(&state, &tenant, &coll, &updated_docs).await?;
```

- [ ] **Step 4: Call from `collections::delete`**

In `delete` (~1250–1307): the handler already computes the matched `_id`s (it calls `delete_multikey_for_doc` per id ~1294 then a `DELETE`). Collect those ids into a `Vec<String>` and after the delete completes call:
```rust
    crate::search::maintain_on_delete(&state, &tenant, &coll, &deleted_ids).await?;
```

- [ ] **Step 5: Build**

Run: `cargo build -p bluedb-server 2>&1` → PASS. (Read the exact insert/update/delete bodies first; thread the doc/id vectors through the existing control flow without changing the SQL path.)

- [ ] **Step 6: Commit**
```bash
git add crates/bluedb-server/src/search.rs crates/bluedb-server/src/collections.rs
git commit -F - <<'EOF'
feat(search): maintain the tantivy index on insert/update/delete

Collections write path now (re)indexes mapped fields (FtsIndex::update upserts
by _id; delete tombstones) when a collection has a search mapping. Batched per
request; writer-only; no-op without a mapping.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 13: `bluedb-server` — `search` query handler + route

**Files:**
- Modify: `crates/bluedb-server/src/search.rs` (search handler)
- Modify: `crates/bluedb-server/src/lib.rs` (route)

- [ ] **Step 1: Write the search handler (append to `search.rs`)**

```rust
use std::time::Instant;

/// `POST /collections/{c}/search` — ES-shaped search.
pub(crate) async fn search(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(coll): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let started = Instant::now();

    let req: bluedb_search::model::SearchRequest =
        serde_json::from_value(body).map_err(|e| AppError::bad_request(format!("bad search body: {e}")))?;

    // Mapping is required to search.
    let spec = get_mapping(&state, &tenant, &coll)
        .await?
        .ok_or_else(|| AppError::not_found(format!("no search mapping for collection [{coll}]")))?;
    let ss = bluedb_search::mapping::compile(&spec).map_err(search_err)?;

    // Lower the ES query to a tantivy Query.
    let compiled = bluedb_search::query::compile_query(&ss, &req.query).map_err(search_err)?;

    // Build a read-only index over the current substrate (works on writer + reader).
    let blob = search_blob(&state).await?;
    let idx = build_fts_index(blob, &tenant, &coll, &ss);

    let page = req.from.saturating_add(req.size);

    // Sort: _score (default) or a single integer field; else error.
    let ranked: Vec<(String, f32)> = if let Some(sc) = req.sort.first() {
        if sc.field == "_score" {
            idx.search_query_ids(compiled.query.as_ref(), page)
                .await
                .map_err(|e| AppError::internal(format!("search: {e}")))?
        } else {
            use bluedb_search::mapping::FieldKindInfo;
            match ss.field(&sc.field).map(|r| r.kind) {
                Some(FieldKindInfo::Integer) => idx
                    .search_query_sorted_ids(compiled.query.as_ref(), &sc.field, sc.descending, page)
                    .await
                    .map_err(|e| AppError::internal(format!("search: {e}")))?,
                _ => {
                    return Err(search_err(bluedb_search::SearchError::UnsortableField(
                        sc.field.clone(),
                    )))
                }
            }
        }
    } else {
        idx.search_query_ids(compiled.query.as_ref(), page)
            .await
            .map_err(|e| AppError::internal(format!("search: {e}")))?
    };

    let total = idx
        .count_query(compiled.query.as_ref())
        .await
        .map_err(|e| AppError::internal(format!("count: {e}")))?;

    // Page slice [from, from+size).
    let page_slice: Vec<(String, f32)> = ranked
        .into_iter()
        .skip(req.from)
        .take(req.size)
        .collect();

    // Fetch _source docs by _id (fast path).
    let sources = if matches!(req.source, bluedb_search::model::SourceSpec::Bool(false)) {
        std::collections::HashMap::new()
    } else {
        fetch_sources(&state, &tenant, &coll, &page_slice).await?
    };

    let block = bluedb_search::hits::assemble(
        &coll,
        &page_slice,
        total,
        sources,
        &req.source,
        &req.highlight_fields(),
        &compiled.terms_by_field,
    );

    let resp = bluedb_search::model::SearchResponse {
        took: started.elapsed().as_millis() as u64,
        timed_out: false,
        hits: block,
    };
    Ok(Json(serde_json::to_value(resp).map_err(|e| AppError::internal(e.to_string()))?))
}

/// Fetch `_source` JSON for the ranked ids, returning id -> doc.
async fn fetch_sources(
    state: &AppState,
    tenant: &str,
    coll: &str,
    ranked: &[(String, f32)],
) -> Result<std::collections::HashMap<String, Value>, AppError> {
    if ranked.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    // Parameterized IN-list: SELECT _id, doc FROM "coll" WHERE _id IN ($1,...).
    let placeholders: Vec<String> = (1..=ranked.len()).map(|i| format!("${i}")).collect();
    let sql = format!(
        "SELECT _id, doc FROM \"{coll}\" WHERE _id IN ({});",
        placeholders.join(", ")
    );
    let params: Vec<Value> = ranked.iter().map(|(id, _)| Value::String(id.clone())).collect();
    let rows = crate::collections::run_read_routed_for_mutation(state, tenant, &sql, &params)
        .await
        .unwrap_or_default();
    let mut map = std::collections::HashMap::with_capacity(rows.len());
    for row in rows {
        let id = row.get("_id").and_then(Value::as_str).map(str::to_string);
        let doc = row
            .get("doc")
            .and_then(Value::as_str)
            .and_then(|s| serde_json::from_str::<Value>(s).ok());
        if let (Some(id), Some(doc)) = (id, doc) {
            map.insert(id, doc);
        }
    }
    Ok(map)
}
```

NOTE: verify `run_read_routed_for_mutation` returns rows whose `doc`/`_id` are accessible as in `collections::find`. If the fast path returns `doc` as an already-parsed JSON object (not a string), adapt `fetch_sources` to use the object directly (check how `find` reads it — explorer §2 said it's a JSON string re-inflated via `serde_json::from_str`).

- [ ] **Step 2: Register the route in `lib.rs`**

In `build_app()` collections block:
```rust
        .route("/collections/{coll}/search", axum::routing::post(search::search))
```

- [ ] **Step 3: Build**

Run: `cargo build -p bluedb-server 2>&1` → PASS.

- [ ] **Step 4: Commit**
```bash
git add crates/bluedb-server/src/search.rs crates/bluedb-server/src/lib.rs
git commit -F - <<'EOF'
feat(search): POST /collections/{c}/search handler + route

Lowers the ES query, runs it on a read-only FtsIndex over the current substrate,
paginates (from/size), counts totals, fetches _source by _id, and returns the ES
hits envelope (took/total/max_score/highlight). _score + integer-field sort.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 14: `bluedb-server` — compaction sweep on promote

**Files:**
- Modify: `crates/bluedb-server/src/search.rs` (sweep fn)
- Modify: `crates/bluedb-server/src/lib.rs` (spawn on promote / abort on demote)

- [ ] **Step 1: Write the sweep (append to `search.rs`)**

```rust
/// Periodic compaction across all mapped collections for all search tenants.
/// Writer-only (guarded). Mirrors the TTL sweep fan-out.
pub(crate) async fn sweep_all_tenants_compaction(state: &AppState) -> Result<(), AppError> {
    if !state.is_writer() {
        return Ok(());
    }
    let engine = state.search().await;
    if engine.writer_blob().is_none() {
        return Ok(());
    }
    for tenant in list_search_tenants(state).await? {
        for coll in list_mapped_collections(state, &tenant).await? {
            let Some(spec) = get_mapping(state, &tenant, &coll).await? else { continue };
            let ss = match bluedb_search::mapping::compile(&spec) {
                Ok(ss) => ss,
                Err(_) => continue,
            };
            if let Ok(idx) = engine.index_for(&tenant, &coll, &ss).await {
                if let Err(e) = idx.maybe_compact().await {
                    eprintln!("bluedb-server: search compaction error ({tenant}/{coll}): {e:?}");
                }
            }
        }
    }
    Ok(())
}
```

- [ ] **Step 2: Spawn on promote / abort on demote**

Mirror the TTL sweep wiring (explorer §8, `promote` ~555–565, `demote` ~621–622). Add an `Option<JoinHandle<()>>` field to `inner` (e.g. `search_compaction_handle: std::sync::Mutex<Option<tokio::task::JoinHandle<()>>>`), initialize to `None`. In `promote()`, after wiring `search`:
```rust
    let sweep_state = self.clone();
    let interval = std::time::Duration::from_secs(
        std::env::var("BLUEDB_SEARCH_COMPACTION_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(120),
    );
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(e) = search::sweep_all_tenants_compaction(&sweep_state).await {
                eprintln!("bluedb-server: search compaction sweep error: {e:?}");
            }
        }
    });
    *self.inner.search_compaction_handle.lock().unwrap() = Some(handle);
```
In `demote()`:
```rust
    if let Some(h) = self.inner.search_compaction_handle.lock().unwrap().take() {
        h.abort();
    }
```

- [ ] **Step 3: Build**

Run: `cargo build -p bluedb-server 2>&1` → PASS.

- [ ] **Step 4: Commit**
```bash
git add crates/bluedb-server/src/search.rs crates/bluedb-server/src/lib.rs
git commit -F - <<'EOF'
feat(search): background compaction sweep (writer-only)

Periodic FtsIndex::maybe_compact across all mapped collections per search tenant,
spawned on promote / aborted on demote, mirroring the TTL sweep.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 15: Integration tests (embedded server)

**Files:**
- Create: `crates/bluedb-server/tests/search.rs`

Follow the existing embedded-server test pattern (`crates/bluedb-server/tests/api.rs`: `build_app` + `AppState` + `tower::ServiceExt::oneshot`). Promote the node so it's the active writer before issuing writes.

- [ ] **Step 1: Write the integration test skeleton + first test**

`crates/bluedb-server/tests/search.rs`:
```rust
// Mirror tests/api.rs for node()/make_app()/call() helpers and promotion.
mod common;

use serde_json::{json, Value};

// Helper assumed from tests/api.rs style: post(app, path, tenant, body) -> (StatusCode, Value).

#[tokio::test]
async fn declare_index_then_search_match() {
    let app = common::active_writer_app().await; // promote to writer

    // declare mapping
    let (st, _b) = common::post(&app, "/collections/articles/searchIndex", json!({
        "fields": {"title": {"analyzer": "english"}, "body": {"analyzer": "english"}, "tag": {"type": "keyword"}, "year": {"type": "integer"}}
    })).await;
    assert!(st.is_success(), "declare: {st}");

    // insert docs
    for (title, body, tag, year) in [
        ("Rust dogs", "fast safe dogs run", "pets", 2020),
        ("Python cats", "lazy cats nap", "pets", 2019),
        ("Go gophers", "gophers dig", "animals", 2021),
    ] {
        let (st, _b) = common::post(&app, "/collections/articles/insert", json!({
            "document": {"title": title, "body": body, "tag": tag, "year": year}
        })).await;
        assert!(st.is_success(), "insert: {st}");
    }

    // search match (read-your-writes — no seal needed)
    let (st, b) = common::post(&app, "/collections/articles/search", json!({
        "query": {"match": {"body": "dogs"}}
    })).await;
    assert!(st.is_success(), "search: {st} {b}");
    assert_eq!(b["hits"]["total"]["value"], 1);
    assert_eq!(b["hits"]["hits"][0]["_id"].as_str().unwrap().len(), 24);
    assert_eq!(b["hits"]["hits"][0]["_source"]["title"], "Rust dogs");
}
```
(Match the actual `/collections/{c}/insert` request shape used by `collections::insert` — read it; the spec/impl may accept `{"document": {...}}` or a bare object. Use whatever the existing collections insert tests use. If there is no `tests/common` module, inline the helpers from `tests/api.rs`.)

- [ ] **Step 2: Run it to confirm it fails, then passes**

Run: `cargo test -p bluedb-server --test search declare_index_then_search_match 2>&1`
Expected: compiles + PASS once helpers match the real API. Iterate on helper shapes until green.

- [ ] **Step 3: Add the remaining tests (one per behavior)**

Add these `#[tokio::test]`s (each: declare → insert → assert):
- `bool_must_should_must_not` — `{"bool":{"must":[{"match":{"body":"cats"}}],"must_not":[{"term":{"tag":"animals"}}]}}` → only the cats doc.
- `range_on_integer` — `{"range":{"year":{"gte":2020}}}` → 2 hits (2020, 2021).
- `match_phrase` — insert a doc with body "quick brown fox"; `{"match_phrase":{"body":"quick brown"}}` matches; `"brown quick"` does not.
- `term_keyword_exact` — `{"term":{"tag":"pets"}}` → 2 hits.
- `exists` — declare an optional field; doc missing it is excluded by `{"exists":{"field":"year"}}` when one doc omits `year`.
- `from_size_pagination` — insert 5 matching docs; `from:2,size:2` returns 2 hits and `total.value==5`.
- `source_false_and_list` — `_source:false` → no `_source`; `_source:["title"]` → only `title`.
- `highlight_wraps_terms` — `{"match":{"body":"dogs"}}` + `highlight:{fields:{body:{}}}` → `<em>dogs</em>` in `highlight.body[0]`.
- `update_reflected_in_search` — insert, search hit; update the doc to remove the term; search no longer hits; search the new term hits.
- `delete_reflected_in_search` — insert, delete, search → 0 hits.
- `numeric_sort` — `sort:[{"year":"asc"}]` orders ascending by year.
- `unmapped_field_errors` — `{"match":{"nope":"x"}}` → 400 with an ES-shaped error body.
- `tenant_isolation` — declare+insert under tenant `t1` (header `X-Bluedb-Tenant: t1`); search under `t2` returns a 404 (no mapping) and never sees t1's docs.
- `search_without_mapping_404` — search a collection with no mapping → 404.

For each, assert status + the specific `hits`/error JSON. Keep each test independent (fresh collection name or fresh app).

- [ ] **Step 4: Run the whole search suite**

Run: `cargo test -p bluedb-server --test search 2>&1`
Expected: ALL PASS.

- [ ] **Step 5: Commit**
```bash
git add crates/bluedb-server/tests/search.rs
git commit -F - <<'EOF'
test(search): embedded-server integration coverage

match/bool/range/match_phrase/term/exists, from/size, _source trimming,
highlight, read-your-writes, update/delete reflection, numeric sort,
unmapped-field error, tenant isolation, and missing-mapping 404.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 16: Docs — compatibility matrix + nav + discoverability

**Files:**
- Create: `docs/collections/search.md`
- Modify: `mkdocs.yml` (nav)
- Modify: `docs/api/rest.md`
- Modify: `docs/index.md`

- [ ] **Step 1: Write `docs/collections/search.md`**

Cover (lowercase "bluecopa" everywhere; this is unreleased so no migration/older-path framing):
- One-paragraph intro: ES-shaped search over `/collections` documents, BM25, API-shape compat (no ES wire protocol / Kibana).
- **Declaring a mapping:** `POST /collections/{c}/searchIndex` with the `{fields:{...}}` body; field types (`text`+analyzer `english`/`standard`/`whitespace`, `keyword`, `integer`); `GET` to describe. Note backfill on (re)declare.
- **Searching:** `POST /collections/{c}/search`; the request body (`query`,`from`,`size`,`sort`,`_source`,`highlight`); the ES hits envelope (`took`,`hits.total`,`max_score`,`hits[]._index/_id/_score/_source/highlight`).
- **Query DSL compat matrix** (table): supported — `match` (incl. `operator:and`), `match_phrase`, `term`, `range` (gt/gte/lt/lte, integer + keyword), `bool` (must/should/must_not/filter), `exists`, `match_all`. Not in v1 — fuzzy/wildcard/prefix, `multi_match`, `query_string`, aggregations/facets, nested/geo, `scroll`/PIT, suggesters, cross-collection.
- **Mapping/result feature matrix:** sort (`_score` + integer fields; text/keyword sort unsupported), `_source` (true/false/include-list), highlight (best-effort whole-token `<em>`, not fragment snippets), pagination (`from`/`size`).
- **Freshness:** read-your-writes on the active writer; reader nodes are eventually consistent. Writer-only `searchIndex` + indexing.
- **Auth:** `schema:admin` for `searchIndex` POST, `data:read` for search + describe; per-tenant via `X-Bluedb-Tenant`.

- [ ] **Step 2: Add to nav + cross-link**

In `mkdocs.yml`, add `search.md` under the existing Collections nav section (next to the collections README/compat page). In `docs/api/rest.md` add a `/collections/{c}/search` + `/searchIndex` entry to the endpoint list. In `docs/index.md` add a one-line bullet pointing at collections search (discoverability, mirroring how `/collections` was added).

- [ ] **Step 3: Build docs strictly**

Run: `mkdocs build --strict 2>&1` (from the repo root, in the docs venv used for the existing docs).
Expected: clean build, no warnings.

- [ ] **Step 4: Commit**
```bash
git add docs/collections/search.md mkdocs.yml docs/api/rest.md docs/index.md
git commit -F - <<'EOF'
docs(search): collections search compat matrix + nav + discoverability

Documents the ES-shaped /collections search + searchIndex surface, the supported
query/sort/source/highlight subset, freshness, and auth.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Task 17: Full workspace green + clippy

**Files:** none (verification + any fixups)

- [ ] **Step 1: Full test run**

Run: `cargo test --workspace 2>&1`
Expected: all suites PASS (bluedb-search unit, bluedb-fts incl. query_object, bluedb-engine incl. fts_query_object, bluedb-server incl. search integration, plus all pre-existing suites). Fix any regressions.

- [ ] **Step 2: Clippy on new/changed crates**

Run: `cargo clippy -p bluedb-search -p bluedb-fts -p bluedb-engine -p bluedb-server 2>&1`
Expected: no warnings on new code. Fix by hand (do NOT run `cargo fmt` — match surrounding style manually).

- [ ] **Step 3: Commit any fixups**
```bash
git add -A
git commit -F - <<'EOF'
chore(search): workspace green + clippy clean

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
EOF
```

---

## Self-Review (filled in by the plan author)

**Spec coverage:**
- §1 mapping model → Tasks 3, 10, 11. ✓
- §2 indexing pipeline + RYW → Task 12 (`FtsIndex::update` upsert), RYW inherent (append is durable; search loads fresh manifest). ✓
- §3 Query DSL → tantivy (match/match_phrase/term/range/bool/exists) → Tasks 4, 5; executed via Tasks 7, 8, 13. ✓
- §4 scoring + ES hits envelope (from/size/sort/_source/highlight) → Tasks 6, 13. Sort scoped to `_score`+integer (documented Task 16). ✓
- §5 API surface (searchIndex POST/GET, search POST; scopes) → Tasks 11, 13. ✓
- §6 components (`bluedb-search` model/query/hits + `mapping`) → Tasks 1–6; server wiring Tasks 9–14. ✓
- §7 testing + compat matrix → Tasks 15, 16. ✓

**Deviations from the spec (called out, with rationale):**
1. The spec says "no changes to the FTS engine itself." Tasks 7–8 make **additive** changes (new `Query`-object functions; existing string-based paths untouched) because the spec's own §3 requires real tantivy `Query` objects, which the existing string API cannot accept. No behavioral change to existing FTS.
2. `bluedb-search` stays pure; the orchestration (open splits / append / search) lives in `bluedb-engine::FtsIndex` (reused) + `bluedb-server`, rather than a separate `query`/`hits` executor — keeps `bluedb-search` unit-testable with zero I/O, as the spec intends.
3. Sort limited to `_score` + integer fields; highlight is best-effort term-wrapping over `_source` (v1-lite, as the spec states). Documented in Task 16.

**Placeholder scan:** no TBD/TODO; every code step shows complete code; commands have expected outcomes. A few steps flag "verify the exact existing signature/shape and adapt" (insert/update/delete bodies, `DEFAULT_TENANT` path, `tests/common` helpers, the in-memory `SlateDbBlobStore` test constructor) — these are real, unavoidable codebase-fit checks, not placeholders; each names exactly what to confirm and where.

**Type consistency:** `SearchSchema`/`ResolvedField`/`FieldKindInfo`/`CompiledQuery`/`SourceSpec`/`SortClause`/`MappingSpec`/`Hit`/`HitsBlock`/`SearchResponse` are defined once (Tasks 2–6) and referenced consistently. `index_id(tenant,coll)` and `build_fts_index` are the single construction points (Task 9) reused by writes (Task 12), reads (Task 13), and the sweep (Task 14). The new engine fns (`multi_split_search_query_filtered_ids` / `_count_` / `_sorted_ids`) match the `FtsIndex` methods that call them (`search_query_ids` / `count_query` / `search_query_sorted_ids`).
