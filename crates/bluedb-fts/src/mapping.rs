//! Field mapping — per-field analyzers, and the tokenizer registrations a
//! lazily-opened split needs.
//!
//! Callers shouldn't hand-build a [`tantivy::schema::Schema`] and remember to
//! register matching custom tokenizers on every reader. An [`IndexMapping`] is
//! the single source of truth: it describes each field's name, type, and
//! [`Analyzer`], and produces BOTH
//! - a [`Schema`] (with each text field's `tokenizer` wired to the right
//!   analyzer name), and
//! - the set of `(field_name, tokenizer_name)` registrations.
//!
//! ## Why registration is a separate, explicit step
//! tantivy stores a tokenizer's *name* in the schema, not the tokenizer itself.
//! The default manager only knows the built-ins (`default`, `raw`, `en_stem`,
//! `whitespace`). A split opened lazily ([`crate::open::open_split_lazy`]) gets a
//! fresh `Index` whose tokenizer manager is the default one, so any **custom**
//! analyzer (anything not a built-in) must be re-registered on that opened index
//! before a [`QueryParser`](tantivy::query::QueryParser) or reader can tokenize
//! against it. [`IndexMapping::register_tokenizers`] does exactly that for an
//! opened index. (The stock analyzers here happen to map onto tantivy built-in
//! names, so registration is a no-op for them — but the API is uniform, and the
//! moment a caller adds a truly custom analyzer it Just Works.)

use tantivy::schema::{
    Schema, TextFieldIndexing, TextOptions, FAST, INDEXED, STORED, STRING,
};
use tantivy::tokenizer::{
    Language, LowerCaser, SimpleTokenizer, Stemmer, TextAnalyzer, WhitespaceTokenizer,
};
use tantivy::Index;

/// How a text field is tokenized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Analyzer {
    /// Exact, untokenized keyword matching — tantivy's built-in `raw` tokenizer
    /// (schema `STRING`). The whole field value is one term; only an exact match
    /// hits. Use for ids, codes, enum-like values.
    Raw,
    /// tantivy's `default` tokenizer: split on non-alphanumeric, lowercase, and
    /// drop tokens longer than 40 bytes. The general-purpose text analyzer.
    Default,
    /// Lowercase + Porter-style English stemming (built-in name `en_stem`): a
    /// query for `running` matches `run`/`runs`. Best for prose/body text.
    EnStem,
    /// Split on whitespace only, lowercased (no stemming, no punctuation
    /// stripping). Built as a custom analyzer over [`WhitespaceTokenizer`].
    Whitespace,
}

impl Analyzer {
    /// The tokenizer name tantivy records in the schema / looks up in the
    /// tokenizer manager for this analyzer.
    pub fn tokenizer_name(self) -> &'static str {
        match self {
            Analyzer::Raw => "raw",
            Analyzer::Default => "default",
            Analyzer::EnStem => "en_stem",
            Analyzer::Whitespace => "whitespace",
        }
    }

    /// Build the [`TextAnalyzer`] this analyzer corresponds to.
    ///
    /// The built-in names (`raw`/`default`/`en_stem`/`whitespace`) are already
    /// present in tantivy's default tokenizer manager, so building them here is
    /// only needed if a caller registers onto a manager that lacks them; we still
    /// expose it for completeness and so a future truly-custom variant slots in.
    pub fn build_analyzer(self) -> TextAnalyzer {
        match self {
            Analyzer::Raw => {
                // `raw` = the whole input as a single token (no splitting).
                TextAnalyzer::builder(tantivy::tokenizer::RawTokenizer::default()).build()
            }
            Analyzer::Default => TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(tantivy::tokenizer::RemoveLongFilter::limit(40))
                .filter(LowerCaser)
                .build(),
            Analyzer::EnStem => TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(tantivy::tokenizer::RemoveLongFilter::limit(40))
                .filter(LowerCaser)
                .filter(Stemmer::new(Language::English))
                .build(),
            Analyzer::Whitespace => TextAnalyzer::builder(WhitespaceTokenizer::default())
                .filter(LowerCaser)
                .build(),
        }
    }
}

/// The type/storage of a single mapped field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldKind {
    /// A tokenized text field analyzed by the given [`Analyzer`]. `stored`
    /// controls whether the value is retrievable (and thus survives compaction;
    /// see [`crate::merge`]).
    Text { analyzer: Analyzer, stored: bool },
    /// An indexed `u64` field. `fast` adds a columnar (fast-field) store; always
    /// `INDEXED` so it can be queried; `stored` controls retrievability.
    U64 { stored: bool, fast: bool },
    /// An indexed `i64` field (e.g. an epoch-millis timestamp). Same options as
    /// [`FieldKind::U64`].
    I64 { stored: bool, fast: bool },
}

/// One field in an [`IndexMapping`].
#[derive(Debug, Clone)]
pub struct FieldMapping {
    /// Field name (becomes the schema field name and the JSON key).
    pub name: String,
    /// The field's type/storage.
    pub kind: FieldKind,
}

impl FieldMapping {
    /// A tokenized text field with the given analyzer, `STORED`.
    pub fn text(name: impl Into<String>, analyzer: Analyzer) -> Self {
        Self {
            name: name.into(),
            kind: FieldKind::Text {
                analyzer,
                stored: true,
            },
        }
    }

    /// An exact-match keyword field (analyzer [`Analyzer::Raw`]), `STORED`.
    /// Convenience for ids/codes.
    pub fn keyword(name: impl Into<String>) -> Self {
        Self::text(name, Analyzer::Raw)
    }

    /// A stored, fast (columnar) `u64` field.
    pub fn u64(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: FieldKind::U64 {
                stored: true,
                fast: true,
            },
        }
    }

    /// A stored, fast (columnar) `i64` field (e.g. an epoch-millis timestamp).
    pub fn i64(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: FieldKind::I64 {
                stored: true,
                fast: true,
            },
        }
    }

    /// Set whether this field's value is `STORED` (retrievable / survives
    /// compaction).
    pub fn stored(mut self, stored: bool) -> Self {
        match &mut self.kind {
            FieldKind::Text { stored: s, .. }
            | FieldKind::U64 { stored: s, .. }
            | FieldKind::I64 { stored: s, .. } => *s = stored,
        }
        self
    }
}

/// A builder over a list of [`FieldMapping`]s that produces a [`Schema`] plus the
/// custom-tokenizer registrations the opened index needs.
#[derive(Debug, Clone, Default)]
pub struct IndexMapping {
    fields: Vec<FieldMapping>,
}

impl IndexMapping {
    /// An empty mapping.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a field. Chainable.
    pub fn field(mut self, field: FieldMapping) -> Self {
        self.fields.push(field);
        self
    }

    /// Add a tokenized text field with `analyzer` (`STORED`). Chainable shortcut
    /// for `.field(FieldMapping::text(name, analyzer))`.
    pub fn text(self, name: impl Into<String>, analyzer: Analyzer) -> Self {
        self.field(FieldMapping::text(name, analyzer))
    }

    /// Add an exact-match keyword field (`STORED`). Chainable.
    pub fn keyword(self, name: impl Into<String>) -> Self {
        self.field(FieldMapping::keyword(name))
    }

    /// The mapped fields, in declaration order.
    pub fn fields(&self) -> &[FieldMapping] {
        &self.fields
    }

    /// Build the [`Schema`]: each text field's indexing options carry its
    /// analyzer's tokenizer name, so the [`QueryParser`](tantivy::query::QueryParser)
    /// and indexer tokenize consistently.
    pub fn build_schema(&self) -> Schema {
        let mut sb = Schema::builder();
        for f in &self.fields {
            match f.kind {
                FieldKind::Text { analyzer, stored } => {
                    if analyzer == Analyzer::Raw {
                        // Keyword/exact: `STRING` already pins the `raw` tokenizer.
                        let mut opts: TextOptions = STRING;
                        if stored {
                            opts = opts | STORED;
                        }
                        sb.add_text_field(&f.name, opts);
                    } else {
                        let indexing = TextFieldIndexing::default()
                            .set_tokenizer(analyzer.tokenizer_name())
                            .set_index_option(
                                tantivy::schema::IndexRecordOption::WithFreqsAndPositions,
                            );
                        let mut opts = TextOptions::default().set_indexing_options(indexing);
                        if stored {
                            opts = opts.set_stored();
                        }
                        sb.add_text_field(&f.name, opts);
                    }
                }
                FieldKind::U64 { stored, fast } => {
                    let mut opts: tantivy::schema::NumericOptions = INDEXED.into();
                    if stored {
                        opts = opts | STORED;
                    }
                    if fast {
                        opts = opts | FAST;
                    }
                    sb.add_u64_field(&f.name, opts);
                }
                FieldKind::I64 { stored, fast } => {
                    let mut opts: tantivy::schema::NumericOptions = INDEXED.into();
                    if stored {
                        opts = opts | STORED;
                    }
                    if fast {
                        opts = opts | FAST;
                    }
                    sb.add_i64_field(&f.name, opts);
                }
            }
        }
        sb.build()
    }

    /// The distinct `(field_name, tokenizer_name)` analyzer registrations this
    /// mapping requires. Used both by [`Self::register_tokenizers`] and exposed
    /// for callers/diagnostics that want to know the wiring.
    pub fn tokenizer_registrations(&self) -> Vec<(String, &'static str)> {
        self.fields
            .iter()
            .filter_map(|f| match f.kind {
                FieldKind::Text { analyzer, .. } => {
                    Some((f.name.clone(), analyzer.tokenizer_name()))
                }
                _ => None,
            })
            .collect()
    }

    /// Register every analyzer used by this mapping on `index`'s tokenizer
    /// manager.
    ///
    /// Call this on a freshly opened index (e.g. from
    /// [`crate::open::open_split_lazy`]) before building a
    /// [`QueryParser`](tantivy::query::QueryParser) or reader — otherwise a
    /// custom analyzer named in the schema would be missing from the opened
    /// index's (default) manager and tokenization would fail. Built-in analyzers
    /// are re-registered idempotently (same name → same behavior), so calling
    /// this is always safe.
    pub fn register_tokenizers(&self, index: &Index) {
        let mgr = index.tokenizers();
        for f in &self.fields {
            if let FieldKind::Text { analyzer, .. } = f.kind {
                mgr_register(mgr, analyzer);
            }
        }
    }
}

/// Register one analyzer under its canonical name on a tokenizer manager.
fn mgr_register(mgr: &tantivy::tokenizer::TokenizerManager, analyzer: Analyzer) {
    mgr.register(analyzer.tokenizer_name(), analyzer.build_analyzer());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_has_expected_fields_and_tokenizers() {
        let mapping = IndexMapping::new()
            .keyword("id")
            .text("body", Analyzer::EnStem)
            .field(FieldMapping::u64("amount"));
        let schema = mapping.build_schema();

        assert!(schema.get_field("id").is_ok());
        assert!(schema.get_field("body").is_ok());
        assert!(schema.get_field("amount").is_ok());

        let regs = mapping.tokenizer_registrations();
        assert_eq!(
            regs,
            vec![
                ("id".to_string(), "raw"),
                ("body".to_string(), "en_stem"),
            ],
            "only text fields contribute tokenizer registrations"
        );
    }

    #[test]
    fn analyzer_names_match_builtins() {
        assert_eq!(Analyzer::Raw.tokenizer_name(), "raw");
        assert_eq!(Analyzer::Default.tokenizer_name(), "default");
        assert_eq!(Analyzer::EnStem.tokenizer_name(), "en_stem");
        assert_eq!(Analyzer::Whitespace.tokenizer_name(), "whitespace");
        // Each builds without panicking.
        for a in [
            Analyzer::Raw,
            Analyzer::Default,
            Analyzer::EnStem,
            Analyzer::Whitespace,
        ] {
            let _ = a.build_analyzer();
        }
    }
}
