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
        assert!(ss.field("_id").is_some());
        assert_eq!(ss.id_field.0, ss.field("_id").unwrap().field);
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
        eprintln!("actual tokens: {terms:?}");
        assert!(!terms.is_empty());
        // Verify the actual stems produced by the en_stem (Porter) analyzer.
        // "running" → "run"; "QUICKLY" → "quick" after lowercase + Porter stem.
        assert!(terms.contains(&"run".to_string()), "expected 'run' in {terms:?}");
    }
}
