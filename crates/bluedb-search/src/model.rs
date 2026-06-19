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
