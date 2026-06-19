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
