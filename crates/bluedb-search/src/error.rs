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

pub type Result<T> = std::result::Result<T, SearchError>;
