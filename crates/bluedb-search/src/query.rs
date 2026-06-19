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

impl std::fmt::Debug for CompiledQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledQuery")
            .field("terms_by_field", &self.terms_by_field)
            .finish_non_exhaustive()
    }
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
        "range" => range::lower_range(ss, obj(q, "range")?),
        "bool" => boolean::lower_bool(ss, obj(q, "bool")?, terms),
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
        return Ok(Box::new(BooleanQuery::new(vec![])));
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
