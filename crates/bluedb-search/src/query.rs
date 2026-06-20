//! ES Query-DSL JSON → a tantivy [`Query`]. Pure; needs only the compiled
//! [`SearchSchema`]. Also records the analyzed terms per field (for highlight).

use std::collections::HashMap;

use serde_json::Value;
use tantivy::query::{
    AllQuery, BooleanQuery, ExistsQuery, Occur, PhraseQuery, Query, RegexQuery, TermQuery,
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
fn single_field(m: &serde_json::Map<String, Value>) -> Result<(&String, &Value)> {
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
        .ok_or_else(|| SearchError::BadRequest("exists needs a `field`".to_string()))?;
    let resolved = resolve_field(ss, name)?;
    // tantivy's `ExistsQuery` only operates on *fast* fields (it reads the
    // fast-field column to test presence). Our `text`/`keyword` fields are
    // inverted-index only, so `ExistsQuery` throws
    // "Schema error: 'Field <f> is not a fast field.'" at search time (a 500).
    // For those, "exists" = "the doc has at least one indexed term in the
    // field", which a `.*` regex over the field's postings matches exactly —
    // the same semantics Elasticsearch gives `exists` on a text/keyword field.
    // Integer fields are fast, so they take the native `ExistsQuery` path.
    Ok(match resolved.kind {
        FieldKindInfo::Integer => Box::new(ExistsQuery::new(name.to_string(), false)),
        FieldKindInfo::Text(_) | FieldKindInfo::Keyword => {
            Box::new(RegexQuery::from_pattern(".*", resolved.field).map_err(anyhow::Error::from)?)
        }
    })
}

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
        let items: Vec<&Value> = match v {
            Value::Array(a) => a.iter().collect(),
            other => vec![other],
        };
        for item in items {
            let q = if matches!(occur, Occur::MustNot) {
                let mut scratch = HashMap::new();
                super::lower(ss, item, &mut scratch)?
            } else {
                super::lower(ss, item, terms)?
            };
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
    fn exists_compiles_for_every_field_kind() {
        // Regression for UAT-COLL-SEARCH-002: tantivy's `ExistsQuery` only
        // works on fast fields, so `exists` on a text/keyword field used to
        // compile fine but throw "Field X is not a fast field." at search time
        // (HTTP 500). It must compile for text, keyword, AND integer — the
        // text/keyword path lowers to a `RegexQuery` over the postings.
        let ss = schema();
        for field in ["title", "tag", "year"] {
            assert!(
                compile_query(&ss, &serde_json::json!({"exists": {"field": field}})).is_ok(),
                "exists on {field} should compile"
            );
        }
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
        assert!(c.terms_by_field.get("title").is_some());
    }

    #[test]
    fn range_on_text_field_errors() {
        let ss = schema();
        assert!(compile_query(&ss, &serde_json::json!({"range": {"title": {"gte": "a"}}})).is_err());
    }
}
