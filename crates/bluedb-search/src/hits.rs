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

/// Wrap whole-token occurrences of any query `terms` in `<em>...</em>`,
/// case-insensitively. `terms` are the *analyzed* (stemmed, lowercased) query
/// terms — e.g. the english analyzer turns "storage" into "storag" — while the
/// source text is raw, so a token matches when an analyzed term is a **prefix**
/// of it (best-effort: ES re-analyzes per token for highlighting; we approximate
/// with the stem, which is a prefix of its inflected forms). Exact match is the
/// `term == word` special case of the prefix test.
fn highlight_text(text: &str, terms: &[String]) -> Option<String> {
    let stems: Vec<String> = terms
        .iter()
        .map(|t| t.to_lowercase())
        .filter(|t| !t.is_empty())
        .collect();
    if stems.is_empty() {
        return None;
    }
    let is_match = |word: &str| {
        let wl = word.to_lowercase();
        stems.iter().any(|t| wl.starts_with(t.as_str()))
    };
    let mut out = String::with_capacity(text.len() + 16);
    let mut hit = false;
    let mut word = String::new();
    let mut flush = |word: &mut String, out: &mut String| {
        if word.is_empty() {
            return;
        }
        if is_match(word) {
            out.push_str("<em>");
            out.push_str(word);
            out.push_str("</em>");
            hit = true;
        } else {
            out.push_str(word);
        }
        word.clear();
    };
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            word.push(ch);
        } else {
            flush(&mut word, &mut out);
            out.push(ch);
        }
    }
    flush(&mut word, &mut out);
    if hit {
        Some(out)
    } else {
        None
    }
}

/// Build the hits envelope. `ranked` is the page slice (already `[from, from+size)`),
/// in display order; `total` is the full match count and `total_relation` is the
/// ES total relation (`"eq"` for an exact count, `"gte"` when the count was
/// capped and is a lower bound).
#[allow(clippy::too_many_arguments)]
pub fn assemble(
    index: &str,
    ranked: &[(String, f32)],
    total: usize,
    total_relation: &'static str,
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
        total: HitsTotal { value: total, relation: total_relation },
        max_score,
        hits,
    }
}

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
            "pets", &ranked, 2, "eq", sources, &SourceSpec::Bool(true), &[], &HashMap::new(),
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
        let out = assemble("c", &ranked, 1, "eq", sources, &SourceSpec::Bool(false), &[], &HashMap::new());
        assert!(out.hits[0]._source.is_none());
    }

    #[test]
    fn source_field_list_trims() {
        let ranked = vec![("a".to_string(), 1.0f32)];
        let mut sources = HashMap::new();
        sources.insert("a".to_string(), src("a", "x", "y"));
        let out = assemble(
            "c", &ranked, 1, "eq", sources,
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
            "c", &ranked, 1, "eq", sources, &SourceSpec::Bool(true),
            &["body".to_string()], &terms,
        );
        let hl = out.hits[0].highlight.as_ref().unwrap();
        assert!(hl.get("body").unwrap()[0].contains("<em>dogs</em>"));
    }

    /// The english analyzer stems query terms ("storage" -> "storag"), but the
    /// source text is raw. A stem must still highlight its inflected word via a
    /// best-effort prefix match — otherwise highlight is silently empty even
    /// though the search matched.
    #[test]
    fn highlights_stemmed_terms_via_prefix() {
        let ranked = vec![("a".to_string(), 1.0f32)];
        let mut sources = HashMap::new();
        sources.insert(
            "a".to_string(),
            src("a", "x", "A database built directly on object storage"),
        );
        let mut terms = HashMap::new();
        terms.insert("body".to_string(), vec!["databas".to_string(), "storag".to_string()]);
        let out = assemble(
            "c", &ranked, 1, "eq", sources, &SourceSpec::Bool(true),
            &["body".to_string()], &terms,
        );
        let body_hl = &out.hits[0].highlight.as_ref().unwrap().get("body").unwrap()[0];
        assert!(body_hl.contains("<em>database</em>"), "got: {body_hl}");
        assert!(body_hl.contains("<em>storage</em>"), "got: {body_hl}");
    }
}
