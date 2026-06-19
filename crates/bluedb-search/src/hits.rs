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
