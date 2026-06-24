use serde_json::{Map, Value};

/// MongoDB-style projection. Inclusion (`{f:1}`) keeps listed fields (and `_id`
/// unless `{_id:0}`); exclusion (`{f:0}`) drops listed fields. Mixed/empty → doc
/// unchanged (v1). Dotted paths not supported in v1 (top-level fields only).
pub fn apply_projection(doc: &Value, projection: &Value) -> Value {
    let proj = match projection.as_object() {
        Some(p) if !p.is_empty() => p,
        _ => return doc.clone(),
    };
    let obj = match doc.as_object() {
        Some(o) => o,
        None => return doc.clone(),
    };
    let inclusion = proj.values().any(truthy);
    let mut out = Map::new();
    if inclusion {
        let keep_id = proj.get("_id").map(truthy).unwrap_or(true);
        if keep_id {
            if let Some(id) = obj.get("_id") {
                out.insert("_id".into(), id.clone());
            }
        }
        for (k, v) in proj {
            if k != "_id" && truthy(v) {
                if let Some(val) = obj.get(k) {
                    out.insert(k.clone(), val.clone());
                }
            }
        }
    } else {
        out = obj.clone();
        for k in proj.keys() {
            out.remove(k);
        }
    }
    Value::Object(out)
}

fn truthy(v: &Value) -> bool {
    matches!(v, Value::Bool(true)) || v.as_i64().is_some_and(|n| n != 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn inclusion_keeps_listed_fields_plus_id() {
        let doc = json!({"_id":"1","name":"ada","age":36,"city":"x"});
        let out = apply_projection(&doc, &json!({"name": 1}));
        assert_eq!(out, json!({"_id":"1","name":"ada"}));
    }
    #[test]
    fn exclusion_drops_listed_fields() {
        let doc = json!({"_id":"1","name":"ada","age":36});
        let out = apply_projection(&doc, &json!({"age": 0}));
        assert_eq!(out, json!({"_id":"1","name":"ada"}));
    }
    #[test]
    fn empty_projection_returns_doc_unchanged() {
        let doc = json!({"_id":"1","name":"ada"});
        assert_eq!(apply_projection(&doc, &json!({})), doc);
    }
}
