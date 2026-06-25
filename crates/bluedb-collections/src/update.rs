use crate::error::MqlError;
use serde_json::Value;

/// Apply a MongoDB update document to `doc` in place. If `update` has no
/// `$`-operators it is a full replacement (preserving `_id`).
pub fn apply_update(doc: &mut Value, update: &Value) -> Result<(), MqlError> {
    let upd = update
        .as_object()
        .ok_or_else(|| MqlError::Malformed("update must be an object".into()))?;
    let has_ops = upd.keys().any(|k| k.starts_with('$'));
    if !has_ops {
        let id = doc.get("_id").cloned();
        *doc = update.clone();
        if let Some(id) = id {
            doc.as_object_mut().unwrap().insert("_id".into(), id);
        }
        return Ok(());
    }
    let obj = doc
        .as_object_mut()
        .ok_or_else(|| MqlError::Malformed("doc must be object".into()))?;
    for (op, body) in upd {
        let fields = body
            .as_object()
            .ok_or_else(|| MqlError::Malformed(format!("{op} takes an object")))?;
        match op.as_str() {
            "$set" => {
                for (k, v) in fields {
                    obj.insert(k.clone(), v.clone());
                }
            }
            "$unset" => {
                for k in fields.keys() {
                    obj.remove(k);
                }
            }
            "$inc" => {
                for (k, v) in fields {
                    let cur = obj.get(k).and_then(|x| x.as_f64()).unwrap_or(0.0);
                    let add = v
                        .as_f64()
                        .ok_or_else(|| MqlError::Malformed("$inc needs a number".into()))?;
                    obj.insert(k.clone(), num(cur + add));
                }
            }
            "$push" => {
                for (k, v) in fields {
                    let arr = obj.entry(k.clone()).or_insert_with(|| Value::Array(vec![]));
                    arr.as_array_mut()
                        .ok_or_else(|| MqlError::Malformed("$push target not an array".into()))?
                        .push(v.clone());
                }
            }
            "$pull" => {
                for (k, v) in fields {
                    if let Some(Value::Array(a)) = obj.get_mut(k) {
                        a.retain(|e| e != v);
                    }
                }
            }
            other => return Err(MqlError::UnsupportedOperator(other.to_string())),
        }
    }
    Ok(())
}

fn num(f: f64) -> Value {
    if f.fract() == 0.0 {
        Value::from(f as i64)
    } else {
        Value::from(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn set_inc_unset_push() {
        let mut d = json!({"_id":"1","n":1,"tags":["a"]});
        apply_update(
            &mut d,
            &json!({"$set":{"name":"ada"},"$inc":{"n":2},
                                     "$unset":{"x":""},"$push":{"tags":"b"}}),
        )
        .unwrap();
        assert_eq!(d, json!({"_id":"1","n":3,"tags":["a","b"],"name":"ada"}));
    }
    #[test]
    fn replacement_document_without_operators_replaces_but_keeps_id() {
        let mut d = json!({"_id":"1","old":true});
        apply_update(&mut d, &json!({"name":"lin"})).unwrap();
        assert_eq!(d, json!({"_id":"1","name":"lin"}));
    }
    #[test]
    fn rejects_unknown_operator() {
        let mut d = json!({"_id":"1"});
        assert!(apply_update(&mut d, &json!({"$bit":{"n":1}})).is_err());
    }
}
