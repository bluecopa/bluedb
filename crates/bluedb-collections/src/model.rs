use crate::id::new_object_id;
use serde_json::Value;

/// Ensure `doc` has a string `_id`, generating one if absent. Returns the id.
/// A non-string existing `_id` is rendered to its text form (v1: text `_id`).
pub fn ensure_id(doc: &mut Value) -> String {
    let obj = doc.as_object_mut().expect("document must be a JSON object");
    let id = match obj.get("_id") {
        Some(Value::String(s)) => s.clone(),
        Some(v) => v.to_string().trim_matches('"').to_string(),
        None => new_object_id(),
    };
    obj.insert("_id".into(), Value::String(id.clone()));
    id
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn ensure_id_generates_when_absent_and_preserves_when_present() {
        let mut d = json!({"name": "ada"});
        let id = ensure_id(&mut d);
        assert_eq!(d["_id"], serde_json::Value::String(id.clone()));
        assert_eq!(id.len(), 24);

        let mut d2 = json!({"_id": "custom", "name": "lin"});
        assert_eq!(ensure_id(&mut d2), "custom");
    }
}
