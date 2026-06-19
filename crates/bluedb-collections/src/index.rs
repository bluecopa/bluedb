//! JSON-path index helpers — naming and value extraction for gateway-maintained
//! derived columns (`__cidx_<path>`) that back secondary indexes on collections.

use serde_json::Value;

/// Hidden index column name for a document path (dots → underscores).
///
/// E.g. `"status"` → `"__cidx_status"`, `"a.b"` → `"__cidx_a_b"`.
pub fn derived_col(path: &str) -> String {
    format!("__cidx_{}", path.replace('.', "_"))
}

/// Extract `path` from `doc` as text (the value stored in the derived column).
///
/// `path` is a dot-delimited key sequence into the JSON object. Returns `None`
/// if any segment is missing. String values are returned as-is; other scalar
/// types are serialized via `Display` (i.e. their JSON representation).
pub fn derive_value(doc: &Value, path: &str) -> Option<String> {
    let mut cur = doc;
    for part in path.split('.') {
        cur = cur.get(part)?;
    }
    Some(match cur {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn derived_col_simple() {
        assert_eq!(derived_col("status"), "__cidx_status");
    }

    #[test]
    fn derived_col_nested() {
        assert_eq!(derived_col("a.b"), "__cidx_a_b");
    }

    #[test]
    fn derive_value_top_level_string() {
        let doc = json!({"status": "active"});
        assert_eq!(derive_value(&doc, "status"), Some("active".into()));
    }

    #[test]
    fn derive_value_nested_path() {
        let doc = json!({"a": {"b": "nested"}});
        assert_eq!(derive_value(&doc, "a.b"), Some("nested".into()));
    }

    #[test]
    fn derive_value_missing_path_is_none() {
        let doc = json!({"a": 1});
        assert_eq!(derive_value(&doc, "missing"), None);
        assert_eq!(derive_value(&doc, "a.b"), None);
    }

    #[test]
    fn derive_value_numeric_serialized() {
        let doc = json!({"count": 42});
        assert_eq!(derive_value(&doc, "count"), Some("42".into()));
    }
}
