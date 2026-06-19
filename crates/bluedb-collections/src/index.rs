//! JSON-path index helpers — naming and value extraction for gateway-maintained
//! derived columns (`__cidx_<path>`) that back secondary indexes on collections.

use serde_json::Value;

/// Hidden index column name for a document path (dots → underscores).
///
/// E.g. `"status"` → `"__cidx_status"`, `"a.b"` → `"__cidx_a_b"`.
pub fn derived_col(path: &str) -> String {
    format!("__cidx_{}", path.replace('.', "_"))
}

/// Return `true` when `path` is safe to embed in DDL.
///
/// A document path is index-safe when every dot-separated segment is a plain
/// identifier (`[A-Za-z_][A-Za-z0-9_]*`). This guarantees `derived_col(path)`
/// and any derived index name contain only `[A-Za-z0-9_]`, so they are safe to
/// interpolate into DDL.
pub fn valid_path(path: &str) -> bool {
    !path.is_empty() && path.split('.').all(|seg| {
        let mut chars = seg.chars();
        matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    })
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

    // ---------------------------------------------------------------------------
    // valid_path
    // ---------------------------------------------------------------------------

    #[test]
    fn valid_path_simple() {
        assert!(valid_path("status"));
    }

    #[test]
    fn valid_path_nested() {
        assert!(valid_path("a.b.c"));
    }

    #[test]
    fn valid_path_leading_underscore() {
        assert!(valid_path("_x"));
    }

    #[test]
    fn valid_path_empty_is_rejected() {
        assert!(!valid_path(""));
    }

    #[test]
    fn valid_path_trailing_dot_empty_segment_is_rejected() {
        assert!(!valid_path("a.b."));
    }

    #[test]
    fn valid_path_injection_is_rejected() {
        assert!(!valid_path("x) TEXT; DROP TABLE people; --"));
    }

    #[test]
    fn valid_path_hyphen_is_rejected() {
        assert!(!valid_path("a-b"));
    }

    #[test]
    fn valid_path_space_is_rejected() {
        assert!(!valid_path("a b"));
    }

    #[test]
    fn valid_path_leading_digit_is_rejected() {
        assert!(!valid_path("1abc"));
    }

    #[test]
    fn valid_path_single_quote_is_rejected() {
        assert!(!valid_path("a'b"));
    }
}
