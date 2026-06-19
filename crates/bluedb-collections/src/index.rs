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

// ---------------------------------------------------------------------------
// Typed index helpers
// ---------------------------------------------------------------------------

/// The SQL column type to use for a gateway-maintained derived index column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexType {
    Number,
    Bool,
    Text,
}

/// True if `v` is a JSON number with no fractional part (an INT index can hold it):
/// integers (`i64`/`u64`), and whole-valued floats like `5.0`, within `i64` range.
pub fn is_whole_number(v: &serde_json::Value) -> bool {
    if v.is_i64() || v.is_u64() {
        return true;
    }
    matches!(v.as_f64(), Some(f) if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64)
}

/// Infer the index column type from a sample of the field's values across docs.
/// All-whole-number → `Number`, all-bool → `Bool`, otherwise (incl. empty,
/// mixed, or any truly-fractional value) → `Text`.
///
/// A field containing any fractional value (e.g. `3.14`) falls to `Text` because
/// the derived column is `INT` and cannot hold fractional values without data loss.
pub fn infer_index_type(values: &[serde_json::Value]) -> IndexType {
    if !values.is_empty() && values.iter().all(is_whole_number) {
        return IndexType::Number;
    }
    if !values.is_empty() && values.iter().all(|v| v.is_boolean()) {
        return IndexType::Bool;
    }
    IndexType::Text
}

/// SQL column type keyword for a typed derived index column.
///
/// `Number` maps to `INT` (GlueSQL `DataType::Int` → `Value::I64`):
/// - `FLOAT`/`F64` cannot be serialized to order-preserving big-endian bytes
///   by bluedb-sql's storage layer, causing an error on every row write.
/// - `DECIMAL` stores correctly but `evaluate_cmp(Decimal, I64)` returns `None`
///   in GlueSQL 0.19 (the match arm for mixed Decimal/integer is missing in
///   `Value::evaluate_cmp`), silently returning empty results for any query
///   whose parameter is bound as `Param::Int`.
/// - `INT` (`Value::I64`) stores without error, and `evaluate_cmp(I64, I64)` and
///   range comparisons both work correctly.
///
/// Fractional JSON numbers (e.g. `3.14`) are stored as NULL for an INT-typed
/// column — `derive_typed_value` returns the value only when the JSON type
/// matches the index type, and `json_to_param` maps floats to `Param::Float`
/// which is rejected by the INT column coercion (the row gets NULL instead).
pub fn index_sql_type(t: IndexType) -> &'static str {
    match t {
        IndexType::Number => "INT",
        IndexType::Bool => "BOOLEAN",
        IndexType::Text => "TEXT",
    }
}

/// Extract `path` from `doc` as a typed value for the derived column.
///
/// For `Number`/`Bool`, returns `None` unless the field is exactly that JSON
/// type (type-mismatched or absent fields store NULL). For `Text`, returns the
/// field's text form: a string unquoted; other scalars/objects as their JSON
/// text.
pub fn derive_typed_value(
    doc: &serde_json::Value,
    path: &str,
    t: IndexType,
) -> Option<serde_json::Value> {
    let mut cur = doc;
    for part in path.split('.') {
        cur = cur.get(part)?;
    }
    match t {
        IndexType::Number => is_whole_number(cur).then(|| cur.clone()),
        IndexType::Bool => cur.is_boolean().then(|| cur.clone()),
        IndexType::Text => Some(serde_json::Value::String(match cur {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        })),
    }
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

    // ---------------------------------------------------------------------------
    // infer_index_type
    // ---------------------------------------------------------------------------

    #[test]
    fn infer_index_type_all_numbers() {
        assert_eq!(infer_index_type(&[json!(1), json!(2)]), IndexType::Number);
    }

    #[test]
    fn infer_index_type_whole_floats_are_number() {
        // 5.0 and 6.0 have no fractional part — an INT column can hold them.
        assert_eq!(
            infer_index_type(&[json!(5.0), json!(6.0)]),
            IndexType::Number
        );
    }

    #[test]
    fn infer_index_type_fractional_is_text() {
        // 3.14 is genuinely fractional — cannot be held by an INT column.
        assert_eq!(infer_index_type(&[json!(3.14)]), IndexType::Text);
    }

    #[test]
    fn infer_index_type_mixed_int_and_fractional_is_text() {
        // Any fractional value in the sample forces the whole field to Text.
        assert_eq!(infer_index_type(&[json!(1), json!(3.14)]), IndexType::Text);
    }

    #[test]
    fn infer_index_type_all_bools() {
        assert_eq!(
            infer_index_type(&[json!(true), json!(false)]),
            IndexType::Bool
        );
    }

    #[test]
    fn infer_index_type_strings_are_text() {
        assert_eq!(infer_index_type(&[json!("a")]), IndexType::Text);
    }

    #[test]
    fn infer_index_type_mixed_is_text() {
        assert_eq!(infer_index_type(&[json!(1), json!("a")]), IndexType::Text);
    }

    #[test]
    fn infer_index_type_empty_is_text() {
        assert_eq!(infer_index_type(&[]), IndexType::Text);
    }

    // ---------------------------------------------------------------------------
    // index_sql_type
    // ---------------------------------------------------------------------------

    #[test]
    fn index_sql_type_number() {
        // INT — FLOAT/DECIMAL have storage or comparison issues in GlueSQL 0.19.
        assert_eq!(index_sql_type(IndexType::Number), "INT");
    }

    #[test]
    fn index_sql_type_bool() {
        assert_eq!(index_sql_type(IndexType::Bool), "BOOLEAN");
    }

    #[test]
    fn index_sql_type_text() {
        assert_eq!(index_sql_type(IndexType::Text), "TEXT");
    }

    // ---------------------------------------------------------------------------
    // derive_typed_value
    // ---------------------------------------------------------------------------

    #[test]
    fn derive_typed_value_number_match() {
        let doc = json!({"age": 36});
        assert_eq!(
            derive_typed_value(&doc, "age", IndexType::Number),
            Some(json!(36))
        );
    }

    #[test]
    fn derive_typed_value_number_type_mismatch_is_none() {
        let doc = json!({"age": "x"});
        assert_eq!(derive_typed_value(&doc, "age", IndexType::Number), None);
    }

    #[test]
    fn derive_typed_value_fractional_number_is_none_for_number_index() {
        // 3.14 is fractional — an INT column cannot hold it, so None (→ NULL).
        let doc = json!({"price": 3.14});
        assert_eq!(derive_typed_value(&doc, "price", IndexType::Number), None);
    }

    #[test]
    fn derive_typed_value_whole_float_is_some_for_number_index() {
        // 5.0 has no fractional part — treated as a whole number.
        let doc = json!({"qty": 5.0});
        assert!(derive_typed_value(&doc, "qty", IndexType::Number).is_some());
    }

    #[test]
    fn derive_typed_value_bool_match() {
        let doc = json!({"ok": true});
        assert_eq!(
            derive_typed_value(&doc, "ok", IndexType::Bool),
            Some(json!(true))
        );
    }

    #[test]
    fn derive_typed_value_bool_type_mismatch_is_none() {
        let doc = json!({"ok": 1});
        assert_eq!(derive_typed_value(&doc, "ok", IndexType::Bool), None);
    }

    #[test]
    fn derive_typed_value_text_string() {
        let doc = json!({"s": "hi"});
        assert_eq!(
            derive_typed_value(&doc, "s", IndexType::Text),
            Some(json!("hi"))
        );
    }

    #[test]
    fn derive_typed_value_text_numeric_serialized() {
        let doc = json!({"n": 5});
        assert_eq!(
            derive_typed_value(&doc, "n", IndexType::Text),
            Some(json!("5"))
        );
    }

    #[test]
    fn derive_typed_value_missing_path_is_none_for_all_types() {
        let doc = json!({"x": 1});
        assert_eq!(
            derive_typed_value(&doc, "missing", IndexType::Number),
            None
        );
        assert_eq!(derive_typed_value(&doc, "missing", IndexType::Bool), None);
        assert_eq!(derive_typed_value(&doc, "missing", IndexType::Text), None);
    }
}
