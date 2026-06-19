use std::collections::HashMap;
use serde_json::Value;
use crate::error::MqlError;
use crate::index::{derived_col, IndexType};

#[derive(Debug, PartialEq)]
pub enum Cmp { Eq, Ne, Gt, Gte, Lt, Lte, In, Nin, Exists, Regex }

#[derive(Debug, PartialEq)]
pub enum Filter {
    Cmp { path: String, op: Cmp, value: Value },
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
    True, // empty filter {} matches all
}

pub fn parse_filter(v: &Value) -> Result<Filter, MqlError> {
    let obj = v.as_object().ok_or_else(|| MqlError::Malformed("filter must be an object".into()))?;
    if obj.is_empty() { return Ok(Filter::True); }
    let mut clauses = Vec::new();
    for (k, val) in obj {
        match k.as_str() {
            "$and" => clauses.push(Filter::And(parse_array(val)?)),
            "$or"  => clauses.push(Filter::Or(parse_array(val)?)),
            "$not" => clauses.push(Filter::Not(Box::new(parse_filter(val)?))),
            field if field.starts_with('$') =>
                return Err(MqlError::UnsupportedOperator(field.to_string())),
            field => clauses.push(parse_field(field, val)?),
        }
    }
    Ok(if clauses.len() == 1 { clauses.pop().unwrap() } else { Filter::And(clauses) })
}

fn parse_array(v: &Value) -> Result<Vec<Filter>, MqlError> {
    v.as_array().ok_or_else(|| MqlError::Malformed("$and/$or take an array".into()))?
        .iter().map(parse_filter).collect()
}

fn parse_field(path: &str, val: &Value) -> Result<Filter, MqlError> {
    if let Value::Object(ops) = val {
        if ops.keys().any(|k| k.starts_with('$')) {
            let mut out = Vec::new();
            for (op, v) in ops {
                let cmp = match op.as_str() {
                    "$eq" => Cmp::Eq, "$ne" => Cmp::Ne, "$gt" => Cmp::Gt, "$gte" => Cmp::Gte,
                    "$lt" => Cmp::Lt, "$lte" => Cmp::Lte, "$in" => Cmp::In, "$nin" => Cmp::Nin,
                    "$exists" => Cmp::Exists, "$regex" => Cmp::Regex,
                    other => return Err(MqlError::UnsupportedOperator(other.to_string())),
                };
                out.push(Filter::Cmp { path: path.into(), op: cmp, value: v.clone() });
            }
            return Ok(if out.len() == 1 { out.pop().unwrap() } else { Filter::And(out) });
        }
    }
    Ok(Filter::Cmp { path: path.into(), op: Cmp::Eq, value: val.clone() })
}

impl Filter {
    /// If this filter is a conjunction of pure top-level equality (`Cmp{op:Eq}`)
    /// nodes (or a single such node), return a map `path → value`.  Any non-Eq
    /// operator, nested `$and`/`$or`, or `Not` node returns `None`.
    ///
    /// Used by the gateway to detect whether a compound index's full key is
    /// covered by the filter so it can route to `{compound_col} = $1`.
    pub fn eq_map(&self) -> Option<HashMap<String, Value>> {
        match self {
            Filter::True => Some(HashMap::new()),
            Filter::Cmp { path, op: Cmp::Eq, value } => {
                let mut m = HashMap::new();
                m.insert(path.clone(), value.clone());
                Some(m)
            }
            Filter::And(v) => {
                let mut m = HashMap::new();
                for f in v {
                    match f {
                        Filter::Cmp { path, op: Cmp::Eq, value } => {
                            m.insert(path.clone(), value.clone());
                        }
                        // Any non-eq child → can't use compound key.
                        _ => return None,
                    }
                }
                Some(m)
            }
            // Anything else: Or, Not, non-Eq Cmp.
            _ => None,
        }
    }

    /// Lower to a SQL boolean expression over `_id` / `doc`. Appends bound values
    /// to `params`; placeholders are `$1..$N` by params.len().
    pub fn to_sql(&self, params: &mut Vec<Value>) -> String {
        match self {
            Filter::True => "TRUE".into(),
            Filter::And(v) => join(v, " AND ", params),
            Filter::Or(v)  => join(v, " OR ", params),
            Filter::Not(f) => format!("NOT ({})", f.to_sql(params)),
            Filter::Cmp { path, op, value } => cmp_sql(path, op, value, params),
        }
    }

    /// Like [`Self::to_sql`] but, for a `Cmp` whose `path` has a typed entry in
    /// `indexed` (keyed by the derived column name `__cidx_<path>`), uses the
    /// derived column instead of the `(doc->>'...')` JSON accessor — so GlueSQL
    /// can use the secondary index.  `_id` and non-indexed paths behave exactly
    /// as [`Self::to_sql`].
    ///
    /// The derived column is typed (`FLOAT`, `BOOLEAN`, or `TEXT`) and the fast
    /// path is taken only when the comparison value's JSON type **matches** the
    /// column's [`IndexType`]:
    /// - `IndexType::Number` → value is `Value::Number` (or, for `$in`/`$nin`,
    ///   all elements are numbers).
    /// - `IndexType::Bool`   → value is `Value::Bool` (or all-bool array).
    /// - `IndexType::Text`   → value is `Value::String` (or all-string array).
    ///
    /// `$exists` always uses the derived column (it is a NULL check, type-agnostic).
    /// Type-mismatched comparisons fall through to the JSON accessor path.
    pub fn to_sql_indexed(&self, params: &mut Vec<Value>, indexed: &HashMap<String, IndexType>) -> String {
        match self {
            Filter::True => "TRUE".into(),
            Filter::And(v) => join_indexed(v, " AND ", params, indexed),
            Filter::Or(v)  => join_indexed(v, " OR ", params, indexed),
            Filter::Not(f) => format!("NOT ({})", f.to_sql_indexed(params, indexed)),
            Filter::Cmp { path, op, value } => {
                let col = derived_col(path);
                if path != "_id" {
                    if let Some(&idx_type) = indexed.get(&col) {
                        // Route to the derived column only when the comparison
                        // value's JSON type matches the column's stored type.
                        let use_index = match op {
                            Cmp::Exists => true,
                            Cmp::In | Cmp::Nin => {
                                value.as_array()
                                    .map(|arr| !arr.is_empty() && arr.iter().all(|v| value_matches_type(v, idx_type)))
                                    .unwrap_or(false)
                            }
                            _ => value_matches_type(value, idx_type),
                        };
                        if use_index {
                            return cmp_sql_col(&col, op, value, params);
                        }
                    }
                }
                cmp_sql(path, op, value, params)
            }
        }
    }
}

fn join(v: &[Filter], sep: &str, params: &mut Vec<Value>) -> String {
    let parts: Vec<String> = v.iter().map(|f| format!("({})", f.to_sql(params))).collect();
    parts.join(sep)
}

fn join_indexed(v: &[Filter], sep: &str, params: &mut Vec<Value>, indexed: &HashMap<String, IndexType>) -> String {
    let parts: Vec<String> = v.iter().map(|f| format!("({})", f.to_sql_indexed(params, indexed))).collect();
    parts.join(sep)
}

/// Return `true` when the JSON value's type matches the expected [`IndexType`].
///
/// For `Number`, only **whole** numbers match (integers, or floats with no
/// fractional part like `5.0`).  Truly fractional values like `3.14` do NOT
/// match — they route to the JSON accessor (analytical) path instead, which
/// compares correctly without silently returning an empty result.
fn value_matches_type(v: &Value, t: IndexType) -> bool {
    match t {
        IndexType::Number => crate::index::is_whole_number(v),
        IndexType::Bool => v.is_boolean(),
        IndexType::Text => v.is_string(),
    }
}

/// Like `cmp_sql` but uses a pre-computed plain column reference (no JSON accessor).
fn cmp_sql_col(col: &str, op: &Cmp, value: &Value, params: &mut Vec<Value>) -> String {
    match op {
        Cmp::Eq  => format!("{col} = {}", bind(params, value)),
        Cmp::Ne  => {
            let b = bind(params, value);
            format!("({col} <> {b} OR {col} IS NULL)")
        }
        Cmp::Gt  => format!("{col} > {}", bind(params, value)),
        Cmp::Gte => format!("{col} >= {}", bind(params, value)),
        Cmp::Lt  => format!("{col} < {}", bind(params, value)),
        Cmp::Lte => format!("{col} <= {}", bind(params, value)),
        Cmp::In  => in_sql(col, value, params),
        Cmp::Nin => in_sql_nin(col, value, params),
        Cmp::Exists => {
            let want = value.as_bool().unwrap_or(true);
            if want { format!("{col} IS NOT NULL") } else { format!("{col} IS NULL") }
        }
        Cmp::Regex => format!("{col} ~ {}", bind(params, value)),
    }
}

/// `_id` → the PK column (bare); any other path → `(doc->>'a'->>'b'...)` text accessor.
/// The outer parens on the json accessor avoid operator-precedence surprises (e.g. `->>`
/// binding tighter than `=` in some SQL dialects).
fn col_ref(path: &str) -> String {
    if path == "_id" { return "_id".into(); }
    let parts: Vec<&str> = path.split('.').collect();
    let mut expr = "doc".to_string();
    for (i, p) in parts.iter().enumerate() {
        let arrow = if i == parts.len() - 1 { "->>" } else { "->" };
        expr = format!("{expr}{arrow}'{}'", p.replace('\'', "''"));
    }
    format!("({expr})")
}

fn bind(params: &mut Vec<Value>, v: &Value) -> String {
    params.push(v.clone());
    format!("${}", params.len())
}

fn cmp_sql(path: &str, op: &Cmp, value: &Value, params: &mut Vec<Value>) -> String {
    let col = col_ref(path);
    match op {
        Cmp::Eq  => format!("{col} = {}", bind(params, value)),
        // MongoDB $ne/$nin match documents where the field is missing/null in
        // addition to documents where it holds a different value. Emit an OR
        // clause so GlueSQL/DataFusion include those rows too.
        Cmp::Ne  => {
            let b = bind(params, value);
            format!("({col} <> {b} OR {col} IS NULL)")
        }
        Cmp::Gt  => format!("{col} > {}", bind(params, value)),
        Cmp::Gte => format!("{col} >= {}", bind(params, value)),
        Cmp::Lt  => format!("{col} < {}", bind(params, value)),
        Cmp::Lte => format!("{col} <= {}", bind(params, value)),
        Cmp::In  => in_sql(&col, value, params),
        Cmp::Nin => in_sql_nin(&col, value, params),
        Cmp::Exists => {
            let want = value.as_bool().unwrap_or(true);
            if want { format!("{col} IS NOT NULL") } else { format!("{col} IS NULL") }
        }
        Cmp::Regex => format!("{col} ~ {}", bind(params, value)),
    }
}

fn in_sql(col: &str, value: &Value, params: &mut Vec<Value>) -> String {
    let items = value.as_array().cloned().unwrap_or_default();
    let placeholders: Vec<String> = items.iter().map(|v| bind(params, v)).collect();
    format!("{col} IN ({})", placeholders.join(", "))
}

/// `$nin`: NOT IN + IS NULL so documents missing the field are also included
/// (MongoDB semantics: `$nin` matches docs where the field is absent or null).
fn in_sql_nin(col: &str, value: &Value, params: &mut Vec<Value>) -> String {
    let items = value.as_array().cloned().unwrap_or_default();
    let placeholders: Vec<String> = items.iter().map(|v| bind(params, v)).collect();
    format!("({col} NOT IN ({}) OR {col} IS NULL)", placeholders.join(", "))
}

// ---------------------------------------------------------------------------
// DataFusion `Expr` lowering (for the aggregation pipeline `$match` stage)
// ---------------------------------------------------------------------------

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{cast, col, lit, Expr, ScalarUDF};
use std::collections::HashSet;
use std::sync::Arc;

/// Lower a JSON scalar to a DataFusion literal [`Expr`], preserving its JSON type
/// (string → `Utf8`, integer → `Int64`, float → `Float64`, bool → `Boolean`).
/// `null` and composite values fall back to their JSON text.
fn json_to_lit(v: &Value) -> Expr {
    match v {
        Value::String(s) => lit(s.as_str()),
        Value::Bool(b) => lit(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                lit(i)
            } else if let Some(f) = n.as_f64() {
                lit(f)
            } else {
                lit(n.to_string())
            }
        }
        // null / array / object: bind as JSON text (rare in a `$match`).
        Value::Null => lit(serde_json::Value::Null.to_string()),
        other => lit(other.to_string()),
    }
}

/// True if `v` (or, for `$in`/`$nin`, every array element) is a numeric literal —
/// so the text `json_get_str` accessor must be cast to `Float64` before comparing.
fn is_numeric(v: &Value) -> bool {
    match v {
        Value::Number(_) => true,
        Value::Array(items) => !items.is_empty() && items.iter().all(|x| x.is_number()),
        _ => false,
    }
}

impl Filter {
    /// Lower this filter to a DataFusion [`Expr`] over a collection's
    /// `(_id TEXT, doc JSON-as-Utf8)` shape, for the aggregation `$match` stage.
    ///
    /// `get_str` is the runtime-registered `json_get_str` UDF (resolved from the
    /// session context by the caller). `materialized` is the set of fields that an
    /// earlier stage (e.g. `$unwind`) has already turned into real Arrow columns.
    /// A field path maps to a column expression:
    /// `_id` → `col("_id")`; a path in `materialized` → `col(path)`; any other path
    /// → `get_str(col("doc"), lit(path))`, which extracts the field as text.
    ///
    /// Because `json_get_str` returns text, a comparison against a **numeric**
    /// literal casts the accessor to `Float64` first so the comparison is numeric,
    /// not lexical. String comparisons compare text directly. (A materialized
    /// column is compared as-is — it is not re-cast.)
    ///
    /// `$regex` is not supported on this path and returns
    /// [`MqlError::UnsupportedOperator`].
    pub fn to_df_expr(
        &self,
        get_str: &Arc<ScalarUDF>,
        materialized: &HashSet<String>,
    ) -> Result<Expr, MqlError> {
        match self {
            Filter::True => Ok(lit(true)),
            Filter::And(v) => {
                let mut it = v.iter();
                let first = match it.next() {
                    Some(f) => f.to_df_expr(get_str, materialized)?,
                    None => return Ok(lit(true)),
                };
                it.try_fold(first, |acc, f| Ok(acc.and(f.to_df_expr(get_str, materialized)?)))
            }
            Filter::Or(v) => {
                let mut it = v.iter();
                let first = match it.next() {
                    Some(f) => f.to_df_expr(get_str, materialized)?,
                    None => return Ok(lit(true)),
                };
                it.try_fold(first, |acc, f| Ok(acc.or(f.to_df_expr(get_str, materialized)?)))
            }
            Filter::Not(f) => Ok(!f.to_df_expr(get_str, materialized)?),
            Filter::Cmp { path, op, value } => cmp_df_expr(path, op, value, get_str, materialized),
        }
    }
}

/// Build the column expression for a field `path`: `_id` is the PK column; a path
/// already materialized into a real column is `col(path)`; any other path extracts
/// the field as text via `json_get_str(col("doc"), path)`.
fn path_expr(path: &str, get_str: &Arc<ScalarUDF>, materialized: &HashSet<String>) -> Expr {
    if path == "_id" {
        col("_id")
    } else if materialized.contains(path) {
        col(path)
    } else {
        get_str.as_ref().clone().call(vec![col("doc"), lit(path)])
    }
}

/// Like [`path_expr`] but casts the (text) accessor to `Float64` when the literal
/// being compared is numeric, so the comparison is numeric. `_id` and materialized
/// columns are left as-is (already typed, not text accessors).
fn path_expr_for(
    path: &str,
    value: &Value,
    get_str: &Arc<ScalarUDF>,
    materialized: &HashSet<String>,
) -> Expr {
    let base = path_expr(path, get_str, materialized);
    if path != "_id" && !materialized.contains(path) && is_numeric(value) {
        cast(base, DataType::Float64)
    } else {
        base
    }
}

fn cmp_df_expr(
    path: &str,
    op: &Cmp,
    value: &Value,
    get_str: &Arc<ScalarUDF>,
    materialized: &HashSet<String>,
) -> Result<Expr, MqlError> {
    Ok(match op {
        Cmp::Eq => path_expr_for(path, value, get_str, materialized).eq(json_to_lit(value)),
        Cmp::Ne => path_expr_for(path, value, get_str, materialized).not_eq(json_to_lit(value)),
        Cmp::Gt => path_expr_for(path, value, get_str, materialized).gt(json_to_lit(value)),
        Cmp::Gte => path_expr_for(path, value, get_str, materialized).gt_eq(json_to_lit(value)),
        Cmp::Lt => path_expr_for(path, value, get_str, materialized).lt(json_to_lit(value)),
        Cmp::Lte => path_expr_for(path, value, get_str, materialized).lt_eq(json_to_lit(value)),
        Cmp::In => {
            let items = value.as_array().cloned().unwrap_or_default();
            let lits: Vec<Expr> = items.iter().map(json_to_lit).collect();
            path_expr_for(path, value, get_str, materialized).in_list(lits, false)
        }
        Cmp::Nin => {
            let items = value.as_array().cloned().unwrap_or_default();
            let lits: Vec<Expr> = items.iter().map(json_to_lit).collect();
            path_expr_for(path, value, get_str, materialized).in_list(lits, true)
        }
        Cmp::Exists => {
            let want = value.as_bool().unwrap_or(true);
            let base = path_expr(path, get_str, materialized);
            if want { base.is_not_null() } else { base.is_null() }
        }
        // `$regex` in the DataFusion expr path is out of scope (the SQL `find`
        // path handles `~`); reject cleanly here.
        Cmp::Regex => return Err(MqlError::UnsupportedOperator("$regex".into())),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_implicit_eq_and_operators() {
        let f = parse_filter(&json!({"status": "active"})).unwrap();
        assert_eq!(f, Filter::Cmp { path: "status".into(), op: Cmp::Eq, value: json!("active") });
        let f = parse_filter(&json!({"age": {"$gte": 18}})).unwrap();
        assert_eq!(f, Filter::Cmp { path: "age".into(), op: Cmp::Gte, value: json!(18) });
        let f = parse_filter(&json!({"$and": [{"a": 1}, {"b": 2}]})).unwrap();
        assert!(matches!(f, Filter::And(v) if v.len() == 2));
    }

    #[test]
    fn rejects_unsupported_operator() {
        let e = parse_filter(&json!({"x": {"$where": "1"}})).unwrap_err();
        assert!(e.to_string().contains("$where"));
    }

    #[test]
    fn to_sql_uses_json_accessors_and_bound_params() {
        let f = parse_filter(&serde_json::json!({"status": "active"})).unwrap();
        let mut params = Vec::new();
        let sql = f.to_sql(&mut params);
        assert_eq!(sql, "(doc->>'status') = $1");
        assert_eq!(params, vec![serde_json::json!("active")]);

        let f = parse_filter(&serde_json::json!({"_id": "x"})).unwrap();
        let mut params = Vec::new();
        // _id maps to the PK column directly, not a json accessor
        assert_eq!(f.to_sql(&mut params), "_id = $1");
    }

    #[test]
    fn to_sql_indexed_uses_derived_col_when_indexed() {
        let f = parse_filter(&json!({"status": "active"})).unwrap();
        // Key is the derived column name; value is the IndexType.
        let indexed: HashMap<String, IndexType> =
            [("__cidx_status".to_string(), IndexType::Text)].into();

        let mut params = Vec::new();
        let sql = f.to_sql_indexed(&mut params, &indexed);
        // indexed path → plain derived column, NOT a JSON accessor
        assert_eq!(sql, "__cidx_status = $1");
        assert_eq!(params, vec![json!("active")]);
    }

    #[test]
    fn to_sql_indexed_falls_back_to_json_accessor_when_not_indexed() {
        let f = parse_filter(&json!({"status": "active"})).unwrap();
        let indexed: HashMap<String, IndexType> = HashMap::new();

        let mut params_indexed = Vec::new();
        let sql_indexed = f.to_sql_indexed(&mut params_indexed, &indexed);

        let mut params_plain = Vec::new();
        let sql_plain = f.to_sql(&mut params_plain);

        assert_eq!(sql_indexed, sql_plain);
        assert_eq!(params_indexed, params_plain);
    }

    #[test]
    fn to_sql_indexed_does_not_use_derived_col_for_id() {
        let f = parse_filter(&json!({"_id": "abc"})).unwrap();
        // Even if someone (mistakenly) listed "_id" as indexed, it must stay as _id.
        let indexed: HashMap<String, IndexType> =
            [("__cidx__id".to_string(), IndexType::Text)].into();
        let mut params = Vec::new();
        let sql = f.to_sql_indexed(&mut params, &indexed);
        assert_eq!(sql, "_id = $1");
    }

    /// A numeric index on `age`: a numeric comparison DOES use the derived column
    /// (fast path) because the column is typed FLOAT and the value is a number.
    #[test]
    fn to_sql_indexed_uses_derived_col_for_numeric_index_and_numeric_value() {
        let f = parse_filter(&json!({"age": 36})).unwrap();
        let indexed: HashMap<String, IndexType> =
            [("__cidx_age".to_string(), IndexType::Number)].into();

        let mut params = Vec::new();
        let sql = f.to_sql_indexed(&mut params, &indexed);

        assert!(
            sql.contains("__cidx_age"),
            "numeric comparison on numeric index should use derived column, got: {sql}"
        );
    }

    /// A TEXT index on `age` (old/wrong setup): a numeric comparison falls back
    /// to the JSON accessor because the types don't match.
    #[test]
    fn to_sql_indexed_falls_back_for_numeric_value_on_text_index() {
        let f = parse_filter(&json!({"age": 36})).unwrap();
        // TEXT index but value is numeric → mismatch → fall back.
        let indexed: HashMap<String, IndexType> =
            [("__cidx_age".to_string(), IndexType::Text)].into();

        let mut params = Vec::new();
        let sql = f.to_sql_indexed(&mut params, &indexed);

        assert!(
            !sql.contains("__cidx_"),
            "numeric value on TEXT index must fall back to JSON accessor, got: {sql}"
        );
        assert!(
            sql.contains("doc->>'age'"),
            "numeric value on TEXT index should use JSON accessor, got: {sql}"
        );
    }

    /// FIX 1: an $exists filter on an indexed field MAY use the derived column
    /// (it is a NULL check and is type-agnostic).
    #[test]
    fn to_sql_indexed_uses_derived_col_for_exists() {
        let f = parse_filter(&json!({"age": {"$exists": true}})).unwrap();
        let indexed: HashMap<String, IndexType> =
            [("__cidx_age".to_string(), IndexType::Number)].into();

        let mut params = Vec::new();
        let sql = f.to_sql_indexed(&mut params, &indexed);

        assert!(
            sql.contains("__cidx_age"),
            "$exists should use derived column (NULL check), got: {sql}"
        );
    }

    /// FIX 1: $in with ALL string elements uses the derived column (fast path) for
    /// a TEXT-indexed column.
    #[test]
    fn to_sql_indexed_uses_derived_col_for_string_in() {
        let f = parse_filter(&json!({"status": {"$in": ["a", "b"]}})).unwrap();
        let indexed: HashMap<String, IndexType> =
            [("__cidx_status".to_string(), IndexType::Text)].into();

        let mut params = Vec::new();
        let sql = f.to_sql_indexed(&mut params, &indexed);

        assert!(
            sql.contains("__cidx_status"),
            "$in(strings) on TEXT index should use derived column, got: {sql}"
        );
    }

    /// $in with numeric elements on a Number-indexed column uses the derived col.
    #[test]
    fn to_sql_indexed_uses_derived_col_for_numeric_in() {
        let f = parse_filter(&json!({"age": {"$in": [30, 40]}})).unwrap();
        let indexed: HashMap<String, IndexType> =
            [("__cidx_age".to_string(), IndexType::Number)].into();

        let mut params = Vec::new();
        let sql = f.to_sql_indexed(&mut params, &indexed);

        assert!(
            sql.contains("__cidx_age"),
            "$in(numbers) on Number index should use derived column, got: {sql}"
        );
    }

    /// A fractional query value on a Number-indexed path must NOT use the derived
    /// column (it would silently produce NULL = empty result); it must fall back to
    /// the JSON accessor so the comparison is correct on the analytical path.
    #[test]
    fn to_sql_indexed_falls_back_for_fractional_value_on_number_index() {
        let f = parse_filter(&json!({"price": 3.14})).unwrap();
        let indexed: HashMap<String, IndexType> =
            [("__cidx_price".to_string(), IndexType::Number)].into();

        let mut params = Vec::new();
        let sql = f.to_sql_indexed(&mut params, &indexed);

        assert!(
            !sql.contains("__cidx_"),
            "fractional value on Number index must fall back to JSON accessor, got: {sql}"
        );
        assert!(
            sql.contains("doc->>'price'"),
            "fractional value on Number index should use JSON accessor, got: {sql}"
        );
    }

    /// A whole-float query value (5.0) on a Number-indexed path DOES use the
    /// derived column — it is canonically an integer.
    #[test]
    fn to_sql_indexed_uses_derived_col_for_whole_float_on_number_index() {
        let f = parse_filter(&json!({"qty": 5.0})).unwrap();
        let indexed: HashMap<String, IndexType> =
            [("__cidx_qty".to_string(), IndexType::Number)].into();

        let mut params = Vec::new();
        let sql = f.to_sql_indexed(&mut params, &indexed);

        assert!(
            sql.contains("__cidx_qty"),
            "whole-float value on Number index should use derived column, got: {sql}"
        );
    }

    /// $in with numeric elements on a TEXT-indexed column falls back.
    #[test]
    fn to_sql_indexed_falls_back_for_numeric_in_on_text_index() {
        let f = parse_filter(&json!({"age": {"$in": [30, 40]}})).unwrap();
        let indexed: HashMap<String, IndexType> =
            [("__cidx_age".to_string(), IndexType::Text)].into();

        let mut params = Vec::new();
        let sql = f.to_sql_indexed(&mut params, &indexed);

        assert!(
            !sql.contains("__cidx_"),
            "$in(numbers) on TEXT index must fall back, got: {sql}"
        );
    }

    /// FIX 3: $ne SQL contains IS NULL (Mongo semantics: match missing/null too).
    #[test]
    fn ne_filter_sql_contains_is_null() {
        let f = parse_filter(&json!({"status": {"$ne": "inactive"}})).unwrap();
        let mut params = Vec::new();
        let sql = f.to_sql(&mut params);

        assert!(
            sql.contains("IS NULL"),
            "$ne must include IS NULL for missing-field semantics, got: {sql}"
        );
    }

    /// FIX 3: $nin SQL contains IS NULL (Mongo semantics: match missing/null too).
    #[test]
    fn nin_filter_sql_contains_is_null() {
        let f = parse_filter(&json!({"role": {"$nin": ["admin", "ops"]}})).unwrap();
        let mut params = Vec::new();
        let sql = f.to_sql(&mut params);

        assert!(
            sql.contains("IS NULL"),
            "$nin must include IS NULL for missing-field semantics, got: {sql}"
        );
    }

    // --- to_df_expr (DataFusion lowering) ---------------------------------
    //
    // A minimal stand-in `json_get_str` UDF (2-arg → Utf8). We don't execute it
    // here — these tests assert the lowering *builds* a structurally correct
    // `Expr`, with the cast applied only for numeric literals and `_id` left as a
    // bare column. The end-to-end exercise of the real `json_get_str` (over the
    // Iceberg mirror) lives in the server's `tests/collections.rs` aggregate test.
    mod df {
        use super::*;
        use datafusion::arrow::datatypes::DataType;
        use datafusion::logical_expr::{
            ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
        };
        use std::any::Any;
        use std::sync::Arc;

        #[derive(Debug, PartialEq, Eq, Hash)]
        struct FakeGetStr(Signature);
        impl FakeGetStr {
            fn new() -> Self {
                Self(Signature::any(2, Volatility::Immutable))
            }
        }
        impl ScalarUDFImpl for FakeGetStr {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &str {
                "json_get_str"
            }
            fn signature(&self) -> &Signature {
                &self.0
            }
            fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
                Ok(DataType::Utf8)
            }
            fn invoke_with_args(
                &self,
                _: ScalarFunctionArgs,
            ) -> datafusion::common::Result<ColumnarValue> {
                unreachable!("structural test never executes the UDF")
            }
        }

        fn udf() -> Arc<ScalarUDF> {
            Arc::new(ScalarUDF::new_from_impl(FakeGetStr::new()))
        }

        /// No materialized fields — the common case for these structural tests.
        fn mat() -> HashSet<String> {
            HashSet::new()
        }

        #[test]
        fn string_field_eq_builds_a_binary_expr() {
            let f = parse_filter(&json!({"status": "active"})).unwrap();
            let e = f.to_df_expr(&udf(), &mat()).unwrap();
            // A string comparison is a BinaryExpr; the accessor is NOT cast.
            let s = format!("{e}");
            assert!(s.contains("json_get_str"), "expr was: {s}");
            assert!(!s.contains("CAST"), "string compare must not cast: {s}");
        }

        #[test]
        fn numeric_field_compare_casts_to_float() {
            let f = parse_filter(&json!({"age": {"$gte": 30}})).unwrap();
            let e = f.to_df_expr(&udf(), &mat()).unwrap();
            let s = format!("{e}");
            assert!(s.contains("CAST"), "numeric compare should cast: {s}");
        }

        #[test]
        fn id_path_is_a_bare_column() {
            let f = parse_filter(&json!({"_id": "x"})).unwrap();
            let e = f.to_df_expr(&udf(), &mat()).unwrap();
            let s = format!("{e}");
            assert!(!s.contains("json_get_str"), "_id must be a bare column: {s}");
        }

        /// A field that an earlier stage materialized resolves to a bare column,
        /// NOT json_get_str(doc, …) — and a numeric comparison against it is not
        /// re-cast (the materialized element is already the right type).
        #[test]
        fn materialized_field_is_a_bare_column_not_accessor() {
            let mat: HashSet<String> = ["tags".to_string()].into_iter().collect();

            let f = parse_filter(&json!({"tags": "x"})).unwrap();
            let e = f.to_df_expr(&udf(), &mat).unwrap();
            let s = format!("{e}");
            assert!(
                !s.contains("json_get_str"),
                "materialized field must be a bare column, got: {s}"
            );

            // A non-materialized field in the SAME filter still uses the accessor.
            let f2 = parse_filter(&json!({"other": "y"})).unwrap();
            let s2 = format!("{}", f2.to_df_expr(&udf(), &mat).unwrap());
            assert!(
                s2.contains("json_get_str"),
                "non-materialized field must use accessor, got: {s2}"
            );
        }

        #[test]
        fn and_or_not_and_in_build() {
            let f = parse_filter(&json!({
                "$and": [
                    {"status": "active"},
                    {"$or": [{"role": "admin"}, {"role": "ops"}]}
                ]
            }))
            .unwrap();
            assert!(f.to_df_expr(&udf(), &mat()).is_ok());

            let f = parse_filter(&json!({"$not": {"status": "active"}})).unwrap();
            assert!(f.to_df_expr(&udf(), &mat()).is_ok());

            let f = parse_filter(&json!({"role": {"$in": ["a", "b"]}})).unwrap();
            assert!(f.to_df_expr(&udf(), &mat()).is_ok());

            let f = parse_filter(&json!({})).unwrap();
            assert!(f.to_df_expr(&udf(), &mat()).is_ok()); // True → lit(true)
        }

        #[test]
        fn regex_is_rejected_on_the_df_path() {
            let f = parse_filter(&json!({"name": {"$regex": "^a"}})).unwrap();
            let e = f.to_df_expr(&udf(), &mat()).unwrap_err();
            assert!(e.to_string().contains("$regex"), "got: {e}");
        }
    }
}
