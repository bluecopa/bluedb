use std::collections::HashSet;
use serde_json::Value;
use crate::error::MqlError;
use crate::index::derived_col;

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

    /// Like [`Self::to_sql`] but, for a `Cmp` whose `path` is in `indexed`, uses
    /// the derived column `__cidx_<path>` (a plain column with a secondary index)
    /// instead of the `(doc->>'...')` JSON accessor — so GlueSQL can use the index.
    /// `_id` and non-indexed paths behave exactly as [`Self::to_sql`].
    pub fn to_sql_indexed(&self, params: &mut Vec<Value>, indexed: &HashSet<String>) -> String {
        match self {
            Filter::True => "TRUE".into(),
            Filter::And(v) => join_indexed(v, " AND ", params, indexed),
            Filter::Or(v)  => join_indexed(v, " OR ", params, indexed),
            Filter::Not(f) => format!("NOT ({})", f.to_sql_indexed(params, indexed)),
            Filter::Cmp { path, op, value } => {
                if path != "_id" && indexed.contains(path.as_str()) {
                    let col = derived_col(path);
                    cmp_sql_col(&col, op, value, params)
                } else {
                    cmp_sql(path, op, value, params)
                }
            }
        }
    }
}

fn join(v: &[Filter], sep: &str, params: &mut Vec<Value>) -> String {
    let parts: Vec<String> = v.iter().map(|f| format!("({})", f.to_sql(params))).collect();
    parts.join(sep)
}

fn join_indexed(v: &[Filter], sep: &str, params: &mut Vec<Value>, indexed: &HashSet<String>) -> String {
    let parts: Vec<String> = v.iter().map(|f| format!("({})", f.to_sql_indexed(params, indexed))).collect();
    parts.join(sep)
}

/// Like `cmp_sql` but uses a pre-computed plain column reference (no JSON accessor).
fn cmp_sql_col(col: &str, op: &Cmp, value: &Value, params: &mut Vec<Value>) -> String {
    match op {
        Cmp::Eq  => format!("{col} = {}", bind(params, value)),
        Cmp::Ne  => format!("{col} <> {}", bind(params, value)),
        Cmp::Gt  => format!("{col} > {}", bind(params, value)),
        Cmp::Gte => format!("{col} >= {}", bind(params, value)),
        Cmp::Lt  => format!("{col} < {}", bind(params, value)),
        Cmp::Lte => format!("{col} <= {}", bind(params, value)),
        Cmp::In  => in_sql(col, value, params, false),
        Cmp::Nin => in_sql(col, value, params, true),
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
        Cmp::Ne  => format!("{col} <> {}", bind(params, value)),
        Cmp::Gt  => format!("{col} > {}", bind(params, value)),
        Cmp::Gte => format!("{col} >= {}", bind(params, value)),
        Cmp::Lt  => format!("{col} < {}", bind(params, value)),
        Cmp::Lte => format!("{col} <= {}", bind(params, value)),
        Cmp::In  => in_sql(&col, value, params, false),
        Cmp::Nin => in_sql(&col, value, params, true),
        Cmp::Exists => {
            let want = value.as_bool().unwrap_or(true);
            if want { format!("{col} IS NOT NULL") } else { format!("{col} IS NULL") }
        }
        Cmp::Regex => format!("{col} ~ {}", bind(params, value)),
    }
}

fn in_sql(col: &str, value: &Value, params: &mut Vec<Value>, negate: bool) -> String {
    let items = value.as_array().cloned().unwrap_or_default();
    let placeholders: Vec<String> = items.iter().map(|v| bind(params, v)).collect();
    let kw = if negate { "NOT IN" } else { "IN" };
    format!("{col} {kw} ({})", placeholders.join(", "))
}

// ---------------------------------------------------------------------------
// DataFusion `Expr` lowering (for the aggregation pipeline `$match` stage)
// ---------------------------------------------------------------------------

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{cast, col, lit, Expr, ScalarUDF};
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
    /// session context by the caller). A field path maps to a column expression:
    /// `_id` → `col("_id")`; any other path → `get_str(col("doc"), lit(path))`,
    /// which extracts the field as text.
    ///
    /// Because `json_get_str` returns text, a comparison against a **numeric**
    /// literal casts the accessor to `Float64` first so the comparison is numeric,
    /// not lexical. String comparisons compare text directly.
    ///
    /// `$regex` is not supported on this path and returns
    /// [`MqlError::UnsupportedOperator`].
    pub fn to_df_expr(&self, get_str: &Arc<ScalarUDF>) -> Result<Expr, MqlError> {
        match self {
            Filter::True => Ok(lit(true)),
            Filter::And(v) => {
                let mut it = v.iter();
                let first = match it.next() {
                    Some(f) => f.to_df_expr(get_str)?,
                    None => return Ok(lit(true)),
                };
                it.try_fold(first, |acc, f| Ok(acc.and(f.to_df_expr(get_str)?)))
            }
            Filter::Or(v) => {
                let mut it = v.iter();
                let first = match it.next() {
                    Some(f) => f.to_df_expr(get_str)?,
                    None => return Ok(lit(true)),
                };
                it.try_fold(first, |acc, f| Ok(acc.or(f.to_df_expr(get_str)?)))
            }
            Filter::Not(f) => Ok(!f.to_df_expr(get_str)?),
            Filter::Cmp { path, op, value } => cmp_df_expr(path, op, value, get_str),
        }
    }
}

/// Build the column expression for a field `path`: `_id` is the PK column; any
/// other path extracts the field as text via `json_get_str(col("doc"), path)`.
fn path_expr(path: &str, get_str: &Arc<ScalarUDF>) -> Expr {
    if path == "_id" {
        col("_id")
    } else {
        get_str.as_ref().clone().call(vec![col("doc"), lit(path)])
    }
}

/// Like [`path_expr`] but casts the (text) accessor to `Float64` when the literal
/// being compared is numeric, so the comparison is numeric. `_id` is left as-is.
fn path_expr_for(path: &str, value: &Value, get_str: &Arc<ScalarUDF>) -> Expr {
    let base = path_expr(path, get_str);
    if path != "_id" && is_numeric(value) {
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
) -> Result<Expr, MqlError> {
    Ok(match op {
        Cmp::Eq => path_expr_for(path, value, get_str).eq(json_to_lit(value)),
        Cmp::Ne => path_expr_for(path, value, get_str).not_eq(json_to_lit(value)),
        Cmp::Gt => path_expr_for(path, value, get_str).gt(json_to_lit(value)),
        Cmp::Gte => path_expr_for(path, value, get_str).gt_eq(json_to_lit(value)),
        Cmp::Lt => path_expr_for(path, value, get_str).lt(json_to_lit(value)),
        Cmp::Lte => path_expr_for(path, value, get_str).lt_eq(json_to_lit(value)),
        Cmp::In => {
            let items = value.as_array().cloned().unwrap_or_default();
            let lits: Vec<Expr> = items.iter().map(json_to_lit).collect();
            path_expr_for(path, value, get_str).in_list(lits, false)
        }
        Cmp::Nin => {
            let items = value.as_array().cloned().unwrap_or_default();
            let lits: Vec<Expr> = items.iter().map(json_to_lit).collect();
            path_expr_for(path, value, get_str).in_list(lits, true)
        }
        Cmp::Exists => {
            let want = value.as_bool().unwrap_or(true);
            let base = path_expr(path, get_str);
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
        let indexed: HashSet<String> = ["status".to_string()].into();

        let mut params = Vec::new();
        let sql = f.to_sql_indexed(&mut params, &indexed);
        // indexed path → plain derived column, NOT a JSON accessor
        assert_eq!(sql, "__cidx_status = $1");
        assert_eq!(params, vec![json!("active")]);
    }

    #[test]
    fn to_sql_indexed_falls_back_to_json_accessor_when_not_indexed() {
        let f = parse_filter(&json!({"status": "active"})).unwrap();
        let indexed: HashSet<String> = HashSet::new();

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
        let indexed: HashSet<String> = ["_id".to_string()].into();
        let mut params = Vec::new();
        let sql = f.to_sql_indexed(&mut params, &indexed);
        assert_eq!(sql, "_id = $1");
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

        #[test]
        fn string_field_eq_builds_a_binary_expr() {
            let f = parse_filter(&json!({"status": "active"})).unwrap();
            let e = f.to_df_expr(&udf()).unwrap();
            // A string comparison is a BinaryExpr; the accessor is NOT cast.
            let s = format!("{e}");
            assert!(s.contains("json_get_str"), "expr was: {s}");
            assert!(!s.contains("CAST"), "string compare must not cast: {s}");
        }

        #[test]
        fn numeric_field_compare_casts_to_float() {
            let f = parse_filter(&json!({"age": {"$gte": 30}})).unwrap();
            let e = f.to_df_expr(&udf()).unwrap();
            let s = format!("{e}");
            assert!(s.contains("CAST"), "numeric compare should cast: {s}");
        }

        #[test]
        fn id_path_is_a_bare_column() {
            let f = parse_filter(&json!({"_id": "x"})).unwrap();
            let e = f.to_df_expr(&udf()).unwrap();
            let s = format!("{e}");
            assert!(!s.contains("json_get_str"), "_id must be a bare column: {s}");
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
            assert!(f.to_df_expr(&udf()).is_ok());

            let f = parse_filter(&json!({"$not": {"status": "active"}})).unwrap();
            assert!(f.to_df_expr(&udf()).is_ok());

            let f = parse_filter(&json!({"role": {"$in": ["a", "b"]}})).unwrap();
            assert!(f.to_df_expr(&udf()).is_ok());

            let f = parse_filter(&json!({})).unwrap();
            assert!(f.to_df_expr(&udf()).is_ok()); // True → lit(true)
        }

        #[test]
        fn regex_is_rejected_on_the_df_path() {
            let f = parse_filter(&json!({"name": {"$regex": "^a"}})).unwrap();
            let e = f.to_df_expr(&udf()).unwrap_err();
            assert!(e.to_string().contains("$regex"), "got: {e}");
        }
    }
}
