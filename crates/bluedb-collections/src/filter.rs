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
}
