use serde_json::Value;
use crate::error::MqlError;

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
}
