//! `jsonb_path_query` family over JSON-as-`Utf8`, on a hand-rolled SQL/JSONPath
//! **navigation** subset (`serde_json`, no path-engine dependency).
//!
//! Supported steps: a leading `strict`/`lax` mode word (ignored), `$` (root),
//! `.key` / `["key"]` (object member), `.*` (all object values), `[n]` (array
//! index), `[*]` (all array elements). Anything richer — filters `? (...)`,
//! methods `.type()`, ranges `[1 to 3]`, `starts with`, arithmetic — is treated
//! as *unsupported*: the path yields no matches and the function returns `NULL`
//! (it plans and runs, it just can't evaluate that path), never an error.
//!
//! **Scalar approximation.** Postgres `jsonb_path_query` is set-returning (one row
//! per match); a DataFusion `ScalarUDF` returns one value per input row. So
//! `jsonb_path_query` / `jsonb_path_query_first` return the **first** match (exact
//! for the common single-match path), and `jsonb_path_query_array` returns all
//! matches as a JSON array (exact for that function).

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use serde_json::Value;

/// Register `jsonb_path_query`, `jsonb_path_query_first`, `jsonb_path_query_array`.
pub fn register(ctx: &mut datafusion::prelude::SessionContext) -> DfResult<()> {
    use datafusion::execution::FunctionRegistry;
    ctx.register_udf(Arc::new(ScalarUDF::new_from_impl(JsonPathQuery::new(
        "jsonb_path_query",
        Output::First,
    ))))?;
    ctx.register_udf(Arc::new(ScalarUDF::new_from_impl(JsonPathQuery::new(
        "jsonb_path_query_first",
        Output::First,
    ))))?;
    ctx.register_udf(Arc::new(ScalarUDF::new_from_impl(JsonPathQuery::new(
        "jsonb_path_query_array",
        Output::Array,
    ))))?;
    Ok(())
}

/// One navigation step of the supported JSONPath subset.
#[derive(Debug, Clone, PartialEq)]
enum Step {
    /// `.key` / `["key"]` — an object member.
    Key(String),
    /// `[n]` — an array index.
    Index(usize),
    /// `.*` — every value of an object.
    AllValues,
    /// `[*]` — every element of an array.
    AllElements,
}

/// Parse the supported navigation subset of a JSONPath, `None` if it uses any
/// unsupported feature (so the caller returns `NULL` rather than guessing).
fn parse_path(raw: &str) -> Option<Vec<Step>> {
    let mut s = raw.trim();
    for kw in ["strict ", "lax "] {
        if let Some(rest) = s.strip_prefix(kw) {
            s = rest.trim_start();
            break;
        }
    }
    let mut chars = s.chars().peekable();
    if chars.next()? != '$' {
        return None;
    }
    let mut steps = Vec::new();
    while let Some(&c) = chars.peek() {
        match c {
            '.' => {
                chars.next();
                if chars.peek() == Some(&'*') {
                    chars.next();
                    steps.push(Step::AllValues);
                } else {
                    let mut key = String::new();
                    while let Some(&c) = chars.peek() {
                        if c == '.' || c == '[' {
                            break;
                        }
                        key.push(c);
                        chars.next();
                    }
                    if key.is_empty() || !key.chars().all(|c| c.is_alphanumeric() || c == '_') {
                        return None; // method call / operator / etc.
                    }
                    steps.push(Step::Key(key));
                }
            }
            '[' => {
                chars.next();
                let mut inner = String::new();
                while let Some(&c) = chars.peek() {
                    if c == ']' {
                        break;
                    }
                    inner.push(c);
                    chars.next();
                }
                if chars.next() != Some(']') {
                    return None;
                }
                let inner = inner.trim();
                if inner == "*" {
                    steps.push(Step::AllElements);
                } else if let Ok(n) = inner.parse::<usize>() {
                    steps.push(Step::Index(n));
                } else if (inner.starts_with('"') && inner.ends_with('"') && inner.len() >= 2)
                    || (inner.starts_with('\'') && inner.ends_with('\'') && inner.len() >= 2)
                {
                    steps.push(Step::Key(inner[1..inner.len() - 1].to_string()));
                } else {
                    return None; // ranges (`1 to 3`), filters, etc.
                }
            }
            _ => return None, // ` ? (...)`, ` starts with`, arithmetic, …
        }
    }
    Some(steps)
}

/// All values matched by `steps` starting at `root`, in document order.
fn eval<'a>(root: &'a Value, steps: &[Step]) -> Vec<&'a Value> {
    let mut current = vec![root];
    for step in steps {
        let mut next = Vec::new();
        for v in current {
            match step {
                Step::Key(k) => {
                    if let Some(x) = v.get(k) {
                        next.push(x);
                    }
                }
                Step::Index(i) => {
                    if let Some(x) = v.get(i) {
                        next.push(x);
                    }
                }
                Step::AllValues => {
                    if let Value::Object(o) = v {
                        next.extend(o.values());
                    }
                }
                Step::AllElements => {
                    if let Value::Array(a) = v {
                        next.extend(a.iter());
                    }
                }
            }
        }
        current = next;
    }
    current
}

/// What a UDF emits from the match set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Output {
    /// The first match's JSON text (set-returning approximated as the first row).
    First,
    /// All matches as a JSON array's text.
    Array,
}

/// Evaluate `path` against `json` and render per `out`. `None` (→ SQL `NULL`) on
/// parse failure, an unsupported path, or no match (for `First`).
fn run(json: &str, path: &str, out: Output) -> Option<String> {
    let root: Value = serde_json::from_str(json).ok()?;
    let steps = parse_path(path)?;
    let matches = eval(&root, &steps);
    match out {
        Output::First => matches.first().map(|v| v.to_string()),
        Output::Array => {
            let arr = Value::Array(matches.into_iter().cloned().collect());
            Some(arr.to_string())
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonPathQuery {
    name: &'static str,
    out: Output,
    signature: Signature,
}

impl JsonPathQuery {
    fn new(name: &'static str, out: Output) -> Self {
        Self {
            name,
            out,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonPathQuery {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let json = cast(&arrays[0], &DataType::Utf8)?;
        let path = cast(&arrays[1], &DataType::Utf8)?;
        let json = json.as_any().downcast_ref::<StringArray>().unwrap();
        let path = path.as_any().downcast_ref::<StringArray>().unwrap();
        let mut b = StringBuilder::new();
        for row in 0..args.number_rows {
            if json.is_null(row) || path.is_null(row) {
                b.append_null();
                continue;
            }
            match run(json.value(row), path.value(row), self.out) {
                Some(s) => b.append_value(s),
                None => b.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(b.finish()) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_steps() {
        assert_eq!(parse_path("$.a"), Some(vec![Step::Key("a".into())]));
        assert_eq!(
            parse_path("strict $.a.b"),
            Some(vec![Step::Key("a".into()), Step::Key("b".into())])
        );
        assert_eq!(parse_path("$[0]"), Some(vec![Step::Index(0)]));
        assert_eq!(parse_path("$[*]"), Some(vec![Step::AllElements]));
        assert_eq!(parse_path("$.*"), Some(vec![Step::AllValues]));
        assert_eq!(parse_path(r#"$["a"]"#), Some(vec![Step::Key("a".into())]));
    }

    #[test]
    fn rejects_unsupported_paths() {
        assert_eq!(parse_path("$.a.b ? (@.x == 1)"), None);
        assert_eq!(parse_path("$.type()"), None);
        assert_eq!(parse_path("$[1 to 3]"), None);
        assert_eq!(parse_path("$ starts with \"x\""), None);
        assert_eq!(parse_path("not a path"), None);
    }

    #[test]
    fn run_first_and_array() {
        let j = r#"{"a":{"b":7},"items":[10,20,30]}"#;
        assert_eq!(run(j, "$.a.b", Output::First), Some("7".into()));
        assert_eq!(run(j, "$.items[1]", Output::First), Some("20".into()));
        assert_eq!(run(j, "$.items[*]", Output::Array), Some("[10,20,30]".into()));
        // string match keeps JSON quotes (it's jsonb)
        assert_eq!(run(r#"{"k":"v"}"#, "$.k", Output::First), Some(r#""v""#.into()));
        // missing / unsupported → None
        assert_eq!(run(j, "$.nope", Output::First), None);
        assert_eq!(run(j, "$.a ? (@.b > 1)", Output::First), None);
    }

    async fn ctx() -> datafusion::prelude::SessionContext {
        let mut ctx = datafusion::prelude::SessionContext::new();
        register(&mut ctx).unwrap();
        ctx
    }

    async fn one(ctx: &datafusion::prelude::SessionContext, sql: &str) -> Option<String> {
        use datafusion::arrow::array::AsArray;
        let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = b[0].column(0).as_string::<i32>();
        (!col.is_null(0)).then(|| col.value(0).to_string())
    }

    #[tokio::test]
    async fn jsonb_path_query_via_sql() {
        let ctx = ctx().await;
        assert_eq!(
            one(&ctx, r#"SELECT jsonb_path_query('{"a":{"b":7}}', '$.a.b')"#).await,
            Some("7".into())
        );
        assert_eq!(
            one(&ctx, r#"SELECT jsonb_path_query_array('{"x":[1,2,3]}', '$.x[*]')"#).await,
            Some("[1,2,3]".into())
        );
        // unsupported path → NULL, not an error
        assert_eq!(
            one(&ctx, r#"SELECT jsonb_path_query('{"a":1}', '$ ? (@.a > 0)')"#).await,
            None
        );
    }

    #[tokio::test]
    async fn jsonpath_type_cast_plans_via_analytical_context() {
        // The `::jsonpath` cast needs the JsonTypePlanner (build-time), so use the
        // full analytical context rather than a bare register().
        let ctx = crate::analytical_context().unwrap();
        assert_eq!(
            one(&ctx, r#"SELECT jsonb_path_query('{"a":42}', '$.a'::jsonpath)"#).await,
            Some("42".into())
        );
    }
}
