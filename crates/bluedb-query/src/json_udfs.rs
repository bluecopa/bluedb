//! JSON accessors over JSON-as-`Utf8` columns, hand-rolled on `serde_json`.
//!
//! We can't use the upstream `datafusion-functions-json` crate: it pulls
//! `jiter → pyo3 0.28`, which links the native `python` library and so conflicts
//! with this workspace's `bluedb-py` testkit (`pyo3 0.29`) — cargo permits only
//! one `links = "python"`. So we provide the same surface ourselves:
//!
//! * `json_get_str(json, key)` → the field as **text** (Postgres `->>`): a string
//!   value is returned unquoted; numbers/bools/objects/arrays as their JSON text.
//! * `json_get(json, key)` → the field as **JSON** (Postgres `->`): the sub-value's
//!   JSON serialization (a string stays quoted).
//! * the `->` / `->>` operators, rewritten to those functions via an [`ExprPlanner`].
//! * `json_array(json, path)` → Arrow `List<Utf8>` of the array field at `path`
//!   (dotted). Used by the `$unwind` aggregation stage to unnest a JSON array
//!   field into a real Arrow list column that DataFusion can `unnest`.
//!
//! `key` is a string (object field) or an integer (array index). A parse failure,
//! a missing key, or a non-object/array all yield `NULL` — never an error — so a
//! column holding ragged JSON never fails a query.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, ListBuilder, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DFSchema, Result as DfResult};
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr, TypePlanner};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Register the JSON accessor functions and the `->` / `->>` operator rewrite on
/// `ctx`. After this, `data->>'k'`, `data->'k'`, `json_get_str(data,'k')` and
/// `json_get(data,'k')` all work over a JSON-as-`Utf8` column.
pub fn register(ctx: &mut datafusion::prelude::SessionContext) -> DfResult<()> {
    use datafusion::execution::FunctionRegistry;

    let get = Arc::new(ScalarUDF::new_from_impl(JsonAccessor::new("json_get", Mode::Json)));
    let get_str = Arc::new(ScalarUDF::new_from_impl(JsonAccessor::new(
        "json_get_str",
        Mode::Text,
    )));
    ctx.register_udf(get.clone())?;
    ctx.register_udf(get_str.clone())?;
    ctx.register_expr_planner(Arc::new(JsonExprPlanner { get, get_str }))?;
    ctx.register_udf(Arc::new(ScalarUDF::new_from_impl(JsonArrayUdf::new())))?;
    Ok(())
}

/// How an extracted sub-value is rendered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Mode {
    /// Postgres `->>`: text — a string unquoted, everything else as JSON text.
    Text,
    /// Postgres `->`: the sub-value's JSON serialization (a string stays quoted).
    Json,
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonAccessor {
    name: &'static str,
    mode: Mode,
    signature: Signature,
}

impl JsonAccessor {
    fn new(name: &'static str, mode: Mode) -> Self {
        Self {
            name,
            mode,
            // (json_text, key) — key is a string field or an integer index.
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonAccessor {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let json = arrays[0].as_any().downcast_ref::<StringArray>();
        let key_str = arrays[1].as_any().downcast_ref::<StringArray>();
        let key_int = arrays[1].as_any().downcast_ref::<Int64Array>();

        let n = args.number_rows;
        let mut out = StringBuilder::new();
        for row in 0..n {
            let value = json.and_then(|j| (!j.is_null(row)).then(|| j.value(row)));
            let result = value.and_then(|text| {
                if let Some(ks) = key_str {
                    if !ks.is_null(row) {
                        return extract(text, &Key::Field(ks.value(row)), self.mode);
                    }
                }
                if let Some(ki) = key_int {
                    if !ki.is_null(row) {
                        return extract(text, &Key::Index(ki.value(row)), self.mode);
                    }
                }
                None
            });
            match result {
                Some(s) => out.append_value(s),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }
}

/// A JSON navigation step.
enum Key<'a> {
    Field(&'a str),
    Index(i64),
}

/// Extract `key` from the JSON `text` and render it per `mode`. `None` on parse
/// failure, missing key, or a value that can't be navigated.
fn extract(text: &str, key: &Key, mode: Mode) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(text).ok()?;
    let sub = match key {
        Key::Field(k) => value.get(k)?,
        Key::Index(i) => {
            let idx = usize::try_from(*i).ok()?;
            value.get(idx)?
        }
    };
    Some(match mode {
        // `->>`: a string is returned raw; everything else as its JSON text.
        Mode::Text => match sub {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        },
        // `->`: the sub-value's JSON serialization (a string stays quoted).
        Mode::Json => sub.to_string(),
    })
}

/// Rewrites the `->` and `->>` binary operators to `json_get` / `json_get_str`
/// calls. DataFusion 52 has no native arrow operators, but exposes this hook.
#[derive(Debug)]
struct JsonExprPlanner {
    get: Arc<ScalarUDF>,
    get_str: Arc<ScalarUDF>,
}

impl ExprPlanner for JsonExprPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> DfResult<PlannerResult<RawBinaryExpr>> {
        use datafusion::sql::sqlparser::ast::BinaryOperator;
        let udf = match expr.op {
            BinaryOperator::Arrow => &self.get,
            BinaryOperator::LongArrow => &self.get_str,
            _ => return Ok(PlannerResult::Original(expr)),
        };
        Ok(PlannerResult::Planned(
            udf.as_ref().clone().call(vec![expr.left, expr.right]),
        ))
    }
}

// ── json_array UDF ─────────────────────────────────────────────────────────

/// Extract the value at a dotted `path` from `doc_text` as a list of strings.
///
/// Semantics (mirrors MongoDB `$unwind` input handling):
/// - The target value is a JSON array → each element, strings unquoted and
///   other scalars/objects rendered as their compact JSON text.
/// - The target value is a non-array scalar or object → treated as a
///   single-element list (MongoDB wraps it, so we do too).
/// - Path is missing, value is JSON `null`, JSON parse fails, or any
///   intermediate navigation step fails → empty list (the row will be dropped
///   by the `$unwind` unnest, which is Mongo's default behaviour).
///
/// Path navigation: dot-separated field names (`"a.b.c"`).
pub(crate) fn extract_array(doc_text: &str, path: &str) -> Vec<String> {
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(doc_text) else {
        return vec![];
    };
    // Navigate each segment of the dotted path.
    for segment in path.split('.') {
        match value {
            serde_json::Value::Object(mut map) => match map.remove(segment) {
                Some(v) => value = v,
                None => return vec![],
            },
            _ => return vec![],
        }
    }
    // Render the reached value as a list of strings.
    match value {
        serde_json::Value::Null => vec![],
        serde_json::Value::Array(arr) => arr
            .into_iter()
            .map(|elem| match elem {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            })
            .collect(),
        // Non-array: wrap in a single-element list (Mongo's scalar-as-array rule).
        serde_json::Value::String(s) => vec![s],
        other => vec![other.to_string()],
    }
}

/// Return type for `json_array`: `List<item: Utf8 nullable>`.
fn json_array_return_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonArrayUdf {
    signature: Signature,
}

impl JsonArrayUdf {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonArrayUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_array"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(json_array_return_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let json_col = arrays[0].as_any().downcast_ref::<StringArray>();
        let path_col = arrays[1].as_any().downcast_ref::<StringArray>();

        let n = args.number_rows;
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in 0..n {
            let doc = json_col.and_then(|c| (!c.is_null(row)).then(|| c.value(row)));
            let path = path_col.and_then(|c| (!c.is_null(row)).then(|| c.value(row)));
            match (doc, path) {
                (Some(doc_text), Some(path_str)) => {
                    let elems = extract_array(doc_text, path_str);
                    for elem in &elems {
                        builder.values().append_value(elem);
                    }
                    builder.append(true);
                }
                _ => {
                    // NULL input → empty (non-null) list so $unwind drops the row.
                    builder.append(true);
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// Maps the SQL `JSON` / `JSONB` types to Arrow `Utf8`, so `CAST(x AS JSONB)` and
/// a JSONB column type plan instead of erroring with "Unsupported SQL type JSONB".
/// Consistent with bluedb's text-backed JSON (`normalize_data_type` does the same
/// `JSON`/`JSONB` → `TEXT` on the GlueSQL side). Registered via the SessionState
/// builder's `with_type_planner` (type planning is build-time, not a UDF).
#[derive(Debug)]
pub(crate) struct JsonTypePlanner;

impl TypePlanner for JsonTypePlanner {
    fn plan_type(
        &self,
        sql_type: &datafusion::sql::sqlparser::ast::DataType,
    ) -> DfResult<Option<DataType>> {
        use datafusion::sql::sqlparser::ast::DataType as SqlDt;
        // JSON / JSONB, and the `jsonpath` custom type (a path literal is just the
        // text we hand to jsonb_path_query) → Utf8.
        let is_text_json = matches!(sql_type, SqlDt::JSON | SqlDt::JSONB)
            || matches!(sql_type, SqlDt::Custom(name, _)
                if name.to_string().eq_ignore_ascii_case("jsonpath"));
        Ok(is_text_json.then_some(DataType::Utf8))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_text_unquotes_strings_and_stringifies_scalars() {
        let json = r#"{"status":"active","n":3,"ok":true,"nested":{"a":1}}"#;
        assert_eq!(extract(json, &Key::Field("status"), Mode::Text), Some("active".into()));
        assert_eq!(extract(json, &Key::Field("n"), Mode::Text), Some("3".into()));
        assert_eq!(extract(json, &Key::Field("ok"), Mode::Text), Some("true".into()));
        // An object as text is its compact JSON.
        assert_eq!(extract(json, &Key::Field("nested"), Mode::Text), Some(r#"{"a":1}"#.into()));
    }

    #[test]
    fn extract_json_keeps_string_quotes() {
        let json = r#"{"status":"active"}"#;
        assert_eq!(extract(json, &Key::Field("status"), Mode::Json), Some(r#""active""#.into()));
    }

    #[test]
    fn extract_array_index() {
        // Direct index on a top-level JSON array.
        assert_eq!(extract("[10,20,30]", &Key::Index(1), Mode::Text), Some("20".into()));
    }

    #[test]
    fn extract_missing_or_ragged_is_none() {
        assert_eq!(extract(r#"{"a":1}"#, &Key::Field("b"), Mode::Text), None);
        assert_eq!(extract("not json", &Key::Field("a"), Mode::Text), None);
        assert_eq!(extract("42", &Key::Field("a"), Mode::Text), None);
    }

    // Wire the UDFs + operator into a real SessionContext over an in-memory table.
    async fn ctx_with_docs() -> datafusion::prelude::SessionContext {
        use datafusion::arrow::array::{Int32Array, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;

        let mut ctx = datafusion::prelude::SessionContext::new();
        register(&mut ctx).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("data", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![
                    r#"{"status":"active","n":90}"#,
                    r#"{"status":"idle","n":40}"#,
                ])),
            ],
        )
        .unwrap();
        ctx.register_batch("docs", batch).unwrap();
        ctx
    }

    #[tokio::test]
    async fn long_arrow_operator_filters_and_projects() {
        let ctx = ctx_with_docs().await;
        // DataFusion's dialect gives `->>` lower precedence than `=`, so the
        // comparison side is parenthesised (the generated SQL does this, or the
        // robust function form is used). The operator rewrite + UDF are what's
        // under test here.
        let df = ctx
            .sql("SELECT id, data->>'status' AS status FROM docs WHERE (data->>'status') = 'active'")
            .await
            .expect("plan ->> query");
        let batches = df.collect().await.expect("run ->> query");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1, "only the active row");
    }

    #[tokio::test]
    async fn json_get_str_function_call_works() {
        let ctx = ctx_with_docs().await;
        let df = ctx
            .sql("SELECT json_get_str(data, 'status') AS s FROM docs ORDER BY id")
            .await
            .expect("plan json_get_str");
        let batches = df.collect().await.expect("run json_get_str");
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
    }

    #[tokio::test]
    async fn json_type_planner_accepts_cast_as_jsonb() {
        use datafusion::arrow::array::AsArray;
        // The full analytical context carries the JSON/JSONB→Utf8 TypePlanner.
        let ctx = crate::analytical_context().unwrap();

        // CAST(... AS JSONB) plans (→ Utf8) instead of "Unsupported SQL type JSONB".
        let b = ctx
            .sql(r#"SELECT CAST('{"a":1}' AS JSONB) AS j"#)
            .await
            .expect("plan CAST AS JSONB")
            .collect()
            .await
            .unwrap();
        assert_eq!(b[0].column(0).as_string::<i32>().value(0), r#"{"a":1}"#);

        // And it composes with the accessor: (cast result) ->> 'a' → "1".
        let b2 = ctx
            .sql(r#"SELECT CAST('{"a":1}' AS JSONB) ->> 'a' AS v"#)
            .await
            .expect("plan JSONB cast + ->>")
            .collect()
            .await
            .unwrap();
        assert_eq!(b2[0].column(0).as_string::<i32>().value(0), "1");
    }

    // ── extract_array unit tests ────────────────────────────────────────────

    #[test]
    fn extract_array_string_elements() {
        let elems = extract_array(r#"{"tags":["a","b"]}"#, "tags");
        assert_eq!(elems, vec!["a", "b"]);
    }

    #[test]
    fn extract_array_numeric_elements_stringified() {
        let elems = extract_array(r#"{"x":[1,2]}"#, "x");
        assert_eq!(elems, vec!["1", "2"]);
    }

    #[test]
    fn extract_array_non_array_scalar_wrapped() {
        // A non-array value is treated as a single-element list.
        let elems = extract_array(r#"{"x":"solo"}"#, "x");
        assert_eq!(elems, vec!["solo"]);
    }

    #[test]
    fn extract_array_missing_path_empty() {
        let elems = extract_array(r#"{"x":1}"#, "y");
        assert!(elems.is_empty());
    }

    #[test]
    fn extract_array_bad_json_empty() {
        let elems = extract_array("not json", "tags");
        assert!(elems.is_empty());
    }

    #[test]
    fn extract_array_json_null_empty() {
        let elems = extract_array(r#"{"tags":null}"#, "tags");
        assert!(elems.is_empty());
    }

    #[test]
    fn extract_array_nested_dotted_path() {
        let elems = extract_array(r#"{"a":{"b":["v"]}}"#, "a.b");
        assert_eq!(elems, vec!["v"]);
    }

    // ── json_array UDF integration test ────────────────────────────────────

    #[tokio::test]
    async fn json_array_udf_returns_list_column() {
        use datafusion::arrow::array::AsArray;

        let ctx = crate::analytical_context().unwrap();
        let batches = ctx
            .sql(r#"SELECT json_array('{"t":["a","b","c"]}', 't') AS arr"#)
            .await
            .expect("plan json_array")
            .collect()
            .await
            .expect("run json_array");

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "one row out");

        let col = batches[0].column(0);
        // The column is a ListArray; cast and inspect the inner string values.
        let list = col.as_list::<i32>();
        let inner = list.value(0); // the single list for row 0
        let strings = inner.as_string::<i32>();
        assert_eq!(strings.len(), 3);
        assert_eq!(strings.value(0), "a");
        assert_eq!(strings.value(1), "b");
        assert_eq!(strings.value(2), "c");
    }
}
