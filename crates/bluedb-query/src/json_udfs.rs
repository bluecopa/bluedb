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
//!
//! `key` is a string (object field) or an integer (array index). A parse failure,
//! a missing key, or a non-object/array all yield `NULL` — never an error — so a
//! column holding ragged JSON never fails a query.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
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
}
