//! JSON containment operators over JSON-as-`Utf8`: Postgres `@>` (contains) and
//! `<@` (contained-by). Hand-rolled on `serde_json` + an [`ExprPlanner`] that
//! rewrites the operators to a `json_contains` UDF (DataFusion 52 parses them but
//! has no physical implementation).
//!
//! Containment follows Postgres `jsonb` `@>` exactly (recursive): an object
//! contains an object when every key/value is contained; an array contains an
//! array when every element is contained in some element; an array contains a
//! scalar equal to one of its elements; scalars contain equal scalars. A parse
//! failure yields `NULL` (never an error).
//!
//! The `?` / `?|` / `?&` key-existence operators are **not** supported: DataFusion's
//! SQL dialect reserves `?` for parameter placeholders, so `data ? 'k'` fails at
//! parse time — before any [`ExprPlanner`] hook runs — and there is no dialect
//! switch for it short of forking DataFusion's parser.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanBuilder, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DfResult;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Register the `json_contains` UDF and the `@>` / `<@` operator rewrite on `ctx`.
pub fn register(ctx: &mut datafusion::prelude::SessionContext) -> DfResult<()> {
    use datafusion::execution::FunctionRegistry;

    let contains = Arc::new(ScalarUDF::new_from_impl(JsonContains::new()));
    ctx.register_udf(contains.clone())?;
    ctx.register_expr_planner(Arc::new(JsonOpsExprPlanner { contains }))?;
    Ok(())
}

/// Parse a JSON cell, `None` on failure (so a query never errors on ragged JSON).
fn parse(s: &str) -> Option<serde_json::Value> {
    serde_json::from_str(s).ok()
}

/// `a @> b` — Postgres `jsonb` deep containment.
fn jsonb_contains(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    use serde_json::Value::{Array, Object};
    match (a, b) {
        (Object(ao), Object(bo)) => bo
            .iter()
            .all(|(k, bv)| ao.get(k).is_some_and(|av| jsonb_contains(av, bv))),
        (Array(aa), Array(ba)) => ba
            .iter()
            .all(|be| aa.iter().any(|ae| jsonb_contains(ae, be))),
        // A scalar is contained in an array that has it as an element.
        (Array(aa), scalar) => aa.iter().any(|ae| ae == scalar),
        // Two scalars (or mismatched composite kinds): equal.
        (a, b) => a == b,
    }
}

fn str_at<'a>(arr: &'a StringArray, row: usize) -> Option<&'a str> {
    (!arr.is_null(row)).then(|| arr.value(row))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonContains {
    signature: Signature,
}
impl JsonContains {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for JsonContains {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "json_contains"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Boolean)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        // Operands may be Utf8View (DataFusion-52 string literals) — cast to Utf8.
        let a = cast(&arrays[0], &DataType::Utf8)?;
        let b = cast(&arrays[1], &DataType::Utf8)?;
        let a = a.as_any().downcast_ref::<StringArray>().unwrap();
        let b = b.as_any().downcast_ref::<StringArray>().unwrap();
        let mut out = BooleanBuilder::new();
        for row in 0..args.number_rows {
            match (str_at(a, row), str_at(b, row)) {
                (Some(at), Some(bt)) => match (parse(at), parse(bt)) {
                    (Some(av), Some(bv)) => out.append_value(jsonb_contains(&av, &bv)),
                    _ => out.append_null(),
                },
                _ => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }
}

/// Rewrites `@>` (contains) and `<@` (contained-by) to `json_contains` calls.
#[derive(Debug)]
struct JsonOpsExprPlanner {
    contains: Arc<ScalarUDF>,
}

impl ExprPlanner for JsonOpsExprPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &datafusion::common::DFSchema,
    ) -> DfResult<PlannerResult<RawBinaryExpr>> {
        use datafusion::sql::sqlparser::ast::BinaryOperator as Op;
        let RawBinaryExpr { op, left, right } = expr;
        let planned = match op {
            Op::AtArrow => self.contains.as_ref().clone().call(vec![left, right]),
            // `a <@ b` ≡ `b @> a`.
            Op::ArrowAt => self.contains.as_ref().clone().call(vec![right, left]),
            _ => return Ok(PlannerResult::Original(RawBinaryExpr { op, left, right })),
        };
        Ok(PlannerResult::Planned(planned))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn contains_objects_arrays_and_scalars() {
        let c = |a: serde_json::Value, b: serde_json::Value| jsonb_contains(&a, &b);
        // object containment, recursive
        assert!(c(json!({"a": 1, "b": 2}), json!({"a": 1})));
        assert!(c(json!({"a": {"x": 1, "y": 2}}), json!({"a": {"x": 1}})));
        assert!(!c(json!({"a": 1}), json!({"a": 2})));
        assert!(!c(json!({"a": 1}), json!({"b": 1})));
        // array containment
        assert!(c(json!([1, 2, 3]), json!([1, 3])));
        assert!(c(json!([1, 2, 3]), json!(2))); // scalar in array
        assert!(!c(json!([1, 2, 3]), json!(5)));
        // scalar equality
        assert!(c(json!("foo"), json!("foo")));
        assert!(!c(json!(1), json!(2)));
    }

    async fn ctx() -> datafusion::prelude::SessionContext {
        let mut ctx = datafusion::prelude::SessionContext::new();
        register(&mut ctx).unwrap();
        ctx
    }

    async fn one_bool(ctx: &datafusion::prelude::SessionContext, sql: &str) -> Option<bool> {
        use datafusion::arrow::array::AsArray;
        let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let col = b[0].column(0).as_boolean();
        (!col.is_null(0)).then(|| col.value(0))
    }

    #[tokio::test]
    async fn at_arrow_and_arrow_at_via_sql() {
        let ctx = ctx().await;
        assert_eq!(
            one_bool(&ctx, r#"SELECT '{"a":1,"b":2}' @> '{"a":1}'"#).await,
            Some(true)
        );
        assert_eq!(
            one_bool(&ctx, r#"SELECT '{"a":1}' @> '{"a":2}'"#).await,
            Some(false)
        );
        assert_eq!(one_bool(&ctx, r#"SELECT '[1,2,3]' @> '[1,3]'"#).await, Some(true));
        // `<@` is the mirror of `@>`.
        assert_eq!(
            one_bool(&ctx, r#"SELECT '{"a":1}' <@ '{"a":1,"b":2}'"#).await,
            Some(true)
        );
    }
}
