//! GlueSQL / PostgreSQL function-name compatibility shim for the DataFusion read
//! path.
//!
//! Reads run on DataFusion, whose function library doesn't use every name the SQL
//! reference promises. This module closes the gap two ways:
//!
//! * **Aliases over a built-in** — only where the semantics match *exactly*:
//!   `sign`→`signum`, `rand`→`random`, `hex`→`to_hex`, `find_idx`→`strpos`,
//!   `generate_uuid`→`uuid`, and the aggregates `variance`→`var_samp`,
//!   `stdev`→`stddev_samp`, plus the approximate aggregates
//!   `approx_count_distinct`→`approx_distinct` and
//!   `approx_percentile`/`approx_quantile`→`approx_percentile_cont`. `variance` /
//!   `stdev` follow PostgreSQL (sample, not population), matching DataFusion's
//!   default.
//! * **Small reimplementations** DataFusion lacks: `add_month`, `last_day`,
//!   `mod`, `div`.
//!
//! Deliberately *not* aliased: the `LIST`/`MAP` helpers (`append`, `prepend`,
//! `slice`, `dedup`, …) and the geometry helpers (`point`, …). DataFusion's
//! `array_*`/`map_*` equivalents differ in argument order or semantics
//! (`array_prepend` is element-first; `array_distinct` drops *all* duplicates, not
//! just consecutive ones), so a blind alias would be silently wrong. Use the
//! `array_*`/`map_*` names directly — see `docs/sql/functions.md`.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Date32Array, Date32Builder, Float64Array, Int64Array,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::temporal_conversions::date32_to_datetime;
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::SessionContext;

use chrono::{Datelike, Months, NaiveDate};

/// Register the GlueSQL/PostgreSQL compatibility names on `ctx`. Built-in
/// functions must already be present (they are, after `with_default_features`).
pub fn register(ctx: &mut SessionContext) -> DfResult<()> {
    // Exact-semantics aliases over built-in scalars.
    alias_scalar(ctx, "signum", &["sign"])?;
    alias_scalar(ctx, "random", &["rand"])?;
    alias_scalar(ctx, "uuid", &["generate_uuid"])?;
    alias_scalar(ctx, "strpos", &["find_idx"])?;
    alias_scalar(ctx, "to_hex", &["hex"])?;

    // Aggregates. `variance`/`stdev` are the PostgreSQL sample statistics.
    alias_agg(ctx, "var_samp", &["variance"])?;
    alias_agg(ctx, "stddev_samp", &["stdev"])?;
    alias_agg(ctx, "approx_distinct", &["approx_count_distinct"])?;
    alias_agg(
        ctx,
        "approx_percentile_cont",
        &["approx_percentile", "approx_quantile"],
    )?;

    // Small reimplementations DataFusion has no built-in for.
    ctx.register_udf(Arc::new(ScalarUDF::new_from_impl(AddMonth::new())))?;
    ctx.register_udf(Arc::new(ScalarUDF::new_from_impl(LastDay::new())))?;
    ctx.register_udf(Arc::new(ScalarUDF::new_from_impl(NumBinary::new("mod"))))?;
    ctx.register_udf(Arc::new(ScalarUDF::new_from_impl(NumBinary::new("div"))))?;
    Ok(())
}

/// Re-register a built-in scalar with extra invocation names.
fn alias_scalar(ctx: &mut SessionContext, target: &str, aliases: &[&'static str]) -> DfResult<()> {
    let udf = ctx
        .state()
        .scalar_functions()
        .get(target)
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "gluesql_compat: built-in scalar '{target}' missing"
            ))
        })?;
    let aliased = (*udf).clone().with_aliases(aliases.iter().copied());
    ctx.register_udf(Arc::new(aliased))?;
    Ok(())
}

/// Re-register a built-in aggregate with extra invocation names.
fn alias_agg(ctx: &mut SessionContext, target: &str, aliases: &[&'static str]) -> DfResult<()> {
    let udf = ctx
        .state()
        .aggregate_functions()
        .get(target)
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "gluesql_compat: built-in aggregate '{target}' missing"
            ))
        })?;
    let aliased = (*udf).clone().with_aliases(aliases.iter().copied());
    ctx.register_udaf(Arc::new(aliased))?;
    Ok(())
}

/// Days between the Unix epoch and `d` (the Arrow `Date32` representation).
fn date_to_days(d: NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (d - epoch).num_days() as i32
}

// --- add_month(date, n) -----------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct AddMonth {
    signature: Signature,
}
impl AddMonth {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for AddMonth {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "add_month"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Date32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let dates = cast(&arrays[0], &DataType::Date32)?;
        let dates = dates.as_any().downcast_ref::<Date32Array>().unwrap();
        let months = cast(&arrays[1], &DataType::Int64)?;
        let months = months.as_any().downcast_ref::<Int64Array>().unwrap();

        let mut out = Date32Builder::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            if dates.is_null(row) || months.is_null(row) {
                out.append_null();
                continue;
            }
            let base = match date32_to_datetime(dates.value(row)) {
                Some(dt) => dt.date(),
                None => {
                    out.append_null();
                    continue;
                }
            };
            let n = months.value(row);
            let shifted = if n >= 0 {
                base.checked_add_months(Months::new(n as u32))
            } else {
                base.checked_sub_months(Months::new(n.unsigned_abs() as u32))
            };
            match shifted {
                Some(d) => out.append_value(date_to_days(d)),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }
}

// --- last_day(date) ---------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct LastDay {
    signature: Signature,
}
impl LastDay {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for LastDay {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "last_day"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Date32)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let dates = cast(&arrays[0], &DataType::Date32)?;
        let dates = dates.as_any().downcast_ref::<Date32Array>().unwrap();

        let mut out = Date32Builder::with_capacity(args.number_rows);
        for row in 0..args.number_rows {
            if dates.is_null(row) {
                out.append_null();
                continue;
            }
            let base = match date32_to_datetime(dates.value(row)) {
                Some(dt) => dt.date(),
                None => {
                    out.append_null();
                    continue;
                }
            };
            // First day of next month, minus one day.
            let first_of_next = base
                .checked_add_months(Months::new(1))
                .and_then(|d| d.with_day(1));
            match first_of_next.and_then(|d| d.pred_opt()) {
                Some(d) => out.append_value(date_to_days(d)),
                None => out.append_null(),
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }
}

// --- mod(a, b) / div(a, b) --------------------------------------------------

/// `mod` (remainder) and `div` (truncated quotient) over numerics, as `Float64`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct NumBinary {
    name: &'static str,
    signature: Signature,
}
impl NumBinary {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}
impl ScalarUDFImpl for NumBinary {
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
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let a = cast(&arrays[0], &DataType::Float64)?;
        let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
        let b = cast(&arrays[1], &DataType::Float64)?;
        let b = b.as_any().downcast_ref::<Float64Array>().unwrap();

        let is_mod = self.name == "mod";
        let mut out = Float64Array::builder(args.number_rows);
        for row in 0..args.number_rows {
            if a.is_null(row) || b.is_null(row) || b.value(row) == 0.0 {
                out.append_null();
                continue;
            }
            let (x, y) = (a.value(row), b.value(row));
            out.append_value(if is_mod { x % y } else { (x / y).trunc() });
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Array, AsArray};
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::datatypes::{DataType, Float64Type};

    /// Read the single cell as text (cast Date32/numeric → Utf8 first).
    async fn one_str(sql: &str) -> Option<String> {
        let ctx = crate::analytical_context().unwrap();
        let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let c = cast(b[0].column(0), &DataType::Utf8).unwrap();
        let col = c.as_string::<i32>();
        (!col.is_null(0)).then(|| col.value(0).to_string())
    }

    /// Read the single cell as f64 (cast Int/UInt/… → Float64 first).
    async fn one_f64(sql: &str) -> f64 {
        let ctx = crate::analytical_context().unwrap();
        let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        let c = cast(b[0].column(0), &DataType::Float64).unwrap();
        c.as_primitive::<Float64Type>().value(0)
    }

    #[tokio::test]
    async fn scalar_aliases_resolve() {
        assert!((one_f64("SELECT sign(-3.0)").await + 1.0).abs() < 1e-9);
        assert_eq!(one_f64("SELECT find_idx('hello', 'l')").await, 3.0);
        assert_eq!(one_str("SELECT hex(255)").await.as_deref(), Some("ff"));
        // rand() in [0,1); generate_uuid() is a 36-char hyphenated string.
        let r = one_f64("SELECT rand()").await;
        assert!((0.0..1.0).contains(&r));
        assert_eq!(
            one_str("SELECT generate_uuid()").await.map(|s| s.len()),
            Some(36)
        );
    }

    #[tokio::test]
    async fn aggregate_aliases_resolve() {
        // variance/stdev are the sample statistics (PostgreSQL semantics).
        let v = one_f64("SELECT variance(c) FROM (VALUES (1.0),(2.0),(3.0)) t(c)").await;
        assert!(
            (v - 1.0).abs() < 1e-9,
            "sample variance of 1,2,3 is 1.0; got {v}"
        );
        let s = one_f64("SELECT stdev(c) FROM (VALUES (1.0),(2.0),(3.0)) t(c)").await;
        assert!(
            (s - 1.0).abs() < 1e-9,
            "sample stddev of 1,2,3 is 1.0; got {s}"
        );
        // approximate-aggregate aliases plan and run.
        let n = one_f64("SELECT approx_count_distinct(c) FROM (VALUES (1),(1),(2),(3)) t(c)").await;
        assert_eq!(n, 3.0);
        let p =
            one_f64("SELECT approx_quantile(c, 0.5) FROM (VALUES (1.0),(2.0),(3.0)) t(c)").await;
        assert!(
            (1.0..=3.0).contains(&p),
            "median estimate within range; got {p}"
        );
    }

    #[tokio::test]
    async fn reimplemented_functions() {
        // add_month clamps to the last valid day (Jan 31 + 1mo -> Feb 29, 2024).
        assert_eq!(
            one_str("SELECT add_month(DATE '2024-01-31', 1)")
                .await
                .as_deref(),
            Some("2024-02-29")
        );
        assert_eq!(
            one_str("SELECT add_month(DATE '2024-03-15', -2)")
                .await
                .as_deref(),
            Some("2024-01-15")
        );
        assert_eq!(
            one_str("SELECT last_day(DATE '2024-02-10')")
                .await
                .as_deref(),
            Some("2024-02-29")
        );
        assert_eq!(one_f64("SELECT mod(7.0, 3.0)").await, 1.0);
        assert_eq!(one_f64("SELECT div(7.0, 3.0)").await, 2.0);
    }
}
