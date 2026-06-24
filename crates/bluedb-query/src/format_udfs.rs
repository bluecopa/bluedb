//! Postgres formatting functions as DataFusion `ScalarUDF`s: `to_number`,
//! `format`, and a numeric `to_char`.
//!
//! These give input-table-v2 the Postgres formatting parity it relies on. They
//! are pragmatic subsets, not bug-for-bug clones:
//!
//! * `to_number(text, fmt)` → `Float64`: strips group separators / currency and
//!   parses the remaining number. The mask is advisory (US-style `.`/`,`).
//! * `format(fmt, ...)` → text: `%s`/`%I`/`%L` substitute the next argument as
//!   text; `%%` is a literal `%`.
//! * `to_char(numeric, fmt)` → text: digit count after `.`/`D` sets the decimals,
//!   `,`/`G` enables thousands grouping. A non-numeric first argument is delegated
//!   to DataFusion's built-in (temporal) `to_char`, captured at registration, so
//!   the date/timestamp form keeps working.

use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, StringArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result as DfResult};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

/// Register `to_number`, `format`, and the numeric-aware `to_char` on `ctx`.
pub fn register(ctx: &mut datafusion::prelude::SessionContext) -> DfResult<()> {
    use datafusion::execution::FunctionRegistry;

    // Capture the built-in (temporal) to_char before overwriting, so the
    // numeric-aware replacement can delegate date/timestamp calls to it.
    let builtin_to_char = ctx.udf("to_char").ok();

    ctx.register_udf(Arc::new(ScalarUDF::new_from_impl(ToNumber::new())))?;
    ctx.register_udf(Arc::new(ScalarUDF::new_from_impl(Format::new())))?;
    ctx.register_udf(Arc::new(ScalarUDF::new_from_impl(ToChar::new(
        builtin_to_char,
    ))))?;
    Ok(())
}

// --- to_number --------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct ToNumber {
    signature: Signature,
}

impl ToNumber {
    fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for ToNumber {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "to_number"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Float64)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let text = cast(&arrays[0], &DataType::Utf8)?;
        let text = text.as_any().downcast_ref::<StringArray>().unwrap();
        let mut out = Float64Array::builder(args.number_rows);
        for row in 0..args.number_rows {
            if text.is_null(row) {
                out.append_null();
            } else {
                match parse_number(text.value(row)) {
                    Some(n) => out.append_value(n),
                    None => out.append_null(),
                }
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }
}

/// Strip group separators / currency and parse what's left as `f64`. `None` if it
/// doesn't parse (e.g. empty after stripping).
fn parse_number(text: &str) -> Option<f64> {
    let cleaned: String = text
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == '-' || *c == '+')
        .collect();
    cleaned.parse::<f64>().ok()
}

// --- format -----------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
struct Format {
    signature: Signature,
}

impl Format {
    fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Format {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "format"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        if args.args.is_empty() {
            return exec_err!("format() requires at least a format string");
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        // Cast every argument to text so any type can be substituted.
        let cols: Vec<ArrayRef> = arrays
            .iter()
            .map(|a| cast(a, &DataType::Utf8))
            .collect::<Result<_, _>>()?;
        let as_str: Vec<&StringArray> = cols
            .iter()
            .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap())
            .collect();

        let mut out = StringBuilder::new();
        for row in 0..args.number_rows {
            if as_str[0].is_null(row) {
                out.append_null();
                continue;
            }
            let fmt = as_str[0].value(row);
            let row_args: Vec<Option<&str>> = as_str[1..]
                .iter()
                .map(|a| (!a.is_null(row)).then(|| a.value(row)))
                .collect();
            out.append_value(format_string(fmt, &row_args));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }
}

/// Substitute `%s`/`%I`/`%L` with successive `args` (as text); `%%` → `%`. A
/// missing argument substitutes the empty string; an unknown `%x` is left as-is.
fn format_string(fmt: &str, args: &[Option<&str>]) -> String {
    let mut out = String::with_capacity(fmt.len());
    let mut chars = fmt.chars().peekable();
    let mut arg_idx = 0usize;
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('%') => out.push('%'),
            Some('s') | Some('I') | Some('L') => {
                if let Some(Some(a)) = args.get(arg_idx) {
                    out.push_str(a);
                }
                arg_idx += 1;
            }
            Some(other) => {
                out.push('%');
                out.push(other);
            }
            None => out.push('%'),
        }
    }
    out
}

// --- to_char (numeric, with temporal delegation) ----------------------------

#[derive(Debug)]
struct ToChar {
    signature: Signature,
    /// DataFusion's built-in (temporal) to_char, for non-numeric first args.
    temporal: Option<Arc<ScalarUDF>>,
}

impl ToChar {
    fn new(temporal: Option<Arc<ScalarUDF>>) -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            temporal,
        }
    }
}

// `Arc<ScalarUDF>` isn't `Eq`/`Hash`, so identify a `ToChar` by its (singleton)
// name — there is only ever one registered.
impl PartialEq for ToChar {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for ToChar {}
impl std::hash::Hash for ToChar {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for ToChar {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "to_char"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arg0 = args.arg_fields[0].data_type();
        // A non-numeric first arg (date/timestamp/…) is delegated to the built-in.
        if !is_numeric(arg0) {
            return match &self.temporal {
                Some(udf) => udf.invoke_with_args(args),
                None => {
                    exec_err!("to_char: non-numeric argument and no temporal to_char available")
                }
            };
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let nums = cast(&arrays[0], &DataType::Float64)?;
        let nums = nums.as_any().downcast_ref::<Float64Array>().unwrap();
        let masks = cast(&arrays[1], &DataType::Utf8)?;
        let masks = masks.as_any().downcast_ref::<StringArray>().unwrap();

        let mut out = StringBuilder::new();
        for row in 0..args.number_rows {
            if nums.is_null(row) || masks.is_null(row) {
                out.append_null();
            } else {
                out.append_value(format_numeric(nums.value(row), masks.value(row)));
            }
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish()) as ArrayRef))
    }
}

fn is_numeric(dt: &DataType) -> bool {
    use DataType::*;
    matches!(
        dt,
        Int8 | Int16
            | Int32
            | Int64
            | UInt8
            | UInt16
            | UInt32
            | UInt64
            | Float16
            | Float32
            | Float64
            | Decimal128(_, _)
            | Decimal256(_, _)
    )
}

/// Format `value` per a Postgres numeric mask: digits after `.`/`D` set the
/// number of decimals; `,`/`G` enables thousands grouping. (`FM`, `9`, `0` are
/// recognised loosely — output is always the minimal, unpadded form.)
fn format_numeric(value: f64, mask: &str) -> String {
    let decimals = mask
        .rsplit_once(['.', 'D'])
        .map(|(_, frac)| frac.chars().filter(|c| *c == '9' || *c == '0').count())
        .unwrap_or(0);
    let grouping = mask.contains(',') || mask.contains('G');

    let neg = value.is_sign_negative() && value != 0.0;
    let rounded = format!("{:.*}", decimals, value.abs());
    let (int_part, frac_part) = match rounded.split_once('.') {
        Some((i, f)) => (i.to_string(), Some(f.to_string())),
        None => (rounded, None),
    };
    let int_part = if grouping {
        group_thousands(&int_part)
    } else {
        int_part
    };

    let mut out = String::new();
    if neg {
        out.push('-');
    }
    out.push_str(&int_part);
    if let Some(f) = frac_part {
        out.push('.');
        out.push_str(&f);
    }
    out
}

/// Insert `,` thousands separators into a run of digits.
fn group_thousands(digits: &str) -> String {
    let n = digits.len();
    let mut out = String::with_capacity(n + n / 3);
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (n - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_number_strips_groups_and_currency() {
        assert_eq!(parse_number("1,234.50"), Some(1234.50));
        assert_eq!(parse_number("$1,000"), Some(1000.0));
        assert_eq!(parse_number("-42"), Some(-42.0));
        assert_eq!(parse_number("nope"), None);
    }

    #[test]
    fn format_string_substitutes_and_escapes() {
        assert_eq!(
            format_string("%s has %s", &[Some("a"), Some("b")]),
            "a has b"
        );
        assert_eq!(format_string("100%%", &[]), "100%");
        assert_eq!(format_string("%s", &[None]), "");
    }

    #[test]
    fn format_numeric_decimals_and_grouping() {
        assert_eq!(format_numeric(1234.5, "FM9,999.00"), "1,234.50");
        assert_eq!(format_numeric(1234.5, "9999.0"), "1234.5");
        assert_eq!(format_numeric(1000000.0, "9G999G999"), "1,000,000");
        assert_eq!(format_numeric(-12.0, "9990.99"), "-12.00");
        assert_eq!(format_numeric(5.0, "999"), "5");
    }

    async fn ctx() -> datafusion::prelude::SessionContext {
        let mut ctx = datafusion::prelude::SessionContext::new();
        register(&mut ctx).unwrap();
        ctx
    }

    async fn one_string(ctx: &datafusion::prelude::SessionContext, sql: &str) -> String {
        use datafusion::arrow::array::AsArray;
        let b = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        b[0].column(0).as_string::<i32>().value(0).to_string()
    }

    #[tokio::test]
    async fn to_char_numeric_via_sql() {
        let ctx = ctx().await;
        assert_eq!(
            one_string(&ctx, "SELECT to_char(1234.5, 'FM9,999.00')").await,
            "1,234.50"
        );
    }

    #[tokio::test]
    async fn format_via_sql() {
        let ctx = ctx().await;
        assert_eq!(
            one_string(&ctx, "SELECT format('%s/%s', 'a', 'b')").await,
            "a/b"
        );
    }

    #[tokio::test]
    async fn to_number_via_sql() {
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::Float64Type;
        let ctx = ctx().await;
        let b = ctx
            .sql("SELECT to_number('1,234.50', '9G999D99')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let v = b[0].column(0).as_primitive::<Float64Type>().value(0);
        assert!((v - 1234.50).abs() < 1e-9, "got {v}");
    }

    #[tokio::test]
    async fn temporal_to_char_still_works() {
        // The numeric to_char must delegate a timestamp arg to the built-in.
        let ctx = ctx().await;
        let out = one_string(
            &ctx,
            "SELECT to_char(arrow_cast('2024-03-15', 'Date32'), '%Y/%m/%d')",
        )
        .await;
        assert_eq!(out, "2024/03/15");
    }
}
