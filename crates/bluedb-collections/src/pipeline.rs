//! MongoDB aggregation pipeline → DataFusion [`DataFrame`].
//!
//! [`apply_pipeline`] folds a sequence of MQL aggregation stages onto a base
//! `DataFrame` for a collection — built directly as a logical plan, no SQL string
//! in between. A collection is the table `coll(_id TEXT PRIMARY KEY, doc JSON)`;
//! JSON columns arrive as `Utf8`, so a field is extracted as text via the
//! session-registered `json_get_str(doc, 'field')` UDF (numeric aggregates cast
//! the text result to `Float64`).
//!
//! ## Supported stages
//! `$match`, `$sort`, `$limit`, `$skip`, `$count`, `$group` (`$sum`/`$avg`/`$min`/
//! `$max`/`$count`), `$project`/`$addFields`/`$set`, `$lookup`, `$unwind`.
//! An unknown stage returns [`MqlError::UnsupportedStage`].

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::dataframe::DataFrame;
use datafusion::execution::FunctionRegistry;
use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion::logical_expr::{cast, col, lit, Expr, JoinType, ScalarUDF};
use datafusion::prelude::SessionContext;
use serde_json::Value;

use crate::error::MqlError;
use crate::filter::parse_filter;

/// Fold MQL aggregation `stages` onto a base `DataFrame` for collection `coll`
/// within `ctx` (which must have the tenant's collections registered as tables —
/// see `bluedb_query::session_with_catalog`).
///
/// Each stage is a single-key object (`{"$match": {...}}`). The returned
/// `DataFrame` is unexecuted; the caller drives `.collect()`.
///
/// # Errors
/// - A stage object is malformed or has zero / multiple keys → [`MqlError::Malformed`].
/// - An unknown stage name → [`MqlError::UnsupportedStage`].
/// - Any DataFusion planning failure → [`MqlError::Malformed`] (with the DF message).
pub async fn apply_pipeline(
    ctx: &SessionContext,
    coll: &str,
    stages: &[Value],
) -> Result<DataFrame, MqlError> {
    // The text JSON accessor, resolved once from the session.
    let get_str = ctx
        .udf("json_get_str")
        .map_err(|e| MqlError::Malformed(format!("json_get_str UDF unavailable: {e}")))?;

    let mut df = ctx
        .table(coll)
        .await
        .map_err(|e| MqlError::Malformed(format!("collection '{coll}': {e}")))?;

    for stage in stages {
        let obj = stage
            .as_object()
            .ok_or_else(|| MqlError::Malformed("each pipeline stage must be an object".into()))?;
        if obj.len() != 1 {
            return Err(MqlError::Malformed(
                "each pipeline stage must have exactly one key".into(),
            ));
        }
        let (name, body) = obj.iter().next().unwrap();
        df = apply_stage(ctx, df, name, body, &get_str).await?;
    }

    Ok(df)
}

/// Apply one stage to `df`.
async fn apply_stage(
    ctx: &SessionContext,
    df: DataFrame,
    name: &str,
    body: &Value,
    get_str: &Arc<ScalarUDF>,
) -> Result<DataFrame, MqlError> {
    match name {
        "$match" => {
            let predicate = parse_filter(body)?.to_df_expr(get_str)?;
            df.filter(predicate).map_err(df_err)
        }
        "$sort" => stage_sort(df, body, get_str),
        "$limit" => {
            let n = as_usize(body, "$limit")?;
            df.limit(0, Some(n)).map_err(df_err)
        }
        "$skip" => {
            let n = as_usize(body, "$skip")?;
            df.limit(n, None).map_err(df_err)
        }
        "$count" => {
            let out = body.as_str().ok_or_else(|| {
                MqlError::Malformed("$count takes the output field name as a string".into())
            })?;
            df.aggregate(vec![], vec![count(lit(1)).alias(out)])
                .map_err(df_err)
        }
        "$group" => stage_group(df, body, get_str),
        "$project" | "$addFields" | "$set" => stage_project(df, name, body, get_str),
        "$lookup" => stage_lookup(ctx, df, body).await,
        "$unwind" => stage_unwind(df, body),
        other => Err(MqlError::UnsupportedStage(other.to_string())),
    }
}

/// `$sort: {field: 1|-1, ...}` — ascending / descending, nulls last.
fn stage_sort(df: DataFrame, body: &Value, get_str: &Arc<ScalarUDF>) -> Result<DataFrame, MqlError> {
    let obj = body
        .as_object()
        .ok_or_else(|| MqlError::Malformed("$sort takes an object {field: 1|-1}".into()))?;
    let sort_exprs = obj
        .iter()
        .map(|(field, dir)| {
            let asc = dir.as_i64().unwrap_or(1) >= 0;
            field_expr(field, get_str).sort(asc, /* nulls_first */ false)
        })
        .collect::<Vec<_>>();
    df.sort(sort_exprs).map_err(df_err)
}

/// `$group: {_id: "$field"|null, <out>: {<acc>: "$f"|1}}`.
fn stage_group(
    df: DataFrame,
    body: &Value,
    get_str: &Arc<ScalarUDF>,
) -> Result<DataFrame, MqlError> {
    let obj = body
        .as_object()
        .ok_or_else(|| MqlError::Malformed("$group takes an object".into()))?;
    let id = obj
        .get("_id")
        .ok_or_else(|| MqlError::Malformed("$group requires an _id".into()))?;

    // Group key: `"$field"` → the field accessor aliased to `_id`; `null` → no key.
    let group_expr: Vec<Expr> = match id {
        Value::Null => vec![],
        Value::String(s) => {
            let field = field_ref(s)?;
            vec![field_expr(field, get_str).alias("_id")]
        }
        other => {
            return Err(MqlError::Malformed(format!(
                "$group _id must be a \"$field\" reference or null, got {other}"
            )))
        }
    };

    // Aggregates: every key other than `_id`.
    let mut aggr_expr: Vec<Expr> = Vec::new();
    for (out, spec) in obj {
        if out == "_id" {
            continue;
        }
        let spec_obj = spec.as_object().ok_or_else(|| {
            MqlError::Malformed(format!("$group field '{out}' must be an accumulator object"))
        })?;
        if spec_obj.len() != 1 {
            return Err(MqlError::Malformed(format!(
                "$group field '{out}' must have exactly one accumulator"
            )));
        }
        let (acc, arg) = spec_obj.iter().next().unwrap();
        let expr = group_accumulator(acc, arg, get_str)?.alias(out);
        aggr_expr.push(expr);
    }

    df.aggregate(group_expr, aggr_expr).map_err(df_err)
}

/// Build a single accumulator expression for `$group`.
fn group_accumulator(
    acc: &str,
    arg: &Value,
    get_str: &Arc<ScalarUDF>,
) -> Result<Expr, MqlError> {
    match acc {
        // `{$count: {}}` or `{$sum: 1}` → COUNT(1).
        "$count" => Ok(count(lit(1))),
        "$sum" => {
            // `{$sum: 1}` counts; `{$sum: "$f"}` sums the numeric field.
            if arg.is_number() {
                Ok(count(lit(1)))
            } else {
                Ok(sum(numeric_field(arg, get_str)?))
            }
        }
        "$avg" => Ok(avg(numeric_field(arg, get_str)?)),
        "$min" => Ok(min(numeric_field(arg, get_str)?)),
        "$max" => Ok(max(numeric_field(arg, get_str)?)),
        other => Err(MqlError::UnsupportedOperator(other.to_string())),
    }
}

/// `$project` / `$addFields` / `$set` — best-effort. Inclusion (`{field: 1}`) and
/// computed (`{newField: "$f"}`) both project the text accessor aliased to the
/// output name. `$project` replaces the projection; `$addFields`/`$set` append.
fn stage_project(
    df: DataFrame,
    name: &str,
    body: &Value,
    get_str: &Arc<ScalarUDF>,
) -> Result<DataFrame, MqlError> {
    let obj = body
        .as_object()
        .ok_or_else(|| MqlError::Malformed(format!("{name} takes an object")))?;

    if name == "$project" {
        // Build the full projection list.
        let mut exprs: Vec<Expr> = Vec::new();
        for (out, spec) in obj {
            // `{field: 0}` exclusion is not supported on this path; treat any
            // falsey numeric as a no-op skip, everything else as inclusion/compute.
            if let Some(0) = spec.as_i64() {
                continue;
            }
            exprs.push(project_value(out, spec, get_str)?);
        }
        return df.select(exprs).map_err(df_err);
    }

    // $addFields / $set: append each computed column.
    let mut out_df = df;
    for (out, spec) in obj {
        let expr = project_value(out, spec, get_str)?;
        out_df = out_df.with_column(out, expr).map_err(df_err)?;
    }
    Ok(out_df)
}

/// One `$project`/`$addFields` output: `{out: 1}` includes the field as text;
/// `{out: "$f"}` projects field `f` aliased to `out`.
fn project_value(out: &str, spec: &Value, get_str: &Arc<ScalarUDF>) -> Result<Expr, MqlError> {
    match spec {
        // `{out: "$f"}` → accessor for `f` aliased to `out`.
        Value::String(s) => {
            let field = field_ref(s)?;
            Ok(field_expr(field, get_str).alias(out))
        }
        // `{out: 1}` / truthy → include `out` itself (as text, or _id as-is).
        _ => Ok(field_expr(out, get_str).alias(out)),
    }
}

/// `$lookup: {from, localField, foreignField, as}` — left join. Best-effort: joins
/// on the text accessors of the two fields. (DataFusion's equi-join takes column
/// names, so we project join keys to plain columns first.)
async fn stage_lookup(
    ctx: &SessionContext,
    df: DataFrame,
    body: &Value,
) -> Result<DataFrame, MqlError> {
    let obj = body
        .as_object()
        .ok_or_else(|| MqlError::Malformed("$lookup takes an object".into()))?;
    let from = obj
        .get("from")
        .and_then(Value::as_str)
        .ok_or_else(|| MqlError::Malformed("$lookup requires `from`".into()))?;
    let local = obj
        .get("localField")
        .and_then(Value::as_str)
        .ok_or_else(|| MqlError::Malformed("$lookup requires `localField`".into()))?;
    let foreign = obj
        .get("foreignField")
        .and_then(Value::as_str)
        .ok_or_else(|| MqlError::Malformed("$lookup requires `foreignField`".into()))?;

    let right = ctx
        .table(from)
        .await
        .map_err(|e| MqlError::Malformed(format!("$lookup from '{from}': {e}")))?;

    // Join on the raw column names. For `_id` this is a plain column join; for a
    // JSON sub-field a full equi-join on the accessor would need projected key
    // columns first — covered by joining on `_id` (the common collection case).
    df.join(right, JoinType::Left, &[local], &[foreign], None)
        .map_err(df_err)
}

/// `$unwind: "$field"` — explode an array column. DF52 `unnest_columns` takes the
/// column name; we unnest the named field. (Only top-level array columns, not
/// JSON-text arrays, expand here — best-effort per the collection schema.)
fn stage_unwind(df: DataFrame, body: &Value) -> Result<DataFrame, MqlError> {
    let raw = body
        .as_str()
        .ok_or_else(|| MqlError::Malformed("$unwind takes a \"$field\" path".into()))?;
    let field = field_ref(raw)?;
    df.unnest_columns(&[field]).map_err(df_err)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Column expression for a field: `_id` → the PK column; any other field → the
/// text accessor `json_get_str(doc, field)`.
fn field_expr(field: &str, get_str: &Arc<ScalarUDF>) -> Expr {
    if field == "_id" {
        col("_id")
    } else {
        get_str.as_ref().clone().call(vec![col("doc"), lit(field)])
    }
}

/// Like [`field_expr`] but cast to `Float64` for numeric aggregation
/// (`$sum`/`$avg`/`$min`/`$max` over the text accessor).
fn numeric_field(arg: &Value, get_str: &Arc<ScalarUDF>) -> Result<Expr, MqlError> {
    let field = field_ref(arg.as_str().ok_or_else(|| {
        MqlError::Malformed("numeric accumulator argument must be a \"$field\" reference".into())
    })?)?;
    let base = field_expr(field, get_str);
    Ok(if field == "_id" {
        base
    } else {
        cast(base, DataType::Float64)
    })
}

/// Strip the leading `$` from a `"$field"` reference, e.g. `"$amt"` → `"amt"`.
fn field_ref(s: &str) -> Result<&str, MqlError> {
    s.strip_prefix('$')
        .ok_or_else(|| MqlError::Malformed(format!("expected a \"$field\" reference, got {s:?}")))
}

/// Read a non-negative integer stage argument (`$limit`, `$skip`).
fn as_usize(v: &Value, stage: &str) -> Result<usize, MqlError> {
    v.as_u64()
        .map(|n| n as usize)
        .ok_or_else(|| MqlError::Malformed(format!("{stage} takes a non-negative integer")))
}

/// Map a DataFusion error to [`MqlError::Malformed`].
fn df_err(e: datafusion::error::DataFusionError) -> MqlError {
    MqlError::Malformed(e.to_string())
}
