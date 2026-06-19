//! MongoDB aggregation pipeline → DataFusion [`DataFrame`].
//!
//! [`apply_pipeline`] folds a sequence of MQL aggregation stages onto a base
//! `DataFrame` for a collection — built directly as a logical plan, no SQL string
//! in between. A collection is the table `coll(_id TEXT PRIMARY KEY, doc JSON)`;
//! JSON columns arrive as `Utf8`, so a field is extracted as text via the
//! session-registered `json_get_str(doc, 'field')` UDF (numeric aggregates cast
//! the text result to `Float64`).
//!
//! ## Materialized fields
//! A document field is *not* a real Arrow column — it lives inside the `doc` JSON
//! text. Some stages, though, must turn a field into a real column:
//! `$unwind: "$tags"` first materializes `tags` as an Arrow `List<Utf8>` column
//! (via the `json_array` UDF) and then unnests it. Once a field has been
//! materialized, **every later stage that references it must read the real column,
//! not `json_get_str(doc, …)`** — otherwise a post-`$unwind` `$group {_id:"$tags"}`
//! would re-extract the original array from `doc` instead of grouping the exploded
//! elements. We track this with a `HashSet<String>` of materialized field names
//! threaded through the fold; [`field_expr`] / [`Filter::to_df_expr`] resolve a
//! referenced field to `col(field)` when it is in that set, else to the accessor.
//!
//! ## Supported stages
//! `$match`, `$sort`, `$limit`, `$skip`, `$count`, `$group` (`$sum`/`$avg`/`$min`/
//! `$max`/`$count`), `$project`/`$addFields`/`$set`, `$lookup`, `$unwind`.
//! An unknown stage returns [`MqlError::UnsupportedStage`].

use std::collections::HashSet;
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

/// The UDFs resolved once from the session and threaded through the stage fold,
/// alongside the running set of fields that have been materialized into real
/// Arrow columns (see the module docs).
struct Ctx<'a> {
    session: &'a SessionContext,
    /// `json_get_str(doc, key)` — a JSON field as text.
    get_str: Arc<ScalarUDF>,
    /// `json_array(doc, path)` — a JSON array field as `List<Utf8>` (for `$unwind`).
    json_array: Arc<ScalarUDF>,
    /// Fields already materialized into real columns; a reference to one of these
    /// resolves to `col(field)` rather than the JSON accessor.
    materialized: HashSet<String>,
}

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
    // The text/array JSON accessors, resolved once from the session.
    let get_str = ctx
        .udf("json_get_str")
        .map_err(|e| MqlError::Malformed(format!("json_get_str UDF unavailable: {e}")))?;
    let json_array = ctx
        .udf("json_array")
        .map_err(|e| MqlError::Malformed(format!("json_array UDF unavailable: {e}")))?;

    let mut state = Ctx {
        session: ctx,
        get_str,
        json_array,
        materialized: HashSet::new(),
    };

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
        df = apply_stage(&mut state, df, name, body).await?;
    }

    Ok(df)
}

/// Apply one stage to `df`. Takes `&mut Ctx` because `$unwind`/`$lookup` may add to
/// the materialized-field set, which later stages must observe.
async fn apply_stage(
    ctx: &mut Ctx<'_>,
    df: DataFrame,
    name: &str,
    body: &Value,
) -> Result<DataFrame, MqlError> {
    match name {
        "$match" => {
            let predicate = parse_filter(body)?.to_df_expr(&ctx.get_str, &ctx.materialized)?;
            df.filter(predicate).map_err(df_err)
        }
        "$sort" => stage_sort(df, body, ctx),
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
        "$group" => stage_group(df, body, ctx),
        "$project" | "$addFields" | "$set" => stage_project(df, name, body, ctx),
        "$lookup" => stage_lookup(ctx, df, body).await,
        "$unwind" => stage_unwind(ctx, df, body),
        other => Err(MqlError::UnsupportedStage(other.to_string())),
    }
}

/// `$sort: {field: 1|-1, ...}` — ascending / descending, nulls last.
fn stage_sort(df: DataFrame, body: &Value, ctx: &Ctx<'_>) -> Result<DataFrame, MqlError> {
    let obj = body
        .as_object()
        .ok_or_else(|| MqlError::Malformed("$sort takes an object {field: 1|-1}".into()))?;
    let sort_exprs = obj
        .iter()
        .map(|(field, dir)| {
            let asc = dir.as_i64().unwrap_or(1) >= 0;
            field_expr(field, ctx).sort(asc, /* nulls_first */ false)
        })
        .collect::<Vec<_>>();
    df.sort(sort_exprs).map_err(df_err)
}

/// `$group: {_id: "$field"|null, <out>: {<acc>: "$f"|1}}`.
fn stage_group(df: DataFrame, body: &Value, ctx: &Ctx<'_>) -> Result<DataFrame, MqlError> {
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
            vec![field_expr(field, ctx).alias("_id")]
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
        let expr = group_accumulator(acc, arg, ctx)?.alias(out);
        aggr_expr.push(expr);
    }

    df.aggregate(group_expr, aggr_expr).map_err(df_err)
}

/// Build a single accumulator expression for `$group`.
fn group_accumulator(acc: &str, arg: &Value, ctx: &Ctx<'_>) -> Result<Expr, MqlError> {
    match acc {
        // `{$count: {}}` or `{$sum: 1}` → COUNT(1).
        "$count" => Ok(count(lit(1))),
        "$sum" => {
            // `{$sum: 1}` counts; `{$sum: "$f"}` sums the numeric field.
            if arg.is_number() {
                Ok(count(lit(1)))
            } else {
                Ok(sum(numeric_field(arg, ctx)?))
            }
        }
        "$avg" => Ok(avg(numeric_field(arg, ctx)?)),
        "$min" => Ok(min(numeric_field(arg, ctx)?)),
        "$max" => Ok(max(numeric_field(arg, ctx)?)),
        other => Err(MqlError::UnsupportedOperator(other.to_string())),
    }
}

/// `$project` / `$addFields` / `$set` — best-effort. Inclusion (`{field: 1}`) and
/// computed (`{newField: "$f"}`) both project the field aliased to the output
/// name. `$project` replaces the projection; `$addFields`/`$set` append.
fn stage_project(
    df: DataFrame,
    name: &str,
    body: &Value,
    ctx: &Ctx<'_>,
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
            exprs.push(project_value(out, spec, ctx)?);
        }
        return df.select(exprs).map_err(df_err);
    }

    // $addFields / $set: append each computed column.
    let mut out_df = df;
    for (out, spec) in obj {
        let expr = project_value(out, spec, ctx)?;
        out_df = out_df.with_column(out, expr).map_err(df_err)?;
    }
    Ok(out_df)
}

/// One `$project`/`$addFields` output: `{out: 1}` includes the field as text;
/// `{out: "$f"}` projects field `f` aliased to `out`.
fn project_value(out: &str, spec: &Value, ctx: &Ctx<'_>) -> Result<Expr, MqlError> {
    match spec {
        // `{out: "$f"}` → accessor (or materialized column) for `f` aliased to `out`.
        Value::String(s) => {
            let field = field_ref(s)?;
            Ok(field_expr(field, ctx).alias(out))
        }
        // `{out: 1}` / truthy → include `out` itself (as text, or _id as-is).
        _ => Ok(field_expr(out, ctx).alias(out)),
    }
}

/// `$lookup: {from, localField, foreignField, as}` — left-outer join that brings
/// the matched foreign document through under the `as` name.
///
/// The join keys are *materialized* first so the join works on document fields,
/// not just real columns:
/// - left key: `col("_id")` if `localField == "_id"`, else `json_get_str(doc, localField)`;
/// - right key: the same over `from`'s `doc`.
///
/// ## What the output looks like (flat, not nested)
/// MongoDB nests the matched foreign docs into an **array** field `as`. Producing
/// that array in DataFusion 52 would mean `array_agg(right.doc)` grouped back to
/// the left row — but the resulting `List<Utf8>` column cannot be serialized to a
/// JSON array by the server's response encoder (it renders list columns via a
/// debug fallback, and only the `doc` column is re-inflated from JSON text). The
/// nested shape would therefore reach the client as a garbage debug string.
///
/// So this is a **flat left join**: the matched foreign `doc` is projected through
/// as a single JSON-text column named `as` (one output row per match; left rows
/// with no match get a NULL `as`). The left collection's own `_id` / `doc` are
/// preserved unchanged. Callers that want the strict MongoDB array shape should
/// wrap the `as` value themselves; this is documented as a known limitation.
async fn stage_lookup(
    ctx: &mut Ctx<'_>,
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
    let as_name = obj
        .get("as")
        .and_then(Value::as_str)
        .ok_or_else(|| MqlError::Malformed("$lookup requires `as`".into()))?;

    // Temp join-key columns — names that can't collide with `_id`/`doc`/`as`.
    let left_key = "__lookup_lkey";
    let right_key = "__lookup_rkey";

    // Left side: keep `_id` + `doc`, add the materialized local key.
    let left = df
        .with_column(left_key, key_expr(local, &ctx.get_str))
        .map_err(df_err)?;

    // Right side: project to JUST the foreign `doc` (aliased to `as`) + its key,
    // so the post-join schema has no `_id`/`doc` collision with the left side.
    let right = ctx
        .session
        .table(from)
        .await
        .map_err(|e| MqlError::Malformed(format!("$lookup from '{from}': {e}")))?
        .select(vec![
            col("doc").alias(as_name),
            key_expr(foreign, &ctx.get_str).alias(right_key),
        ])
        .map_err(df_err)?;

    // Left-outer join on the materialized keys; drop the temp key columns so the
    // output is the left fields plus the `as` (foreign doc) column.
    let joined = left
        .join(right, JoinType::Left, &[left_key], &[right_key], None)
        .map_err(df_err)?;

    // Project away the two temp key columns, keeping `_id`, `doc`, and `as`.
    let keep: Vec<Expr> = joined
        .schema()
        .fields()
        .iter()
        .filter(|f| f.name() != left_key && f.name() != right_key)
        .map(|f| col(f.name()))
        .collect();
    let out = joined.select(keep).map_err(df_err)?;

    // The `as` column is foreign JSON text, not a materialized scalar field, so it
    // is NOT added to `materialized`. (It re-inflates as JSON in the response.)
    Ok(out)
}

/// `$unwind: "$field"` — materialize the JSON array `field` into a real
/// `List<Utf8>` Arrow column, then unnest it to one row per element.
///
/// `json_array(doc, "field")` builds the list column (empty/missing arrays → an
/// empty list, which `unnest` drops — MongoDB's default). After unnesting, `field`
/// is a real `Utf8` column, so it is recorded as materialized and every later
/// stage referencing `field` reads `col("field")` instead of re-extracting it from
/// `doc`.
fn stage_unwind(ctx: &mut Ctx<'_>, df: DataFrame, body: &Value) -> Result<DataFrame, MqlError> {
    let raw = body
        .as_str()
        .ok_or_else(|| MqlError::Malformed("$unwind takes a \"$field\" path".into()))?;
    let field = field_ref(raw)?.to_string();

    // 1) materialize the array field as a real List<Utf8> column named `field`,
    // 2) unnest → one row per element (`field` becomes Utf8).
    let array_expr = ctx
        .json_array
        .as_ref()
        .clone()
        .call(vec![col("doc"), lit(field.as_str())]);
    let out = df
        .with_column(&field, array_expr)
        .map_err(df_err)?
        .unnest_columns(&[&field])
        .map_err(df_err)?;

    // Downstream stages must now read the real column, not json_get_str(doc, field).
    ctx.materialized.insert(field);
    Ok(out)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Column expression for a referenced field. `_id` → the PK column; a field that
/// has been materialized into a real column → `col(field)`; any other field → the
/// text accessor `json_get_str(doc, field)`.
fn field_expr(field: &str, ctx: &Ctx<'_>) -> Expr {
    if field == "_id" {
        col("_id")
    } else if ctx.materialized.contains(field) {
        col(field)
    } else {
        ctx.get_str.as_ref().clone().call(vec![col("doc"), lit(field)])
    }
}

/// Like [`field_expr`] but cast to `Float64` for numeric aggregation
/// (`$sum`/`$avg`/`$min`/`$max`). `_id` and materialized columns are not cast — a
/// materialized `$unwind` element is already the right type for the aggregate, and
/// casting `_id` would be wrong.
fn numeric_field(arg: &Value, ctx: &Ctx<'_>) -> Result<Expr, MqlError> {
    let field = field_ref(arg.as_str().ok_or_else(|| {
        MqlError::Malformed("numeric accumulator argument must be a \"$field\" reference".into())
    })?)?;
    let base = field_expr(field, ctx);
    Ok(if field == "_id" || ctx.materialized.contains(field) {
        base
    } else {
        cast(base, DataType::Float64)
    })
}

/// The join-key expression for a `$lookup` field over a `(_id, doc)` collection:
/// `_id` → the PK column; any other field → `json_get_str(doc, field)` (text).
fn key_expr(field: &str, get_str: &Arc<ScalarUDF>) -> Expr {
    if field == "_id" {
        col("_id")
    } else {
        get_str.as_ref().clone().call(vec![col("doc"), lit(field)])
    }
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
