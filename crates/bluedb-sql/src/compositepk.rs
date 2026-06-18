//! Composite primary keys via a hidden `__bluedb_pk` surrogate column.
//!
//! gluesql 0.19 has no composite-key support (`Key` is scalar; a
//! `PRIMARY KEY(a,b)` table constraint errors in `translate`). So bluedb rewrites
//! such a table — at the SQL-string level, before gluesql sees it — into one with
//! a single hidden `__bluedb_pk BYTEA PRIMARY KEY` whose bytes are the
//! order-preserving concat of the component keys ([`crate::pkcodec`]). The user
//! PK columns are remembered in a per-table [`PkCatalog`].
//!
//! [`prepare`] is the single entry point, called from the execution chokepoint
//! ([`bluedb_engine::rest_sql::execute_sql`]) on every statement:
//! - `CREATE TABLE … PRIMARY KEY(a,b)` → strip the constraint, append
//!   `__bluedb_pk`, persist the catalog, return the rewritten DDL.
//! - `INSERT` into a composite-PK table → compute `__bluedb_pk` from the row's
//!   component values and inject it.
//! - `SELECT`/`UPDATE`/`DELETE` → rewrite component predicates to `__bluedb_pk`
//!   ranges and hide `__bluedb_pk` from `SELECT *` (see [`dml`]).
//!
//! Statements on non-composite tables are returned **unchanged** (the original
//! string, never a re-serialized one) so the shim can never make a query worse.

use gluesql_core::ast::DataType as GlueDataType;
use gluesql_core::data::{Key, Value};
use gluesql_core::store::Store;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    ColumnDef, ColumnOption, ColumnOptionDef, DataType, Expr, Ident, ObjectName, SetExpr,
    Statement, TableConstraint, UnaryOperator, Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::SqlError;
use crate::jsoncat::JsonCatalog;
use crate::pkcodec::encode_composite_key;
use crate::rewrite::normalize_data_type;
use crate::storage::SlateDbStorage;

/// The hidden surrogate primary-key column. Reserved: user columns may not use
/// this name or the `__bluedb_` prefix. Its value is the same string as
/// `storage::PK_PSEUDO_INDEX` (intentional — the clustered PK pseudo-index sits
/// on exactly this column).
pub const PK_COL: &str = "__bluedb_pk";

/// Reserved column-name prefix for bluedb internals.
const RESERVED_PREFIX: &str = "__bluedb_";

/// A table's composite-primary-key catalog: the user PK column names, in key
/// order. Persisted per composite-PK table (absent for single-column PK tables).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PkCatalog {
    pub columns: Vec<String>,
}

/// Rewrite `sql` for composite-primary-key support, persisting/reading the
/// per-table [`PkCatalog`] through `storage`. Returns the SQL to execute —
/// unchanged (the original string) for anything that isn't a composite-PK
/// statement.
///
/// `params` are the bound `$N` values (in `$1`-first order); a PK component
/// written as a placeholder is resolved from them to build `__bluedb_pk`. A
/// multi-statement string (e.g. the data plane's `BEGIN; INSERT…; COMMIT;`) is
/// handled statement-by-statement.
pub async fn prepare(
    storage: &mut SlateDbStorage,
    sql: &str,
    params: &[Value],
) -> Result<String, SqlError> {
    let dialect = GenericDialect {};
    let Ok(mut statements) = Parser::parse_sql(&dialect, sql) else {
        return Ok(sql.to_string()); // not parseable here → let gluesql handle it
    };
    let mut changed = false;
    for stmt in &mut statements {
        changed |= apply(storage, stmt, params).await?;
    }
    // Re-serialize only when we actually rewrote a composite-PK statement; every
    // other statement returns the original string untouched.
    if !changed {
        return Ok(sql.to_string());
    }
    Ok(statements
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join("; "))
}

/// Rewrite `stmt` in place for composite-PK support and data-type normalisation.
/// Returns whether it changed.
///
/// For `CREATE TABLE` statements this always normalises column data types (e.g.
/// `DECIMAL(12,2)` → bare `DECIMAL`, `VARCHAR(n)` → `TEXT`, …) so gluesql's
/// translate step never sees a type it would reject with `UnsupportedDataType`.
/// Precision/scale declared in `DECIMAL(p,s)` / `NUMERIC(p,s)` are coerced to
/// gluesql's internal Decimal(38,18); enforcement of the declared p,s is out of
/// scope for this shim.
async fn apply(
    storage: &mut SlateDbStorage,
    stmt: &mut Statement,
    params: &[Value],
) -> Result<bool, SqlError> {
    if let Statement::CreateTable(create) = stmt {
        let table = object_table_name(&create.name);
        // Capture which columns are JSON/JSONB *before* normalisation rewrites
        // them to TEXT (erasing the JSON-ness), so the read path can re-inflate.
        let json_cols = crate::jsoncat::json_columns(create);
        // Normalise column types for ALL CREATE TABLE statements: DECIMAL(p,s) →
        // bare DECIMAL, VARCHAR(n) → TEXT, INT(n) → INTEGER, JSON → TEXT, etc.
        // This is the single execution chokepoint; doing it here means the
        // normalisation applies to every path (REST API, /admin/sql, direct
        // Glue::execute via prepare_composite_pk).
        let mut type_changed = false;
        for col in &mut create.columns {
            if normalize_data_type(&mut col.data_type) {
                type_changed = true;
            }
        }
        if !json_cols.is_empty() {
            storage
                .write_json_catalog(&table, &JsonCatalog { columns: json_cols })
                .await?;
        }
        let Some(catalog) = strip_composite_pk(create)? else {
            return Ok(type_changed);
        };
        storage.write_pk_catalog(&table, &catalog).await?;
        return Ok(true);
    }

    // DML: act only on a table that has a composite-PK catalog.
    let Some(table) = statement_table(stmt) else {
        return Ok(false);
    };
    let Some(catalog) = storage.read_pk_catalog(&table).await? else {
        return Ok(false);
    };
    let columns = fetch_columns(storage, &table).await?;
    let user_cols: Vec<String> = columns.iter().map(|(n, _)| n.clone()).collect();
    let types: std::collections::HashMap<String, GlueDataType> = columns.into_iter().collect();
    match stmt {
        Statement::Insert(insert) => rewrite_insert(insert, &catalog, &user_cols, &types, params)?,
        _ => dml::rewrite(stmt, &catalog, &user_cols, &types, params)?,
    }
    Ok(true)
}

/// The table a DML statement targets (for the composite-catalog lookup).
fn statement_table(stmt: &Statement) -> Option<String> {
    match stmt {
        Statement::Insert(insert) => Some(object_table_name(&insert.table_name)),
        _ => dml::target_table(stmt),
    }
}

/// The user-facing columns of `table` as `(name, type)` in declaration order
/// (schema order minus the hidden `__bluedb_pk`). The names make positional
/// INSERTs explicit and expand `SELECT *`; the types coerce component values to a
/// canonical, surface-independent key encoding (so `a = 1` and `a = '1'` from the
/// data plane agree with inline SQL).
async fn fetch_columns(
    storage: &SlateDbStorage,
    table: &str,
) -> Result<Vec<(String, GlueDataType)>, SqlError> {
    let schema = Store::fetch_schema(storage, table)
        .await
        .map_err(|e| SqlError::CompositePk(e.to_string()))?
        .ok_or_else(|| SqlError::CompositePk(format!("table '{table}' not found")))?;
    let cols = schema
        .column_defs
        .map(|defs| {
            defs.into_iter()
                .filter(|c| c.name != PK_COL)
                .map(|c| (c.name, c.data_type))
                .collect()
        })
        .unwrap_or_default();
    Ok(cols)
}

/// The last segment of an [`ObjectName`] as a plain string (the table name).
pub(crate) fn object_table_name(name: &ObjectName) -> String {
    name.0
        .last()
        .map(|ident| ident.value.clone())
        .unwrap_or_default()
}

/// If `create` declares a composite (≥2 column) `PRIMARY KEY(a,b,…)` table
/// constraint, strip it, force the component columns `NOT NULL`, append the
/// `__bluedb_pk BYTEA NOT NULL PRIMARY KEY` surrogate column, and return the
/// [`PkCatalog`]. Returns `None` if there is no composite PK (single-column PKs
/// and PK column-options are left untouched).
fn strip_composite_pk(
    create: &mut sqlparser::ast::CreateTable,
) -> Result<Option<PkCatalog>, SqlError> {
    // Find a table-level PRIMARY KEY with ≥2 columns.
    let pk_pos = create.constraints.iter().position(|c| {
        matches!(c, TableConstraint::PrimaryKey { columns, .. } if columns.len() >= 2)
    });
    let Some(pk_pos) = pk_pos else {
        return Ok(None);
    };
    let TableConstraint::PrimaryKey { columns, .. } = create.constraints.remove(pk_pos) else {
        unreachable!("position matched a PrimaryKey constraint");
    };
    let component_names: Vec<String> = columns.iter().map(|i| i.value.clone()).collect();

    // No user column may use the reserved name/prefix.
    for col in &create.columns {
        if col.name.value == PK_COL || col.name.value.starts_with(RESERVED_PREFIX) {
            return Err(SqlError::CompositePk(format!(
                "column name '{}' is reserved",
                col.name.value
            )));
        }
    }
    // Every component must be a real column; force it NOT NULL (a PK can't be null).
    for name in &component_names {
        let col = create
            .columns
            .iter_mut()
            .find(|c| &c.name.value == name)
            .ok_or_else(|| {
                SqlError::CompositePk(format!("PRIMARY KEY column '{name}' is not defined"))
            })?;
        force_not_null(col);
    }

    create.columns.push(surrogate_column());
    Ok(Some(PkCatalog {
        columns: component_names,
    }))
}

/// Ensure a column carries `NOT NULL` and not a contradictory `NULL` option.
fn force_not_null(col: &mut ColumnDef) {
    col.options
        .retain(|o| !matches!(o.option, ColumnOption::Null));
    if !col
        .options
        .iter()
        .any(|o| matches!(o.option, ColumnOption::NotNull))
    {
        col.options.push(ColumnOptionDef {
            name: None,
            option: ColumnOption::NotNull,
        });
    }
}

/// The `__bluedb_pk BYTEA NOT NULL PRIMARY KEY` surrogate column definition.
fn surrogate_column() -> ColumnDef {
    ColumnDef {
        name: Ident::new(PK_COL),
        data_type: DataType::Bytea,
        collation: None,
        options: vec![
            ColumnOptionDef {
                name: None,
                option: ColumnOption::NotNull,
            },
            ColumnOptionDef {
                name: None,
                option: ColumnOption::Unique {
                    is_primary: true,
                    characteristics: None,
                },
            },
        ],
    }
}

/// Inject the computed `__bluedb_pk` value into every row of an `INSERT`, making
/// the column list explicit (so a positional insert lines up after the append).
fn rewrite_insert(
    insert: &mut sqlparser::ast::Insert,
    catalog: &PkCatalog,
    user_cols: &[String],
    types: &std::collections::HashMap<String, GlueDataType>,
    params: &[Value],
) -> Result<(), SqlError> {
    // The columns each VALUES row supplies, in order: explicit list if given,
    // else the table's user columns (positional insert).
    let mut row_cols: Vec<String> = if insert.columns.is_empty() {
        user_cols.to_vec()
    } else {
        insert.columns.iter().map(|i| i.value.clone()).collect()
    };
    if row_cols.iter().any(|c| c == PK_COL) {
        return Err(SqlError::CompositePk(format!(
            "column '{PK_COL}' is reserved and cannot be set directly"
        )));
    }
    // Each PK component: its position in the row + its declared type (for coercion).
    let mut component_idx = Vec::with_capacity(catalog.columns.len());
    for pk in &catalog.columns {
        let idx = row_cols.iter().position(|c| c == pk).ok_or_else(|| {
            SqlError::CompositePk(format!("INSERT omits primary-key column '{pk}'"))
        })?;
        component_idx.push((idx, types.get(pk)));
    }

    let Some(source) = insert.source.as_mut() else {
        return Err(SqlError::CompositePk(
            "INSERT … SELECT into a composite-key table is not supported".into(),
        ));
    };
    let SetExpr::Values(values) = source.body.as_mut() else {
        return Err(SqlError::CompositePk(
            "INSERT … SELECT into a composite-key table is not supported".into(),
        ));
    };

    for row in &mut values.rows {
        if row.len() != row_cols.len() {
            return Err(SqlError::CompositePk(
                "INSERT value count does not match the column list".into(),
            ));
        }
        let components: Vec<Key> = component_idx
            .iter()
            .map(|&(i, ty)| value_to_key(&row[i], params, ty))
            .collect::<Result<_, _>>()?;
        let encoded = encode_composite_key(&components)?;
        row.push(Expr::Value(SqlValue::HexStringLiteral(to_hex(&encoded))));
    }

    // The column list is now explicit and includes the surrogate.
    row_cols.push(PK_COL.to_string());
    insert.columns = row_cols.into_iter().map(Ident::new).collect();
    Ok(())
}

/// Convert a SQL literal (or bound `$N` placeholder) to the [`Key`] used to build
/// `__bluedb_pk`. The value is **coerced to the component's declared column type**
/// (`target`) before keying, so the same logical key encodes identically whether
/// it arrives inline, as a typed param, or as a data-plane string. INSERT and
/// predicate rewrites share this so encodings always agree.
pub(crate) fn value_to_key(
    expr: &Expr,
    params: &[Value],
    target: Option<&GlueDataType>,
) -> Result<Key, SqlError> {
    let value = expr_to_value(expr, params)?;
    let value = match target {
        Some(ty) => value
            .cast(ty)
            .map_err(|e| SqlError::CompositePk(format!("primary-key component: {e}")))?,
        None => value,
    };
    Key::try_from(value).map_err(|e| SqlError::CompositePk(format!("primary-key component: {e}")))
}

/// Turn a literal/placeholder expression into a gluesql [`Value`] (pre-coercion).
fn expr_to_value(expr: &Expr, params: &[Value]) -> Result<Value, SqlError> {
    match expr {
        Expr::Value(SqlValue::Number(n, _)) => number_value(&n.to_string(), false),
        Expr::Value(SqlValue::SingleQuotedString(s)) => Ok(Value::Str(s.clone())),
        Expr::Value(SqlValue::Boolean(b)) => Ok(Value::Bool(*b)),
        Expr::Value(SqlValue::Null) => Err(SqlError::CompositePk(
            "a primary-key column cannot be NULL".into(),
        )),
        Expr::Value(SqlValue::Placeholder(p)) => resolve_placeholder(p, params),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(SqlValue::Number(n, _)) => number_value(&n.to_string(), true),
            other => Err(unsupported_component(other)),
        },
        other => Err(unsupported_component(other)),
    }
}

/// A numeric literal as a gluesql [`Value`] (`i64` if it fits, else `f64`).
fn number_value(digits: &str, negative: bool) -> Result<Value, SqlError> {
    let text = if negative {
        format!("-{digits}")
    } else {
        digits.to_string()
    };
    if let Ok(i) = text.parse::<i64>() {
        Ok(Value::I64(i))
    } else if let Ok(f) = text.parse::<f64>() {
        Ok(Value::F64(f))
    } else {
        Err(SqlError::CompositePk(format!(
            "primary-key component '{text}' is not a valid number"
        )))
    }
}

/// Resolve a `$N` placeholder to its bound parameter value.
fn resolve_placeholder(placeholder: &str, params: &[Value]) -> Result<Value, SqlError> {
    let index: usize = placeholder
        .strip_prefix('$')
        .and_then(|n| n.parse().ok())
        .filter(|&n| n >= 1)
        .ok_or_else(|| SqlError::CompositePk(format!("invalid placeholder '{placeholder}'")))?;
    let value = params.get(index - 1).ok_or_else(|| {
        SqlError::CompositePk(format!(
            "placeholder '{placeholder}' has no bound parameter (only {} given)",
            params.len()
        ))
    })?;
    if matches!(value, Value::Null) {
        return Err(SqlError::CompositePk(
            "a primary-key column cannot be NULL".into(),
        ));
    }
    Ok(value.clone())
}

fn unsupported_component(expr: &Expr) -> SqlError {
    SqlError::CompositePk(format!(
        "composite primary-key component must be a literal value, got `{expr}`"
    ))
}

/// Lowercase hex encoding (for `X'…'` bytea literals).
fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// SELECT/UPDATE/DELETE rewrites against a composite-PK table.
///
/// Component predicates that form a **leading prefix** of the key (equalities on
/// `a`, `a,b`, … plus an optional trailing range on the next component) are
/// rewritten to `__bluedb_pk` bounds — the same set of shapes Postgres's
/// multicolumn B-tree serves. The bounds ride the clustered-PK pseudo-index
/// (`storage::scan_pk_range`), so on a guarded connection a composite-key lookup
/// is an allowed point/range scan rather than a rejected non-indexed filter.
/// `SELECT *` is expanded to the user columns so `__bluedb_pk` never surfaces.
pub(crate) mod dml {
    use std::collections::HashMap;

    use sqlparser::ast::{
        AssignmentTarget, BinaryOperator, FromTable, OrderByExpr, Query, SelectItem, SetExpr,
        TableFactor, TableWithJoins,
    };

    use super::*;
    use crate::keyspace::prefix_upper_bound;

    /// The single table a DML statement targets (composite rewrites apply only to
    /// single-table SELECT/UPDATE/DELETE; joins/subqueries are left alone).
    pub(crate) fn target_table(stmt: &Statement) -> Option<String> {
        match stmt {
            Statement::Query(query) => match query.body.as_ref() {
                SetExpr::Select(select) => single_table(&select.from),
                _ => None,
            },
            Statement::Update { table, .. } => factor_table(&table.relation),
            Statement::Delete(delete) => match &delete.from {
                FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => single_table(t),
            },
            _ => None,
        }
    }

    fn single_table(from: &[TableWithJoins]) -> Option<String> {
        match from {
            [only] if only.joins.is_empty() => factor_table(&only.relation),
            _ => None,
        }
    }

    fn factor_table(factor: &TableFactor) -> Option<String> {
        match factor {
            TableFactor::Table { name, .. } => Some(object_table_name(name)),
            _ => None,
        }
    }

    pub(crate) fn rewrite(
        stmt: &mut Statement,
        catalog: &PkCatalog,
        user_cols: &[String],
        types: &HashMap<String, GlueDataType>,
        params: &[Value],
    ) -> Result<(), SqlError> {
        match stmt {
            Statement::Query(query) => rewrite_query(query, catalog, user_cols, types, params)?,
            Statement::Update {
                assignments,
                selection,
                ..
            } => {
                reject_pk_assignment(assignments, catalog)?;
                rewrite_selection(selection, catalog, types, params)?;
            }
            Statement::Delete(delete) => {
                rewrite_selection(&mut delete.selection, catalog, types, params)?
            }
            _ => {}
        }
        Ok(())
    }

    fn rewrite_query(
        query: &mut Query,
        catalog: &PkCatalog,
        user_cols: &[String],
        types: &HashMap<String, GlueDataType>,
        params: &[Value],
    ) -> Result<(), SqlError> {
        if let SetExpr::Select(select) = query.body.as_mut() {
            hide_surrogate(&mut select.projection, user_cols);
            rewrite_selection(&mut select.selection, catalog, types, params)?;
        }
        if let Some(order) = query.order_by.as_mut() {
            rewrite_order_by(&mut order.exprs, catalog);
        }
        Ok(())
    }

    /// Expand `*`/`t.*` to the explicit user columns so `__bluedb_pk` is hidden.
    fn hide_surrogate(projection: &mut Vec<SelectItem>, user_cols: &[String]) {
        let has_wildcard = projection.iter().any(|item| {
            matches!(
                item,
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
            )
        });
        if !has_wildcard {
            return;
        }
        let mut expanded = Vec::new();
        for item in std::mem::take(projection) {
            match item {
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                    for col in user_cols {
                        expanded.push(SelectItem::UnnamedExpr(Expr::Identifier(Ident::new(col))));
                    }
                }
                other => expanded.push(other),
            }
        }
        *projection = expanded;
    }

    fn rewrite_selection(
        selection: &mut Option<Expr>,
        catalog: &PkCatalog,
        types: &HashMap<String, GlueDataType>,
        params: &[Value],
    ) -> Result<(), SqlError> {
        if let Some(expr) = selection.take() {
            selection.replace(rewrite_predicate(expr, catalog, types, params)?);
        }
        Ok(())
    }

    /// Reject an UPDATE that assigns a primary-key component (the row's identity,
    /// hence `__bluedb_pk`, would change — delete+reinsert; not supported in v1).
    fn reject_pk_assignment(
        assignments: &[sqlparser::ast::Assignment],
        catalog: &PkCatalog,
    ) -> Result<(), SqlError> {
        for assignment in assignments {
            let targets = match &assignment.target {
                AssignmentTarget::ColumnName(name) => vec![object_table_name(name)],
                AssignmentTarget::Tuple(names) => names.iter().map(object_table_name).collect(),
            };
            for target in targets {
                if target == PK_COL || catalog.columns.contains(&target) {
                    return Err(SqlError::CompositePk(format!(
                        "UPDATE of primary-key column '{target}' is not supported"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Rewrite a leading-prefix of component predicates into `__bluedb_pk` bounds.
    fn rewrite_predicate(
        selection: Expr,
        catalog: &PkCatalog,
        types: &HashMap<String, GlueDataType>,
        params: &[Value],
    ) -> Result<Expr, SqlError> {
        let pk_index: HashMap<&str, usize> = catalog
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.as_str(), i))
            .collect();

        // Split the top-level AND chain; classify each conjunct as a usable
        // component comparison (keeping the original expr to restore if unused).
        let mut eqs: HashMap<usize, (Key, Expr)> = HashMap::new();
        let mut ranges: Vec<(usize, BinaryOperator, Key, Expr)> = Vec::new();
        // Row-value comparisons `(a,b) <op> (x,y)` (keyset) → direct __bluedb_pk
        // bounds, AND-combined with the rest.
        let mut row_value_preds: Vec<Expr> = Vec::new();
        let mut residual: Vec<Expr> = Vec::new();
        for conjunct in split_and(selection) {
            if let Some(pred) = row_value_predicate(&conjunct, catalog, types, params)? {
                row_value_preds.push(pred);
                continue;
            }
            match component_compare(&conjunct, &pk_index, catalog, types, params) {
                Some((idx, BinaryOperator::Eq, key)) if !eqs.contains_key(&idx) => {
                    eqs.insert(idx, (key, conjunct));
                }
                Some((idx, op, key)) if is_range(&op) => {
                    ranges.push((idx, op, key, conjunct));
                }
                _ => residual.push(conjunct),
            }
        }

        // The usable equality prefix: contiguous from column 0.
        let mut prefix_keys = Vec::new();
        let mut k = 0;
        while let Some((key, _)) = eqs.remove(&k) {
            prefix_keys.push(key);
            k += 1;
        }
        // Equalities past the contiguous prefix (or with a gap) stay as filters.
        for (_, (_, expr)) in eqs {
            residual.push(expr);
        }
        // At most one range, and only on the column right after the prefix.
        let mut chosen_range = None;
        let n = catalog.columns.len();
        for (idx, op, key, expr) in ranges {
            if chosen_range.is_none() && idx == k && k < n {
                chosen_range = Some((op, key));
            } else {
                residual.push(expr);
            }
        }

        let mut all: Vec<Expr> = Vec::new();
        if !(prefix_keys.is_empty() && chosen_range.is_none()) {
            all.push(build_pk_predicate(&prefix_keys, chosen_range, n)?);
        }
        all.extend(row_value_preds);
        all.extend(residual);
        Ok(rebuild_and(all))
    }

    /// Translate a row-value comparison `(a, b, …) <op> (x, y, …)` on a leading
    /// PK prefix into a single `__bluedb_pk` bound (keyset pagination). Returns
    /// `None` if `expr` isn't such a comparison.
    fn row_value_predicate(
        expr: &Expr,
        catalog: &PkCatalog,
        types: &HashMap<String, GlueDataType>,
        params: &[Value],
    ) -> Result<Option<Expr>, SqlError> {
        let Expr::BinaryOp { left, op, right } = expr else {
            return Ok(None);
        };
        if !is_range(op) {
            return Ok(None);
        }
        let (Expr::Tuple(cols), Expr::Tuple(vals)) = (left.as_ref(), right.as_ref()) else {
            return Ok(None);
        };
        let n = catalog.columns.len();
        if cols.is_empty() || cols.len() != vals.len() || cols.len() > n {
            return Ok(None);
        }
        // The left tuple must be a leading prefix of the key columns, in order.
        for (i, col) in cols.iter().enumerate() {
            if ident_name(col).as_deref() != Some(catalog.columns[i].as_str()) {
                return Ok(None);
            }
        }
        let keys: Vec<Key> = vals
            .iter()
            .enumerate()
            .map(|(i, v)| value_to_key(v, params, types.get(&catalog.columns[i])))
            .collect::<Result<_, _>>()?;
        let enc = encode_composite_key(&keys)?;
        let pred = if cols.len() == n {
            // Full key: byte comparison == tuple comparison exactly.
            pk_cmp(op.clone(), &enc)
        } else {
            // Partial prefix: bound past/through the whole `(x, y, …, *)` range.
            match op {
                BinaryOperator::Gt => {
                    pk_cmp(BinaryOperator::GtEq, &prefix_upper_bound(&enc).unwrap_or(enc))
                }
                BinaryOperator::GtEq => pk_cmp(BinaryOperator::GtEq, &enc),
                BinaryOperator::Lt => pk_cmp(BinaryOperator::Lt, &enc),
                BinaryOperator::LtEq => match prefix_upper_bound(&enc) {
                    Some(upper) => pk_cmp(BinaryOperator::Lt, &upper),
                    None => pk_cmp(BinaryOperator::LtEq, &enc),
                },
                _ => unreachable!("is_range gated the operator"),
            }
        };
        Ok(Some(pred))
    }

    /// Construct the `__bluedb_pk` predicate for an equality prefix + optional
    /// trailing range, using the order-preserving encoding's prefix bounds.
    fn build_pk_predicate(
        prefix_keys: &[Key],
        range: Option<(BinaryOperator, Key)>,
        n: usize,
    ) -> Result<Expr, SqlError> {
        let base = encode_composite_key(prefix_keys)?;
        match range {
            None if prefix_keys.len() == n => Ok(pk_cmp(BinaryOperator::Eq, &base)),
            None => Ok(range_pred(base.clone(), prefix_upper_bound(&base))),
            Some((op, value)) => {
                let mut with_range = prefix_keys.to_vec();
                with_range.push(value);
                let point = encode_composite_key(&with_range)?;
                let base_upper = prefix_upper_bound(&base);
                let (low, high) = match op {
                    BinaryOperator::Gt => (
                        prefix_upper_bound(&point).unwrap_or_else(|| point.clone()),
                        base_upper,
                    ),
                    BinaryOperator::GtEq => (point, base_upper),
                    BinaryOperator::Lt => (base, Some(point)),
                    BinaryOperator::LtEq => (base, prefix_upper_bound(&point)),
                    _ => unreachable!("is_range gated the operator"),
                };
                Ok(range_pred(low, high))
            }
        }
    }

    /// `__bluedb_pk >= X'low' [AND __bluedb_pk < X'high']`.
    fn range_pred(low: Vec<u8>, high: Option<Vec<u8>>) -> Expr {
        let lower = pk_cmp(BinaryOperator::GtEq, &low);
        match high {
            Some(high) => and(lower, pk_cmp(BinaryOperator::Lt, &high)),
            None => lower,
        }
    }

    /// `__bluedb_pk <op> X'bytes'`.
    fn pk_cmp(op: BinaryOperator, bytes: &[u8]) -> Expr {
        Expr::BinaryOp {
            left: Box::new(Expr::Identifier(Ident::new(PK_COL))),
            op,
            right: Box::new(Expr::Value(SqlValue::HexStringLiteral(to_hex(bytes)))),
        }
    }

    fn and(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        }
    }

    fn rebuild_and(mut conjuncts: Vec<Expr>) -> Expr {
        if conjuncts.is_empty() {
            return Expr::Value(SqlValue::Boolean(true));
        }
        let mut acc = conjuncts.remove(0);
        for next in conjuncts {
            acc = and(acc, next);
        }
        acc
    }

    /// If `expr` is `<pk-component> <cmp> <literal-or-$param>`, return
    /// `(index, op, key)` with the value coerced to the component's column type.
    fn component_compare(
        expr: &Expr,
        pk_index: &HashMap<&str, usize>,
        catalog: &PkCatalog,
        types: &HashMap<String, GlueDataType>,
        params: &[Value],
    ) -> Option<(usize, BinaryOperator, Key)> {
        match expr {
            Expr::Nested(inner) => component_compare(inner, pk_index, catalog, types, params),
            Expr::BinaryOp { left, op, right } if is_cmp(op) => {
                let col = ident_name(left)?;
                let idx = *pk_index.get(col.as_str())?;
                let key = value_to_key(right, params, types.get(&catalog.columns[idx])).ok()?;
                Some((idx, op.clone(), key))
            }
            _ => None,
        }
    }

    fn ident_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.clone()),
            Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.clone()),
            Expr::Nested(inner) => ident_name(inner),
            _ => None,
        }
    }

    fn split_and(expr: Expr) -> Vec<Expr> {
        match expr {
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let mut out = split_and(*left);
                out.extend(split_and(*right));
                out
            }
            Expr::Nested(inner) => split_and(*inner),
            other => vec![other],
        }
    }

    fn is_cmp(op: &BinaryOperator) -> bool {
        matches!(op, BinaryOperator::Eq) || is_range(op)
    }

    fn is_range(op: &BinaryOperator) -> bool {
        matches!(
            op,
            BinaryOperator::Gt | BinaryOperator::Lt | BinaryOperator::GtEq | BinaryOperator::LtEq
        )
    }

    /// Rewrite `ORDER BY a[, b, …]` (a leading PK prefix, uniform direction) to
    /// `ORDER BY __bluedb_pk` so the ordered scan is index-served (the pk order
    /// equals the tuple order).
    fn rewrite_order_by(exprs: &mut Vec<OrderByExpr>, catalog: &PkCatalog) {
        if exprs.is_empty() || exprs.len() > catalog.columns.len() {
            return;
        }
        let is_leading_prefix = exprs
            .iter()
            .enumerate()
            .all(|(i, e)| ident_name(&e.expr).as_deref() == Some(catalog.columns[i].as_str()));
        let uniform_direction = exprs.iter().all(|e| e.asc == exprs[0].asc);
        if !is_leading_prefix || !uniform_direction {
            return;
        }
        let mut collapsed = exprs[0].clone();
        collapsed.expr = Expr::Identifier(Ident::new(PK_COL));
        *exprs = vec![collapsed];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_one(sql: &str) -> Statement {
        Parser::parse_sql(&GenericDialect {}, sql)
            .unwrap()
            .pop()
            .unwrap()
    }

    fn create_with_composite() -> (Statement, PkCatalog) {
        let mut stmt =
            parse_one("CREATE TABLE t (a INTEGER, b TEXT, payload TEXT, PRIMARY KEY (a, b))");
        let Statement::CreateTable(create) = &mut stmt else {
            panic!("expected create");
        };
        let catalog = strip_composite_pk(create).unwrap().unwrap();
        (stmt, catalog)
    }

    #[test]
    fn strips_composite_pk_and_appends_surrogate() {
        let (stmt, catalog) = create_with_composite();
        assert_eq!(catalog.columns, vec!["a", "b"]);
        let sql = stmt.to_string();
        // The composite constraint is gone; the surrogate is the PRIMARY KEY.
        assert!(!sql.contains("PRIMARY KEY (a, b)"), "constraint not stripped: {sql}");
        assert!(sql.contains("__bluedb_pk BYTEA NOT NULL PRIMARY KEY"), "surrogate missing: {sql}");
        // Component columns are forced NOT NULL.
        assert!(sql.contains("a INTEGER NOT NULL"));
        assert!(sql.contains("b TEXT NOT NULL"));
    }

    #[test]
    fn single_column_pk_is_left_untouched() {
        let mut stmt = parse_one("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
        let Statement::CreateTable(create) = &mut stmt else {
            panic!();
        };
        assert!(strip_composite_pk(create).unwrap().is_none());
    }

    #[test]
    fn reserved_column_name_is_rejected() {
        let mut stmt =
            parse_one("CREATE TABLE t (a INTEGER, __bluedb_pk TEXT, PRIMARY KEY (a, __bluedb_pk))");
        let Statement::CreateTable(create) = &mut stmt else {
            panic!();
        };
        assert!(strip_composite_pk(create).is_err());
    }

    fn insert_of(sql: &str) -> sqlparser::ast::Insert {
        match parse_one(sql) {
            Statement::Insert(insert) => insert,
            other => panic!("expected insert, got {other:?}"),
        }
    }

    #[test]
    fn insert_injects_surrogate_for_explicit_columns() {
        let (_, catalog) = create_with_composite();
        let mut insert = insert_of("INSERT INTO t (a, b, payload) VALUES (1, 'x', 'p')");
        rewrite_insert(&mut insert, &catalog, &["a".into(), "b".into(), "payload".into()], &std::collections::HashMap::new(), &[]).unwrap();
        let sql = Statement::Insert(insert).to_string();
        assert!(sql.contains("__bluedb_pk"), "surrogate column not added: {sql}");
        assert!(sql.contains("X'"), "surrogate value not a bytea literal: {sql}");
    }

    #[test]
    fn insert_makes_positional_explicit() {
        let (_, catalog) = create_with_composite();
        let mut insert = insert_of("INSERT INTO t VALUES (1, 'x', 'p')");
        rewrite_insert(&mut insert, &catalog, &["a".into(), "b".into(), "payload".into()], &std::collections::HashMap::new(), &[]).unwrap();
        let sql = Statement::Insert(insert).to_string();
        assert!(sql.contains("(a, b, payload, __bluedb_pk)"), "columns not made explicit: {sql}");
    }

    #[test]
    fn insert_rejects_missing_pk_component() {
        let (_, catalog) = create_with_composite();
        let mut insert = insert_of("INSERT INTO t (a, payload) VALUES (1, 'p')");
        let err = rewrite_insert(&mut insert, &catalog, &["a".into(), "b".into(), "payload".into()], &std::collections::HashMap::new(), &[]);
        assert!(err.is_err(), "missing PK component should be rejected");
    }

    /// Pull the first VALUES literal expr out of a parsed INSERT (so numeric
    /// literals carry a real `BigDecimal`, like production SQL).
    fn lit(literal: &str) -> Expr {
        let insert = insert_of(&format!("INSERT INTO t VALUES ({literal})"));
        let query = insert.source.expect("values source");
        match *query.body {
            SetExpr::Values(values) => values.rows.into_iter().next().unwrap().pop().unwrap(),
            other => panic!("expected VALUES, got {other:?}"),
        }
    }

    #[test]
    fn literal_to_key_handles_ints_strings_bools_and_negatives() {
        assert_eq!(value_to_key(&lit("5"), &[], None).unwrap(), Key::I64(5));
        assert_eq!(value_to_key(&lit("-7"), &[], None).unwrap(), Key::I64(-7));
        assert_eq!(value_to_key(&lit("'x'"), &[], None).unwrap(), Key::Str("x".into()));
        assert_eq!(value_to_key(&lit("true"), &[], None).unwrap(), Key::Bool(true));
        assert!(value_to_key(&lit("NULL"), &[], None).is_err());
    }

    fn rewrite_query_sql(sql: &str) -> String {
        let catalog = PkCatalog {
            columns: vec!["a".into(), "b".into()],
        };
        let user_cols = vec!["a".to_string(), "b".to_string(), "payload".to_string()];
        let mut stmt = parse_one(sql);
        dml::rewrite(&mut stmt, &catalog, &user_cols, &std::collections::HashMap::new(), &[]).unwrap();
        stmt.to_string()
    }

    #[test]
    fn full_key_equality_becomes_a_point_predicate() {
        let sql = rewrite_query_sql("SELECT payload FROM t WHERE a = 1 AND b = 'x'");
        assert!(sql.contains("__bluedb_pk = X'"), "not a point lookup: {sql}");
        assert!(!sql.contains("a = 1"), "component predicate not consumed: {sql}");
    }

    #[test]
    fn leading_prefix_becomes_a_range() {
        let sql = rewrite_query_sql("SELECT payload FROM t WHERE a = 1");
        assert!(sql.contains("__bluedb_pk >= X'"), "no lower bound: {sql}");
        assert!(sql.contains("__bluedb_pk < X'"), "no upper bound: {sql}");
    }

    #[test]
    fn non_pk_predicate_is_left_untouched() {
        let sql = rewrite_query_sql("SELECT a FROM t WHERE payload = 'p'");
        assert!(sql.contains("payload = 'p'"), "residual predicate lost: {sql}");
        assert!(!sql.contains("__bluedb_pk"), "spurious rewrite: {sql}");
    }

    #[test]
    fn select_star_expands_to_user_columns() {
        let sql = rewrite_query_sql("SELECT * FROM t WHERE a = 1 AND b = 'x'");
        assert!(sql.contains("SELECT a, b, payload"), "wildcard not expanded: {sql}");
        assert!(!sql.contains('*'), "wildcard remains: {sql}");
    }

    #[test]
    fn same_components_encode_identically() {
        // INSERT and predicate rewrites must agree on the encoding.
        let k1 = value_to_key(&lit("5"), &[], None).unwrap();
        let k2 = value_to_key(&lit("5"), &[], None).unwrap();
        assert_eq!(
            encode_composite_key(&[k1, Key::Str("x".into())]).unwrap(),
            encode_composite_key(&[k2, Key::Str("x".into())]).unwrap()
        );
    }
}
