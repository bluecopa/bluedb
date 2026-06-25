//! Equi-join predicate pushdown — a `Planner::plan` pass.
//!
//! The comma-join shim ([`crate::rewrite`]) turns `FROM a, b` into
//! `FROM a JOIN b ON TRUE`. GlueSQL runs `ON TRUE` as a nested-loop **cartesian
//! product** because it sees no join key. But GlueSQL already has a hash-join
//! executor: its `plan_join` pass builds [`JoinExecutor::Hash`] whenever an
//! equality (`x = y`) appears in a join's `ON`. So here we move equi-join
//! conjuncts out of the `WHERE` and into the relevant join's `ON`; `plan_join`
//! then turns each into a hash join and the product never materializes.
//!
//! This runs inside [`crate::storage`]'s `Planner::plan` override, where the
//! schema map is available to resolve unqualified columns to their tables.
//!
//! Conservative by construction: only `INNER` joins are touched (so moving a
//! predicate from `WHERE` to `ON` is semantically identical), only simple
//! `col = col` equalities across two distinct tables are moved, and any conjunct
//! we can't confidently classify is left in the `WHERE`.

use std::collections::HashMap;

use gluesql_core::ast::{
    BinaryOperator, Expr, Join, JoinConstraint, JoinOperator, Query, Select, SetExpr, Statement,
    TableFactor,
};
use gluesql_core::data::{Schema, Value};
use gluesql_core::error::{Error, Result as GlueResult};

type SchemaMap = HashMap<String, Schema>;

/// Move `WHERE` equi-join predicates into the matching join's `ON` so GlueSQL's
/// `plan_join` can build hash joins.
pub fn pushdown_equijoins(schema_map: &SchemaMap, statement: Statement) -> Statement {
    match statement {
        Statement::Query(query) => Statement::Query(rewrite_query(schema_map, query)),
        other => other,
    }
}

/// Fast-fail any inner join left without a join key after pushdown — `ON TRUE`
/// over multiple tables would materialize a full cartesian product on GlueSQL's
/// nested-loop executor. Rejecting at plan time means it fails instantly,
/// before a single row is built.
pub fn reject_cross_products(statement: &Statement) -> GlueResult<()> {
    let Statement::Query(query) = statement else {
        return Ok(());
    };
    let SetExpr::Select(select) = &query.body else {
        return Ok(());
    };
    for join in &select.from.joins {
        if let JoinOperator::Inner(JoinConstraint::On(on)) = &join.join_operator {
            if is_trivially_true(on) {
                return Err(Error::StorageMsg(
                    "unsupported: multi-table query without an equi-join key \
                     (would materialize a cartesian product)"
                        .to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn is_trivially_true(expr: &Expr) -> bool {
    match expr {
        Expr::Value(Value::Bool(true)) => true,
        Expr::Nested(inner) => is_trivially_true(inner),
        _ => false,
    }
}

fn rewrite_query(schema_map: &SchemaMap, mut query: Query) -> Query {
    if let SetExpr::Select(select) = query.body {
        query.body = SetExpr::Select(Box::new(rewrite_select(schema_map, *select)));
    }
    query
}

fn rewrite_select(schema_map: &SchemaMap, mut select: Select) -> Select {
    if select.from.joins.is_empty() {
        return select;
    }
    let Some(selection) = select.selection.take() else {
        return select;
    };

    // Table position: 0 = base relation, i+1 = joins[i].
    let mut table_pos: HashMap<String, usize> = HashMap::new();
    let mut tables: Vec<(String, usize)> = Vec::new();
    register_table(&select.from.relation, 0, &mut table_pos, &mut tables);
    for (i, join) in select.from.joins.iter().enumerate() {
        register_table(&join.relation, i + 1, &mut table_pos, &mut tables);
    }
    let col_pos = build_column_index(schema_map, &tables);

    let mut residual: Vec<Expr> = Vec::new();
    let mut to_on: HashMap<usize, Vec<Expr>> = HashMap::new();

    for conjunct in split_and(selection) {
        match equijoin_target(&conjunct, &table_pos, &col_pos) {
            Some((join_idx, eq)) if is_inner(&select.from.joins[join_idx]) => {
                to_on.entry(join_idx).or_default().push(eq);
            }
            _ => residual.push(conjunct),
        }
    }

    for (idx, exprs) in to_on {
        if let Some(added) = combine_and(exprs) {
            merge_into_on(&mut select.from.joins[idx], added);
        }
    }

    select.selection = combine_and(residual);
    select
}

fn register_table(
    factor: &TableFactor,
    pos: usize,
    table_pos: &mut HashMap<String, usize>,
    tables: &mut Vec<(String, usize)>,
) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            tables.push((name.clone(), pos));
            table_pos.insert(name.clone(), pos);
            if let Some(alias) = alias {
                table_pos.insert(alias.name.clone(), pos);
            }
        }
        TableFactor::Derived { alias, .. } | TableFactor::Series { alias, .. } => {
            table_pos.insert(alias.name.clone(), pos);
        }
        _ => {}
    }
}

/// Unqualified column -> table position, only for column names unique across the
/// involved tables (ambiguous names are skipped and stay in the `WHERE`).
fn build_column_index(
    schema_map: &SchemaMap,
    tables: &[(String, usize)],
) -> HashMap<String, usize> {
    let mut seen: HashMap<String, (usize, usize)> = HashMap::new();
    for (table, pos) in tables {
        if let Some(schema) = schema_map.get(table) {
            if let Some(columns) = &schema.column_defs {
                for column in columns {
                    let entry = seen.entry(column.name.clone()).or_insert((*pos, 0));
                    entry.0 = *pos;
                    entry.1 += 1;
                }
            }
        }
    }
    seen.into_iter()
        .filter(|(_, (_, count))| *count == 1)
        .map(|(name, (pos, _))| (name, pos))
        .collect()
}

fn col_position(
    expr: &Expr,
    table_pos: &HashMap<String, usize>,
    col_pos: &HashMap<String, usize>,
) -> Option<usize> {
    match expr {
        Expr::Identifier(col) => col_pos.get(col).copied(),
        Expr::CompoundIdentifier { alias, .. } => table_pos.get(alias).copied(),
        Expr::Nested(inner) => col_position(inner, table_pos, col_pos),
        _ => None,
    }
}

/// If `conjunct` is `lhs = rhs` with lhs/rhs resolving to two different tables,
/// return the joins index that should carry it (the join introducing the later
/// table) and the bare equality to place in its `ON`.
fn equijoin_target(
    conjunct: &Expr,
    table_pos: &HashMap<String, usize>,
    col_pos: &HashMap<String, usize>,
) -> Option<(usize, Expr)> {
    let inner = unnest(conjunct);
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = inner
    {
        let p = col_position(left, table_pos, col_pos)?;
        let q = col_position(right, table_pos, col_pos)?;
        if p != q {
            return Some((p.max(q) - 1, inner.clone()));
        }
    }
    None
}

fn unnest(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(inner) => unnest(inner),
        other => other,
    }
}

fn is_inner(join: &Join) -> bool {
    matches!(join.join_operator, JoinOperator::Inner(_))
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
        other => vec![other],
    }
}

fn combine_and(exprs: Vec<Expr>) -> Option<Expr> {
    let mut iter = exprs.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, expr| Expr::BinaryOp {
        left: Box::new(acc),
        op: BinaryOperator::And,
        right: Box::new(expr),
    }))
}

fn merge_into_on(join: &mut Join, added: Expr) {
    if let JoinOperator::Inner(constraint) = &mut join.join_operator {
        let existing = std::mem::replace(constraint, JoinConstraint::None);
        *constraint = match existing {
            JoinConstraint::On(expr) => JoinConstraint::On(Expr::BinaryOp {
                left: Box::new(expr),
                op: BinaryOperator::And,
                right: Box::new(added),
            }),
            JoinConstraint::None => JoinConstraint::On(added),
        };
    }
}
