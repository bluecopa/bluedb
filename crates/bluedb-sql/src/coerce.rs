//! Implicit type coercion for comparisons — a `Planner::plan` pass.
//!
//! GlueSQL does no implicit coercion across the **text/number boundary** in
//! comparisons. Its `evaluate_eq` / `evaluate_cmp` segregate operands by class:
//! a number compared to a string takes a `(Number, Text)` path that returns
//! `false` (eq) / incomparable (cmp) with **no error**. So `WHERE price = '9.99'`
//! (a numeric column vs a quoted number), `'1' = 1`, `qty < '5'` silently return
//! the wrong rows.
//!
//! Numeric-affinity engines (SQLite via column affinity, MySQL) — and our
//! correctness corpus, DuckDB — compare such operands *numerically* instead.
//! This pass restores that behavior using the one extension point that has the
//! types: the `Planner::plan` hook already owns the schema map. It walks
//! comparison expressions and, when one side is number-typed and the other is a
//! numeric string, wraps the string side in `CAST(… AS <numeric type>)`.
//! GlueSQL's own (working) CAST executor then does the conversion — we only
//! insert the cast node GlueSQL failed to. **No engine fork required.**
//!
//! Conservative by construction — it never turns a working query into a wrong
//! result or a new runtime error:
//! - Only the **text ↔ number** boundary is touched. Same-class comparisons and
//!   booleans/dates are left exactly as GlueSQL evaluates them (so `true = 1`
//!   keeps its current behavior — `bool`↔`int` coercion is intentionally out of
//!   scope; it is genuinely engine-specific).
//! - The text side is only ever a **string literal** (or `Value::Str`), never a
//!   text *column* — we never reinterpret stored text, so a table of names is
//!   never force-cast to a number.
//! - A string literal is coerced only if it parses into the target numeric type
//!   using the *same* parser GlueSQL's CAST uses (`parse::<i64>()`, `parse::<f64>()`,
//!   `Decimal::from_str`), so the inserted CAST is guaranteed to succeed.
//! - Columns whose type can't be resolved (schemaless tables, aliased/correlated
//!   columns, ambiguous unqualified names, function results) are left untouched.

use std::collections::{HashMap, HashSet};

use gluesql_core::ast::{
    BinaryOperator, DataType, Expr, Function, JoinConstraint, JoinOperator, Literal, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
};
use gluesql_core::data::{Schema, Value};

type SchemaMap = HashMap<String, Schema>;

/// Insert implicit `CAST`s so text↔number comparisons compare numerically,
/// matching DuckDB/affinity-engine semantics instead of GlueSQL's silent
/// type-segregated mismatch. Statements other than `SELECT`/`UPDATE`/`DELETE`
/// pass through unchanged.
pub fn coerce_comparisons(schema_map: &SchemaMap, statement: Statement) -> Statement {
    match statement {
        Statement::Query(query) => Statement::Query(coerce_query(schema_map, query)),
        Statement::Update {
            table_name,
            assignments,
            selection,
        } => {
            let scope = Scope::single(schema_map, &table_name);
            let selection = selection.map(|e| coerce_expr(schema_map, &scope, e));
            Statement::Update {
                table_name,
                assignments,
                selection,
            }
        }
        Statement::Delete {
            table_name,
            selection,
        } => {
            let scope = Scope::single(schema_map, &table_name);
            let selection = selection.map(|e| coerce_expr(schema_map, &scope, e));
            Statement::Delete {
                table_name,
                selection,
            }
        }
        other => other,
    }
}

fn coerce_query(schema_map: &SchemaMap, mut query: Query) -> Query {
    query.body = match query.body {
        SetExpr::Select(select) => SetExpr::Select(Box::new(coerce_select(schema_map, *select))),
        // `SetExpr::Values` has no column scope to resolve against; leave it.
        other => other,
    };
    query
}

fn coerce_select(schema_map: &SchemaMap, mut select: Select) -> Select {
    // The scope is the set of base tables in this SELECT's FROM. Build it before
    // we move `from` so identifiers in WHERE/projection/ON resolve to types.
    let scope = Scope::from_table_with_joins(schema_map, &select.from);

    select.selection = select.selection.map(|e| coerce_expr(schema_map, &scope, e));
    select.having = select.having.map(|e| coerce_expr(schema_map, &scope, e));
    select.group_by = select
        .group_by
        .into_iter()
        .map(|e| coerce_expr(schema_map, &scope, e))
        .collect();
    select.projection = select
        .projection
        .into_iter()
        .map(|item| match item {
            SelectItem::Expr { expr, label } => SelectItem::Expr {
                expr: coerce_expr(schema_map, &scope, expr),
                label,
            },
            other => other,
        })
        .collect();

    // Join `ON` predicates live in this scope; derived subqueries carry their own
    // (rebuilt inside the recursive `coerce_query`).
    let mut from = select.from;
    from.relation = coerce_table_factor(schema_map, from.relation);
    from.joins = from
        .joins
        .into_iter()
        .map(|mut join| {
            join.join_operator = match join.join_operator {
                JoinOperator::Inner(c) => {
                    JoinOperator::Inner(coerce_constraint(schema_map, &scope, c))
                }
                JoinOperator::LeftOuter(c) => {
                    JoinOperator::LeftOuter(coerce_constraint(schema_map, &scope, c))
                }
            };
            join.relation = coerce_table_factor(schema_map, join.relation);
            join
        })
        .collect();
    select.from = from;

    select
}

fn coerce_constraint(schema_map: &SchemaMap, scope: &Scope, c: JoinConstraint) -> JoinConstraint {
    match c {
        JoinConstraint::On(e) => JoinConstraint::On(coerce_expr(schema_map, scope, e)),
        JoinConstraint::None => JoinConstraint::None,
    }
}

fn coerce_table_factor(schema_map: &SchemaMap, factor: TableFactor) -> TableFactor {
    match factor {
        TableFactor::Derived { subquery, alias } => TableFactor::Derived {
            subquery: coerce_query(schema_map, subquery),
            alias,
        },
        other => other,
    }
}

/// Recursively rewrite an expression, coercing comparison operands as it goes.
fn coerce_expr(schema_map: &SchemaMap, scope: &Scope, expr: Expr) -> Expr {
    let recur = |e: Expr| coerce_expr(schema_map, scope, e);
    match expr {
        Expr::BinaryOp { left, op, right } => {
            let left = recur(*left);
            let right = recur(*right);
            let (left, right) = if is_comparison(&op) {
                coerce_pair(schema_map, scope, left, right)
            } else {
                (left, right)
            };
            Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }
        }
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op,
            expr: Box::new(recur(*expr)),
        },
        Expr::Nested(e) => Expr::Nested(Box::new(recur(*e))),
        Expr::IsNull(e) => Expr::IsNull(Box::new(recur(*e))),
        Expr::IsNotNull(e) => Expr::IsNotNull(Box::new(recur(*e))),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Expr::Between {
            expr: Box::new(recur(*expr)),
            negated,
            low: Box::new(recur(*low)),
            high: Box::new(recur(*high)),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(recur(*expr)),
            list: list.into_iter().map(recur).collect(),
            negated,
        },
        Expr::Case {
            operand,
            when_then,
            else_result,
        } => Expr::Case {
            operand: operand.map(|e| Box::new(recur(*e))),
            when_then: when_then
                .into_iter()
                .map(|(w, t)| (recur(w), recur(t)))
                .collect(),
            else_result: else_result.map(|e| Box::new(recur(*e))),
        },
        // Subqueries rebuild their own scope (correlated outer refs resolve to
        // nothing here, so they are simply left uncoerced — conservative).
        Expr::Subquery(q) => Expr::Subquery(Box::new(coerce_query(schema_map, *q))),
        Expr::Exists { subquery, negated } => Expr::Exists {
            subquery: Box::new(coerce_query(schema_map, *subquery)),
            negated,
        },
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Expr::InSubquery {
            expr: Box::new(recur(*expr)),
            subquery: Box::new(coerce_query(schema_map, *subquery)),
            negated,
        },
        // Function args, aggregates, etc. are left as-is: not the documented gap.
        other => other,
    }
}

fn is_comparison(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
    )
}

/// One operand of a comparison, classified for coercion.
enum Side {
    /// Numeric operand; the `DataType` is the target to cast the *other* (text)
    /// side to. For a numeric column it is the column's type; for a bare numeric
    /// literal it is `Int`/`Float` (the only kinds GlueSQL's literal comparison
    /// path handles — it has no `Decimal` arm).
    Num(DataType),
    /// A string literal (or `Value::Str`) — the only thing we will cast.
    TextLit(String),
    /// Anything we won't touch (text column, NULL, boolean, function result, …).
    Skip,
}

/// If exactly one side is numeric and the other is a string literal that parses
/// into the numeric target, cast the string side. Otherwise return unchanged.
fn coerce_pair(schema_map: &SchemaMap, scope: &Scope, left: Expr, right: Expr) -> (Expr, Expr) {
    match (
        classify(schema_map, scope, &left),
        classify(schema_map, scope, &right),
    ) {
        (Side::Num(dt), Side::TextLit(s)) if string_castable_to(&s, &dt) => {
            (left, cast(right, dt))
        }
        (Side::TextLit(s), Side::Num(dt)) if string_castable_to(&s, &dt) => {
            (cast(left, dt), right)
        }
        _ => (left, right),
    }
}

fn classify(schema_map: &SchemaMap, scope: &Scope, expr: &Expr) -> Side {
    match expr {
        Expr::Nested(inner) => classify(schema_map, scope, inner),
        Expr::Literal(Literal::Number(n)) => Side::Num(if n.is_integer() {
            DataType::Int
        } else {
            DataType::Float
        }),
        Expr::Literal(Literal::QuotedString(s)) => Side::TextLit(s.clone()),
        Expr::Value(Value::Str(s)) => Side::TextLit(s.clone()),
        Expr::Value(v) => value_numeric_target(v).map_or(Side::Skip, Side::Num),
        Expr::Identifier(name) => scope
            .column_type(name)
            .and_then(|dt| numeric_target(&dt))
            .map_or(Side::Skip, Side::Num),
        Expr::CompoundIdentifier { alias, ident } => scope
            .qualified_type(schema_map, alias, ident)
            .and_then(|dt| numeric_target(&dt))
            .map_or(Side::Skip, Side::Num),
        _ => Side::Skip,
    }
}

fn cast(expr: Expr, data_type: DataType) -> Expr {
    Expr::Function(Box::new(Function::Cast { expr, data_type }))
}

/// The numeric `DataType`s, mapped to themselves; everything else (Text, Bool,
/// Date, …) yields `None` so it is never treated as the number side.
fn numeric_target(dt: &DataType) -> Option<DataType> {
    use DataType::*;
    matches!(
        dt,
        Int8 | Int16
            | Int32
            | Int
            | Int128
            | Uint8
            | Uint16
            | Uint32
            | Uint64
            | Uint128
            | Float32
            | Float
            | Decimal
    )
    .then(|| dt.clone())
}

fn value_numeric_target(v: &Value) -> Option<DataType> {
    use DataType::*;
    Some(match v {
        Value::I8(_) => Int8,
        Value::I16(_) => Int16,
        Value::I32(_) => Int32,
        Value::I64(_) => Int,
        Value::I128(_) => Int128,
        Value::U8(_) => Uint8,
        Value::U16(_) => Uint16,
        Value::U32(_) => Uint32,
        Value::U64(_) => Uint64,
        Value::U128(_) => Uint128,
        Value::F32(_) => Float32,
        Value::F64(_) => Float,
        Value::Decimal(_) => Decimal,
        _ => return None,
    })
}

/// True iff `s` parses into `dt` using the *same* parser GlueSQL's CAST uses
/// (`src/data/value/convert.rs`), so the inserted CAST is guaranteed to succeed.
fn string_castable_to(s: &str, dt: &DataType) -> bool {
    use DataType::*;
    match dt {
        Int8 => s.parse::<i8>().is_ok(),
        Int16 => s.parse::<i16>().is_ok(),
        Int32 => s.parse::<i32>().is_ok(),
        Int => s.parse::<i64>().is_ok(),
        Int128 => s.parse::<i128>().is_ok(),
        Uint8 => s.parse::<u8>().is_ok(),
        Uint16 => s.parse::<u16>().is_ok(),
        Uint32 => s.parse::<u32>().is_ok(),
        Uint64 => s.parse::<u64>().is_ok(),
        Uint128 => s.parse::<u128>().is_ok(),
        Float32 => s.parse::<f32>().is_ok(),
        Float => s.parse::<f64>().is_ok(),
        // GlueSQL casts text→Decimal via `rust_decimal::Decimal::from_str`, which
        // accepts a plain signed decimal. Mirror a conservative subset (no
        // exponent / inf / nan) that `from_str` always accepts.
        Decimal => is_plain_decimal(s),
        _ => false,
    }
}

fn is_plain_decimal(s: &str) -> bool {
    let mut bytes = s.bytes().peekable();
    if matches!(bytes.peek(), Some(b'+') | Some(b'-')) {
        bytes.next();
    }
    let mut digits = 0usize;
    let mut seen_dot = false;
    for b in bytes {
        match b {
            b'0'..=b'9' => digits += 1,
            b'.' if !seen_dot => seen_dot = true,
            _ => return false,
        }
    }
    digits > 0
}

/// Column-type lookup for the tables in one SELECT's (or one DML statement's) scope.
struct Scope {
    /// Unqualified column name → type, for names that are unique across the scope.
    cols: HashMap<String, DataType>,
    /// Column names appearing in more than one in-scope table (unresolvable bare).
    ambiguous: HashSet<String>,
    /// Table alias (and bare table name) → real table name, for `alias.col` lookups.
    alias_to_table: HashMap<String, String>,
}

impl Scope {
    fn single(schema_map: &SchemaMap, table_name: &str) -> Scope {
        Scope::from_tables(schema_map, &[(table_name.to_owned(), None)])
    }

    fn from_table_with_joins(schema_map: &SchemaMap, from: &TableWithJoins) -> Scope {
        let mut tables = Vec::new();
        push_table(&from.relation, &mut tables);
        for join in &from.joins {
            push_table(&join.relation, &mut tables);
        }
        Scope::from_tables(schema_map, &tables)
    }

    fn from_tables(schema_map: &SchemaMap, tables: &[(String, Option<String>)]) -> Scope {
        let mut alias_to_table = HashMap::new();
        let mut counts: HashMap<String, usize> = HashMap::new();
        let mut cols: HashMap<String, DataType> = HashMap::new();
        for (table, alias) in tables {
            alias_to_table.insert(table.clone(), table.clone());
            if let Some(a) = alias {
                alias_to_table.insert(a.clone(), table.clone());
            }
            if let Some(defs) = schema_map.get(table).and_then(|s| s.column_defs.as_ref()) {
                for c in defs {
                    *counts.entry(c.name.clone()).or_insert(0) += 1;
                    cols.insert(c.name.clone(), c.data_type.clone());
                }
            }
        }
        let ambiguous = counts
            .into_iter()
            .filter(|(_, n)| *n > 1)
            .map(|(name, _)| name)
            .collect();
        Scope {
            cols,
            ambiguous,
            alias_to_table,
        }
    }

    fn column_type(&self, name: &str) -> Option<DataType> {
        if self.ambiguous.contains(name) {
            None
        } else {
            self.cols.get(name).cloned()
        }
    }

    fn qualified_type(&self, schema_map: &SchemaMap, alias: &str, ident: &str) -> Option<DataType> {
        let table = self.alias_to_table.get(alias)?;
        let defs = schema_map.get(table)?.column_defs.as_ref()?;
        defs.iter()
            .find(|c| c.name == ident)
            .map(|c| c.data_type.clone())
    }
}

fn push_table(factor: &TableFactor, out: &mut Vec<(String, Option<String>)>) {
    if let TableFactor::Table { name, alias, .. } = factor {
        out.push((name.clone(), alias.as_ref().map(|a| a.name.clone())));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gluesql_core::ast::{ColumnDef, ToSql};

    fn schema(table: &str, cols: &[(&str, DataType)]) -> Schema {
        Schema {
            table_name: table.to_owned(),
            column_defs: Some(
                cols.iter()
                    .map(|(name, dt)| ColumnDef {
                        name: (*name).to_owned(),
                        data_type: dt.clone(),
                        nullable: true,
                        default: None,
                        unique: None,
                        comment: None,
                    })
                    .collect(),
            ),
            indexes: Vec::new(),
            engine: None,
            foreign_keys: Vec::new(),
            comment: None,
        }
    }

    fn map(schemas: Vec<Schema>) -> SchemaMap {
        schemas
            .into_iter()
            .map(|s| (s.table_name.clone(), s))
            .collect()
    }

    /// Parse + translate `sql` to a GlueSQL statement, run the pass, and return
    /// the rewritten WHERE clause rendered back to SQL.
    fn where_after(schema_map: &SchemaMap, sql: &str) -> String {
        let parsed = gluesql_core::parse_sql::parse(sql).unwrap();
        let stmt = gluesql_core::translate::translate(&parsed[0]).unwrap();
        let out = coerce_comparisons(schema_map, stmt);
        match out {
            Statement::Query(q) => match q.body {
                SetExpr::Select(s) => s.selection.as_ref().map(ToSql::to_sql).unwrap_or_default(),
                _ => String::new(),
            },
            _ => String::new(),
        }
    }

    #[test]
    fn casts_string_literal_to_numeric_column() {
        let m = map(vec![schema("t", &[("id", DataType::Int)])]);
        let out = where_after(&m, "SELECT * FROM t WHERE id = '5'");
        assert!(out.contains("CAST"), "expected a CAST, got: {out}");
        assert!(out.contains("INT"), "expected INT target, got: {out}");
    }

    #[test]
    fn casts_on_both_orderings() {
        let m = map(vec![schema("t", &[("id", DataType::Int)])]);
        assert!(where_after(&m, "SELECT * FROM t WHERE '5' = id").contains("CAST"));
        assert!(where_after(&m, "SELECT * FROM t WHERE id < '5'").contains("CAST"));
    }

    #[test]
    fn casts_literal_vs_literal() {
        // `'1' = 1`: the documented gotcha. Text literal → INT (number literal's type).
        let m = map(vec![schema("t", &[("id", DataType::Int)])]);
        let out = where_after(&m, "SELECT * FROM t WHERE '1' = 1");
        assert!(out.contains("CAST"), "expected a CAST, got: {out}");
    }

    #[test]
    fn casts_to_decimal_column() {
        let m = map(vec![schema("t", &[("price", DataType::Decimal)])]);
        let out = where_after(&m, "SELECT * FROM t WHERE price = '9.99'");
        assert!(out.contains("CAST"), "expected a CAST, got: {out}");
        assert!(out.contains("DECIMAL"), "expected DECIMAL target, got: {out}");
    }

    #[test]
    fn does_not_cast_text_column() {
        // We never reinterpret a stored text column as a number.
        let m = map(vec![schema("t", &[("name", DataType::Text)])]);
        let out = where_after(&m, "SELECT * FROM t WHERE name = 5");
        assert!(!out.contains("CAST"), "should not cast a text column, got: {out}");
    }

    #[test]
    fn does_not_cast_non_numeric_string() {
        // 'abc' can't become an INT, so we leave the comparison alone (no new
        // runtime error) rather than inject a CAST that would fail.
        let m = map(vec![schema("t", &[("id", DataType::Int)])]);
        let out = where_after(&m, "SELECT * FROM t WHERE id = 'abc'");
        assert!(!out.contains("CAST"), "should not cast a non-numeric string, got: {out}");
    }

    #[test]
    fn does_not_cast_float_string_to_int_column() {
        // '9.99' does not `parse::<i64>()`, so casting to an INT column would
        // error in GlueSQL — skip it.
        let m = map(vec![schema("t", &[("id", DataType::Int)])]);
        let out = where_after(&m, "SELECT * FROM t WHERE id = '9.99'");
        assert!(!out.contains("CAST"), "should not cast '9.99' to INT, got: {out}");
    }

    #[test]
    fn leaves_same_class_comparisons() {
        let m = map(vec![schema("t", &[("id", DataType::Int), ("name", DataType::Text)])]);
        // number vs number, text vs text — nothing to coerce.
        assert!(!where_after(&m, "SELECT * FROM t WHERE id = 5").contains("CAST"));
        assert!(!where_after(&m, "SELECT * FROM t WHERE name = 'x'").contains("CAST"));
    }

    #[test]
    fn leaves_boolean_comparison() {
        // bool↔int is intentionally out of scope (engine-specific).
        let m = map(vec![schema("t", &[("flag", DataType::Boolean)])]);
        let out = where_after(&m, "SELECT * FROM t WHERE flag = 1");
        assert!(!out.contains("CAST"), "bool↔int must stay uncoerced, got: {out}");
    }

    #[test]
    fn skips_unknown_columns() {
        // Column not in any schema (schemaless / unknown) → left untouched.
        let m = map(vec![schema("t", &[("id", DataType::Int)])]);
        let out = where_after(&m, "SELECT * FROM t WHERE other = '5'");
        assert!(!out.contains("CAST"), "unknown column must stay uncoerced, got: {out}");
    }

    #[test]
    fn coerces_qualified_column() {
        let m = map(vec![schema("t", &[("id", DataType::Int)])]);
        let out = where_after(&m, "SELECT * FROM t WHERE t.id = '5'");
        assert!(out.contains("CAST"), "expected a CAST for qualified column, got: {out}");
    }

    #[test]
    fn coerces_inside_and_or() {
        let m = map(vec![schema("t", &[("id", DataType::Int), ("name", DataType::Text)])]);
        let out = where_after(&m, "SELECT * FROM t WHERE id = '5' AND name = 'x'");
        assert!(out.contains("CAST"), "expected nested comparison coerced, got: {out}");
    }
}
