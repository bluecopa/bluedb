//! Typed request model and the rendering primitives shared by every request.
//!
//! This module holds the data structures a caller can build directly
//! ([`RestQuery`], [`Filter`], [`InsertRequest`], [`UpdateRequest`],
//! [`DeleteRequest`]) plus the rendering rules that keep the generated SQL
//! safe:
//!
//! * **Identifier safety** — [`validate_ident`] rejects any table/column name
//!   that is not a bare SQL identifier (`^[A-Za-z_][A-Za-z0-9_]*$`). Because
//!   identifiers cannot be parameterized, this allow-list is the injection
//!   guard for the structural parts of the query.
//! * **Bound-parameter path (injection-proof)** — [`render_param`] types a
//!   stringly-typed value into a [`Param`] variant; [`bind`] appends it to a
//!   `Vec<Param>` and returns the corresponding `$N` placeholder.
//!   [`Filter::to_sql_with_params`] uses this path: user values never appear
//!   in SQL text, so SQL-injection via filter values is structurally impossible.

use crate::error::RestError;
use serde::{Deserialize, Serialize};

/// A typed value to be bound as a gluesql `$N` parameter — never interpolated
/// into SQL text. This is the injection-proof replacement for emitting a literal:
/// user data flows through `params`, so it can never change query structure.
#[derive(Debug, Clone, PartialEq)]
pub enum Param {
    /// SQL `NULL`.
    Null,
    /// Boolean.
    Bool(bool),
    /// 64-bit integer.
    Int(i64),
    /// 64-bit float.
    Float(f64),
    /// UTF-8 string (the default for anything not recognized as the above).
    Str(String),
}

/// Type a stringly-typed DSL value into a [`Param`].
///
/// Typing rule: `null`/`true`/`false` (case-insensitive) → those variants;
/// text that parses as `i64` → [`Param::Int`]; else `f64` → [`Param::Float`];
/// everything else → [`Param::Str`].
pub(crate) fn render_param(value: &str) -> Param {
    let lower = value.to_ascii_lowercase();
    if lower == "null" {
        return Param::Null;
    }
    if lower == "true" {
        return Param::Bool(true);
    }
    if lower == "false" {
        return Param::Bool(false);
    }
    if let Ok(i) = value.parse::<i64>() {
        return Param::Int(i);
    }
    if let Ok(f) = value.parse::<f64>() {
        return Param::Float(f);
    }
    Param::Str(value.to_string())
}

/// Push `render_param(value)` onto `params` and return its 1-based `$N`
/// placeholder (gluesql positional-parameter syntax).
pub(crate) fn bind(value: &str, params: &mut Vec<Param>) -> String {
    params.push(render_param(value));
    format!("${}", params.len())
}

/// A PostgREST filter operator.
///
/// # Operator mapping
///
/// | DSL token | SQL rendering (column `c`, value `v`)          |
/// |-----------|------------------------------------------------|
/// | `eq`      | `c = v`                                        |
/// | `neq`     | `c <> v`                                       |
/// | `gt`      | `c > v`                                        |
/// | `gte`     | `c >= v`                                        |
/// | `lt`      | `c < v`                                         |
/// | `lte`     | `c <= v`                                        |
/// | `like`    | `c LIKE v`                                      |
/// | `ilike`   | `c ILIKE v`                                     |
/// | `in`      | `c IN (v1, v2, …)` (value is `(a,b,c)`)         |
/// | `is`      | `c IS NULL` / `c IS TRUE` / `c IS FALSE`        |
///
/// A [`Filter`] may also be `negated`, which wraps the predicate: `eq` →
/// `c <> v` becomes `NOT (c = v)`; `IS NULL` → `IS NOT NULL`; `IN` → `NOT IN`;
/// `LIKE` → `NOT LIKE`. The negation is rendered uniformly as `NOT (…)` for
/// the comparison/`like`/`in` families and as the dedicated `IS NOT` form for
/// `is`, matching PostgREST's `not.` prefix semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operator {
    /// `=`
    Eq,
    /// `<>`
    Neq,
    /// `>`
    Gt,
    /// `>=`
    Gte,
    /// `<`
    Lt,
    /// `<=`
    Lte,
    /// `LIKE`
    Like,
    /// `ILIKE`
    Ilike,
    /// `IN (…)`
    In,
    /// `IS NULL` / `IS TRUE` / `IS FALSE`
    Is,
}

impl Operator {
    /// Parse a PostgREST operator token (e.g. `"eq"`, `"gte"`, `"ilike"`).
    pub fn parse(token: &str) -> Result<Self, RestError> {
        Ok(match token {
            "eq" => Operator::Eq,
            "neq" => Operator::Neq,
            "gt" => Operator::Gt,
            "gte" => Operator::Gte,
            "lt" => Operator::Lt,
            "lte" => Operator::Lte,
            "like" => Operator::Like,
            "ilike" => Operator::Ilike,
            "in" => Operator::In,
            "is" => Operator::Is,
            other => return Err(RestError::UnknownOperator(other.to_string())),
        })
    }

    /// The DSL token for this operator (inverse of [`Operator::parse`]).
    pub fn token(self) -> &'static str {
        match self {
            Operator::Eq => "eq",
            Operator::Neq => "neq",
            Operator::Gt => "gt",
            Operator::Gte => "gte",
            Operator::Lt => "lt",
            Operator::Lte => "lte",
            Operator::Like => "like",
            Operator::Ilike => "ilike",
            Operator::In => "in",
            Operator::Is => "is",
        }
    }
}

/// Sort direction for an `ORDER BY` key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    /// `ASC` — the default when no `.asc`/`.desc` suffix is given.
    #[default]
    Asc,
    /// `DESC`
    Desc,
}

/// A single `ORDER BY` key: a column plus a direction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderKey {
    /// Column to sort on (validated as an identifier when rendered).
    pub column: String,
    /// Sort direction.
    pub direction: Direction,
}

/// One `WHERE`-clause predicate.
///
/// `value` is the raw DSL value text. Its interpretation depends on `op`:
/// scalar operators ([`Operator::Eq`], `gt`, `like`, …) are bound as typed `$N`
/// parameters; [`Operator::In`] expects a `(a,b,c)` list; [`Operator::Is`]
/// expects `null` / `true` / `false`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filter {
    /// Column the predicate applies to.
    pub column: String,
    /// Comparison operator.
    pub op: Operator,
    /// Whether a PostgREST `not.` prefix wraps the predicate in negation.
    pub negated: bool,
    /// Raw DSL value text (interpretation depends on `op`).
    pub value: String,
}

impl Filter {
    /// Convenience constructor for a non-negated filter.
    pub fn new(column: impl Into<String>, op: Operator, value: impl Into<String>) -> Self {
        Filter {
            column: column.into(),
            op,
            negated: false,
            value: value.into(),
        }
    }

    /// Render this predicate with bound parameters: returns the SQL fragment
    /// (no leading `WHERE`) and appends each value to `params` as a `$N` bind.
    /// Identifiers are still allow-listed (they cannot be parameters).
    pub fn to_sql_with_params(&self, params: &mut Vec<Param>) -> Result<String, RestError> {
        let col = render_column_ref(&self.column)?;
        let inner = match self.op {
            Operator::Is => {
                let predicate = match self.value.to_ascii_lowercase().as_str() {
                    "null" => "IS NULL",
                    "true" => "IS TRUE",
                    "false" => "IS FALSE",
                    _ => {
                        return Err(RestError::MalformedValue {
                            op: "is".to_string(),
                            value: self.value.clone(),
                        })
                    }
                };
                let predicate = if self.negated {
                    match predicate {
                        "IS NULL" => "IS NOT NULL",
                        "IS TRUE" => "IS NOT TRUE",
                        "IS FALSE" => "IS NOT FALSE",
                        _ => unreachable!("predicate is one of the three above"),
                    }
                } else {
                    predicate
                };
                return Ok(format!("{col} {predicate}"));
            }
            Operator::In => {
                let list = render_in_list_params(&self.value, params)?;
                format!("{col} IN ({list})")
            }
            Operator::Eq => format!("{col} = {}", bind(&self.value, params)),
            Operator::Neq => format!("{col} <> {}", bind(&self.value, params)),
            Operator::Gt => format!("{col} > {}", bind(&self.value, params)),
            Operator::Gte => format!("{col} >= {}", bind(&self.value, params)),
            Operator::Lt => format!("{col} < {}", bind(&self.value, params)),
            Operator::Lte => format!("{col} <= {}", bind(&self.value, params)),
            Operator::Like => format!("{col} LIKE {}", bind(&self.value, params)),
            Operator::Ilike => format!("{col} ILIKE {}", bind(&self.value, params)),
        };
        if self.negated {
            Ok(format!("NOT ({inner})"))
        } else {
            Ok(inner)
        }
    }
}

/// A read query: `SELECT … FROM table [WHERE …] [ORDER BY …] [LIMIT …] [OFFSET …]`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RestQuery {
    /// Table to read from.
    pub table: String,
    /// Projected columns. Empty → `SELECT *`.
    pub select: Vec<String>,
    /// `WHERE` predicates, AND'd together.
    pub filters: Vec<Filter>,
    /// `ORDER BY` keys, in order.
    pub order: Vec<OrderKey>,
    /// `LIMIT` value.
    pub limit: Option<u64>,
    /// `OFFSET` value.
    pub offset: Option<u64>,
}

/// An `INSERT INTO table (cols…) VALUES (…)[, (…)]` request.
///
/// `columns` names the column order; each entry in `rows` is the value tuple
/// for that column order. All rows must have `columns.len()` values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InsertRequest {
    /// Target table.
    pub table: String,
    /// Column order shared by every row.
    pub columns: Vec<String>,
    /// One inner `Vec` per row, each aligned to `columns`.
    pub rows: Vec<Vec<String>>,
}

/// An `UPDATE table SET … WHERE …` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateRequest {
    /// Target table.
    pub table: String,
    /// `(column, value)` assignments for the `SET` clause, in order.
    pub assignments: Vec<(String, String)>,
    /// `WHERE` predicates. Required: an empty set is refused at render time.
    pub filters: Vec<Filter>,
}

/// A `DELETE FROM table WHERE …` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteRequest {
    /// Target table.
    pub table: String,
    /// `WHERE` predicates. Required: an empty set is refused at render time.
    pub filters: Vec<Filter>,
}

/// Validate a SQL identifier (table or column name).
///
/// Returns the name unchanged when it matches `^[A-Za-z_][A-Za-z0-9_]*$`,
/// otherwise [`RestError::InvalidIdentifier`]. This hand-rolled check avoids a
/// regex dependency while being exactly equivalent. Because identifiers are
/// interpolated into the SQL structurally (they cannot be bound parameters),
/// this allow-list is the injection guard for table/column names.
pub fn validate_ident(name: &str) -> Result<&str, RestError> {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return Err(RestError::InvalidIdentifier(name.to_string())),
    }
    if chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(name)
    } else {
        Err(RestError::InvalidIdentifier(name.to_string()))
    }
}

/// True if `col` carries a PostgREST JSON path (`col->>key` / `col->key`).
pub fn is_json_path(col: &str) -> bool {
    col.contains("->")
}

/// Render a column reference that may carry a single-level PostgREST JSON path:
/// `col->>key` → `json_get_str(col, 'key')` (text) and `col->key` →
/// `json_get(col, 'key')` (JSON), matching the accessor UDFs the analytical
/// engine registers. The base **and** key are each `validate_ident`'d, so the
/// key becomes a single-quoted literal whose content is a bare identifier and
/// cannot break out of the quotes — the same injection guard as a plain column.
/// A column with no `->` renders as the plain validated identifier.
///
/// JSON paths are only ever served by the analytical engine (GlueSQL has no JSON
/// functions); the server routes a query carrying one there (see
/// [`RestQuery::has_json_path`]).
pub(crate) fn render_column_ref(col: &str) -> Result<String, RestError> {
    if let Some((base, key)) = col.split_once("->>") {
        let base = validate_ident(base)?;
        let key = validate_ident(key)?;
        return Ok(format!("json_get_str({base}, '{key}')"));
    }
    if let Some((base, key)) = col.split_once("->") {
        let base = validate_ident(base)?;
        let key = validate_ident(key)?;
        return Ok(format!("json_get({base}, '{key}')"));
    }
    validate_ident(col).map(str::to_string)
}

impl RestQuery {
    /// True if any filter, order key, or projected column carries a JSON path.
    /// Such a query can only be served by the analytical engine, so the server
    /// routes it to DataFusion rather than the GlueSQL fast path.
    pub fn has_json_path(&self) -> bool {
        self.filters.iter().any(|f| is_json_path(&f.column))
            || self.order.iter().any(|k| is_json_path(&k.column))
            || self.select.iter().any(|c| is_json_path(c))
    }
}

#[cfg(test)]
mod param_tests {
    use super::{render_param, Param};

    #[test]
    fn render_param_types_like_the_old_rule() {
        assert_eq!(render_param("null"), Param::Null);
        assert_eq!(render_param("NULL"), Param::Null);
        assert_eq!(render_param("true"), Param::Bool(true));
        assert_eq!(render_param("false"), Param::Bool(false));
        assert_eq!(render_param("10"), Param::Int(10));
        assert_eq!(render_param("-3"), Param::Int(-3));
        // 10.5 is exact in f64; PartialEq is safe here
        assert_eq!(render_param("10.5"), Param::Float(10.5));
        assert_eq!(render_param("hello"), Param::Str("hello".to_string()));
        // The injection payload becomes plain string DATA, never structure:
        assert_eq!(
            render_param("'); DROP TABLE t; --"),
            Param::Str("'); DROP TABLE t; --".to_string())
        );
    }

    use super::{Filter, Operator};

    #[test]
    fn filter_binds_values_as_placeholders() {
        let mut params = Vec::new();
        let f = Filter::new("age", Operator::Gt, "20");
        assert_eq!(f.to_sql_with_params(&mut params).unwrap(), "age > $1");
        assert_eq!(params, vec![Param::Int(20)]);

        // `in` binds each element; placeholders continue the running index.
        let mut params = Vec::new();
        let f = Filter::new("id", Operator::In, "(1,2,3)");
        assert_eq!(
            f.to_sql_with_params(&mut params).unwrap(),
            "id IN ($1, $2, $3)"
        );
        assert_eq!(params, vec![Param::Int(1), Param::Int(2), Param::Int(3)]);

        // `is` binds nothing (keywords only).
        let mut params = Vec::new();
        let f = Filter {
            column: "x".into(),
            op: Operator::Is,
            negated: true,
            value: "null".into(),
        };
        assert_eq!(f.to_sql_with_params(&mut params).unwrap(), "x IS NOT NULL");
        assert!(params.is_empty());

        // negation wraps the comparison.
        let mut params = Vec::new();
        let f = Filter {
            column: "name".into(),
            op: Operator::Eq,
            negated: true,
            value: "amy".into(),
        };
        assert_eq!(
            f.to_sql_with_params(&mut params).unwrap(),
            "NOT (name = $1)"
        );
        assert_eq!(params, vec![Param::Str("amy".into())]);
    }
}

/// Bind each element of an `IN (…)` list as a `$N` parameter and return
/// the comma-separated placeholder list (`$1, $2, …`).
fn render_in_list_params(value: &str, params: &mut Vec<Param>) -> Result<String, RestError> {
    let trimmed = value.trim();
    let body = trimmed
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(trimmed);
    let placeholders: Vec<String> = body
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|elem| bind(elem, params))
        .collect();
    if placeholders.is_empty() {
        return Err(RestError::MalformedValue {
            op: "in".to_string(),
            value: value.to_string(),
        });
    }
    Ok(placeholders.join(", "))
}
