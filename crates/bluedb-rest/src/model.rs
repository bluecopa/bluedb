//! Typed request model and the rendering primitives shared by every request.
//!
//! This module holds the data structures a caller can build directly
//! ([`RestQuery`], [`Filter`], [`InsertRequest`], [`UpdateRequest`],
//! [`DeleteRequest`]) plus the two rendering rules that keep the generated SQL
//! safe:
//!
//! * **Identifier safety** — [`validate_ident`] rejects any table/column name
//!   that is not a bare SQL identifier (`^[A-Za-z_][A-Za-z0-9_]*$`). Because
//!   identifiers cannot be parameterized, this allow-list is the injection
//!   guard for the structural parts of the query.
//! * **Value typing** — [`render_value`] decides how a stringly-typed DSL value
//!   becomes a SQL literal. The rule (documented on that function) is: `null`
//!   → `NULL`, `true`/`false` → boolean, anything that parses as an integer or
//!   float → numeric literal, everything else → a single-quoted string with
//!   embedded quotes doubled.

use crate::error::RestError;
use serde::{Deserialize, Serialize};

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
/// scalar operators ([`Operator::Eq`], `gt`, `like`, …) feed it through
/// [`render_value`]; [`Operator::In`] expects a `(a,b,c)` list; [`Operator::Is`]
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

    /// Render this predicate to its SQL fragment (without the leading `WHERE`).
    pub fn to_sql(&self) -> Result<String, RestError> {
        let col = validate_ident(&self.column)?;
        let inner = match self.op {
            Operator::Is => {
                // `is` does not go through render_value: only null/true/false.
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
                // `is` carries its own negated form (IS NOT …) so we handle the
                // whole predicate here and short-circuit the generic negation.
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
                let list = render_in_list(&self.value)?;
                format!("{col} IN ({list})")
            }
            Operator::Eq => format!("{col} = {}", render_value(&self.value)),
            Operator::Neq => format!("{col} <> {}", render_value(&self.value)),
            Operator::Gt => format!("{col} > {}", render_value(&self.value)),
            Operator::Gte => format!("{col} >= {}", render_value(&self.value)),
            Operator::Lt => format!("{col} < {}", render_value(&self.value)),
            Operator::Lte => format!("{col} <= {}", render_value(&self.value)),
            Operator::Like => format!("{col} LIKE {}", render_value(&self.value)),
            Operator::Ilike => format!("{col} ILIKE {}", render_value(&self.value)),
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

/// Render a stringly-typed DSL value into a SQL literal.
///
/// # Value-typing rule (chosen, documented)
///
/// PostgREST is stringly-typed on the wire; we pick a defensible, predictable
/// mapping from the raw text to a SQL literal:
///
/// 1. `null` (case-insensitive) → `NULL`.
/// 2. `true` / `false` (case-insensitive) → `TRUE` / `FALSE`.
/// 3. A value that parses as an `i64` **or** `f64` → emitted verbatim as a
///    numeric literal (no quotes). The original text is preserved so
///    `10` stays `10` and `10.50` stays `10.50`.
/// 4. Everything else → a single-quoted string literal with embedded single
///    quotes doubled (`O'Brien` → `'O''Brien'`). This is the escaping that
///    blocks string-literal injection.
///
/// Note: the only way to force a numeric-looking value to be treated as a
/// string is out of band — this rule is intentionally simple and total so the
/// rendered SQL is fully determined by the input text.
pub fn render_value(value: &str) -> String {
    let lower = value.to_ascii_lowercase();
    if lower == "null" {
        return "NULL".to_string();
    }
    if lower == "true" {
        return "TRUE".to_string();
    }
    if lower == "false" {
        return "FALSE".to_string();
    }
    if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
        return value.to_string();
    }
    quote_string(value)
}

/// Single-quote a string literal, doubling any embedded single quotes.
fn quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Render the comma-separated body of an `IN (…)` list.
///
/// Accepts both the PostgREST wire form `(a,b,c)` (parens included) and a bare
/// `a,b,c`. Each element is run through [`render_value`], so numeric/bool/null
/// typing and string escaping apply per element. An empty list is rejected as
/// malformed.
fn render_in_list(value: &str) -> Result<String, RestError> {
    let trimmed = value.trim();
    let body = trimmed
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .unwrap_or(trimmed);
    let elems: Vec<String> = body
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(render_value)
        .collect();
    if elems.is_empty() {
        return Err(RestError::MalformedValue {
            op: "in".to_string(),
            value: value.to_string(),
        });
    }
    Ok(elems.join(", "))
}
