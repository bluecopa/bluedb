//! Error type for the PostgREST-style DSL → SQL translation layer.
//!
//! Every fallible operation — parsing a raw query string, validating an
//! identifier, or rendering a request to SQL — funnels through [`RestError`].
//! The generated SQL is executed downstream, so the failure modes here are
//! mostly *safety* guards (rejecting injection-shaped identifiers, malformed
//! operators, empty mutations) rather than ordinary I/O errors.

use thiserror::Error;

/// Errors raised while parsing or rendering a REST query.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum RestError {
    /// An identifier (table or column) failed the strict
    /// `^[A-Za-z_][A-Za-z0-9_]*$` validation. The string is the offending name.
    ///
    /// This is the primary SQL-injection guard: any identifier carrying
    /// quotes, semicolons, whitespace, or comment markers is rejected before
    /// it can reach the rendered SQL.
    #[error("invalid identifier: {0:?} (must match ^[A-Za-z_][A-Za-z0-9_]*$)")]
    InvalidIdentifier(String),

    /// A filter operator token was not one of the recognized PostgREST
    /// operators (`eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `like`, `ilike`,
    /// `in`, `is`).
    #[error("unknown filter operator: {0:?}")]
    UnknownOperator(String),

    /// A filter value was malformed for its operator — e.g. an `in.(…)` list
    /// missing its parentheses, or an `is.…` predicate whose operand was not
    /// `null` / `true` / `false`.
    #[error("malformed value for operator {op:?}: {value:?}")]
    MalformedValue {
        /// The operator the value was supplied for.
        op: String,
        /// The offending value text.
        value: String,
    },

    /// A raw query-string parameter was not in `key=value` form, or named a
    /// reserved keyword (`select`, `order`, `limit`, `offset`) with a value
    /// that could not be parsed.
    #[error("malformed query parameter: {0:?}")]
    MalformedParam(String),

    /// An `UPDATE` or `DELETE` was requested with no `WHERE` filters. Rendering
    /// an unfiltered mutation is refused to avoid accidental full-table writes.
    #[error("refusing to render {0} with no filters (would affect every row)")]
    UnfilteredMutation(&'static str),

    /// An `INSERT`/`UPDATE` carried no columns, or a multi-row `INSERT` had rows
    /// with differing column sets.
    #[error("empty or inconsistent column set: {0}")]
    BadColumnSet(String),
}
