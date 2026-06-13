//! Parser from the raw PostgREST query-string form into the typed model.
//!
//! Given a table name and a query string like
//! `select=a,b&age=gt.20&name=not.eq.foo&order=age.desc,name&limit=10&offset=5`,
//! [`parse_query`] produces a [`RestQuery`]. The reserved keys `select`,
//! `order`, `limit`, `offset` are interpreted structurally; every other
//! `key=value` pair is a filter where the key is the column and the value is
//! `[not.]<op>.<operand>`.
//!
//! [`parse_filters`] exposes just the filter-parsing half so the `UPDATE` /
//! `DELETE` request builders can reuse it over a filter-only query string.
//!
//! This parser intentionally does **not** percent-decode: callers are expected
//! to hand it already-decoded parameter text (the same assumption a router
//! makes after extracting query params). Identifier validation still happens
//! at render time via [`crate::model::validate_ident`], so a malicious column
//! name surfaces as a [`RestError`] from `to_sql`.

use crate::error::RestError;
use crate::model::{Direction, Filter, Operator, OrderKey, RestQuery};

/// Split a query string into `(key, value)` pairs on `&` and `=`.
///
/// A parameter without `=` is rejected as [`RestError::MalformedParam`].
fn split_pairs(query: &str) -> Result<Vec<(&str, &str)>, RestError> {
    let mut pairs = Vec::new();
    for raw in query.split('&') {
        if raw.is_empty() {
            continue;
        }
        match raw.split_once('=') {
            Some((k, v)) => pairs.push((k, v)),
            None => return Err(RestError::MalformedParam(raw.to_string())),
        }
    }
    Ok(pairs)
}

/// Parse a single filter value of the form `[not.]<op>.<operand>` for `column`.
fn parse_filter(column: &str, value: &str) -> Result<Filter, RestError> {
    let (negated, rest) = match value.strip_prefix("not.") {
        Some(rest) => (true, rest),
        None => (false, value),
    };
    let (op_token, operand) = rest.split_once('.').ok_or_else(|| RestError::MalformedValue {
        op: rest.to_string(),
        value: value.to_string(),
    })?;
    let op = Operator::parse(op_token)?;
    Ok(Filter {
        column: column.to_string(),
        op,
        negated,
        value: operand.to_string(),
    })
}

/// Parse only the filter pairs out of a query string (ignoring reserved keys).
///
/// Useful for building `UPDATE`/`DELETE` requests whose `WHERE` clause comes
/// from a query string while the `SET`/row data comes from a body.
pub fn parse_filters(query: &str) -> Result<Vec<Filter>, RestError> {
    let mut filters = Vec::new();
    for (key, value) in split_pairs(query)? {
        if matches!(key, "select" | "order" | "limit" | "offset") {
            continue;
        }
        filters.push(parse_filter(key, value)?);
    }
    Ok(filters)
}

/// Parse an `order` value: comma-separated `col[.asc|.desc]` keys.
fn parse_order(value: &str) -> Result<Vec<OrderKey>, RestError> {
    let mut keys = Vec::new();
    for part in value.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (col, direction) = match part.rsplit_once('.') {
            Some((col, "asc")) => (col, Direction::Asc),
            Some((col, "desc")) => (col, Direction::Desc),
            // No (or unrecognized) suffix → treat the whole token as a column
            // with the default ASC direction. PostgREST defaults to ascending.
            _ => (part, Direction::Asc),
        };
        keys.push(OrderKey {
            column: col.to_string(),
            direction,
        });
    }
    Ok(keys)
}

/// Parse a full read query string into a [`RestQuery`] for `table`.
///
/// Reserved keys: `select` (comma list of columns; absent → `*`), `order`,
/// `limit`, `offset`. Any other key is a column filter (`[not.]<op>.<value>`).
pub fn parse_query(table: &str, query: &str) -> Result<RestQuery, RestError> {
    let mut q = RestQuery {
        table: table.to_string(),
        ..Default::default()
    };

    for (key, value) in split_pairs(query)? {
        match key {
            "select" => {
                q.select = value
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect();
            }
            "order" => q.order = parse_order(value)?,
            "limit" => {
                q.limit = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| RestError::MalformedParam(format!("limit={value}")))?,
                );
            }
            "offset" => {
                q.offset = Some(
                    value
                        .parse::<u64>()
                        .map_err(|_| RestError::MalformedParam(format!("offset={value}")))?,
                );
            }
            _ => q.filters.push(parse_filter(key, value)?),
        }
    }

    Ok(q)
}
