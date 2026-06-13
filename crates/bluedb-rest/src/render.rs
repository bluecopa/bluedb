//! `to_sql()` implementations: request model → SQL string.
//!
//! Each request type renders to a single terminated SQL statement. Every table
//! and column name is run through [`validate_ident`] before interpolation, and
//! every value through [`render_value`] (see [`crate::model`]), so the only
//! free-form text that reaches the output is inside properly-escaped string
//! literals.

use crate::error::RestError;
use crate::model::{
    render_value, validate_ident, DeleteRequest, InsertRequest, RestQuery, UpdateRequest,
};

/// Build the shared `WHERE …` clause (including the leading space + keyword),
/// or the empty string when there are no filters.
fn render_where(filters: &[crate::model::Filter]) -> Result<String, RestError> {
    if filters.is_empty() {
        return Ok(String::new());
    }
    let parts: Vec<String> = filters
        .iter()
        .map(|f| f.to_sql())
        .collect::<Result<_, _>>()?;
    Ok(format!(" WHERE {}", parts.join(" AND ")))
}

impl RestQuery {
    /// Render this read query to a `SELECT … ;` statement.
    pub fn to_sql(&self) -> Result<String, RestError> {
        let table = validate_ident(&self.table)?;

        let projection = if self.select.is_empty() {
            "*".to_string()
        } else {
            let cols: Vec<&str> = self
                .select
                .iter()
                .map(|c| validate_ident(c))
                .collect::<Result<_, _>>()?;
            cols.join(", ")
        };

        let mut sql = format!("SELECT {projection} FROM {table}");
        sql.push_str(&render_where(&self.filters)?);

        if !self.order.is_empty() {
            let keys: Vec<String> = self
                .order
                .iter()
                .map(|k| {
                    let col = validate_ident(&k.column)?;
                    Ok(format!("{col} {}", direction_sql(k.direction)))
                })
                .collect::<Result<_, RestError>>()?;
            sql.push_str(&format!(" ORDER BY {}", keys.join(", ")));
        }

        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }

        sql.push(';');
        Ok(sql)
    }
}

impl InsertRequest {
    /// Render this insert to an `INSERT INTO … VALUES … ;` statement.
    ///
    /// Supports single- and multi-row inserts. Errors if there are no columns,
    /// no rows, or any row's arity differs from the column count.
    pub fn to_sql(&self) -> Result<String, RestError> {
        let table = validate_ident(&self.table)?;
        if self.columns.is_empty() {
            return Err(RestError::BadColumnSet("INSERT has no columns".to_string()));
        }
        if self.rows.is_empty() {
            return Err(RestError::BadColumnSet("INSERT has no rows".to_string()));
        }
        let cols: Vec<&str> = self
            .columns
            .iter()
            .map(|c| validate_ident(c))
            .collect::<Result<_, _>>()?;

        let mut tuples = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            if row.len() != self.columns.len() {
                return Err(RestError::BadColumnSet(format!(
                    "row has {} values but there are {} columns",
                    row.len(),
                    self.columns.len()
                )));
            }
            let vals: Vec<String> = row.iter().map(|v| render_value(v)).collect();
            tuples.push(format!("({})", vals.join(", ")));
        }

        Ok(format!(
            "INSERT INTO {table} ({}) VALUES {};",
            cols.join(", "),
            tuples.join(", ")
        ))
    }
}

impl UpdateRequest {
    /// Render this update to an `UPDATE … SET … WHERE … ;` statement.
    ///
    /// Refuses an empty filter set ([`RestError::UnfilteredMutation`]) to avoid
    /// an accidental full-table update.
    pub fn to_sql(&self) -> Result<String, RestError> {
        let table = validate_ident(&self.table)?;
        if self.assignments.is_empty() {
            return Err(RestError::BadColumnSet(
                "UPDATE has no assignments".to_string(),
            ));
        }
        if self.filters.is_empty() {
            return Err(RestError::UnfilteredMutation("UPDATE"));
        }
        let sets: Vec<String> = self
            .assignments
            .iter()
            .map(|(col, val)| {
                let col = validate_ident(col)?;
                Ok(format!("{col} = {}", render_value(val)))
            })
            .collect::<Result<_, RestError>>()?;

        Ok(format!(
            "UPDATE {table} SET {}{};",
            sets.join(", "),
            render_where(&self.filters)?
        ))
    }
}

impl DeleteRequest {
    /// Render this delete to a `DELETE FROM … WHERE … ;` statement.
    ///
    /// Refuses an empty filter set ([`RestError::UnfilteredMutation`]) to avoid
    /// an accidental full-table delete.
    pub fn to_sql(&self) -> Result<String, RestError> {
        let table = validate_ident(&self.table)?;
        if self.filters.is_empty() {
            return Err(RestError::UnfilteredMutation("DELETE"));
        }
        Ok(format!(
            "DELETE FROM {table}{};",
            render_where(&self.filters)?
        ))
    }
}

/// `Direction` SQL keyword. Kept here so the public `Direction` enum need not
/// expose its rendering.
fn direction_sql(direction: crate::model::Direction) -> &'static str {
    match direction {
        crate::model::Direction::Asc => "ASC",
        crate::model::Direction::Desc => "DESC",
    }
}
