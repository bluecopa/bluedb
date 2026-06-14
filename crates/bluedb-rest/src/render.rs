//! `to_sql_with_params()` implementations: request model → (SQL string, params).
//!
//! Each request type renders to a single terminated SQL statement with all
//! user-supplied values bound as `$N` parameters. Every table and column name
//! is run through [`validate_ident`] before interpolation — identifiers cannot
//! be parameterized, so the allow-list is their injection guard.

use crate::error::RestError;
use crate::model::{
    bind, validate_ident, DeleteRequest, InsertRequest, Param, RestQuery, UpdateRequest,
};

/// Param-aware `WHERE …` builder: appends each filter's binds to `params`.
fn render_where_params(
    filters: &[crate::model::Filter],
    params: &mut Vec<Param>,
) -> Result<String, RestError> {
    if filters.is_empty() {
        return Ok(String::new());
    }
    let parts: Vec<String> = filters
        .iter()
        .map(|f| f.to_sql_with_params(params))
        .collect::<Result<_, _>>()?;
    Ok(format!(" WHERE {}", parts.join(" AND ")))
}

impl RestQuery {
    /// Render to `SELECT … ;` with bound `$N` parameters for all filter values.
    pub fn to_sql_with_params(&self) -> Result<(String, Vec<Param>), RestError> {
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

        let mut params = Vec::new();
        let mut sql = format!("SELECT {projection} FROM {table}");
        sql.push_str(&render_where_params(&self.filters, &mut params)?);

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
        Ok((sql, params))
    }
}

impl InsertRequest {
    /// Render one **single-row** `INSERT` statement per row (no terminating `;`),
    /// with placeholders numbered **globally** across all rows so the statements
    /// can share one params slice (gluesql binds `$N` against a single shared
    /// vector). Avoids multi-row `VALUES (..),(..)`, which sqlparser rejects past
    /// ~50 tuples. The caller wraps the statements in `BEGIN; … ; COMMIT;`.
    pub fn row_statements_with_params(&self) -> Result<(Vec<String>, Vec<Param>), RestError> {
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
        let col_list = cols.join(", ");

        let mut params = Vec::new();
        let mut stmts = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            if row.len() != self.columns.len() {
                return Err(RestError::BadColumnSet(format!(
                    "row has {} values but there are {} columns",
                    row.len(),
                    self.columns.len()
                )));
            }
            let placeholders: Vec<String> = row.iter().map(|v| bind(v, &mut params)).collect();
            stmts.push(format!(
                "INSERT INTO {table} ({col_list}) VALUES ({})",
                placeholders.join(", ")
            ));
        }
        Ok((stmts, params))
    }
}

impl UpdateRequest {
    /// Render to `UPDATE … SET … WHERE … ;` with bound `$N` parameters.
    ///
    /// Refuses an empty filter set ([`RestError::UnfilteredMutation`]) to avoid
    /// an accidental full-table update.
    pub fn to_sql_with_params(&self) -> Result<(String, Vec<Param>), RestError> {
        let table = validate_ident(&self.table)?;
        if self.assignments.is_empty() {
            return Err(RestError::BadColumnSet("UPDATE has no assignments".to_string()));
        }
        if self.filters.is_empty() {
            return Err(RestError::UnfilteredMutation("UPDATE"));
        }
        let mut params = Vec::new();
        let sets: Vec<String> = self
            .assignments
            .iter()
            .map(|(col, val)| {
                let col = validate_ident(col)?;
                Ok(format!("{col} = {}", bind(val, &mut params)))
            })
            .collect::<Result<_, RestError>>()?;
        let where_clause = render_where_params(&self.filters, &mut params)?;
        Ok((format!("UPDATE {table} SET {}{};", sets.join(", "), where_clause), params))
    }
}

impl DeleteRequest {
    /// Render to `DELETE FROM … WHERE … ;` with bound `$N` parameters.
    ///
    /// Refuses an empty filter set ([`RestError::UnfilteredMutation`]) to avoid
    /// an accidental full-table delete.
    pub fn to_sql_with_params(&self) -> Result<(String, Vec<Param>), RestError> {
        let table = validate_ident(&self.table)?;
        if self.filters.is_empty() {
            return Err(RestError::UnfilteredMutation("DELETE"));
        }
        let mut params = Vec::new();
        let where_clause = render_where_params(&self.filters, &mut params)?;
        Ok((format!("DELETE FROM {table}{};", where_clause), params))
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

#[cfg(test)]
mod params_render {
    use crate::model::{Filter, InsertRequest, Operator, OrderKey, Direction, Param, RestQuery, UpdateRequest, DeleteRequest};

    #[test]
    fn select_binds_filters_keeps_structure_literal() {
        let q = RestQuery {
            table: "users".into(),
            select: vec!["id".into(), "name".into()],
            filters: vec![Filter::new("age", Operator::Gt, "20")],
            order: vec![OrderKey { column: "name".into(), direction: Direction::Asc }],
            limit: Some(10),
            offset: Some(5),
        };
        let (sql, params) = q.to_sql_with_params().unwrap();
        assert_eq!(
            sql,
            "SELECT id, name FROM users WHERE age > $1 ORDER BY name ASC LIMIT 10 OFFSET 5;"
        );
        assert_eq!(params, vec![Param::Int(20)]);
    }

    #[test]
    fn update_binds_sets_then_filters_in_order() {
        let u = UpdateRequest {
            table: "t".into(),
            assignments: vec![("name".into(), "amy".into()), ("age".into(), "9".into())],
            filters: vec![Filter::new("id", Operator::Eq, "1")],
        };
        let (sql, params) = u.to_sql_with_params().unwrap();
        assert_eq!(sql, "UPDATE t SET name = $1, age = $2 WHERE id = $3;");
        assert_eq!(params, vec![Param::Str("amy".into()), Param::Int(9), Param::Int(1)]);
    }

    #[test]
    fn delete_binds_filters() {
        let d = DeleteRequest {
            table: "t".into(),
            filters: vec![Filter::new("id", Operator::Eq, "42")],
        };
        let (sql, params) = d.to_sql_with_params().unwrap();
        assert_eq!(sql, "DELETE FROM t WHERE id = $1;");
        assert_eq!(params, vec![Param::Int(42)]);
    }

    #[test]
    fn insert_emits_one_statement_per_row_with_global_indices() {
        let req = InsertRequest {
            table: "docs".into(),
            columns: vec!["id".into(), "body".into()],
            rows: vec![
                vec!["1".into(), "hi".into()],
                vec!["2".into(), "yo".into()],
            ],
        };
        let (stmts, params) = req.row_statements_with_params().unwrap();
        assert_eq!(
            stmts,
            vec![
                "INSERT INTO docs (id, body) VALUES ($1, $2)".to_string(),
                "INSERT INTO docs (id, body) VALUES ($3, $4)".to_string(),
            ]
        );
        assert_eq!(
            params,
            vec![Param::Int(1), Param::Str("hi".into()), Param::Int(2), Param::Str("yo".into())]
        );
    }

    #[test]
    fn select_multi_filter_joins_with_and_and_continues_index() {
        // One negated `eq` and one `IN` — exercises NOT(...) wrapping, the AND join,
        // and the running $N index spanning multiple filters.
        let q = RestQuery {
            table: "t".into(),
            select: vec![],
            filters: vec![
                Filter { column: "name".into(), op: Operator::Eq, negated: true, value: "amy".into() },
                Filter::new("id", Operator::In, "(1,2)"),
            ],
            order: vec![],
            limit: None,
            offset: None,
        };
        let (sql, params) = q.to_sql_with_params().unwrap();
        assert_eq!(sql, "SELECT * FROM t WHERE NOT (name = $1) AND id IN ($2, $3);");
        assert_eq!(params, vec![Param::Str("amy".into()), Param::Int(1), Param::Int(2)]);
    }
}
