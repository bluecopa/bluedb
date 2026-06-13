//! String-comparison tests for the PostgREST DSL → SQL translation.
//!
//! Tests assert the *exact* generated SQL — this crate's whole contract is the
//! string it produces — and cover: each filter operator, negation, projection,
//! ordering, limit/offset, INSERT/UPDATE/DELETE, the injection guard, and the
//! parser/builder round-trip equivalence.

use bluedb_rest::{
    parse_filters, parse_query, DeleteRequest, Direction, Filter, InsertRequest, Operator,
    OrderKey, RestError, RestQuery, UpdateRequest,
};

// --- SELECT: filters ---------------------------------------------------------

#[test]
fn single_eq_filter() {
    let q = RestQuery {
        table: "t".into(),
        filters: vec![Filter::new("col", Operator::Eq, "val")],
        ..Default::default()
    };
    assert_eq!(q.to_sql().unwrap(), "SELECT * FROM t WHERE col = 'val';");
}

#[test]
fn multiple_filters_anded() {
    let q = RestQuery {
        table: "t".into(),
        filters: vec![
            Filter::new("col", Operator::Eq, "val"),
            Filter::new("age", Operator::Gt, "20"),
        ],
        ..Default::default()
    };
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT * FROM t WHERE col = 'val' AND age > 20;"
    );
}

#[test]
fn numeric_gt_and_lte() {
    let q = RestQuery {
        table: "t".into(),
        filters: vec![
            Filter::new("a", Operator::Gt, "20"),
            Filter::new("b", Operator::Lte, "3.5"),
        ],
        ..Default::default()
    };
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT * FROM t WHERE a > 20 AND b <= 3.5;"
    );
}

#[test]
fn like_and_ilike() {
    let q = RestQuery {
        table: "t".into(),
        filters: vec![
            Filter::new("name", Operator::Like, "%foo%"),
            Filter::new("city", Operator::Ilike, "lon%"),
        ],
        ..Default::default()
    };
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT * FROM t WHERE name LIKE '%foo%' AND city ILIKE 'lon%';"
    );
}

#[test]
fn in_list() {
    let q = RestQuery {
        table: "t".into(),
        filters: vec![Filter::new("id", Operator::In, "(1,2,3)")],
        ..Default::default()
    };
    assert_eq!(q.to_sql().unwrap(), "SELECT * FROM t WHERE id IN (1, 2, 3);");
}

#[test]
fn in_list_strings() {
    let q = RestQuery {
        table: "t".into(),
        filters: vec![Filter::new("status", Operator::In, "(open,closed)")],
        ..Default::default()
    };
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT * FROM t WHERE status IN ('open', 'closed');"
    );
}

#[test]
fn is_null() {
    let q = RestQuery {
        table: "t".into(),
        filters: vec![Filter::new("deleted_at", Operator::Is, "null")],
        ..Default::default()
    };
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT * FROM t WHERE deleted_at IS NULL;"
    );
}

#[test]
fn is_true_and_false() {
    let q = RestQuery {
        table: "t".into(),
        filters: vec![
            Filter::new("active", Operator::Is, "true"),
            Filter::new("archived", Operator::Is, "false"),
        ],
        ..Default::default()
    };
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT * FROM t WHERE active IS TRUE AND archived IS FALSE;"
    );
}

#[test]
fn negation_wraps_comparison() {
    let q = RestQuery {
        table: "t".into(),
        filters: vec![Filter {
            column: "col".into(),
            op: Operator::Eq,
            negated: true,
            value: "val".into(),
        }],
        ..Default::default()
    };
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT * FROM t WHERE NOT (col = 'val');"
    );
}

#[test]
fn negation_of_is_null_uses_is_not() {
    let q = RestQuery {
        table: "t".into(),
        filters: vec![Filter {
            column: "deleted_at".into(),
            op: Operator::Is,
            negated: true,
            value: "null".into(),
        }],
        ..Default::default()
    };
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT * FROM t WHERE deleted_at IS NOT NULL;"
    );
}

// --- SELECT: projection / order / limit / offset -----------------------------

#[test]
fn default_projection_is_star() {
    let q = RestQuery {
        table: "t".into(),
        ..Default::default()
    };
    assert_eq!(q.to_sql().unwrap(), "SELECT * FROM t;");
}

#[test]
fn select_projection() {
    let q = RestQuery {
        table: "t".into(),
        select: vec!["a".into(), "b".into()],
        ..Default::default()
    };
    assert_eq!(q.to_sql().unwrap(), "SELECT a, b FROM t;");
}

#[test]
fn multi_key_order_asc_desc() {
    let q = RestQuery {
        table: "t".into(),
        order: vec![
            OrderKey {
                column: "age".into(),
                direction: Direction::Desc,
            },
            OrderKey {
                column: "name".into(),
                direction: Direction::Asc,
            },
        ],
        ..Default::default()
    };
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT * FROM t ORDER BY age DESC, name ASC;"
    );
}

#[test]
fn limit_and_offset() {
    let q = RestQuery {
        table: "t".into(),
        limit: Some(10),
        offset: Some(5),
        ..Default::default()
    };
    assert_eq!(q.to_sql().unwrap(), "SELECT * FROM t LIMIT 10 OFFSET 5;");
}

#[test]
fn full_select_from_spec_example() {
    // Mirrors the example from the crate spec.
    let q = parse_query(
        "table",
        "select=a,b&col=eq.val&age=gt.20&order=age.desc,name.asc&limit=10&offset=5",
    )
    .unwrap();
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT a, b FROM table WHERE col = 'val' AND age > 20 \
         ORDER BY age DESC, name ASC LIMIT 10 OFFSET 5;"
    );
}

// --- INSERT ------------------------------------------------------------------

#[test]
fn insert_single_row() {
    let req = InsertRequest {
        table: "t".into(),
        columns: vec!["id".into(), "name".into()],
        rows: vec![vec!["1".into(), "alice".into()]],
    };
    assert_eq!(
        req.to_sql().unwrap(),
        "INSERT INTO t (id, name) VALUES (1, 'alice');"
    );
}

#[test]
fn insert_multi_row() {
    let req = InsertRequest {
        table: "t".into(),
        columns: vec!["id".into(), "name".into()],
        rows: vec![
            vec!["1".into(), "alice".into()],
            vec!["2".into(), "bob".into()],
        ],
    };
    assert_eq!(
        req.to_sql().unwrap(),
        "INSERT INTO t (id, name) VALUES (1, 'alice'), (2, 'bob');"
    );
}

#[test]
fn insert_escapes_quotes_and_types_values() {
    let req = InsertRequest {
        table: "t".into(),
        columns: vec!["name".into(), "active".into(), "note".into()],
        rows: vec![vec!["O'Brien".into(), "true".into(), "null".into()]],
    };
    assert_eq!(
        req.to_sql().unwrap(),
        "INSERT INTO t (name, active, note) VALUES ('O''Brien', TRUE, NULL);"
    );
}

#[test]
fn insert_rejects_ragged_rows() {
    let req = InsertRequest {
        table: "t".into(),
        columns: vec!["a".into(), "b".into()],
        rows: vec![vec!["1".into()]],
    };
    assert!(matches!(
        req.to_sql(),
        Err(RestError::BadColumnSet(_))
    ));
}

// --- UPDATE ------------------------------------------------------------------

#[test]
fn update_with_filters() {
    let req = UpdateRequest {
        table: "t".into(),
        assignments: vec![("name".into(), "bob".into()), ("age".into(), "30".into())],
        filters: vec![Filter::new("id", Operator::Eq, "1")],
    };
    assert_eq!(
        req.to_sql().unwrap(),
        "UPDATE t SET name = 'bob', age = 30 WHERE id = 1;"
    );
}

#[test]
fn update_without_filters_is_refused() {
    let req = UpdateRequest {
        table: "t".into(),
        assignments: vec![("name".into(), "bob".into())],
        filters: vec![],
    };
    assert_eq!(
        req.to_sql(),
        Err(RestError::UnfilteredMutation("UPDATE"))
    );
}

// --- DELETE ------------------------------------------------------------------

#[test]
fn delete_with_filters() {
    let req = DeleteRequest {
        table: "t".into(),
        filters: vec![Filter::new("id", Operator::Eq, "1")],
    };
    assert_eq!(req.to_sql().unwrap(), "DELETE FROM t WHERE id = 1;");
}

#[test]
fn delete_without_filters_is_refused() {
    let req = DeleteRequest {
        table: "t".into(),
        filters: vec![],
    };
    assert_eq!(
        req.to_sql(),
        Err(RestError::UnfilteredMutation("DELETE"))
    );
}

// --- Injection guard ---------------------------------------------------------

#[test]
fn malicious_table_identifier_is_rejected() {
    let q = RestQuery {
        table: "name); DROP TABLE x;--".into(),
        ..Default::default()
    };
    assert!(matches!(
        q.to_sql(),
        Err(RestError::InvalidIdentifier(_))
    ));
}

#[test]
fn malicious_column_identifier_is_rejected() {
    let q = RestQuery {
        table: "t".into(),
        filters: vec![Filter::new("a; DROP TABLE x", Operator::Eq, "1")],
        ..Default::default()
    };
    assert!(matches!(
        q.to_sql(),
        Err(RestError::InvalidIdentifier(_))
    ));
}

#[test]
fn malicious_select_column_is_rejected() {
    let q = RestQuery {
        table: "t".into(),
        select: vec!["a, (SELECT password FROM secrets)".into()],
        ..Default::default()
    };
    assert!(matches!(
        q.to_sql(),
        Err(RestError::InvalidIdentifier(_))
    ));
}

#[test]
fn string_value_with_quote_is_escaped_not_rejected() {
    // Values are escaped (not rejected): a quote-bearing value is safe.
    let q = RestQuery {
        table: "t".into(),
        filters: vec![Filter::new("name", Operator::Eq, "x' OR '1'='1")],
        ..Default::default()
    };
    assert_eq!(
        q.to_sql().unwrap(),
        "SELECT * FROM t WHERE name = 'x'' OR ''1''=''1';"
    );
}

// --- Parser ↔ builder round-trip --------------------------------------------

#[test]
fn parser_roundtrips_to_same_sql_as_builder() {
    let built = RestQuery {
        table: "users".into(),
        select: vec!["id".into(), "name".into()],
        filters: vec![
            Filter::new("name", Operator::Eq, "foo"),
            Filter::new("age", Operator::Gt, "20"),
        ],
        order: vec![
            OrderKey {
                column: "age".into(),
                direction: Direction::Desc,
            },
            OrderKey {
                column: "name".into(),
                direction: Direction::Asc,
            },
        ],
        limit: Some(10),
        offset: Some(5),
    };

    let parsed = parse_query(
        "users",
        "select=id,name&name=eq.foo&age=gt.20&order=age.desc,name.asc&limit=10&offset=5",
    )
    .unwrap();

    assert_eq!(parsed, built);
    assert_eq!(parsed.to_sql().unwrap(), built.to_sql().unwrap());
}

#[test]
fn parser_handles_not_prefix() {
    let q = parse_query("t", "col=not.eq.val").unwrap();
    assert_eq!(q.to_sql().unwrap(), "SELECT * FROM t WHERE NOT (col = 'val');");
}

#[test]
fn parser_order_defaults_to_asc() {
    let q = parse_query("t", "order=name").unwrap();
    assert_eq!(q.to_sql().unwrap(), "SELECT * FROM t ORDER BY name ASC;");
}

#[test]
fn parse_filters_reused_for_delete() {
    let filters = parse_filters("id=eq.1&status=not.eq.closed").unwrap();
    let req = DeleteRequest {
        table: "t".into(),
        filters,
    };
    assert_eq!(
        req.to_sql().unwrap(),
        "DELETE FROM t WHERE id = 1 AND NOT (status = 'closed');"
    );
}

#[test]
fn parser_rejects_unknown_operator() {
    assert!(matches!(
        parse_query("t", "col=foo.val"),
        Err(RestError::UnknownOperator(_))
    ));
}

#[test]
fn parser_rejects_param_without_equals() {
    assert!(matches!(
        parse_query("t", "limit"),
        Err(RestError::MalformedParam(_))
    ));
}
