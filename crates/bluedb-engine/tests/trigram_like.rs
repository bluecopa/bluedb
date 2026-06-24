//! End-to-end correctness for the trigram-accelerated `LIKE '%lit%'` rewrite
//! (Spec B §4.6, B3 part 1). The contract: a `LIKE` SELECT routed through the
//! FTS engine returns EXACTLY the same rows as a plain full-table scan — the
//! `pk IN (trigram candidates)` prefilter only prunes non-matching rows, and
//! gluesql's `LIKE` stays in the query as the authoritative verify. The prefilter
//! is added ONLY for a clean ≥3-char infix literal with a trigram index present;
//! every other shape passes through to the exact scan.

use std::sync::Arc;

use bluedb_engine::{rest_sql, FtsEngine};
use bluedb_sql::Database;
use gluesql_core::prelude::{Glue, Payload, Value};
use slatedb::object_store::memory::InMemory;
use slatedb::Db;

/// Run `sql` THROUGH the FTS engine (so the trigram LIKE rewrite applies if a
/// trigram index is declared) and return the matching `id`s, sorted.
async fn engine_ids(fts: &FtsEngine, database: &Database, sql: &str) -> Vec<i64> {
    let mut g = Glue::new(database.connection_serialized());
    let out = fts.execute_fts(&mut g, sql, &[], None).await.unwrap();
    ids_from(out)
}

/// Run `sql` as a plain full-table scan (NO engine rewrite) and return the
/// matching `id`s, sorted — the correctness baseline.
async fn scan_ids(database: &Database, sql: &str) -> Vec<i64> {
    let mut g = Glue::new(database.connection_serialized());
    let out = rest_sql::execute_sql(&mut g, sql, &[], false)
        .await
        .unwrap();
    ids_from(out)
}

fn ids_from(out: Vec<Payload>) -> Vec<i64> {
    match out.into_iter().next().unwrap() {
        Payload::Select { rows, .. } => {
            let mut ids: Vec<i64> = rows
                .iter()
                .map(|r| match &r[0] {
                    Value::I64(n) => *n,
                    other => panic!("expected I64 id, got {other:?}"),
                })
                .collect();
            ids.sort_unstable();
            ids
        }
        other => panic!("{other:?}"),
    }
}

/// Seed a `docs` table + trigram index on `body`, insert the standard fixture
/// rows on an observed connection (maintains the live trigram segment).
async fn seed(name: &str) -> (Arc<FtsEngine>, Database) {
    let db = Arc::new(Db::open(name, Arc::new(InMemory::new())).await.unwrap());
    let database = Database::new(db);
    let fts = FtsEngine::new();

    {
        let mut g = Glue::new(database.connection_serialized());
        g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
    }
    fts.create_trigram_index(&database.connection(), "docs", "body", "id")
        .await
        .unwrap();
    {
        let mut g = Glue::new(database.connection().with_commit_observer(fts.clone()));
        g.execute(
            "INSERT INTO docs (id, body) VALUES \
             (1, 'quarterly invoice overdue'), \
             (2, 'weather sunny'), \
             (3, 'overdue notice');",
        )
        .await
        .unwrap();
    }
    (fts, database)
}

/// The core case: `LIKE '%overdue%'` is trigram-accelerated AND correct.
/// The rewritten SQL carries both `id IN (` (prefilter) and the original `LIKE`
/// (verify); the result equals the full-scan baseline `{1, 3}`.
#[tokio::test]
async fn like_infix_is_accelerated_and_matches_scan() {
    let (fts, database) = seed("trgm-like-core").await;
    let sql = "SELECT id FROM docs WHERE body LIKE '%overdue%'";

    let rewritten = fts
        .rewrite_for(sql, &[])
        .await
        .unwrap()
        .expect("LIKE rewritten");
    assert!(
        rewritten.contains("id IN ("),
        "expected a pk prefilter, got: {rewritten}"
    );
    assert!(
        rewritten.contains("LIKE '%overdue%'"),
        "the original LIKE must remain as gluesql's authoritative verify, got: {rewritten}"
    );

    assert_eq!(
        engine_ids(&fts, &database, sql).await,
        scan_ids(&database, sql).await,
        "engine result must equal the full-scan baseline"
    );
    assert_eq!(
        scan_ids(&database, sql).await,
        vec![1, 3],
        "baseline sanity"
    );
}

/// A literal whose trigrams aren't all present in any row → empty candidates →
/// never-match conjunct, matching the (empty) scan baseline.
#[tokio::test]
async fn like_no_match_is_empty_like_scan() {
    let (fts, database) = seed("trgm-like-empty").await;
    let sql = "SELECT id FROM docs WHERE body LIKE '%xyznotpresent%'";
    assert_eq!(engine_ids(&fts, &database, sql).await, Vec::<i64>::new());
    assert_eq!(
        engine_ids(&fts, &database, sql).await,
        scan_ids(&database, sql).await
    );
}

/// A < 3-char literal cannot be trigram-pruned → pass-through (no `pk IN`), still
/// scan-correct. `'%ab%'` matches no fixture row's body; pass-through must not
/// turn that into a false negative or a spurious prefilter.
#[tokio::test]
async fn like_short_literal_passes_through() {
    let (fts, database) = seed("trgm-like-short").await;
    let sql = "SELECT id FROM docs WHERE body LIKE '%ab%'";
    // No prefilter added (pass-through to gluesql's exact scan).
    assert!(
        fts.rewrite_for(sql, &[]).await.unwrap().is_none(),
        "a <3-char LIKE literal must pass through unchanged"
    );
    assert_eq!(
        engine_ids(&fts, &database, sql).await,
        scan_ids(&database, sql).await
    );

    // And a <3-char literal that DOES match ('er' is in quarterly/overdue/weather)
    // still matches via the scan — proving pass-through is correct, not lossy.
    let sql2 = "SELECT id FROM docs WHERE body LIKE '%er%'";
    assert!(fts.rewrite_for(sql2, &[]).await.unwrap().is_none());
    assert_eq!(
        engine_ids(&fts, &database, sql2).await,
        scan_ids(&database, sql2).await
    );
    assert_eq!(
        scan_ids(&database, sql2).await,
        vec![1, 2, 3],
        "'er' appears in quart(er)ly/ov(er)due, weath(er), ov(er)due"
    );
}

/// A column with NO trigram index → LIKE passes through unchanged (gluesql scans).
#[tokio::test]
async fn like_without_trigram_index_passes_through() {
    let db = Arc::new(
        Db::open("trgm-like-noidx", Arc::new(InMemory::new()))
            .await
            .unwrap(),
    );
    let database = Database::new(db);
    let fts = FtsEngine::new();
    {
        let mut g = Glue::new(database.connection_serialized());
        g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
        g.execute("INSERT INTO docs (id, body) VALUES (1, 'overdue invoice'), (2, 'sunny');")
            .await
            .unwrap();
    }
    let sql = "SELECT id FROM docs WHERE body LIKE '%overdue%'";
    assert!(
        fts.rewrite_for(sql, &[]).await.unwrap().is_none(),
        "no trigram index on the column → pass-through"
    );
    assert_eq!(
        engine_ids(&fts, &database, sql).await,
        scan_ids(&database, sql).await
    );
    assert_eq!(scan_ids(&database, sql).await, vec![1]);
}

/// Prefix (`'lit%'`) and suffix (`'%lit'`) clean infixes of length ≥3 are also
/// accelerated and scan-correct (the rewrite strips ONE optional leading + ONE
/// optional trailing `%`).
#[tokio::test]
async fn like_prefix_and_suffix_infix_match_scan() {
    let (fts, database) = seed("trgm-like-affix").await;

    // Suffix anchor: rows whose body CONTAINS 'overdue' at the end pattern.
    let suffix = "SELECT id FROM docs WHERE body LIKE '%overdue'";
    assert_eq!(
        engine_ids(&fts, &database, suffix).await,
        scan_ids(&database, suffix).await
    );

    // Prefix anchor.
    let prefix = "SELECT id FROM docs WHERE body LIKE 'overdue%'";
    assert_eq!(
        engine_ids(&fts, &database, prefix).await,
        scan_ids(&database, prefix).await
    );
    assert_eq!(
        scan_ids(&database, prefix).await,
        vec![3],
        "only row 3 starts with 'overdue'"
    );
}

/// A negated LIKE (`NOT LIKE`) must pass through — a trigram prefilter on a
/// negation would be a false-negative trap.
#[tokio::test]
async fn like_negated_passes_through() {
    let (fts, database) = seed("trgm-like-neg").await;
    let sql = "SELECT id FROM docs WHERE body NOT LIKE '%overdue%'";
    assert!(
        fts.rewrite_for(sql, &[]).await.unwrap().is_none(),
        "NOT LIKE must pass through (no prefilter)"
    );
    assert_eq!(
        engine_ids(&fts, &database, sql).await,
        scan_ids(&database, sql).await
    );
    assert_eq!(
        scan_ids(&database, sql).await,
        vec![2],
        "only the non-overdue row"
    );
}

/// An internal wildcard (`'%ov_rdue%'`) is NOT a clean infix → pass-through, still
/// scan-correct.
#[tokio::test]
async fn like_internal_wildcard_passes_through() {
    let (fts, database) = seed("trgm-like-wild").await;
    let sql = "SELECT id FROM docs WHERE body LIKE '%ov_rdue%'";
    assert!(
        fts.rewrite_for(sql, &[]).await.unwrap().is_none(),
        "a literal with an internal wildcard must pass through"
    );
    assert_eq!(
        engine_ids(&fts, &database, sql).await,
        scan_ids(&database, sql).await
    );
}

/// The trigram-LIKE candidates stay a correct superset ACROSS a seal: with a
/// durable engine, seal mid-way then re-query — the durable trigram tier serves
/// the prefilter and the result still equals the full-scan baseline. Also covers
/// an update that flips a row's membership across the seal boundary.
#[tokio::test]
async fn like_correct_across_seal_durable() {
    let db = Arc::new(
        Db::open("trgm-like-seal", Arc::new(InMemory::new()))
            .await
            .unwrap(),
    );
    let database = Database::new(db);
    let fts = FtsEngine::new_durable(database.substrate());

    {
        let mut g = Glue::new(database.connection_serialized());
        g.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT);")
            .await
            .unwrap();
    }
    fts.create_trigram_index(&database.connection(), "docs", "body", "id")
        .await
        .unwrap();
    {
        let mut g = Glue::new(database.connection().with_commit_observer(fts.clone()));
        g.execute(
            "INSERT INTO docs (id, body) VALUES (1, 'quarterly invoice overdue'), (2, 'weather sunny'), (3, 'overdue notice');",
        )
        .await
        .unwrap();
    }

    let sql = "SELECT id FROM docs WHERE body LIKE '%overdue%'";
    assert_eq!(
        engine_ids(&fts, &database, sql).await,
        vec![1, 3],
        "pre-seal live"
    );

    // Seal: fold the live trigram segment into a durable `trgm/...` split.
    fts.seal().await.unwrap();
    assert_eq!(
        engine_ids(&fts, &database, sql).await,
        scan_ids(&database, sql).await,
        "post-seal: durable trigram prefilter still equals the scan"
    );
    assert_eq!(
        engine_ids(&fts, &database, sql).await,
        vec![1, 3],
        "post-seal still 1 and 3"
    );

    // Update row 1 so it no longer contains 'overdue', insert row 4 that does.
    {
        let mut g = Glue::new(database.connection().with_commit_observer(fts.clone()));
        g.execute("UPDATE docs SET body = 'sunny forecast' WHERE id = 1;")
            .await
            .unwrap();
        g.execute("INSERT INTO docs (id, body) VALUES (4, 'overdue reminder');")
            .await
            .unwrap();
    }
    // Live covers pk 1 (re-indexed, no 'overdue' trigrams) → masks the stale durable
    // candidate; row 4 (live) is a new candidate. Result equals the scan: {3, 4}.
    assert_eq!(
        engine_ids(&fts, &database, sql).await,
        scan_ids(&database, sql).await,
        "across-seal update: engine equals scan"
    );
    assert_eq!(engine_ids(&fts, &database, sql).await, vec![3, 4]);
}
