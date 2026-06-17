//! Gated read-dialect conformance for the DataFusion front door.
//!
//! Every record in the corpus under `crates/bluedb-sqltest/df/` must execute
//! correctly through [`DataFusionTester`]: writes + DDL run on the GlueSQL path,
//! and every `SELECT` is planned + executed by DataFusion over the bluedb schema
//! provider — the same split as the server's `POST /sql`. This is the regression
//! gate proving the front-door flip serves the analytical dialect (joins, window
//! functions, CTEs, set operations, subqueries) that GlueSQL cannot.
//!
//! Coverage breadth (the broad external SQLite/DuckDB corpus through this same
//! backend) is reported separately by the `conformance` binary; this test gates a
//! curated, hand-verified corpus so the suite stays green and deterministic.

use std::path::{Path, PathBuf};

use bluedb_sqltest::{lenient_validator, DataFusionTester};
use sqllogictest::{parse_file, DefaultColumnType, Runner};

/// Recursively collect every `.slt` file under `root`.
fn collect(root: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(root)
        .unwrap_or_else(|e| panic!("read corpus dir {}: {e}", root.display()))
        .flatten()
    {
        let p = entry.path();
        if p.is_dir() {
            collect(&p, out);
        } else if p.extension().is_some_and(|x| x == "slt") {
            out.push(p);
        }
    }
}

#[tokio::test]
async fn datafusion_read_dialect_corpus() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/df");
    let mut files = Vec::new();
    collect(Path::new(dir), &mut files);
    files.sort();
    assert!(!files.is_empty(), "no .slt files found under {dir}");

    for file in &files {
        let records = parse_file::<DefaultColumnType>(file)
            .unwrap_or_else(|e| panic!("parse {}: {e}", file.display()));

        // Fresh engine per file so files never see each other's schema or rows.
        let mut runner = Runner::new(|| async { DataFusionTester::connect().await });
        // Numeric-aware, layout-tolerant comparison (e.g. `15` == `15.0`).
        runner.with_validator(lenient_validator);

        for record in records {
            if let Err(e) = runner.run_async(record).await {
                panic!("{}: {e}", file.display());
            }
        }
    }
}
