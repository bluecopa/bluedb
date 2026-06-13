//! Standard-SQL conformance baseline runner (per-record).
//!
//! Usage: `cargo run -p bluedb-sqltest --bin conformance -- [DIR]`
//! (DIR defaults to `crates/bluedb-sqltest/slt`).
//!
//! Walks DIR for `.slt`/`.test` files, parses each into records, and runs every
//! statement and query against GlueSQL-on-SlateDB (fresh in-memory engine per
//! file). It reports:
//!
//! * **engine-accept rate** — fraction of records the engine ran without
//!   returning an error. This is formatting-independent and reliable.
//! * **rejected-feature backlog** — rejected SQL grouped by the engine's error,
//!   ranked by count. This is the actionable output: what to implement next.
//!
//! PASS vs WRONG-RESULT (among accepted records) is reported too, but it is a
//! *lower bound* on correctness: our value rendering is still minimal, so some
//! correctly-executed queries show as WRONG-RESULT until rendering is tightened.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use bluedb_sqltest::GlueTester;
use sqllogictest::{parse_file, DefaultColumnType, Record, Runner};

enum Cat {
    Unsupported,
    WrongResult,
    Other,
}

fn classify(msg: &str) -> Cat {
    let m = msg.to_lowercase();
    // "result mismatch" => engine RAN it but output differed. Check first so a
    // silently mis-executed feature isn't hidden in the rejected bucket.
    if m.contains("mismatch") || m.contains("differ") {
        Cat::WrongResult
    } else if m.contains("unsupported") || m.contains("not supported") {
        Cat::Unsupported
    } else {
        Cat::Other
    }
}

/// Reduce an engine error to a stable key for grouping the backlog.
fn feature_key(msg: &str) -> String {
    let line = msg.lines().next().unwrap_or("").trim();
    // Drop sqllogictest's "query failed: " / "statement failed: " prefix to get
    // at the engine's own message.
    line.rsplit_once("failed: ")
        .map(|(_, rest)| rest)
        .unwrap_or(line)
        .trim()
        .to_string()
}

fn one_line(s: &str) -> String {
    let line = s.lines().next().unwrap_or("").trim();
    line.chars().take(90).collect()
}

fn collect(root: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            collect(&p, out);
        } else if p.extension().is_some_and(|x| x == "slt" || x == "test") {
            out.push(p);
        }
    }
}

#[derive(Default)]
struct Tally {
    pass: usize,
    unsupported: usize,
    wrong: usize,
    other: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "crates/bluedb-sqltest/slt".to_string());

    let mut files = Vec::new();
    collect(Path::new(&dir), &mut files);
    files.sort();
    if files.is_empty() {
        anyhow::bail!("no .slt/.test files found under {dir}");
    }

    let mut t = Tally::default();
    let mut parse_errors = 0usize;
    let mut backlog: BTreeMap<String, usize> = BTreeMap::new();
    let mut wrong_examples: Vec<String> = Vec::new();
    let mut other_examples: Vec<String> = Vec::new();

    for file in &files {
        let records = match parse_file::<DefaultColumnType>(file) {
            Ok(records) => records,
            Err(e) => {
                parse_errors += 1;
                eprintln!("parse error: {} :: {e}", file.display());
                continue;
            }
        };

        // Fresh engine per file so files never see each other's state.
        let mut runner = Runner::new(|| async { GlueTester::connect().await });

        for record in records {
            // Only statements and queries are scored; everything else (control,
            // conditions, comments, hash-threshold, ...) is still applied so the
            // runner state stays correct.
            let scored_sql = match &record {
                Record::Statement { sql, .. } | Record::Query { sql, .. } => Some(sql.clone()),
                _ => None,
            };
            let outcome = runner.run_async(record).await;
            let Some(sql) = scored_sql else { continue };

            match outcome {
                Ok(_) => t.pass += 1,
                Err(e) => {
                    let msg = e.to_string();
                    match classify(&msg) {
                        Cat::WrongResult => {
                            t.wrong += 1;
                            if wrong_examples.len() < 8 {
                                wrong_examples.push(one_line(&sql));
                            }
                        }
                        Cat::Unsupported => {
                            t.unsupported += 1;
                            *backlog.entry(feature_key(&msg)).or_default() += 1;
                        }
                        Cat::Other => {
                            t.other += 1;
                            if other_examples.len() < 8 {
                                other_examples.push(format!("{}  ::  {}", one_line(&sql), feature_key(&msg)));
                            }
                        }
                    }
                }
            }
        }
    }

    let scored = t.pass + t.unsupported + t.wrong + t.other;
    let accepted = t.pass + t.wrong;
    let rejected = t.unsupported + t.other;

    println!("\n========== STANDARD-SQL CONFORMANCE BASELINE ==========");
    println!("files: {}   (parse errors: {parse_errors})", files.len());
    println!("scored records (statements + queries): {scored}");
    println!();
    println!("  ENGINE-ACCEPTED  {accepted:>6}  ({:.1}%)   <- ran without engine error (reliable)", pct(accepted, scored));
    println!("  ENGINE-REJECTED  {rejected:>6}  ({:.1}%)", pct(rejected, scored));
    println!("      unsupported  {:>6}", t.unsupported);
    println!("      other errors {:>6}", t.other);
    println!();
    println!("  of accepted (output comparison — lower bound, rendering still minimal):");
    println!("      PASS         {:>6}", t.pass);
    println!("      WRONG-RESULT {:>6}", t.wrong);
    println!("=======================================================");

    if !backlog.is_empty() {
        let mut ranked: Vec<(&String, &usize)> = backlog.iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
        println!("\nTOP REJECTED FEATURES (the backlog — implement these to raise coverage):");
        for (key, count) in ranked.iter().take(25) {
            println!("  {count:>5}  {key}");
        }
        if ranked.len() > 25 {
            println!("  ... and {} more distinct rejections", ranked.len() - 25);
        }
    }

    print_examples("WRONG-RESULT (accepted, output differed)", &wrong_examples);
    print_examples("OTHER errors", &other_examples);

    Ok(())
}

fn pct(n: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        100.0 * n as f64 / total as f64
    }
}

fn print_examples(label: &str, examples: &[String]) {
    if examples.is_empty() {
        return;
    }
    println!("\n--- sample {label} ---");
    for ex in examples {
        println!("  {ex}");
    }
}
