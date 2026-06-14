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
    Cascade,
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
    } else if m.contains("table not found") || m.contains("does not exist") || m.contains("not exists") {
        // Downstream of an earlier failed CREATE/INSERT — not a feature gap.
        Cat::Cascade
    } else {
        Cat::Other
    }
}

/// Reduce an engine error to a stable key for grouping the backlog.
fn feature_key(msg: &str) -> String {
    let line = msg.lines().next().unwrap_or("").trim();
    // Drop sqllogictest's "query failed: " / "statement failed: " prefix to get
    // at the engine's own message.
    let raw = line
        .rsplit_once("failed: ")
        .map(|(_, rest)| rest)
        .unwrap_or(line)
        .trim();

    // Normalize away query-specific text so similar rejections aggregate into
    // one ranked feature instead of thousands of unique singletons.
    let Some(rest) = raw.strip_prefix("translate: ") else {
        return raw.to_string();
    };
    if rest.starts_with("unsupported query set expr") {
        return "set operation (UNION / INTERSECT / EXCEPT)".to_string();
    }
    if let Some(dt) = rest.strip_prefix("unsupported data type: ") {
        let base: String = dt.chars().take_while(|c| c.is_alphabetic()).collect();
        return format!("unsupported data type: {base}(n)");
    }
    if let Some(stmt) = rest.strip_prefix("unsupported statement: ") {
        let kw = stmt.split_whitespace().take(2).collect::<Vec<_>>().join(" ");
        return format!("unsupported statement: {kw}");
    }
    rest.to_string()
}

fn one_line(s: &str) -> String {
    let line = s.lines().next().unwrap_or("").trim();
    line.chars().take(90).collect()
}

/// Strip ANSI color escapes (sqllogictest colorizes its diffs).
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            for n in chars.by_ref() {
                if n == 'm' {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
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
    cascade: usize,
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
    let mut wrong_hashed = 0usize;

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
                            // Hashed results (corpus uses hash-threshold 8) need
                            // byte-exact value formatting to match; literal-block
                            // mismatches are more likely just rendering/order.
                            if msg.contains("hashing") {
                                wrong_hashed += 1;
                            }
                            if wrong_examples.len() < 12 {
                                // Capture the value diff (lines after "[Diff]"),
                                // ANSI stripped, so we can see expected (-) vs
                                // actual (+) and tell rendering from real bugs.
                                let diff: String = strip_ansi(&msg)
                                    .lines()
                                    .skip_while(|l| !l.contains("[Diff]"))
                                    .skip(1)
                                    .take(6)
                                    .collect::<Vec<_>>()
                                    .join("  ");
                                wrong_examples.push(format!(
                                    "{}\n      {}",
                                    one_line(&sql),
                                    diff.chars().take(200).collect::<String>()
                                ));
                            }
                        }
                        Cat::Unsupported => {
                            t.unsupported += 1;
                            *backlog.entry(feature_key(&msg)).or_default() += 1;
                        }
                        Cat::Cascade => t.cascade += 1,
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

    let scored = t.pass + t.unsupported + t.wrong + t.cascade + t.other;
    let accepted = t.pass + t.wrong;
    let rejected = t.unsupported + t.other;

    println!("\n========== STANDARD-SQL CONFORMANCE BASELINE ==========");
    println!("files: {}   (parse errors: {parse_errors})", files.len());
    println!("scored records (statements + queries): {scored}");
    println!();
    println!("  ENGINE-ACCEPTED  {accepted:>6}  ({:.1}%)   <- ran without engine error (reliable)", pct(accepted, scored));
    println!("  ENGINE-REJECTED  {rejected:>6}  ({:.1}%)   (real feature gaps)", pct(rejected, scored));
    println!("      unsupported  {:>6}", t.unsupported);
    println!("      other errors {:>6}", t.other);
    println!("  CASCADE          {:>6}  ({:.1}%)   (downstream of a failed setup stmt — excluded above)", t.cascade, pct(t.cascade, scored));
    println!();
    println!("  of accepted (output comparison — lower bound, rendering still minimal):");
    println!("      PASS         {:>6}", t.pass);
    println!("      WRONG-RESULT {:>6}  ({wrong_hashed} hashed / {} literal)", t.wrong, t.wrong - wrong_hashed);
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
