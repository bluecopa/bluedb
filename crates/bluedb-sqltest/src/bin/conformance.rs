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

use bluedb_sqltest::{lenient_validator, DataFusionTester, GlueTester};
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
    } else if m.contains("not found") || m.contains("does not exist") || m.contains("not exists") {
        // Downstream of an earlier failed CREATE/INSERT — not a feature gap.
        // DataFusion words it "table 'x' not found" (name between the words), so
        // match the looser "not found" rather than the literal "table not found".
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

/// One corpus run's full result, so the per-engine run loop (a macro, since the
/// two backends are distinct concrete types) can hand a single value back.
struct Report {
    /// All scored records (statements + queries) — the overall coverage figure.
    t: Tally,
    /// Query records only — the read-path figure. Statements still run (they set
    /// up the data a query reads), but their pass/fail is a write-path signal, so
    /// they are excluded here to isolate the read dialect.
    qt: Tally,
    backlog: BTreeMap<String, usize>,
    wrong_examples: Vec<String>,
    other_examples: Vec<String>,
    wrong_hashed: usize,
    parse_errors: usize,
}

/// Which backend to score the corpus against.
#[derive(Clone, Copy)]
enum Engine {
    /// GlueSQL write / storage path (the baseline).
    Glue,
    /// DataFusion read front door — every SELECT via `query_via_catalog`.
    DataFusion,
}

impl Engine {
    fn label(self) -> &'static str {
        match self {
            Engine::Glue => "gluesql-slatedb (write/storage path)",
            Engine::DataFusion => "datafusion front door (read path)",
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Args: an optional corpus DIR (positional) and `--engine glue|df` (default
    // glue). `--engine df` runs every single-SELECT through the DataFusion front
    // door; non-SELECT records still run on the GlueSQL write path. Note the
    // DataFusion path requires a PRIMARY KEY on every table, so PK-less corpus
    // tables surface as rejects — an honest regime constraint, not a dialect gap.
    let mut dir: Option<String> = None;
    let mut engine = Engine::Glue;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--engine" => {
                engine = match args.next().unwrap_or_default().as_str() {
                    "df" | "datafusion" => Engine::DataFusion,
                    "glue" | "gluesql" => Engine::Glue,
                    other => anyhow::bail!("unknown --engine '{other}' (want glue|df)"),
                };
            }
            other => dir = Some(other.to_string()),
        }
    }
    let dir = dir.unwrap_or_else(|| "crates/bluedb-sqltest/slt".to_string());

    let mut files = Vec::new();
    collect(Path::new(&dir), &mut files);
    files.sort();
    if files.is_empty() {
        anyhow::bail!("no .slt/.test files found under {dir}");
    }

    // The per-file scoring loop is identical for both backends, but `Runner` is
    // generic over the concrete connection type, so a macro stamps it out for each
    // (cleaner than boxing an `AsyncDB` across the async boundary). `$make` is a
    // fresh connection factory, re-evaluated per file.
    macro_rules! run_all {
        ($make:expr) => {{
            let mut t = Tally::default();
            let mut qt = Tally::default();
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
                let mut runner = Runner::new($make);
                // The DuckDB corpus mixes tab-separated-row and one-value-per-line
                // result layouts; accept either so correct-but-differently-laid-out
                // results count.
                runner.with_validator(lenient_validator);

                for record in records {
                    // Only statements and queries are scored; everything else
                    // (control, conditions, comments, hash-threshold, ...) is still
                    // applied so the runner state stays correct.
                    let (scored_sql, is_query) = match &record {
                        Record::Query { sql, .. } => (Some(sql.clone()), true),
                        Record::Statement { sql, .. } => (Some(sql.clone()), false),
                        _ => (None, false),
                    };
                    let outcome = runner.run_async(record).await;
                    let Some(sql) = scored_sql else { continue };

                    match outcome {
                        Ok(_) => {
                            t.pass += 1;
                            if is_query {
                                qt.pass += 1;
                            }
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            match classify(&msg) {
                                Cat::WrongResult => {
                                    t.wrong += 1;
                                    if is_query {
                                        qt.wrong += 1;
                                    }
                                    // Hashed results (corpus uses hash-threshold 8)
                                    // need byte-exact value formatting to match;
                                    // literal-block mismatches are more likely just
                                    // rendering/order.
                                    if msg.contains("hashing") {
                                        wrong_hashed += 1;
                                    }
                                    if wrong_examples.len() < 12 {
                                        // Capture the value diff (lines after
                                        // "[Diff]"), ANSI stripped, so we can see
                                        // expected (-) vs actual (+) and tell
                                        // rendering from real bugs.
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
                                    if is_query {
                                        qt.unsupported += 1;
                                    }
                                    *backlog.entry(feature_key(&msg)).or_default() += 1;
                                }
                                Cat::Cascade => {
                                    t.cascade += 1;
                                    if is_query {
                                        qt.cascade += 1;
                                    }
                                }
                                Cat::Other => {
                                    t.other += 1;
                                    if is_query {
                                        qt.other += 1;
                                    }
                                    if other_examples.len() < 8 {
                                        other_examples.push(format!(
                                            "{}  ::  {}",
                                            one_line(&sql),
                                            feature_key(&msg)
                                        ));
                                    }
                                }
                            }
                        }
                    }
                }
            }

            Report {
                t,
                qt,
                backlog,
                wrong_examples,
                other_examples,
                wrong_hashed,
                parse_errors,
            }
        }};
    }

    let Report {
        t,
        qt,
        backlog,
        wrong_examples,
        other_examples,
        wrong_hashed,
        parse_errors,
    } = match engine {
        Engine::Glue => run_all!(|| async { GlueTester::connect().await }),
        Engine::DataFusion => run_all!(|| async { DataFusionTester::connect().await }),
    };

    let scored = t.pass + t.unsupported + t.wrong + t.cascade + t.other;
    let accepted = t.pass + t.wrong;
    let rejected = t.unsupported + t.other;

    println!("\n========== STANDARD-SQL CONFORMANCE BASELINE ==========");
    println!("engine: {}", engine.label());
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

    // The read-path-only figure: score QUERY records, exclude cascades (queries
    // blocked by a failed CREATE/INSERT — a write-path gap, not a read-dialect
    // one). This isolates how much of the read dialect the engine actually serves.
    let q_scored = qt.pass + qt.unsupported + qt.wrong + qt.cascade + qt.other;
    let q_base = q_scored - qt.cascade; // non-cascade queries = the real denominator
    let q_accept = qt.pass + qt.wrong;
    println!("\n  READ PATH ONLY (query records; cascades excluded — downstream of a");
    println!("  failed setup statement, i.e. a write-path gap, not the read dialect):");
    println!(
        "    queries: {q_scored}   non-cascade: {q_base}   (cascade-blocked: {})",
        qt.cascade
    );
    println!(
        "    READ-ACCEPTED  {q_accept:>6}  ({:.1}% of non-cascade)",
        pct(q_accept, q_base)
    );
    println!(
        "        PASS         {:>6}  ({:.1}% correct of accepted)",
        qt.pass,
        pct(qt.pass, q_accept)
    );
    println!("        WRONG-RESULT {:>6}", qt.wrong);
    println!(
        "    READ-REJECTED  {:>6}  ({:.1}%)   unsupported {} / other {}",
        qt.unsupported + qt.other,
        pct(qt.unsupported + qt.other, q_base),
        qt.unsupported,
        qt.other
    );
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
