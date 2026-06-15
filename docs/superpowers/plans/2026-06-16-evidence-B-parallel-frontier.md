# Evidence B — Parallel frontier expansion for `reachable`

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Make `reachable` expand each BFS level concurrently (bounded), so latency tracks graph diameter rather than nodes-reached. Output stays sorted ⇒ identical and deterministic. `widest_path` is untouched.

**Spec:** `docs/superpowers/specs/2026-06-16-evidence-hardening-design.md` (Component B). Read it.

**Worktree:** Work ONLY in this worktree on branch `feat/evidence-frontier`. Commit-only, NEVER push. Co-author trailer `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. NO `cargo fmt`. Run tests directly with `2>&1` (no tail/grep pipes). "bluedb"/"bluecopa" lowercase.

**Touches ONLY:** `crates/bluedb-evidence/src/traverse.rs` and `crates/bluedb-evidence/tests/traverse.rs` and `docs/evidence/graph.md`. (The `#[derive(Clone)]` on `EvidenceKeyspace` is already on the base branch — do NOT edit `keyspace.rs`.) This keeps the component disjoint from Component A.

## Background facts

- `traverse.rs` has `pub(crate) async fn reachable(substrate: &Substrate, ks: &EvidenceKeyspace, graph: &str, from: &[String], floor: i64, directed: bool) -> Result<Vec<String>, EvidenceError>` — currently a sequential `VecDeque` BFS that awaits `out_neighbors` (and `in_neighbors` if `!directed`) per dequeued node, dedups via a `HashSet`, returns `sorted(visited)`.
- `out_neighbors`/`in_neighbors(substrate, ks, graph, node, floor) -> Result<Vec<(String,i64,String)>>` return `(neighbor, weight, type)`.
- `Substrate` is `#[derive(Clone)]` (Arc-backed). `EvidenceKeyspace` is `#[derive(Clone)]` on the base (wraps a `Clone` `bluedb_sql::Keyspace`). `tokio` is a dependency; `futures` is NOT.
- `Graph::reachable` (in `graph.rs`) delegates here — its signature does NOT change.

## Task B1: level-parallel `reachable`

**Files:** `crates/bluedb-evidence/src/traverse.rs`

- [ ] **Step 1:** Add a bounded-concurrency level-expansion helper and rewrite `reachable`'s loop to use it. Keep `reachable`'s signature, seeding, dedup, and `sorted(visited)` output exactly.

```rust
/// Concurrency cap for level expansion (in-flight neighbor scans).
const REACHABLE_FANOUT: usize = 16;

/// Scan every node in `nodes` for its out- (and in-, when undirected) neighbors
/// concurrently, bounded to `REACHABLE_FANOUT` in-flight scans. Returns all
/// discovered neighbor ids (with duplicates; the caller dedups). Order is
/// unspecified — the caller sorts, so the final result is deterministic.
async fn expand_level(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    graph: &str,
    nodes: &[String],
    floor: i64,
    directed: bool,
) -> Result<Vec<String>, EvidenceError> {
    let mut set: tokio::task::JoinSet<Result<Vec<String>, EvidenceError>> = tokio::task::JoinSet::new();
    let mut iter = nodes.iter();

    let mut spawn_one = |set: &mut tokio::task::JoinSet<Result<Vec<String>, EvidenceError>>, node: &str| {
        let sub = substrate.clone();
        let ks = ks.clone();
        let g = graph.to_string();
        let n = node.to_string();
        set.spawn(async move {
            let mut ns: Vec<String> = out_neighbors(&sub, &ks, &g, &n, floor)
                .await?
                .into_iter()
                .map(|(v, _, _)| v)
                .collect();
            if !directed {
                ns.extend(in_neighbors(&sub, &ks, &g, &n, floor).await?.into_iter().map(|(v, _, _)| v));
            }
            Ok(ns)
        });
    };

    for _ in 0..REACHABLE_FANOUT {
        match iter.next() {
            Some(n) => spawn_one(&mut set, n),
            None => break,
        }
    }

    let mut out: Vec<String> = Vec::new();
    while let Some(joined) = set.join_next().await {
        let ns = joined
            .map_err(|e| EvidenceError::Storage(anyhow::anyhow!("traversal join: {e}")))??;
        out.extend(ns);
        if let Some(n) = iter.next() {
            spawn_one(&mut set, n);
        }
    }
    Ok(out)
}
```
And rewrite `reachable`:
```rust
pub(crate) async fn reachable(
    substrate: &Substrate,
    ks: &EvidenceKeyspace,
    graph: &str,
    from: &[String],
    floor: i64,
    directed: bool,
) -> Result<Vec<String>, EvidenceError> {
    let mut visited: HashSet<String> = HashSet::new();
    let mut frontier: Vec<String> = Vec::new();
    for n in from {
        if visited.insert(n.clone()) {
            frontier.push(n.clone());
        }
    }
    while !frontier.is_empty() {
        let neighbors = expand_level(substrate, ks, graph, &frontier, floor, directed).await?;
        let mut next: Vec<String> = Vec::new();
        for v in neighbors {
            if visited.insert(v.clone()) {
                next.push(v);
            }
        }
        frontier = next;
    }
    let mut out: Vec<String> = visited.into_iter().collect();
    out.sort();
    Ok(out)
}
```
Imports: ensure `use std::collections::HashSet;` remains (the `VecDeque` import may now be unused — remove it if `widest_path` doesn't use it; check and keep clippy clean).

> **Send fallback:** if `JoinSet::spawn` fails to compile because the scan future isn't `Send`, switch `expand_level` to `futures::stream::iter(nodes).map(|n| async { … borrow &substrate/&ks … }).buffer_unordered(REACHABLE_FANOUT).try_collect()` and add `futures = { workspace = true }` (or `futures = "0.3"`) to `crates/bluedb-evidence/Cargo.toml`. This runs on one task (no `Send`/`'static`, no clones). Prefer the `JoinSet` version if it compiles (no new dependency).

- [ ] **Step 2:** Run the EXISTING reachable tests — they are the correctness gate (identity-stable under parallelism):
`cargo test -p bluedb-evidence --test traverse 2>&1` — all existing cases PASS (seed-inclusion, floor prune, undirected in-edges, sink, multi-seed dedup, unknown seed).
- [ ] **Step 3:** `cargo test -p bluedb-evidence 2>&1` (full crate green) and `cargo clippy -p bluedb-evidence --all-targets 2>&1` (no new warnings — watch for an unused `VecDeque` import).
- [ ] **Step 4:** Commit `feat(evidence): parallel BFS level expansion in reachable (bounded fan-out)`.

## Task B2: wide-graph + dedup tests, docs, green

**Files:** `crates/bluedb-evidence/tests/traverse.rs`, `docs/evidence/graph.md`

- [ ] **Step 1:** Add tests exercising multi-node levels and concurrent dedup (reuse the file's `db()` / `Graph` / `EdgeUpsert` / `Merge` helpers):
```rust
#[tokio::test]
async fn reachable_wide_fanout_then_converge() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    // root -> c0..c19 (wide level), each ci -> sink.
    let mut edges = Vec::new();
    for i in 0..20 {
        edges.push(EdgeUpsert { src: "root".into(), dst: format!("c{i}"), weight: 1, etype: String::new() });
        edges.push(EdgeUpsert { src: format!("c{i}"), dst: "sink".into(), weight: 1, etype: String::new() });
    }
    g.upsert("g", &edges, Merge::Set).await.unwrap();
    let r = g.reachable("g", &["root".into()], i64::MIN, true).await.unwrap();
    let mut expected: Vec<String> = vec!["root".into(), "sink".into()];
    for i in 0..20 { expected.push(format!("c{i}")); }
    expected.sort();
    assert_eq!(r, expected);
}

#[tokio::test]
async fn reachable_diamond_dedups_shared_child() {
    let database = db().await;
    let g = Graph::new(&database, "_");
    // A->B, A->C, B->D, C->D  (D reached via two parents in one level)
    g.upsert("g", &[
        EdgeUpsert { src: "A".into(), dst: "B".into(), weight: 1, etype: String::new() },
        EdgeUpsert { src: "A".into(), dst: "C".into(), weight: 1, etype: String::new() },
        EdgeUpsert { src: "B".into(), dst: "D".into(), weight: 1, etype: String::new() },
        EdgeUpsert { src: "C".into(), dst: "D".into(), weight: 1, etype: String::new() },
    ], Merge::Set).await.unwrap();
    let r = g.reachable("g", &["A".into()], i64::MIN, true).await.unwrap();
    assert_eq!(r, vec!["A", "B", "C", "D"]); // D appears once
}
```
- [ ] **Step 2:** Update `docs/evidence/graph.md`: in **Traversal** and **Limitations (v1)**, replace the "sequential frontier expansion" bullet — `reachable` now expands each BFS level concurrently (bounded fan-out, default 16); note `widest_path` remains sequential (priority-queue Dijkstra).
- [ ] **Step 3:** `cargo test -p bluedb-evidence --test traverse 2>&1` + `cargo test -p bluedb-evidence 2>&1` — PASS. `cargo clippy -p bluedb-evidence --all-targets 2>&1` — clean.
- [ ] **Step 4:** Commit `feat(evidence): wide-graph traversal tests + docs (parallel frontier)`.

## Acceptance
- All existing `reachable` tests pass unchanged (the identity gate).
- Wide fan-out and diamond/cycle dedup cases pass.
- `widest_path` untouched; `Graph::reachable` signature unchanged.
- `cargo test -p bluedb-evidence` green; no new clippy warnings; `graph.md` limitation updated.
