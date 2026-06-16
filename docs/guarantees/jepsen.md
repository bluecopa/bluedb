# Jepsen report

bluedb ships a real [Jepsen](https://jepsen.io) test: randomized concurrent
operations, injected faults, and a formal checker over the recorded history. It
is how the [consistency guarantees](consistency.md) are kept honest.

The suite lives in [`jepsen/`](https://github.com/bluecopa/bluedb/tree/dev/jepsen)
and drives the [Docker Compose cluster](../deployment/local.md).

## Workloads

| Workload | What it does | What it proves |
|----------|--------------|----------------|
| **`set`** (default) | Clients append unique integers through the active writer; a final read reads the whole set back | **No lost writes** (every acked add is present) and **no fabrication** (nothing appears that wasn't added) — durability across failover |
| **`list-append`** | Each transaction is a `BEGIN; …; COMMIT;` of appends + reads, run as one explicit transaction | **Serializability** via Elle — flags G0/G1/G2, write skew, lost update; checked `serializable` or `strict-serializable` |
| **`counter`** | Concurrent autocommit `UPDATE c SET n = n + 1` | **No lost updates** (no read below the count of acked increments) |
| **`unique`** | Many clients race to `INSERT` the same fresh primary key | **No double-apply** (an id is acked at most once) |
| **`evidence`** | Append-heavy load on one fresh [evidence chain](../evidence/chains.md); a final read reads the whole chain back | **No acked loss** + the server-assigned `seq` is **dense, gap-free, unique `1..N`** — durability-before-ack and the [R1 dense-sequence](../evidence/chains.md) invariant hold across failover |
| **`graph`** | The writer atomically swaps a diamond between `R→A→Z` and `R→B→Z` via [`POST /graph/{g}/mutate`](../evidence/graph.md#post-graphgraphmutate-atomic-edge-rewire); clients run `reachable(R)` concurrently | **Snapshot isolation** for [graph traversal](../evidence/graph.md#consistency) — every traversal observes one *whole* config (`{R,A,Z}` or `{R,B,Z}`), the sink `Z` is never dropped; a non-snapshot read would tear mid-swap |

In the set/list-append/counter workloads the cluster is driven as one logical
writer whose identity moves on failover; the client discovers the active writer
via `/admin/status` and re-discovers on a `503` — it **follows the leader**
across promotions.

!!! warning "Harness ported to the schema regime — some workloads pending re-run"
    The merged [schema regime](../sql/query-guardrail.md) (required `PRIMARY KEY`,
    no schemaless auto-create, bounded reads) made the original Jepsen client
    stale — it assumed a table auto-creates on first insert and that one read
    returns the whole set. Two workloads have been ported and **re-verified on the
    live cluster against the post-group-commit write path** (see Results):

    - **`set`** — creates `jset (v INTEGER PRIMARY KEY)` up front and
      keyset-paginates the final read.
    - **`list-append`** — redesigned for
      [index-organized](../concepts/architecture.md#storage-model-index-organized-tables)
      (PK-clustered) storage, where `SELECT … WHERE k = ?` no longer returns
      *insertion* order. Each element is one row keyed by a surrogate
      `id = k·stride + position` (the appended values are unique only within a
      key, so they can't be the key, and the encoding clusters a key's rows into
      one PK range *in append order*); the read is a PK-range scan. Its explicit
      `BEGIN…COMMIT` transactions run through `POST /admin/sql`.

    The **`counter`** and **`unique`** workloads have since been ported too (each
    creates its PK'd table up front in `setup!` and drives the writer via the
    JSON `/sql` + `/tables` surfaces), plus a new **`dur`** probe (a grow-only set
    written through the `/sql` autocommit path — counter's exact write path — with
    identifiable elements, to time any acked-write loss). See Results.

    The **`ledger`** workload still needs the same port before it can be re-run;
    its earlier green result predates the schema regime — treat it as pending
    re-validation.

## Faults (nemesis)

Injected against the Compose stack via the `docker` CLI:

| Nemesis | Fault | Tests |
|---------|-------|-------|
| `kill` | `docker kill` the writer | durability through the WAL window; a standby promotes |
| `partition` | disconnect the writer from Postgres + object store + peers | clean failover, no split-brain |
| `partition-half` | isolate the writer + one peer (a minority) | the minority steps down |
| `pause` | `docker pause` the writer (SIGSTOP) | frozen writer stops renewing; on resume it wakes to an expired lease + bumped epoch and steps down |
| `skew` | shift the writer's clock 8s backward | the `writer_epoch` fence still blocks divergent writes despite lease over-estimation |
| `arbiter` | pause Postgres (the lease arbiter) | writer self-fences, standbys can't acquire → writer-less, no split-brain |
| `arbiter-hard` | stop/start Postgres | also tests lease-provider reconnect |
| `storage` | pause the object store | writer keeps its lease but can't durably write → writes don't ack |
| `disk-full` | fill the object store to ENOSPC | durable writes fail until space is freed |
| `mix` / `chaos` | combinations | combined |

## Results

- **Post-group-commit re-verification (2026-06-16):** the **`set`** workload is
  **`:valid? true`** with **`lost-count 0`** across `none` / `kill` / `partition`
  / `mix` / `skew` on the live 3-node cluster — every acknowledged write survives
  a hard crash, a network partition, combined faults, and clock skew (the
  `writer_epoch` fence holds). This is the current verification on the
  group-commit write path.
- The dependency faults (`arbiter` / `arbiter-hard` / `storage` / `disk-full`)
  are **`:valid? true`** with **`lost-count 0`** — bluedb stays **consistent**
  (no split-brain, no lost acked writes) while losing **availability** when a
  dependency is down. This is the **CP** behavior, demonstrated.
- **`list-append` re-verification (2026-06-16):** ported to the schema regime and
  **`:valid? true`** on the live 3-node cluster — a clean baseline (`none`, every
  transaction commits), **`mix`** (kill + partition) serializable through
  failover, and **`strict-serializable`** under `kill` (adds real-time order,
  through crashes). Elle finds no G0/G1/G2, lost update, write skew, or
  incompatible-order anomaly on the explicit-transaction (`/admin/sql`) path.
- **Evidence chain (2026-06-16):** the **`evidence`** workload is **`:valid?
  true`** across `none` / `kill` / `partition` / `mix` on the live 3-node
  cluster — **zero acked appends lost** and the server-assigned `seq` stayed
  **dense, gap-free `1..N`** through ~22 leadership epochs. Durability-before-ack
  and dense sequencing survive crash / partition / pause + failover.
- **Graph-traversal snapshot isolation (2026-06-16):** the **`graph`** workload
  is **`:valid? true`** across `pause` / `kill` / `partition` / `mix` — **0
  violations, 0 sink-drops** over ~7,000 traversals racing ~2,700 atomic swaps,
  leadership moving across ~29 epochs. Every traversal pinned one snapshot and
  observed a single consistent cut; the sink was never dropped, even under
  `pause` (which freezes a traversal between scans, straddling a swap). This
  demonstrates [snapshot-consistent traversal](../evidence/graph.md#consistency).
- **`counter` re-verification + a failover fix (2026-06-16):** ported to the
  schema regime, the `counter` × `kill` run first surfaced a real **failover
  read-staleness** bug — *not* a lost write. A just-promoted node briefly
  advertised `role: "active"` (it had the lease) while still bound to its
  pre-failover replica `Db`, and `GET /tables` reads are not writer-gated, so a
  client following the leader read the lagging replica and saw the counter
  *below* its acknowledged count (~45% of runs). The fix makes a node report
  `active` only once its writer `Db` is installed (see
  [Read consistency](consistency.md#routing-across-failover) and
  [Active-passive HA](../ha/active-passive.md)). After the fix, `counter` × `kill`
  is **`:valid? true`** with **no lost updates across ~22 runs** (from ~45%
  failing). A standalone SlateDB reproducer
  (`crates/bluedb-storage/tests/hotkey_recovery.rs`) independently confirmed the
  storage layer never loses a durable (`await_durable=true`) write across an
  abrupt crash + reopen, which had ruled out a durability cause.

The dependency-fault results (`arbiter` / `storage` / `disk-full`) above were
established before the schema regime landed; they are being re-run as the
remaining workloads are ported (see the warning above). The `set`,
`list-append`, `evidence`, and `graph` re-verifications are current.

!!! note "Methodology"
    Fault windows straddle the lease TTL so failover completes inside each
    window. The clock-skew nemesis uses a `libfaketime` entrypoint. Fault
    injection uses the `docker` CLI (no SSH) — the natural seam for a
    containerized deployment.

## Running it yourself

Prerequisites: the cluster up (`docker compose up -d` from the repo root) and
**Java 21+** on `$PATH`. Leiningen is vendored at `jepsen/bin/lein`.

```bash
cd jepsen
export LEIN_HOME="$PWD/.lein"
NODES="--node node1 --node node2 --node node3"
bin/lein run test --workload set      --nemesis kill      --time-limit 120 $NODES
bin/lein run test --workload set      --nemesis partition --time-limit 120 $NODES
bin/lein run test --workload evidence --nemesis mix       --time-limit 180 $NODES
bin/lein run test --workload graph    --nemesis pause     --time-limit 120 $NODES
```

(Pass the `--node` flags explicitly — a shell that doesn't word-split an
unquoted variable will otherwise hand them to lein as one argument.)

See [`jepsen/README.md`](https://github.com/bluecopa/bluedb/blob/dev/jepsen/README.md)
for all flags and the schema-regime notes for the other workloads.
