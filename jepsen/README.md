# bluedb Jepsen test

A real [Jepsen](https://jepsen.io) consistency test for the bluedb cluster:
randomized concurrent operations + injected faults + a formal checker over the
recorded history.

## What it checks

Six workloads, pick with `--workload`:

**`set` (default) — grow-only set.** Clients append unique integers through the
active writer; a final-read phase reads the whole set back. The `set-full`
checker proves two things across failover:

- **no lost writes** — every *acknowledged* add (HTTP 200) is present in the
  final read, and
- **no fabrication** — no element appears that was never added.

Best for durability / no-lost-update under failover.

**`list-append` — Elle list-append.** Each Jepsen transaction is a `BEGIN; …;
COMMIT;` of appends + reads over several keys, sent as one `POST /admin/sql` so it
runs as one explicit transaction on the active writer (`/sql` takes a single
statement; `/admin/sql` is the multi-statement/transaction surface). Elle
reconstructs the transaction dependency graph from the observed reads and flags
any serializability anomaly (G0/G1/G2, write skew, lost update). Checked against
`--consistency serializable` (default) or `strict-serializable` (also adds
real-time order). Exercises the explicit-transaction path under concurrency. See
its namespace docstring for the data model (surrogate `id = k*stride + position`
primary key, writer-stamped position, PK-range ordered reads).

**`counter` — lost-update probe.** Concurrent autocommit `UPDATE cnt SET n=n+1`
(a single-statement read-modify-write), checked with `jepsen.checker/counter`:
any read below the count of acknowledged increments is a lost update.

**`unique` — same-primary-key race.** Many clients race to `INSERT` the same
fresh primary key; the checker flags any id acknowledged (HTTP 200) more than
once. Uses a tiny id space + no stagger to maximize the narrow insert TOCTOU.

**`evidence` — append durability + dense seq.** Append-heavy load on one fresh
verified evidence chain. The checker proves every acknowledged append survives,
nothing is fabricated, and the final chain's server-assigned seqs are exactly
`1..N` (dense, gap-free, unique) across failover. Best for the evidence chain's
durability-before-ack + dense-sequencing guarantee.

**`graph` — graph-traversal snapshot isolation.** The writer atomically swaps a
tiny diamond between config A (`R→A→Z`) and config B (`R→B→Z`) via the atomic
rewire `POST /graph/{g}/mutate`; clients run `reachable(R)` concurrently. The
checker enforces that every acknowledged result is one *whole* config — `{R,A,Z}`
or `{R,B,Z}`, sink `Z` always present. A traversal pins one snapshot (Phase 2),
so its multiple scans see a single cut; a non-snapshot read that scanned `R`
before a swap and the bridge after would drop `Z` (`:sink-dropped`) and fail.
The `pause` nemesis — freezing a traversal between scans, across a swap — is the
sharpest stressor.

In the set/list-append/counter cases the cluster is driven as a single logical writer whose identity
moves on failover (bluedb's guarantee: one serial writer + async read replicas).
The client discovers the active writer via `/admin/status` and re-discovers on a
`503` (its node became a replica) — i.e. it follows the leader across promotions.

## Faults (nemesis)

Injected via the `docker` CLI against the compose stack — the natural
fault-injection seam for a containerized deployment (no SSH; `:ssh {:dummy?
true}`):

| `--nemesis` | fault | tests |
|---|---|---|
| `kill` | `docker kill` the active writer (crash) | durability of acked writes through SlateDB's WAL window; a standby promotes |
| `partition` | `docker network disconnect` the writer from Postgres + MinIO + peers | clean failover, no split-brain (the isolated writer loses its lease and storage at once) |
| `partition-half` | isolate the writer **+ one peer** (a 2-node minority) from the network | a lone node + infra must take/keep leadership; the arbiter-less minority steps down |
| `skew` | shift the writer's wall clock 8 s **backward** (via libfaketime) | the writer over-estimates its lease validity, so a standby can acquire concurrently — checks the SlateDB `writer_epoch` fence still blocks divergent writes |
| `pause` | `docker pause` the writer (SIGSTOP, no crash) | the frozen writer stops renewing; a standby promotes; on resume it wakes to an expired lease + bumped epoch and must step down |
| `arbiter` | `docker pause` Postgres (the lease arbiter) | writer can't renew → self-fences; standbys can't acquire → cluster goes writer-less (no split-brain) → recovers on thaw |
| `arbiter-hard` | `docker stop`/`start` Postgres (kills the connection) | like `arbiter`, but tests `PostgresLeaseProvider` reconnect — a node re-acquires once Postgres is back |
| `storage` | `docker pause` MinIO (the object store) | writer keeps its lease but can't durably write → writes don't ack → recovers on thaw |
| `disk-full` | fill MinIO's bounded `/data` tmpfs → ENOSPC | durable writes fail until space is freed |
| `mix` | kill + partition, alternating | combined |
| `chaos` | kill + pause + skew + partition-half | everything |
| `none` | — | baseline (no faults) |

The `arbiter`/`storage`/`disk-full` faults verify bluedb stays **consistent**
(no split-brain, no lost acked writes) while losing availability when a
dependency fails — CP, not AP. All three are `:valid? true` / `lost-count 0`.

Fault windows straddle the 10 s lease TTL so failover completes inside each
window. The clock-skew nemesis needs the image's libfaketime entrypoint
(`docker-entrypoint.sh`); it writes the offset into each node's
`/faketime/offset` via `docker exec`.

## Running

Prereqs: the cluster must be **up** (`docker compose up -d` from the repo root)
and **Java 21+** installed (a transitive dep needs `java.util.SequencedCollection`;
JDK 17 fails to load it). Leiningen is vendored at `bin/lein`; the wrapper
selects Java 21+ from `LEIN_JAVA_CMD`, `JAVA_CMD`, `JAVA_HOME`, `PATH`, macOS
`/usr/libexec/java_home`, or Homebrew `openjdk@21`, and fails fast if none is
available.

> **Schema-regime note.** The merged schema regime removed schemaless table
> auto-create and caps a single read at 100 rows. Two workloads are ported for it:
>
> - **`set`** — its client drops+creates `jset (v INTEGER PRIMARY KEY)` once per
>   run (via `POST /schema/tables`) and `read-set` keyset-paginates over the PK.
> - **`list-append`** — one row per element with a surrogate primary key
>   `id = k*stride + position`. Elle's appended values are unique only *within* a
>   key, so `v` can't be the PK; the encoding also clusters a key's rows into one
>   contiguous PK range *in append order*, so reads are PK-range scans (no
>   secondary index, no in-memory sort the guardrail would reject). The writer
>   stamps `position` via `INSERT … SELECT COALESCE((SELECT COUNT(*) …),0)`. Its
>   explicit `BEGIN…COMMIT` transactions use **`POST /admin/sql`**; the compose
>   image sets `BLUEDB_ENABLE_ADMIN_SQL=1` so that surface is available. (Full
>   rationale in `src/bluedb/jepsen/list_append.clj`.)
>
> **`counter` / `unique` / `ledger` are NOT yet ported** — each needs its tables
> created with a PK first (and the ledger has its own API). Run `set` and
> `list-append` until they are.
>
> **Parallel runs / a second cluster.** To run two sessions without contending
> for one stack, bring up the isolated `bluedb2` cluster (node ports 8091–8093,
> its own minio/postgres) and point Jepsen at it via env:
>
> ```bash
> docker compose -f docker-compose.bluedb2.yml up -d
> BLUEDB_JEPSEN_PROJECT=bluedb2 BLUEDB_JEPSEN_BASE_PORT=8091 \
>   ./bin/lein run test --workload list-append --nemesis none --time-limit 60 \
>     --concurrency 10 --node node1 --node node2 --node node3
> ```
>
> `BLUEDB_JEPSEN_PROJECT`/`BLUEDB_JEPSEN_BASE_PORT` default to `bluedb`/`8081`, so
> omitting them targets the primary stack exactly as before.
>
> **zsh:** the `NODES="--node …"` + unquoted `$NODES` pattern below word-splits in
> bash but **not in zsh** (it becomes one arg → "Unknown option"). On zsh, pass
> the `--node node1 --node node2 --node node3` flags literally.

```bash
cd jepsen
export LEIN_HOME="$PWD/.lein"
# Optional override when multiple JDKs are installed:
# export JAVA_HOME=/opt/homebrew/opt/openjdk@21

NODES="--node node1 --node node2 --node node3"

# set workload (durability / no-lost-update), each fault mode
./bin/lein run test --workload set --nemesis none      --time-limit 60  --concurrency 10 $NODES
./bin/lein run test --workload set --nemesis kill      --time-limit 120 --concurrency 10 $NODES
./bin/lein run test --workload set --nemesis partition --time-limit 120 --concurrency 10 $NODES
./bin/lein run test --workload set --nemesis mix       --time-limit 180 --concurrency 10 $NODES

# list-append workload (serializability) — baseline and through failover
./bin/lein run test --workload list-append --nemesis none --time-limit 60  --concurrency 10 $NODES
./bin/lein run test --workload list-append --nemesis mix  --time-limit 120 --concurrency 10 $NODES
# stronger: strict-serializable (adds real-time order)
./bin/lein run test --workload list-append --consistency strict-serializable --nemesis chaos --time-limit 150 --concurrency 10 $NODES

# counter (lost-update) and unique (same-PK) — use high concurrency
./bin/lein run test --workload counter --nemesis kill --time-limit 90 --concurrency 10 $NODES
./bin/lein run test --workload unique  --nemesis none --time-limit 15 --concurrency 40 $NODES

# evidence (append durability + dense/gap-free seq on a verified evidence chain)
./bin/lein run test --workload evidence --nemesis kill      --time-limit 120 --concurrency 10 $NODES
./bin/lein run test --workload evidence --nemesis partition --time-limit 120 --concurrency 10 $NODES
./bin/lein run test --workload evidence --nemesis mix       --time-limit 180 --concurrency 10 $NODES

# graph (traversal snapshot isolation — atomic diamond swap vs concurrent reachable)
./bin/lein run test --workload graph --nemesis pause     --time-limit 120 --concurrency 10 $NODES
./bin/lein run test --workload graph --nemesis kill      --time-limit 120 --concurrency 10 $NODES
./bin/lein run test --workload graph --nemesis partition --time-limit 120 --concurrency 10 $NODES
./bin/lein run test --workload graph --nemesis mix       --time-limit 180 --concurrency 10 $NODES
```

Results land in `store/`; `store/latest/results.edn` holds the verdict and
`store/latest/timeline.html` a per-process timeline. `:valid? true` means the
checker found no anomalies.

## Layout

- `src/bluedb/jepsen/http.clj` — REST/`/admin` HTTP layer + leader discovery
- `src/bluedb/jepsen/client.clj` — leader-aware set client (add / read)
- `src/bluedb/jepsen/list_append.clj` — Elle list-append client (txn → `/sql`)
- `src/bluedb/jepsen/counter.clj` — counter client (autocommit RMW increments)
- `src/bluedb/jepsen/unique.clj` — same-PK insert client + soundness checker
- `src/bluedb/jepsen/evidence.clj` — evidence-chain append client + dense-seq checker
- `src/bluedb/jepsen/graph.clj` — graph-swap client + snapshot-isolation checker
- `src/bluedb/jepsen/nemesis.clj` — docker-driven kill / partition / pause / skew
- `src/bluedb/jepsen/core.clj` — workloads, generator, checker, CLI
