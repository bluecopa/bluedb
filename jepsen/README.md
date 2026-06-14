# bluedb Jepsen test

A real [Jepsen](https://jepsen.io) consistency test for the bluedb cluster:
randomized concurrent operations + injected faults + a formal checker over the
recorded history.

## What it checks

Two workloads, pick with `--workload`:

**`set` (default) — grow-only set.** Clients append unique integers through the
active writer; a final-read phase reads the whole set back. The `set-full`
checker proves two things across failover:

- **no lost writes** — every *acknowledged* add (HTTP 200) is present in the
  final read, and
- **no fabrication** — no element appears that was never added.

Best for durability / no-lost-update under failover.

**`list-append` — Elle list-append.** Each Jepsen transaction is a `BEGIN; …;
COMMIT;` of appends + reads over several keys, sent as one `POST /sql` so it runs
as one explicit transaction on the active writer. Elle reconstructs the
transaction dependency graph from the observed reads and flags any
serializability anomaly (G0/G1/G2, write skew, lost update). This exercises the
explicit-transaction path under concurrency — the stronger guarantee that
`set-full` doesn't cover.

In both cases the cluster is driven as a single logical writer whose identity
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
| `mix` | kill + partition, alternating | combined |
| `chaos` | kill + pause + skew + partition-half | everything |
| `none` | — | baseline (no faults) |

Fault windows straddle the 10 s lease TTL so failover completes inside each
window. The clock-skew nemesis needs the image's libfaketime entrypoint
(`docker-entrypoint.sh`); it writes the offset into each node's
`/faketime/offset` via `docker exec`.

## Running

Prereqs: the cluster must be **up** (`docker compose up -d` from the repo root)
and **Java 21+** on `$PATH` (a transitive dep needs `java.util.SequencedCollection`;
JDK 17 fails to load it). Leiningen is vendored at `bin/lein`.

```bash
cd jepsen
export LEIN_HOME="$PWD/.lein"
# point at a JDK 21+ if your default `java` is older, e.g. on macOS:
# export JAVA_HOME=$(/usr/libexec/java_home -v 24); export PATH="$JAVA_HOME/bin:$PATH"

NODES="--node node1 --node node2 --node node3"

# set workload (durability / no-lost-update), each fault mode
./bin/lein run test --workload set --nemesis none      --time-limit 60  --concurrency 10 $NODES
./bin/lein run test --workload set --nemesis kill      --time-limit 120 --concurrency 10 $NODES
./bin/lein run test --workload set --nemesis partition --time-limit 120 --concurrency 10 $NODES
./bin/lein run test --workload set --nemesis mix       --time-limit 180 --concurrency 10 $NODES

# list-append workload (serializability) — baseline and through failover
./bin/lein run test --workload list-append --nemesis none --time-limit 60  --concurrency 10 $NODES
./bin/lein run test --workload list-append --nemesis mix  --time-limit 120 --concurrency 10 $NODES
```

Results land in `store/`; `store/latest/results.edn` holds the verdict and
`store/latest/timeline.html` a per-process timeline. `:valid? true` means the
checker found no anomalies.

## Layout

- `src/bluedb/jepsen/http.clj` — REST/`/admin` HTTP layer + leader discovery
- `src/bluedb/jepsen/client.clj` — leader-aware set client (add / read)
- `src/bluedb/jepsen/list_append.clj` — Elle list-append client (txn → `/sql`)
- `src/bluedb/jepsen/nemesis.clj` — docker-driven kill / partition / heal
- `src/bluedb/jepsen/core.clj` — workloads, generator, checker, CLI
