# bluedb Jepsen test

A real [Jepsen](https://jepsen.io) consistency test for the bluedb cluster:
randomized concurrent operations + injected faults + a formal checker over the
recorded history.

## What it checks

**Workload — grow-only set.** Clients append unique integers through the active
writer; a final-read phase reads the whole set back. The `set-full` checker
proves two things across failover:

- **no lost writes** — every *acknowledged* add (HTTP 200) is present in the
  final read, and
- **no fabrication** — no element appears that was never added.

The cluster is driven as a single logical linearizable writer whose identity
moves on failover (bluedb's actual guarantee: one linearizable writer + async
read replicas). The client discovers the active writer via `/admin/status` and
re-discovers when it gets a `503` (its node is now a replica) — i.e. it follows
the leader across promotions.

## Faults (nemesis)

Injected via the `docker` CLI against the compose stack — the natural
fault-injection seam for a containerized deployment (no SSH; `:ssh {:dummy?
true}`):

| `--nemesis` | fault | tests |
|---|---|---|
| `kill` | `docker kill` the active writer (crash) | durability of acked writes through SlateDB's WAL window; a standby promotes |
| `partition` | `docker network disconnect` the writer from Postgres + MinIO + peers | clean failover, no split-brain (the isolated writer loses its lease and storage at once) |
| `mix` | both, alternating | combined |
| `none` | — | baseline (no faults) |

Fault windows straddle the 10 s lease TTL so failover completes inside each
window.

## Running

Prereqs: the cluster must be **up** (`docker compose up -d` from the repo root)
and Java 17+ on `$PATH`. Leiningen is vendored at `bin/lein`.

```bash
cd jepsen
export LEIN_HOME="$PWD/.lein"

# baseline, then each fault mode
./bin/lein run test --nemesis none      --time-limit 60  --concurrency 10 \
  --node node1 --node node2 --node node3
./bin/lein run test --nemesis kill      --time-limit 120 --concurrency 10 \
  --node node1 --node node2 --node node3
./bin/lein run test --nemesis partition --time-limit 120 --concurrency 10 \
  --node node1 --node node2 --node node3
./bin/lein run test --nemesis mix       --time-limit 180 --concurrency 10 \
  --node node1 --node node2 --node node3
```

Results land in `store/`; `store/latest/results.edn` holds the verdict and
`store/latest/timeline.html` a per-process timeline. `:valid? true` means the
checker found no anomalies.

## Layout

- `src/bluedb/jepsen/http.clj` — REST/`/admin` HTTP layer + leader discovery
- `src/bluedb/jepsen/client.clj` — leader-aware set client (add / read)
- `src/bluedb/jepsen/nemesis.clj` — docker-driven kill / partition / heal
- `src/bluedb/jepsen/core.clj` — workload, generator, checker, CLI
