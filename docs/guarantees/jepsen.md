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

In the set/list-append/counter workloads the cluster is driven as one logical
writer whose identity moves on failover; the client discovers the active writer
via `/admin/status` and re-discovers on a `503` — it **follows the leader**
across promotions.

!!! warning "Harness ported to the schema regime — some workloads pending re-run"
    The merged [schema regime](../sql/query-guardrail.md) (required `PRIMARY KEY`,
    no schemaless auto-create, bounded reads) made the original Jepsen client
    stale — it assumed a table auto-creates on first insert and that one read
    returns the whole set. The **`set`** workload has been ported (it now creates
    `jset (v INTEGER PRIMARY KEY)` up front and keyset-paginates the final read)
    and **re-verified on the live cluster against the post-group-commit write
    path** (see Results). The **`list-append`**, **`counter`**, **`unique`**, and
    **`ledger`** workloads still need the same port before they can be re-run —
    `list-append` additionally needs a redesign, since it assumed
    `SELECT … WHERE k = ?` returns *insertion* order, which no longer holds under
    [index-organized](../concepts/architecture.md#storage-model-index-organized-tables)
    (PK-clustered) storage. Their earlier green results predate the schema regime;
    treat them as pending re-validation.

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
- `list-append` passes the Elle checker up to **strict-serializable** for the
  explicit-transaction path.

The dependency-fault and `list-append` results above were established before the
schema regime landed; they are being re-run as the remaining workloads are ported
(see the warning above). The `set` re-verification is current.

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
bin/lein run test --workload set --nemesis kill      --time-limit 120 $NODES
bin/lein run test --workload set --nemesis partition --time-limit 120 $NODES
```

(Pass the `--node` flags explicitly — a shell that doesn't word-split an
unquoted variable will otherwise hand them to lein as one argument.)

See [`jepsen/README.md`](https://github.com/bluecopa/bluedb/blob/dev/jepsen/README.md)
for all flags and the schema-regime notes for the other workloads.
