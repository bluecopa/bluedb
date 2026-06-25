# Sizing & capacity

This page answers the practical question: **how many concurrent write clients can
one node sustain, and what writes/sec does that give me?** Plus how to size
reads and storage.

## The write equation

All writes go through the single writer (one lease holder), and each insert is
acked only when durable, i.e. on the next WAL flush. Because the WAL
**group-commits** every in-flight write into that one flush:

```
max writes/sec  ≈  (concurrent write clients)  ÷  (flush_interval)
```

So at the default `flush_interval = 25 ms`, **one** client can sustain up to
~40 inserts/s in the storage engine, and N clients can sustain roughly N × 40/s
**until the deployed system hits its knee** (below). In production, public load
balancing, HTTP overhead, Kubernetes forwarding, writer CPU, and object-store PUT
latency all lower that optimistic curve.

| flush_interval | per-client | 100 clients | 500 clients |
|--:|--:|--:|--:|
| 25 ms (default) | ~40/s | ~4,000/s | ~20,000/s |
| 10 ms | ~100/s | ~10,000/s | ~50,000/s |
| 50 ms | ~20/s | ~2,000/s | ~10,000/s |

(These hold only while clients are at or below the node's knee. They are the *linear region*,
not a promise at any N.)

## Current Civo write SLO

Use this as the current customer-facing baseline until you benchmark your own
cluster. It is intentionally conservative and measured through the public API,
not inside the process.

**Measured 2026-06-25** on the Civo UAT deployment: 3-pod Kubernetes HA cluster,
Kubernetes lease, public `LoadBalancer`, server-side forwarding enabled, Civo
object-store backend, default `flush_interval = 25 ms`, primary-key `/tables`
single-row inserts, Iceberg mirror off, 15s measured windows with 2s warmup.

| Concurrent clients | Writes/sec | p50 | p99 | Interpretation |
|--:|--:|--:|--:|---|
| 1 | 14.9 | 55.9 ms | 154 ms | serial-client floor |
| 8 | 99.3 | 73.9 ms | 184 ms | still comfortably linear |
| 16 | 163.3 | 88.8 ms | 205 ms | still comfortably linear |
| 32 | 222.2 | 139 ms | 372 ms | below the knee |
| 64 | 264.3 | 236 ms | 494 ms | below the knee |
| 128 | 287.3 | 432 ms | 855 ms | knee begins |
| 256 | 312.1 | 748 ms | 2,056 ms | ceiling/tail blowout |

For sizing conversations today, treat this deployment as **roughly linear to
about 128 concurrent write clients**, with an observed ceiling of **about
312 writes/sec**. Past the knee, p99 moves into seconds; do not sell or size the
current Civo profile above that point without admission control, batching, larger
writer resources, or log-path work that beats this baseline.

If you need a practical bound from this exact environment:

- p99 under ~500 ms: stay at or below ~64 concurrent writers, about 260 writes/s.
- p99 under ~1 s: stay at or below ~128 concurrent writers, about 285 writes/s.
- Absolute observed ceiling: about 312 writes/s, but p99 was already ~2 s.

The local-SSD benchmark later on this page shows the engine's group-commit
ceiling, not the current Civo/customer sizing number.

**Iceberg mirror sanity check:** the same Civo sweep with
`PRAGMA lakehouse_mirror = on` for one isolated tenant did **not** reintroduce a
per-tenant serialization cap. It reached **322.3 writes/sec** at 256 concurrent
clients with p99 **1,554 ms**. The writer and node were CPU-saturated during the
high-concurrency steps, so treat mirror-on sizing as "same order of magnitude,
but CPU-bound sooner"; remeasure if the seal cadence, row width, or mirrored
table count changes.

## What caps concurrent clients on one node (the knee)

Throughput scales linearly with clients until one resource saturates. In order of
what you'll usually hit first:

1. **Object-store PUT throughput.** Each flush is one PUT carrying that window's
   batch (~N rows at steady state). If a PUT takes longer than `flush_interval`,
   flushes back up: latency climbs and throughput plateaus. On networked storage
   (S3/GCS/Azure) the **PUT latency is the real floor**: effective write latency ≈
   `max(flush_interval, PUT latency)`, and a slow PUT lowers the knee. This is why
   the same node has a much higher knee on local SSD than on S3.
2. **Writer CPU.** Every insert parses + plans + executes SQL and takes a brief
   `SeqAllocator` lock. At high rates this is the ceiling; scale up the writer's
   cores.
3. **RAM.** In-flight request state + the SlateDB cache. Rarely the first limit for
   writes, but it bounds the read cache (below).

**Lab reference point:** on the local-SSD bench node, write throughput scaled
linearly **past 256 clients (~9,600 inserts/s)** with p50 still roughly 27 ms;
the knee was beyond what we measured. Treat this as an engine upper bound. On
networked object storage expect a **lower** knee, set by PUT latency, CPU, and
HTTP/forwarding overhead; the Civo UAT baseline above is the current
production-facing reference.

## How to find your node's number

The knee is empirical. Measure it on your target node size **and** object store.
For the HTTP surface, use the external load harness:

```bash
BLUEDB_TOKEN=... node scripts/load/bluedb-http-load.mjs \
  --base-url http://<public-bluedb>:8080 \
  --profiles write \
  --steps 1,2,4,8,16,32,64,128,256 \
  --duration-seconds 15 \
  --warmup-seconds 2 \
  --seed-rows 0 \
  --stop-at-knee false
```

For the storage-engine lab benchmark:

```bash
cargo test --release -p bluedb-sql --test throughput_bench -- --ignored --nocapture
```

Read the concurrency sweep: the knee is where writes/sec stops rising linearly
and/or p99 starts climbing. Then:

```
your node's max writes/sec  ≈  (knee clients)  ÷  (flush_interval)
```

To match production, point the bench's backend at your real object store (swap the
in-memory/local-disk store for S3/Azure/GCS) and use your deployed `flush_interval`.

## Capping it

**Today there is no built-in admission control.** Past the knee the node degrades
without backpressure: latency rises and tail latencies blow out rather than
clients being shed. So you cap by **provisioning**: size the writer (and pick
`flush_interval`) so your *peak* concurrent write load stays in the linear region,
with headroom.

A hard, predictable ceiling (a write-concurrency limit such as a semaphore or
connection cap set at the measured knee, so excess clients queue or get `503`
instead of thrashing) is a natural next lever but is **not yet a configurable
option**. Until then, enforce limits at your load balancer or client pool.

## Reads and storage

- **Reads scale horizontally.** Add read-replica nodes; the read Service
  load-balances across them. Each serves from its local SlateDB cache, so size
  **node RAM** to your working set (warm reads are sub-ms; a cold read is one
  object-store GET) and add replicas for more read QPS. Reads do **not** contend
  with the single writer.
- **Storage is the object store, and it is elastic.** Not a node-sizing concern (nodes are
  stateless). Cost tracks bytes stored + request volume: PUTs ≈ the flush rate
  (`1 / flush_interval`) plus explicit transaction commits; GETs ≈ the cold-read
  rate.

## Worked example

Target: 8,000 writes/s, p99 < 40 ms, on S3.

1. Pick `flush_interval` for the latency budget: 25 ms leaves room under 40 ms
   once S3's PUT (~tens of ms) is added; verify `max(25 ms, PUT latency)` meets
   your SLO.
2. Needed concurrency ≈ 8,000 × 0.025 s = **~200 concurrent clients in flight**.
3. Run the sweep on your S3 + node size; confirm the knee is **above ~200** with
   p99 in budget. If the knee is below 200, either scale the writer up, lower
   `flush_interval` (more/faster PUTs), batch writes into `BEGIN…COMMIT`, or shard
   tenants across separate clusters (keys are tenant-namespaced; bluedb does not
   scale a single writer horizontally).
