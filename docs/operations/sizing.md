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

So at the default `flush_interval = 25 ms`, **one** client sustains ~40 inserts/s,
and N clients sustain roughly N × 40/s, **until the node hits its knee** (below). Latency
per write stays about the flush interval regardless of N; only throughput scales.

| flush_interval | per-client | 100 clients | 500 clients |
|--:|--:|--:|--:|
| 25 ms (default) | ~40/s | ~4,000/s | ~20,000/s |
| 10 ms | ~100/s | ~10,000/s | ~50,000/s |
| 50 ms | ~20/s | ~2,000/s | ~10,000/s |

(These hold only while clients are at or below the node's knee. They are the *linear region*,
not a promise at any N.)

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

**Reference point:** on the local-SSD bench node, write throughput scaled linearly
**past 256 clients (~9,600 inserts/s)** with p50 still roughly 27 ms; the knee was beyond
what we measured. On networked object storage expect a **lower** knee, set by PUT
latency. There is no single universal number; it depends on the node and the
bucket.

## How to find your node's number

The knee is empirical. Measure it on your target node size **and** object store:

```bash
cargo test --release -p bluedb-sql --test throughput_bench -- --ignored --nocapture
```

Read the concurrency sweep: the knee is where inserts/sec stops rising linearly
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
