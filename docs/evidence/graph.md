# Native graph store

`bluedb-evidence` includes a **native, adjacency-indexed graph store** exposed at
`/graph/*`. A *graph* is a first-class object per `(tenant, graph)`: a set of
**directed, weighted, optionally-typed edges** with `out`/`in` adjacency indexes,
so neighbor lookup and traversal are bounded prefix range scans rather than
table scans (the HelixDB layout, realized on SlateDB's ordered keyspace).

It pairs with [evidence chains](chains.md): an event can carry the edges it
justifies, so the **graph is a reproducible projection of the chain**. Replaying
the chain rebuilds the graph. Standalone edge writes exist for bulk import,
rebuild, and as-of analysis.

## Edge model

An edge is `(src, dst, weight, type)`:

- **Identity** is `(graph, src, dst, type)`: at most one weight per identity; a
  re-upsert updates it.
- **Parallel edges** between the same ordered pair are modeled with distinct
  `type` values. For an untyped graph (`type = ""`), `(src, dst)` is unique and a
  re-upsert combines by the chosen **merge** mode.
- **Nodes are `TEXT`, weights are 64-bit `INTEGER`.** Nodes are implicit: the
  union of all `src`/`dst`, with no separate node table.
- The weight is folded **order-preserving** into the adjacency keys, so a node's
  out-edges are stored sorted by weight ascending. That makes `reachable`'s
  weight floor a range lower-bound and `widest_path`'s best-first search a range
  walk.

## Append-with-edges

The hot path is **append-with-edges**: an [evidence
append](chains.md#post-evidencechainentries-append-events) whose events carry an
`edges[]` array. bluedb applies each edge's `out`/`in`/canonical maintenance in
the **same `WriteBatch`** as the chain entry, so the chain and graph advance
atomically, exactly-once. Each `edges[]` element is:

```json
{ "graph": "lineage", "src": "doc:A", "dst": "doc:B",
  "weight": 5, "type": "", "op": "upsert", "merge": "set" }
```

- `op` is `"upsert"` (default) or `"delete"`; `merge` is `"set"` (default) or
  `"max"` (for upserts). `weight` defaults to `0`, `type` to `""`.
- Because each entry stores its edge-deltas and they are folded into the verified
  [`leaf_hash`](chains.md#merkle-verification-verified-chains), replaying the chain
  reconstructs the graph. The graph is a projection, not separate truth.

[Hard-deleting](chains.md#erasure) an entry retracts its edges from the graph by
default (`?retract_edges=true`). Retraction removes the referenced edge
identities; it does not restore a prior weight (the graph is rebuildable).

## Standalone edge writes

Use these for edges with no originating event (bulk load, rebuild, as-of replay).

### `PUT /graph/{graph}/edges`: upsert edges

```bash
curl -s -X PUT localhost:8081/graph/lineage/edges \
  -H 'content-type: application/json' \
  -d '{ "edges": [{"src":"A","dst":"B","weight":5}], "merge": "max" }'
```

```json
{ "graph": "lineage", "upserted": 1 }
```

Body `{"edges": [{src, dst, weight, type?}], "merge"?: "set"|"max"}` (default
`set`). For each edge bluedb reads the canonical entry and, if the weight
changes, deletes the stale `out`/`in` index entries before writing the new ones,
all in one atomic batch, with read-your-own-writes across edges in the same
request. Scope: `data:write`.

### `DELETE /graph/{graph}/edges`: delete edges by identity

```bash
curl -s -X DELETE localhost:8081/graph/lineage/edges \
  -H 'content-type: application/json' \
  -d '{ "edges": [{"src":"A","dst":"B"}] }'
```

```json
{ "graph": "lineage", "deleted": 1 }
```

Body `{"edges": [{src, dst, type?}]}`. Deleting a non-existent edge is a no-op.
Because the graph is a rebuildable projection, this is ordinary maintenance
(scope `data:write`, not `schema:admin`). Scope: `data:write`.

### `POST /graph/{graph}/mutate`: atomic edge rewire

```bash
curl -s -X POST localhost:8081/graph/lineage/mutate \
  -H 'content-type: application/json' \
  -d '{ "upserts": [{"src":"R","dst":"B","weight":1},{"src":"B","dst":"Z","weight":1}],
        "deletes": [{"src":"R","dst":"A"},{"src":"A","dst":"Z"}] }'
```

```json
{ "graph": "lineage", "upserted": 2, "deleted": 2 }
```

Applies `upserts` **and** `deletes` in **one** `WriteBatch`, an atomic rewire.
Every reader observes all the changes at one sequence or none of them, so a path
can be swapped (delete the old edges, add the new ones) without ever exposing a
torn graph where a node is transiently unreachable. Body `{"upserts"?:
[{src,dst,weight,type?}], "deletes"?: [{src,dst,type?}], "merge"?:
"set"|"max"}`; `merge` applies to the upserts. Upserts are applied before
deletes, so if the same edge identity appears in both, the delete wins. Scope:
`data:write`. (This is the primitive the Jepsen graph-swap snapshot-isolation
workload uses.)

### `DELETE /graph/{graph}`: drop an entire graph

```bash
curl -s -X DELETE localhost:8081/graph/lineage
```

```json
{ "graph": "lineage", "dropped": 12 }
```

Range-deletes **all** of the graph's edges (canonical + both adjacency indexes)
in one atomic batch and returns the edge count. `dropped: 0` for an unknown/empty
graph. Scope: `data:write`.

## Traversal

Both traversals run read-only over the adjacency indexes (no write lease needed;
they work on a read replica). `directed: true` (the default) follows `out` edges only;
`directed: false` follows `out` **and** `in` (each stored edge usable both ways
at its weight).

### `POST /graph/{graph}/reachable`: reachable set

Breadth-first reachability over edges with `weight ≥ floor`:

```bash
curl -s -X POST localhost:8081/graph/lineage/reachable \
  -H 'content-type: application/json' \
  -d '{ "from": ["A"], "floor": 0, "directed": true }'
```

```json
{ "graph": "lineage", "nodes": ["A", "B", "C", "D"] }
```

Body `{"from": [...], "floor"?, "directed"?}`. The **seed nodes are included** in
the result (a node reaches itself); `floor` defaults to no floor; `directed`
defaults to `true`; output is **sorted** (order-independent). Each BFS level is
expanded **concurrently** (bounded fan-out, default 16 in-flight scans), so
latency tracks graph *diameter* rather than node count; the `visited` set dedups
regardless of completion order, so the result is identical and deterministic.
Scope: `data:read`.

### `POST /graph/{graph}/widest-path`: max-bottleneck path

The *widest path* maximizes the minimum edge weight along the path (maximin):

```bash
curl -s -X POST localhost:8081/graph/lineage/widest-path \
  -H 'content-type: application/json' \
  -d '{ "from": "A", "to": "D", "directed": true }'
```

```json
{ "connected": true, "bottleneck": 3 }
```

Body `{"from", "to", "directed"?}`. `connected: false` (with **no** `bottleneck`
key) when `to` is unreachable (that is not an error). `from == to` →
`{"connected": true}` with no bottleneck (empty path). The maximin value is
unique, so the result is deterministic. Scope: `data:read`.

## As-of analysis (replacing "scratch")

Because a graph is just a `(tenant, name)` namespace, **any graph name is already
an isolated sandbox**. There is no separate scratch subsystem. To analyze the
graph *as it was* at a point in the chain:

1. Fold [`read_range(1, N)`](chains.md#get-evidencechainentries-read-entries) up
   to the as-of sequence and write the resulting edges into a uniquely-named
   graph (e.g. `asof_decision42`) via `PUT /graph/{name}/edges`.
2. Run `reachable` / `widest_path` against that graph. The live graph is
   untouched.
3. `DELETE /graph/{name}` to tear it down cheaply when finished.

This gives isolated as-of replay + cheap teardown without copying any live data.

## Multi-tenancy

Graphs are tenant-scoped via the `X-Bluedb-Tenant` header (absent ⇒ the default
tenant `_`); the same graph name under two tenants is fully isolated. With
[authorization](../operations/admin.md) on, routes require the noted scope plus a
matching `tenant:<name>` binding (a `superuser` token reaches any).

## Consistency

A traversal (`reachable` / `widest_path`) pins **one read view for its whole
run** before its first scan and reads every adjacency scan through it, across
every BFS level including the concurrently-expanded ones.

- **On the active writer** the view is a true MVCC snapshot (SlateDB
  `Db::snapshot`): every read is served at one sequence number, so an edge
  written after the traversal starts is invisible, and because each edge is
  written as one atomic batch (canonical + out + in keys) the traversal never
  sees a torn edge. A traversal therefore reflects a single consistent cut of
  the graph (**snapshot isolation**). This is Jepsen-checked under faults (see
  the graph-traversal workload).
- **On a read replica** SlateDB exposes no point-in-time snapshot, so the view
  is the live reader: consistent within a single scan but free to advance to a
  newer checkpoint between scans. Replica traversals are best-effort
  checkpoint-consistent, not strictly point-in-time. Drive snapshot-isolated
  traversals against the writer.

## Limitations (v1)

- **`widest_path` frontier is sequential.** `reachable` now expands each BFS
  level concurrently (bounded fan-out, default 16), so its latency tracks graph
  *diameter* rather than node count. `widest_path` keeps its sequential
  priority-queue (maximin Dijkstra) frontier. Its best-first ordering is
  inherently serial, so parallelizing it is low-value and deferred.
- **`widest_path` scans ascending.** It uses a max-heap maximin Dijkstra over the
  forward-ordered index rather than the descending best-first scan with an early
  cutoff; the result is identical, the cutoff micro-optimization is deferred.
- **Drops are O(keys), one batch.** `DELETE /graph/{graph}` (and edge deletes)
  scan and delete each key in a single `WriteBatch` (SlateDB has no native
  range-delete), so dropping a very large graph is one large batch.
- **Nodes are implicit.** There is no explicit isolated-node set; a node exists
  only as the endpoint of an edge.
- **No similarity or vector search.** This is an exact graph store (adjacency,
  weights, maximin) with no HNSW or vector index.
