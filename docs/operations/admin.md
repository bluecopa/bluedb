# Administration

Each `bluedb-server` node exposes admin and health endpoints that drive and
observe the single-writer role.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health` | Liveness probe |
| `GET` | `/admin/status` | This node's writer role, fencing epoch, lease expiry |
| `POST` | `/admin/promote` | Acquire the lease and open the writer database |
| `POST` | `/admin/demote` | Release the lease and rebind as a read replica |

## Checking role

```bash
curl -s localhost:8081/admin/status
```

Returns whether this node is the **active writer** or a **passive replica**, its
current fencing **epoch**, and the lease **expiry**. Clients use this to find and
follow the leader.

## Manual failover

Failover is automatic on writer loss (see [Active-passive HA](../ha/active-passive.md)),
but you can drive it by hand for maintenance:

```bash
# Drain the current writer (it releases the lease, becomes read-only)
curl -s -X POST localhost:8081/admin/demote

# Promote a chosen standby (it acquires the lease with a higher epoch)
curl -s -X POST localhost:8082/admin/promote
```

`promote` is safe by construction: it acquires the lease with a bumped epoch, and
SlateDB's compare-and-set fences any node still on the old epoch, so a manual
promote cannot create two writers.

!!! warning "Writes target the active writer"
    `POST /sql`, `POST/PATCH/DELETE /tables/{table}` are accepted only by the
    active writer. A replica returns `503`, the signal for a client to
    re-discover the leader via `/admin/status`. Reads (`GET`) are served by any
    node.

## Health checks

`GET /health` is a liveness probe (the process is up). For **readiness that
should route writes**, gate on `/admin/status` reporting active (see the
[Kubernetes](../deployment/kubernetes.md#routing-writes-to-the-leader) routing
options).

## Observing failover

Kill or pause the writer and watch a standby take over within the lease TTL:

```bash
docker kill bluedb-node1-1
watch -n1 'for p in 8081 8082 8083; do echo -n "$p: "; curl -s localhost:$p/admin/status; echo; done'
```

The [Jepsen](../guarantees/jepsen.md) suite automates exactly this and verifies
no acknowledged write is lost.
