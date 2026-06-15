# Design: cloud-portable bluedb on k3s (Kubernetes deployment + chaos/load target)

**Date:** 2026-06-15
**Status:** Draft — awaiting review
**Supersedes:** the EKS draft (rejected: most expensive, and Chaos Mesh can't
replicate Jepsen's checker — EKS bought nothing the cheaper paths don't).

## Why this exists

**Kubernetes is bluedb's main deployment target, and it must run on S3 (AWS),
Azure Blob (ABS), and GCS — unchanged.** This work delivers:

1. The **real, cloud-portable K8s deployment artifact** — one production image +
   one manifest set that targets any of the three clouds by env vars alone.
2. A **standing cluster to chaos- and load-test** on a real object store,
   measuring throughput, the dip under faults, and failover recovery.

Substrate: **k3s on a single big EC2 VM** — CNCF-conformant Kubernetes whose
manifests port unchanged to EKS/GKE/AKS, at a fraction of EKS cost (no
control-plane fee, no NAT/LB charges, stoppable when idle), with full root for
chaos tooling.

### The one engine change this requires

bluedb does **not** run on Azure/GCS today. `bluedb-server`'s
[`build_object_store()`](crates/bluedb-server/src/main.rs) constructs **only S3**
(`AmazonS3Builder`) + local-fs + in-memory — there is no Azure or GCS builder
anywhere in the code, despite the docs advertising all three. The underlying
`object_store` crate supports all three; the server never wired them.

**So the only Rust change is making `build_object_store()` multi-cloud** — one
self-contained function, done once, that makes the cloud-agnostic promise true.
Everything else (image, Terraform, manifests, auth) is infra + config with **no
engine changes**. (The earlier `BLUEDB_REQUIRE_AUTH` boot-guard and IMDS-credential
tweaks were dropped — auth is a pure env var, and credentials are handled by the
multi-cloud builder below.)

### Testing division of labor (the Chaos Mesh ≠ Jepsen lesson)

- **Chaos Mesh** injects faults into the K8s deployment (pod kill, partition,
  skew, IO/stress) → tests the *deployment's* resilience and, with a load
  generator + metrics, gives **throughput-under-chaos**.
- **Chaos Mesh verifies nothing.** Proving **no acknowledged write is lost or
  reordered** needs Jepsen's workload+checker re-pointed at the cluster (slice 7).
- The existing `docker-compose` Jepsen suite stays as the local engine-correctness
  gate, untouched.

## Decomposition (sequenced; each its own spec → plan → build)

1. **Multi-cloud object store** (engine) — `build_object_store()` selects S3 /
   Azure / GCS from config; `object_store` azure+gcp features enabled; tests.
2. **Production image** — release `bluedb-server`, auth-on via env.
3. **k3s VM + bluedb deployment** — Terraform (1 VM + S3 + IAM) + k3s + manifests;
   healthy 3-pod cluster + verified failover. *(the "first thing")*
4. **Observability** — Prometheus + Grafana (throughput, latency, node roles).
5. **Load generator** — in-cluster, leader-aware, high-rate, exports metrics.
6. **Chaos Mesh** — install + experiments + a runbook.
7. **Correctness under K8s chaos** — Jepsen workload+checker re-pointed at the
   cluster, faults via Chaos Mesh; closes the safety half on the real deployment.

**This spec covers slices 1–3** — a cloud-portable cluster, deployed on S3 first.
Slices 4–7 are out of scope here and get their own specs.

## Decisions (locked)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Deployment target | **Kubernetes**, cloud-portable | Primary; must run on S3/ABS/GCS unchanged. |
| Substrate | **k3s on one big EC2 VM** | Portable manifests; ~$140/mo running / ~$0 stopped; full chaos control. |
| Lifecycle | Standing (stoppable) | Kept running; stop the instance when idle. |
| First-deploy backend | **S3, `us-east-1`** | VM co-located; Azure/GCS reachable by config after slice 1. |
| Lease arbiter | **In-cluster Postgres** (single instance) | Chaos-faultable; cheap; self-contained. |
| Provisioning | **Terraform** | 1 VM + S3 + IAM. |
| Object-store creds | **Static keys in a Secret** (first deploy) | Zero extra code; ambient identity (instance profile / managed identity / workload identity) is a per-cloud follow-on the same builder already accommodates via `from_env()`. |
| Workload primitive | **StatefulSet**, 3 replicas | Stable identity for leader discovery (state is in S3, no PVCs). |
| Write routing | **Client-side leader discovery** | Matches `kubernetes.md` + the Jepsen client. |
| Auth posture | API-key gate **on** via `BLUEDB_AUTHZ_TOKENS` | Network-reachable; pure env, no code. |
| Cluster size | **Single-node k3s** (start) | Validates the deployment + pod-level chaos; node-level faults are a multi-node escalation. |

## Component design

### 1. Multi-cloud `build_object_store()` (the only engine change)

Extend the existing "first config match wins" chain in `bluedb-server/src/main.rs`
to all three clouds, each built by `object_store`'s native builder:

| Backend | Selector env | Other env | Credentials |
|---------|-------------|-----------|-------------|
| **S3** | `BLUEDB_S3_BUCKET` | `BLUEDB_S3_REGION`, `BLUEDB_S3_ENDPOINT` (MinIO/R2) | `BLUEDB_S3_ACCESS_KEY_ID`/`_SECRET_ACCESS_KEY`, else `from_env()` chain (IMDS/IRSA) |
| **Azure** | `BLUEDB_AZURE_CONTAINER` | `BLUEDB_AZURE_ACCOUNT` | `BLUEDB_AZURE_ACCESS_KEY`, else `from_env()` chain (managed identity) |
| **GCS** | `BLUEDB_GCS_BUCKET` | — | `BLUEDB_GCS_SERVICE_ACCOUNT` (JSON path), else `from_env()` chain (workload identity / ADC) |
| local | `BLUEDB_DATA_DIR` | — | — |
| memory | (none) | — | — |

- **Pattern:** for each cloud, attach explicit creds when the `BLUEDB_*` key var is
  set, else fall back to the builder's `from_env()` so ambient cloud identity works
  keylessly. One uniform shape across all three — and it subsumes the credential
  handling I earlier mis-scoped as separate AWS work.
- **Cargo:** add `object_store = { version = "=0.12.5", features = ["aws","azure","gcp"] }`
  as a direct dep of `bluedb-server`, pinned to the version `slatedb` 0.13 already
  uses so the `Arc<dyn ObjectStore>` types unify (slatedb takes a backend-agnostic
  store, so no slatedb change needed).
- **Tests:** unit-test backend selection + config parsing; S3 round-trip against the
  existing MinIO; Azure/GCS round-trip via emulators (Azurite / fake-gcs-server) is a
  nice-to-have — at minimum construct-and-config them. **Acceptance: the image runs
  unchanged against S3, Azure, and GCS, differing only by env vars.**

### 2. Production image (`Dockerfile.release`)

Separate from the Jepsen image. Multi-stage `cargo build --release -p bluedb-server`
→ binary on `debian:bookworm-slim` + `ca-certificates`. Binds `0.0.0.0:8080`.
Pushed to a registry k3s can pull (GHCR, or `ctr images import` onto the node for a
single-node cluster).

### 3. Terraform (`deploy/terraform/`)

`us-east-1`: one EC2 instance (`m6i.xlarge`, tune later) in a public subnet + a
security group (SSH from your IP, the `bluedb-read` LB port, k3s API local only); an
S3 bucket (`bluedb-k3s-<suffix>`, private); k3s installed via user-data. (No IAM
instance profile needed for the first deploy — we use static S3 keys in a Secret;
add the instance profile later if you want keyless.) Outputs: public IP/DNS, bucket
name, kubeconfig fetch command.

### 4. Kubernetes manifests (`deploy/k8s/`)

Plain YAML (Kustomize-friendly), namespace `bluedb`:

- **ConfigMap** `bluedb-config`: the object-store selector + bucket/region,
  `BLUEDB_DB_PATH`, `BLUEDB_LEASE_PG_URL`, lease TTL/margin. The same manifest set
  retargets Azure/GCS by swapping these keys.
- **Secrets** `bluedb-objstore` (cloud access keys), `bluedb-auth`
  (`BLUEDB_AUTHZ_TOKENS`), `bluedb-pg` (PG password).
- **Postgres**: single-replica `StatefulSet` (+ PVC on k3s local-path) + headless
  `Service postgres`.
- **bluedb `StatefulSet`** (3 replicas): `serviceName: bluedb`, `BLUEDB_NODE_ID`
  from the downward API (`metadata.name`), env from ConfigMap + Secrets,
  liveness/readiness = `GET /health`, no `volumeClaimTemplates`.
- **Service `bluedb`** (headless) — per-pod DNS for leader discovery.
- **Service `bluedb-read`** (`type: LoadBalancer` → k3s klipper servLB) — reads +
  public entry/health.

**Write routing:** client-side leader discovery — poll `/admin/status`, write to the
`active` pod; a write to a replica returns `503` and the client re-discovers (the
model in `kubernetes.md` + the Jepsen client). No custom controller.

**Failover:** all pods contend for the same Postgres lease (keyed on
`BLUEDB_DB_PATH`); one promotes to writer, the rest attach as readers; the HA loop
renews while writer and takes over on lease expiry. Killing the writer pod → a
standby claims the freed lease (~TTL); the rescheduled pod rejoins as a replica.

## Acceptance criteria

1. `build_object_store()` selects S3/Azure/GCS by config (unit-tested); the image
   runs against **S3 live** and against Azure + GCS **differing only by env vars**
   (emulator round-trip or, at minimum, documented + construct-verified).
2. `terraform apply` stands up the VM + S3 + k3s from scratch; the node is `Ready`.
3. A 3-replica StatefulSet runs; `GET /admin/status` shows exactly **one `active`**,
   two replicas. A write with a valid key to the active pod succeeds and reads back;
   a write to a replica returns `503`; no/wrong key → 401/403.
4. **Failover:** `kubectl delete pod <writer>` → within ~TTL a different pod is
   `active` and accepts writes; no acknowledged write is lost (read-back); the
   deleted pod reschedules and rejoins as a replica.

## Out of scope (later slices)

Prometheus/Grafana; load generator; Chaos Mesh + experiments; the Jepsen-checker
re-point; CI build/push; TLS/ingress hardening; multi-node k3s; keyless ambient
credentials (instance profile / managed identity / workload identity).

## Risks & open items

- **Azure/GCS live verification** needs emulators (Azurite / fake-gcs-server) or
  real accounts; the spec proceeds with S3 live + Azure/GCS construct-and-config,
  and a documented runbook to switch.
- **`object_store` version pin** must match slatedb's (`=0.12.5`) or the store types
  won't unify — verify on `cargo build`.
- **Single-node k3s** can't test node-level faults — only pod-level chaos.
  Multi-node k3s is the escalation.
- **Single-instance in-cluster Postgres** is a single point of failure for writer
  election — fine for a test env, and itself a chaos target.
- **Cost** (standing): ~$140/mo running, ~$0 stopped.
