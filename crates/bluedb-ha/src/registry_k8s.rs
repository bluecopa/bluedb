//! A Kubernetes-native [`NodeRegistry`] — liveness from the K8s API.
//!
//! In Kubernetes, "is this node live?" is already answered authoritatively by the
//! control plane: a pod backing a Service appears as a **Ready** endpoint only
//! while it passes its readiness probe. So this backend does not run its own
//! heartbeat — [`heartbeat`](NodeRegistry::heartbeat) is a documented no-op
//! (readiness *is* the heartbeat) — and instead discovers live nodes by listing
//! the [`EndpointSlice`]s of the coordinator Service.
//!
//! ## Why EndpointSlices (not pods, not Endpoints)
//! An [`EndpointSlice`] is the API's own materialized view of *which addresses
//! are currently serving a Service*: the kubelet/endpoint controller adds an
//! endpoint only when its pod is Ready, and each endpoint carries both a
//! `targetRef` (the backing pod) and `conditions.ready`. Listing slices labeled
//! `kubernetes.io/service-name=<service>` therefore yields exactly the live
//! coordinators — no need to re-implement readiness by intersecting Pod status
//! with a label selector, and `EndpointSlice` scales past the 1000-endpoint
//! limit of the legacy `Endpoints` object.
//!
//! ## node_id ↔ pod alignment
//! The registry's `node_id` is the SAME id the lease elects on
//! (`BLUEDB_NODE_ID`). The canonical bluedb deployment is a StatefulSet, whose
//! pods get stable ordinal names (`bluedb-0`, `bluedb-1`, …) and set
//! `BLUEDB_NODE_ID` to their own pod name (the downward-API `metadata.name` /
//! `$HOSTNAME`). An endpoint's `targetRef.name` is that pod name, so it lines up
//! with the elected `node_id` directly; `hostname` is the fallback. This is the
//! contract the deployment must honor for `url_for(lease.holder)` to resolve.
//!
//! ## URL derivation
//! Each endpoint exposes one or more pod IPs in `addresses`. The reachable URL
//! is `http://<addr>:<port>`, where `<port>` is the configured coordinator port.
//! (A stable per-pod DNS name via a headless Service is an equivalent target; the
//! IP is used here because it is always present on the endpoint without extra
//! lookups.)
//!
//! Enable with the `kubernetes` crate feature.

use anyhow::{Context, Result};
use async_trait::async_trait;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::{Api, ListParams};
use kube::Client;

use crate::registry::NodeRegistry;

/// True if this process is running inside a Kubernetes pod.
///
/// Kubernetes injects `KUBERNETES_SERVICE_HOST` into every pod's environment
/// (the in-cluster API server address). Its presence is the standard in-cluster
/// detection used by every Kubernetes client library. `bluedb-server` uses this
/// to decide whether to select the K8s registry backend.
pub fn in_cluster() -> bool {
    std::env::var("KUBERNETES_SERVICE_HOST")
        .map(|v| !v.is_empty())
        .unwrap_or(false)
}

/// A [`NodeRegistry`] that discovers live nodes from the Kubernetes API by
/// listing the [`EndpointSlice`]s of the coordinator Service.
pub struct K8sNodeRegistry {
    client: Client,
    /// Namespace the coordinator Service lives in.
    namespace: String,
    /// The coordinator Service name (slices are selected by
    /// `kubernetes.io/service-name=<service>`).
    service: String,
    /// The coordinator port appended to each endpoint address to form its URL.
    port: u16,
}

impl K8sNodeRegistry {
    /// Connect using the in-cluster config (the pod's mounted ServiceAccount).
    /// Discovers Ready endpoints of `service` in `namespace`, forming each URL as
    /// `http://<pod-ip>:<port>`.
    pub async fn connect(
        namespace: impl Into<String>,
        service: impl Into<String>,
        port: u16,
    ) -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("build kubernetes client (in-cluster config)")?;
        Ok(Self::with_client(client, namespace, service, port))
    }

    /// Build over an existing kube [`Client`] (tests / custom config).
    pub fn with_client(
        client: Client,
        namespace: impl Into<String>,
        service: impl Into<String>,
        port: u16,
    ) -> Self {
        Self {
            client,
            namespace: namespace.into(),
            service: service.into(),
            port,
        }
    }

    /// List the coordinator Service's [`EndpointSlice`]s (label-selected by
    /// `kubernetes.io/service-name`).
    async fn list_slices(&self) -> Result<Vec<EndpointSlice>> {
        let api: Api<EndpointSlice> = Api::namespaced(self.client.clone(), &self.namespace);
        let lp =
            ListParams::default().labels(&format!("kubernetes.io/service-name={}", self.service));
        let list = api
            .list(&lp)
            .await
            .context("list coordinator EndpointSlices")?;
        Ok(list.items)
    }
}

/// Map a set of [`EndpointSlice`]s to `(node_id, url)` for every **Ready**
/// endpoint, forming each URL as `http://<addr>:<port>`.
///
/// Pure (no I/O) so the mapping is unit-testable with fixtures off-cluster:
///
/// - An endpoint is included only when `conditions.ready` is `Some(true)` *or*
///   absent (absent `ready` is treated as ready, per the EndpointSlice spec's
///   "unknown ⇒ ready for non-Service-mesh consumers" convention).
/// - `node_id` is `targetRef.name` (the backing pod, == `BLUEDB_NODE_ID`),
///   falling back to the endpoint `hostname`. An endpoint with neither is skipped
///   (we cannot key it to a lease holder).
/// - The URL uses the endpoint's first address.
pub(crate) fn endpoints_to_nodes(slices: &[EndpointSlice], port: u16) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for slice in slices {
        for ep in &slice.endpoints {
            // Readiness: explicit false excludes; true or unset includes.
            let ready = ep.conditions.as_ref().and_then(|c| c.ready).unwrap_or(true);
            if !ready {
                continue;
            }
            // node_id: prefer the backing pod name, fall back to hostname.
            let node_id = ep
                .target_ref
                .as_ref()
                .and_then(|r| r.name.clone())
                .or_else(|| ep.hostname.clone());
            let Some(node_id) = node_id else { continue };
            // URL from the first address.
            let Some(addr) = ep.addresses.first() else {
                continue;
            };
            out.push((node_id, format!("http://{addr}:{port}")));
        }
    }
    out
}

#[async_trait]
impl NodeRegistry for K8sNodeRegistry {
    async fn live_nodes(&self) -> Result<Vec<(String, String)>> {
        let slices = self.list_slices().await?;
        Ok(endpoints_to_nodes(&slices, self.port))
    }

    async fn url_for(&self, node_id: &str) -> Result<Option<String>> {
        Ok(self
            .live_nodes()
            .await?
            .into_iter()
            .find(|(id, _)| id == node_id)
            .map(|(_, url)| url))
    }

    /// No-op: Kubernetes readiness is the heartbeat. A node's liveness in this
    /// backend is maintained entirely by the control plane (its readiness probe);
    /// there is nothing for the node to push. Present so the trait is uniform and
    /// the server's heartbeat loop can be skipped for this backend.
    async fn heartbeat(&self, _node_id: &str, _url: &str) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::api::core::v1::ObjectReference;
    use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions};

    /// Build an endpoint with the given readiness, target-pod name, hostname, and
    /// addresses. `None` fields are omitted.
    fn endpoint(
        ready: Option<bool>,
        target_name: Option<&str>,
        hostname: Option<&str>,
        addresses: &[&str],
    ) -> Endpoint {
        Endpoint {
            addresses: addresses.iter().map(|s| s.to_string()).collect(),
            conditions: Some(EndpointConditions {
                ready,
                ..Default::default()
            }),
            hostname: hostname.map(|s| s.to_string()),
            target_ref: target_name.map(|n| ObjectReference {
                name: Some(n.to_string()),
                kind: Some("Pod".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn slice(endpoints: Vec<Endpoint>) -> EndpointSlice {
        EndpointSlice {
            address_type: "IPv4".to_string(),
            endpoints,
            ..Default::default()
        }
    }

    #[test]
    fn maps_ready_endpoints_to_node_id_and_url() {
        // targetRef.name (the pod name) becomes node_id; URL = http://ip:port.
        let s = slice(vec![
            endpoint(
                Some(true),
                Some("bluedb-0"),
                Some("bluedb-0"),
                &["10.0.0.1"],
            ),
            endpoint(
                Some(true),
                Some("bluedb-1"),
                Some("bluedb-1"),
                &["10.0.0.2"],
            ),
        ]);
        let mut nodes = endpoints_to_nodes(&[s], 8080);
        nodes.sort();
        assert_eq!(
            nodes,
            vec![
                ("bluedb-0".to_string(), "http://10.0.0.1:8080".to_string()),
                ("bluedb-1".to_string(), "http://10.0.0.2:8080".to_string()),
            ]
        );
    }

    #[test]
    fn excludes_not_ready_endpoints() {
        let s = slice(vec![
            endpoint(Some(true), Some("bluedb-0"), None, &["10.0.0.1"]),
            endpoint(Some(false), Some("bluedb-1"), None, &["10.0.0.2"]), // not Ready
        ]);
        let nodes = endpoints_to_nodes(&[s], 8080);
        assert_eq!(
            nodes,
            vec![("bluedb-0".to_string(), "http://10.0.0.1:8080".to_string())]
        );
    }

    #[test]
    fn unset_ready_is_treated_as_ready() {
        // ready = None (unknown) ⇒ included.
        let s = slice(vec![endpoint(None, Some("bluedb-0"), None, &["10.0.0.1"])]);
        let nodes = endpoints_to_nodes(&[s], 9000);
        assert_eq!(
            nodes,
            vec![("bluedb-0".to_string(), "http://10.0.0.1:9000".to_string())]
        );
    }

    #[test]
    fn falls_back_to_hostname_when_no_target_ref() {
        let s = slice(vec![endpoint(
            Some(true),
            None,
            Some("bluedb-7"),
            &["10.0.0.7"],
        )]);
        let nodes = endpoints_to_nodes(&[s], 8080);
        assert_eq!(
            nodes,
            vec![("bluedb-7".to_string(), "http://10.0.0.7:8080".to_string())]
        );
    }

    #[test]
    fn skips_endpoints_with_no_identity_or_no_address() {
        // No targetRef AND no hostname → cannot key to a node_id → skipped.
        let no_id = endpoint(Some(true), None, None, &["10.0.0.1"]);
        // Has identity but no address → no URL → skipped.
        let no_addr = endpoint(Some(true), Some("bluedb-3"), None, &[]);
        let nodes = endpoints_to_nodes(&[slice(vec![no_id, no_addr])], 8080);
        assert!(nodes.is_empty());
    }

    #[test]
    fn flattens_endpoints_across_multiple_slices() {
        let s1 = slice(vec![endpoint(
            Some(true),
            Some("bluedb-0"),
            None,
            &["10.0.0.1"],
        )]);
        let s2 = slice(vec![endpoint(
            Some(true),
            Some("bluedb-1"),
            None,
            &["10.0.0.2"],
        )]);
        let mut nodes = endpoints_to_nodes(&[s1, s2], 8080);
        nodes.sort();
        assert_eq!(
            nodes,
            vec![
                ("bluedb-0".to_string(), "http://10.0.0.1:8080".to_string()),
                ("bluedb-1".to_string(), "http://10.0.0.2:8080".to_string()),
            ]
        );
    }
}
