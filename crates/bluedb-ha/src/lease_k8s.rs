//! A Kubernetes-backed [`LeaseProvider`] using `coordination.k8s.io/v1` Lease.
//!
//! The Kubernetes API server is the shared arbiter. Each write path reads the
//! current Lease object, applies the same fencing-token state transition used by
//! the in-memory/Postgres providers, and writes it back with the object's
//! `resourceVersion` still attached. Kubernetes rejects a stale `resourceVersion`
//! with `409 Conflict`, which gives us the compare-and-swap boundary across pods.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use k8s_openapi::api::coordination::v1::{Lease as K8sLease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use k8s_openapi::chrono::{TimeZone, Utc};
use kube::api::{Api, PostParams};
use kube::{Client, Error as KubeError};

use crate::lease::{Lease, LeaseProvider};

const EPOCH_ANNOTATION: &str = "bluedb.io/lease-epoch";
const MAX_UPDATE_ATTEMPTS: usize = 8;

/// A [`LeaseProvider`] backed by a Kubernetes `coordination.k8s.io/v1` Lease.
pub struct K8sLeaseProvider {
    api: Api<K8sLease>,
    namespace: String,
    name: String,
}

impl K8sLeaseProvider {
    /// Connect using kube-rs default config resolution. In-cluster, this uses the
    /// pod's mounted ServiceAccount token and cluster CA.
    pub async fn connect(namespace: impl Into<String>, name: impl Into<String>) -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("build kubernetes client for lease provider")?;
        Ok(Self::with_client(client, namespace, name))
    }

    /// Build over an existing kube [`Client`] (tests / custom config).
    pub fn with_client(
        client: Client,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        let namespace = namespace.into();
        Self {
            api: Api::namespaced(client, &namespace),
            namespace,
            name: name.into(),
        }
    }

    async fn get_lease(&self) -> Result<Option<K8sLease>> {
        match self.api.get(&self.name).await {
            Ok(lease) => Ok(Some(lease)),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(err).context("get kubernetes Lease"),
        }
    }
}

#[async_trait]
impl LeaseProvider for K8sLeaseProvider {
    async fn try_acquire(
        &self,
        holder: &str,
        ttl: Duration,
        now_millis: i64,
    ) -> Result<Option<Lease>> {
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            match self.get_lease().await? {
                None => {
                    let lease =
                        new_lease_object(&self.namespace, &self.name, holder, ttl, now_millis);
                    let grant = lease_to_grant(&lease)
                        .context("new kubernetes Lease did not produce a grant")?;
                    match self.api.create(&PostParams::default(), &lease).await {
                        Ok(_) => return Ok(Some(grant)),
                        Err(err) if is_conflict(&err) => continue,
                        Err(err) => return Err(err).context("create kubernetes Lease"),
                    }
                }
                Some(mut lease) => {
                    let Some(grant) = apply_acquire(&mut lease, holder, ttl, now_millis) else {
                        return Ok(None);
                    };
                    match self
                        .api
                        .replace(&self.name, &PostParams::default(), &lease)
                        .await
                    {
                        Ok(_) => return Ok(Some(grant)),
                        Err(err) if is_conflict(&err) => continue,
                        Err(err) => return Err(err).context("update kubernetes Lease for acquire"),
                    }
                }
            }
        }
        bail!("kubernetes Lease acquire conflicted after {MAX_UPDATE_ATTEMPTS} attempts");
    }

    async fn renew(
        &self,
        holder: &str,
        epoch: u64,
        ttl: Duration,
        now_millis: i64,
    ) -> Result<Option<Lease>> {
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            let Some(mut lease) = self.get_lease().await? else {
                return Ok(None);
            };
            let Some(grant) = apply_renew(&mut lease, holder, epoch, ttl, now_millis) else {
                return Ok(None);
            };
            match self
                .api
                .replace(&self.name, &PostParams::default(), &lease)
                .await
            {
                Ok(_) => return Ok(Some(grant)),
                Err(err) if is_conflict(&err) => continue,
                Err(err) => return Err(err).context("update kubernetes Lease for renew"),
            }
        }
        bail!("kubernetes Lease renew conflicted after {MAX_UPDATE_ATTEMPTS} attempts");
    }

    async fn release(&self, holder: &str, epoch: u64) -> Result<()> {
        for _ in 0..MAX_UPDATE_ATTEMPTS {
            let Some(mut lease) = self.get_lease().await? else {
                return Ok(());
            };
            if !apply_release(&mut lease, holder, epoch) {
                return Ok(());
            }
            match self
                .api
                .replace(&self.name, &PostParams::default(), &lease)
                .await
            {
                Ok(_) => return Ok(()),
                Err(err) if is_conflict(&err) => continue,
                Err(err) => return Err(err).context("update kubernetes Lease for release"),
            }
        }
        bail!("kubernetes Lease release conflicted after {MAX_UPDATE_ATTEMPTS} attempts");
    }
}

fn is_not_found(err: &KubeError) -> bool {
    matches!(err, KubeError::Api(api) if api.code == 404 || api.reason == "NotFound")
}

fn is_conflict(err: &KubeError) -> bool {
    matches!(err, KubeError::Api(api) if api.code == 409 || api.reason == "Conflict")
}

fn new_lease_object(
    namespace: &str,
    name: &str,
    holder: &str,
    ttl: Duration,
    now_millis: i64,
) -> K8sLease {
    let mut lease = K8sLease {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        spec: Some(LeaseSpec {
            acquire_time: Some(micro_time(now_millis)),
            holder_identity: Some(holder.to_string()),
            lease_duration_seconds: Some(ttl_seconds(ttl)),
            lease_transitions: Some(1),
            renew_time: Some(micro_time(now_millis)),
            ..Default::default()
        }),
    };
    set_epoch(&mut lease, 1);
    lease
}

fn apply_acquire(
    lease: &mut K8sLease,
    holder: &str,
    ttl: Duration,
    now_millis: i64,
) -> Option<Lease> {
    let current_epoch = object_epoch(lease);
    let existing_holder = lease
        .spec
        .as_ref()
        .and_then(|s| s.holder_identity.as_deref())
        .filter(|h| !h.is_empty())
        .map(str::to_string);
    let existing_expiry = expires_at_millis(lease);
    let held_by_self_live =
        existing_holder.as_deref() == Some(holder) && existing_expiry > now_millis;

    if existing_holder.as_deref().is_some_and(|h| h != holder) && existing_expiry > now_millis {
        return None;
    }

    let next_epoch = if held_by_self_live {
        current_epoch.max(1)
    } else if current_epoch == 0 {
        1
    } else {
        current_epoch.checked_add(1)?
    };

    let spec = lease.spec.get_or_insert_with(LeaseSpec::default);
    if !held_by_self_live {
        spec.acquire_time = Some(micro_time(now_millis));
    } else if spec.acquire_time.is_none() {
        spec.acquire_time = Some(micro_time(now_millis));
    }
    spec.holder_identity = Some(holder.to_string());
    spec.lease_duration_seconds = Some(ttl_seconds(ttl));
    spec.renew_time = Some(micro_time(now_millis));
    set_epoch(lease, next_epoch);

    Some(Lease {
        holder: holder.to_string(),
        epoch: next_epoch,
        expires_at_millis: now_millis + ttl_millis(ttl),
    })
}

fn apply_renew(
    lease: &mut K8sLease,
    holder: &str,
    epoch: u64,
    ttl: Duration,
    now_millis: i64,
) -> Option<Lease> {
    let current_epoch = object_epoch(lease);
    let spec = lease.spec.as_mut()?;
    let current_holder = spec.holder_identity.as_deref()?;
    if current_holder != holder
        || current_epoch != epoch
        || expires_at_spec_millis(spec) <= now_millis
    {
        return None;
    }

    spec.lease_duration_seconds = Some(ttl_seconds(ttl));
    spec.renew_time = Some(micro_time(now_millis));
    set_epoch(lease, epoch);
    Some(Lease {
        holder: holder.to_string(),
        epoch,
        expires_at_millis: now_millis + ttl_millis(ttl),
    })
}

fn apply_release(lease: &mut K8sLease, holder: &str, epoch: u64) -> bool {
    if object_epoch(lease) != epoch {
        return false;
    }
    let Some(spec) = lease.spec.as_mut() else {
        return false;
    };
    if spec.holder_identity.as_deref() != Some(holder) {
        return false;
    }
    spec.lease_duration_seconds = Some(0);
    spec.renew_time = Some(micro_time(0));
    set_epoch(lease, epoch);
    true
}

fn lease_to_grant(lease: &K8sLease) -> Option<Lease> {
    let spec = lease.spec.as_ref()?;
    Some(Lease {
        holder: spec.holder_identity.clone()?,
        epoch: object_epoch(lease),
        expires_at_millis: expires_at_spec_millis(spec),
    })
}

fn object_epoch(lease: &K8sLease) -> u64 {
    lease
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(EPOCH_ANNOTATION))
        .and_then(|v| v.parse::<u64>().ok())
        .or_else(|| {
            lease
                .spec
                .as_ref()
                .and_then(|s| s.lease_transitions)
                .map(|v| v.max(0) as u64)
        })
        .unwrap_or(0)
}

fn set_epoch(lease: &mut K8sLease, epoch: u64) {
    let spec = lease.spec.get_or_insert_with(LeaseSpec::default);
    spec.lease_transitions = Some(epoch.min(i32::MAX as u64) as i32);
    let annotations = lease.metadata.annotations.get_or_insert_with(BTreeMap::new);
    annotations.insert(EPOCH_ANNOTATION.to_string(), epoch.to_string());
}

fn expires_at_millis(lease: &K8sLease) -> i64 {
    lease.spec.as_ref().map(expires_at_spec_millis).unwrap_or(0)
}

fn expires_at_spec_millis(spec: &LeaseSpec) -> i64 {
    let Some(renew) = &spec.renew_time else {
        return 0;
    };
    let duration_ms = i64::from(spec.lease_duration_seconds.unwrap_or_default().max(0)) * 1_000;
    renew.0.timestamp_millis() + duration_ms
}

fn ttl_seconds(ttl: Duration) -> i32 {
    let millis = ttl.as_millis().max(1);
    ((millis + 999) / 1_000).min(i32::MAX as u128) as i32
}

fn ttl_millis(ttl: Duration) -> i64 {
    i64::from(ttl_seconds(ttl)) * 1_000
}

fn micro_time(millis: i64) -> MicroTime {
    MicroTime(
        Utc.timestamp_millis_opt(millis)
            .single()
            .expect("lease clock millis must be representable"),
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use k8s_openapi::api::coordination::v1::{Lease as K8sLease, LeaseSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
    use k8s_openapi::chrono::{TimeZone, Utc};

    use super::{apply_acquire, apply_release, apply_renew, new_lease_object};

    fn mt(ms: i64) -> MicroTime {
        MicroTime(Utc.timestamp_millis_opt(ms).single().unwrap())
    }

    fn lease(holder: &str, epoch: i32, renew_ms: i64, duration_secs: i32) -> K8sLease {
        K8sLease {
            metadata: ObjectMeta {
                name: Some("bluedb-writer".to_string()),
                namespace: Some("default".to_string()),
                resource_version: Some("rv1".to_string()),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                acquire_time: Some(mt(renew_ms - 1_000)),
                holder_identity: Some(holder.to_string()),
                lease_duration_seconds: Some(duration_secs),
                lease_transitions: Some(epoch),
                renew_time: Some(mt(renew_ms)),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn new_lease_grants_epoch_one_and_sets_kubernetes_fields() {
        let grant = new_lease_object(
            "default",
            "bluedb-writer",
            "bluedb-0",
            Duration::from_secs(15),
            1_000,
        );

        let spec = grant.spec.as_ref().unwrap();
        assert_eq!(grant.metadata.name.as_deref(), Some("bluedb-writer"));
        assert_eq!(grant.metadata.namespace.as_deref(), Some("default"));
        assert_eq!(spec.holder_identity.as_deref(), Some("bluedb-0"));
        assert_eq!(spec.lease_transitions, Some(1));
        assert_eq!(spec.lease_duration_seconds, Some(15));
        assert_eq!(spec.renew_time, Some(mt(1_000)));
        assert_eq!(spec.acquire_time, Some(mt(1_000)));
    }

    #[test]
    fn acquire_denies_different_live_holder_without_mutating_object() {
        let mut current = lease("bluedb-0", 4, 1_000, 15);
        let before = current.clone();

        let grant = apply_acquire(&mut current, "bluedb-1", Duration::from_secs(15), 5_000);

        assert_eq!(grant, None);
        assert_eq!(current, before);
    }

    #[test]
    fn acquire_after_expiry_transfers_holder_and_bumps_epoch() {
        let mut current = lease("bluedb-0", 4, 1_000, 2);

        let grant =
            apply_acquire(&mut current, "bluedb-1", Duration::from_secs(15), 4_000).unwrap();

        assert_eq!(grant.holder, "bluedb-1");
        assert_eq!(grant.epoch, 5);
        assert_eq!(grant.expires_at_millis, 19_000);
        let spec = current.spec.as_ref().unwrap();
        assert_eq!(spec.holder_identity.as_deref(), Some("bluedb-1"));
        assert_eq!(spec.lease_transitions, Some(5));
        assert_eq!(spec.renew_time, Some(mt(4_000)));
        assert_eq!(spec.acquire_time, Some(mt(4_000)));
    }

    #[test]
    fn acquire_by_current_live_holder_extends_without_bumping_epoch() {
        let mut current = lease("bluedb-0", 4, 1_000, 15);

        let grant =
            apply_acquire(&mut current, "bluedb-0", Duration::from_secs(15), 5_000).unwrap();

        assert_eq!(grant.holder, "bluedb-0");
        assert_eq!(grant.epoch, 4);
        assert_eq!(grant.expires_at_millis, 20_000);
        let spec = current.spec.as_ref().unwrap();
        assert_eq!(spec.lease_transitions, Some(4));
        assert_eq!(spec.renew_time, Some(mt(5_000)));
        assert_eq!(spec.acquire_time, Some(mt(0)));
    }

    #[test]
    fn renew_requires_matching_holder_epoch_and_live_lease() {
        let mut current = lease("bluedb-0", 4, 1_000, 15);

        assert_eq!(
            apply_renew(&mut current, "bluedb-0", 3, Duration::from_secs(15), 5_000),
            None
        );
        assert_eq!(
            apply_renew(&mut current, "bluedb-1", 4, Duration::from_secs(15), 5_000),
            None
        );

        let renewed =
            apply_renew(&mut current, "bluedb-0", 4, Duration::from_secs(15), 5_000).unwrap();
        assert_eq!(renewed.epoch, 4);
        assert_eq!(renewed.expires_at_millis, 20_000);
        let spec = current.spec.as_ref().unwrap();
        assert_eq!(spec.renew_time, Some(mt(5_000)));
        assert_eq!(spec.lease_transitions, Some(4));
    }

    #[test]
    fn release_expires_lease_without_resetting_epoch() {
        let mut current = lease("bluedb-0", 4, 1_000, 15);

        assert!(apply_release(&mut current, "bluedb-0", 4));
        let spec = current.spec.as_ref().unwrap();
        assert_eq!(spec.lease_duration_seconds, Some(0));
        assert_eq!(spec.renew_time, Some(mt(0)));
        assert_eq!(spec.lease_transitions, Some(4));

        let next = apply_acquire(&mut current, "bluedb-1", Duration::from_secs(15), 2_000).unwrap();
        assert_eq!(next.epoch, 5);
    }
}
