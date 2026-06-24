//! Bearer-token → scopes authorization. In-memory static map (dev/config); the
//! identity provider is pluggable later. Enforced only when an `Authz` is
//! configured on `AppState` (open mode otherwise).
//!
//! A token may also be **bound to one or more tenants** via `tenant:<name>`
//! pseudo-scopes (e.g. `tenant:acme`). A request's `X-Bluedb-Tenant` must match
//! one of the token's bound tenants — unless the token is `superuser` (any
//! tenant). A token with **no** `tenant:` binding may reach only the default
//! tenant, preserving single-tenant deployments.
use std::collections::{HashMap, HashSet};

use bluedb_sql::DEFAULT_TENANT;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scope {
    DataRead,
    DataWrite,
    DataQuery,
    SchemaAdmin,
    Superuser,
}

impl Scope {
    pub fn parse(s: &str) -> Option<Scope> {
        Some(match s.trim() {
            "data:read" => Scope::DataRead,
            "data:write" => Scope::DataWrite,
            "data:query" => Scope::DataQuery,
            "schema:admin" => Scope::SchemaAdmin,
            "superuser" => Scope::Superuser,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct Authz {
    tokens: HashMap<String, HashSet<Scope>>,
    /// token → bound tenants (from `tenant:<name>` entries). Absent/empty ⇒ the
    /// token may reach only the default tenant.
    tenants: HashMap<String, HashSet<String>>,
}

impl Authz {
    pub fn insert(&mut self, token: String, scopes: HashSet<Scope>) {
        self.tokens.insert(token, scopes);
    }

    /// Bind `token` to the given tenants (in addition to its scopes).
    pub fn insert_tenants(&mut self, token: String, tenants: HashSet<String>) {
        self.tenants.insert(token, tenants);
    }

    /// `true` iff `token` is known and holds `required` (or `Superuser`).
    pub fn allows(&self, token: Option<&str>, required: Scope) -> bool {
        match token.and_then(|t| self.tokens.get(t)) {
            Some(scopes) => scopes.contains(&required) || scopes.contains(&Scope::Superuser),
            None => false,
        }
    }

    /// `true` iff `token` may act on `tenant`. A `superuser` token reaches any
    /// tenant; a token bound to tenants must list `tenant`; an unbound token may
    /// reach only the default tenant (single-tenant back-compat). An unknown
    /// token is denied.
    pub fn allows_tenant(&self, token: Option<&str>, tenant: &str) -> bool {
        let Some(tok) = token else { return false };
        let Some(scopes) = self.tokens.get(tok) else {
            return false;
        };
        if scopes.contains(&Scope::Superuser) {
            return true;
        }
        match self.tenants.get(tok) {
            Some(set) if !set.is_empty() => set.contains(tenant),
            _ => tenant == DEFAULT_TENANT,
        }
    }

    /// Parse `"tok=scope,scope;tok2=tenant:acme,data:read"`. Each comma-separated
    /// item is either a recognized scope or a `tenant:<name>` binding. Returns
    /// `None` if any item is unrecognized or a tenant name is empty (fail-closed).
    pub fn parse_env(raw: &str) -> Option<Authz> {
        let mut a = Authz::default();
        for entry in raw.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            let (tok, items) = entry.split_once('=')?;
            let mut scopes = HashSet::new();
            let mut tenants = HashSet::new();
            for item in items.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                if let Some(name) = item.strip_prefix("tenant:") {
                    if name.is_empty() {
                        return None;
                    }
                    tenants.insert(name.to_string());
                } else {
                    scopes.insert(Scope::parse(item)?);
                }
            }
            let tok = tok.trim().to_string();
            a.insert(tok.clone(), scopes);
            if !tenants.is_empty() {
                a.insert_tenants(tok, tenants);
            }
        }
        Some(a)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map() -> Authz {
        let mut a = Authz::default();
        a.insert("rotoken".into(), [Scope::DataRead].into_iter().collect());
        a.insert("super".into(), [Scope::Superuser].into_iter().collect());
        a
    }

    #[test]
    fn scope_parses_and_checks() {
        assert_eq!(Scope::parse("data:read"), Some(Scope::DataRead));
        assert_eq!(Scope::parse("superuser"), Some(Scope::Superuser));
        assert_eq!(Scope::parse("bogus"), None);
        let a = map();
        assert!(a.allows(Some("rotoken"), Scope::DataRead));
        assert!(a.allows(Some("super"), Scope::SchemaAdmin));
        assert!(!a.allows(Some("rotoken"), Scope::DataWrite));
        assert!(!a.allows(Some("nope"), Scope::DataRead));
        assert!(!a.allows(None, Scope::DataRead));
    }

    #[test]
    fn parse_env_builds_map() {
        let a = Authz::parse_env("tokenA=data:read,data:write;tokenB=superuser").unwrap();
        assert!(a.allows(Some("tokenA"), Scope::DataWrite));
        assert!(a.allows(Some("tokenB"), Scope::DataQuery));
        assert!(!a.allows(Some("tokenA"), Scope::Superuser));
    }

    #[test]
    fn tenant_binding_scopes_access() {
        let a = Authz::parse_env(
            "acme=data:read,tenant:acme;multi=data:read,tenant:acme,tenant:globex;\
             plain=data:read;root=superuser",
        )
        .unwrap();
        // Scopes still parse alongside tenant bindings.
        assert!(a.allows(Some("acme"), Scope::DataRead));
        // A bound token reaches only its tenants.
        assert!(a.allows_tenant(Some("acme"), "acme"));
        assert!(!a.allows_tenant(Some("acme"), "globex"));
        assert!(a.allows_tenant(Some("multi"), "globex"));
        // An unbound token reaches only the default tenant.
        assert!(a.allows_tenant(Some("plain"), "_"));
        assert!(!a.allows_tenant(Some("plain"), "acme"));
        // Superuser reaches any tenant; unknown tokens reach none.
        assert!(a.allows_tenant(Some("root"), "anything"));
        assert!(!a.allows_tenant(Some("nope"), "_"));
        // Empty tenant name is rejected (fail-closed).
        assert!(Authz::parse_env("t=tenant:").is_none());
    }
}
