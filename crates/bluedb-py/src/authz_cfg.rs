//! Translate Python-supplied auth config into a server `Authz` plus the default
//! bearer token (if any) that `db.headers()` should auto-inject.
use bluedb_server::authz::Authz;

/// Default superuser token value when auth is on and no token is supplied.
pub const DEFAULT_TOKEN: &str = "bluedb-test-superuser";

/// Normalized auth configuration handed down from the PyO3 layer.
#[derive(Debug, Clone)]
pub enum AuthzSpec {
    /// No enforcement (`serve(authz=False)`).
    Open,
    /// One superuser token, auto-injected by `db.headers()` (the default).
    DefaultSuperuser { token: String },
    /// Custom token -> items, each item a scope or `tenant:<name>`.
    /// Token strings and tenant names must not contain `=`, `,`, or `;`
    /// (they are re-serialized into the `tok=scope,..;..` form parsed by
    /// `Authz::parse_env`, which tokenizes on those characters).
    Map(Vec<(String, Vec<String>)>),
    /// Raw `tok=scope,..;tok2=..` string (prod parity).
    RawEnv(String),
}

impl Default for AuthzSpec {
    fn default() -> Self {
        AuthzSpec::DefaultSuperuser { token: DEFAULT_TOKEN.to_string() }
    }
}

impl AuthzSpec {
    /// Build the optional `Authz` and the default token to auto-inject.
    /// `Open` => `(None, None)`. Custom maps/strings => no auto token.
    pub fn resolve(&self) -> anyhow::Result<(Option<Authz>, Option<String>)> {
        match self {
            AuthzSpec::Open => Ok((None, None)),
            AuthzSpec::DefaultSuperuser { token } => {
                let raw = format!("{token}=superuser");
                let authz = Authz::parse_env(&raw)
                    .ok_or_else(|| anyhow::anyhow!("invalid default token '{token}'"))?;
                Ok((Some(authz), Some(token.clone())))
            }
            AuthzSpec::Map(entries) => {
                let raw = entries
                    .iter()
                    .map(|(tok, items)| format!("{tok}={}", items.join(",")))
                    .collect::<Vec<_>>()
                    .join(";");
                let authz = Authz::parse_env(&raw)
                    .ok_or_else(|| anyhow::anyhow!("invalid authz map (unknown scope or empty tenant)"))?;
                Ok((Some(authz), None))
            }
            AuthzSpec::RawEnv(raw) => {
                let authz = Authz::parse_env(raw)
                    .ok_or_else(|| anyhow::anyhow!("invalid authz string"))?;
                Ok((Some(authz), None))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bluedb_server::authz::Scope;

    #[test]
    fn default_is_superuser_with_token() {
        let (authz, token) = AuthzSpec::default().resolve().unwrap();
        let a = authz.expect("default has authz");
        assert!(a.allows(Some(DEFAULT_TOKEN), Scope::SchemaAdmin));
        assert_eq!(token.as_deref(), Some(DEFAULT_TOKEN));
    }

    #[test]
    fn open_has_no_authz_no_token() {
        let (authz, token) = AuthzSpec::Open.resolve().unwrap();
        assert!(authz.is_none());
        assert!(token.is_none());
    }

    #[test]
    fn map_builds_scoped_tokens_without_default() {
        let spec = AuthzSpec::Map(vec![
            ("reader".into(), vec!["data:read".into()]),
            ("acme".into(), vec!["data:read".into(), "tenant:acme".into()]),
        ]);
        let (authz, token) = spec.resolve().unwrap();
        let a = authz.unwrap();
        assert!(a.allows(Some("reader"), Scope::DataRead));
        assert!(!a.allows(Some("reader"), Scope::DataWrite));
        assert!(a.allows_tenant(Some("acme"), "acme"));
        assert!(!a.allows_tenant(Some("acme"), "globex"));
        assert!(token.is_none());
    }

    #[test]
    fn bad_scope_errors() {
        let spec = AuthzSpec::Map(vec![("x".into(), vec!["data:bogus".into()])]);
        assert!(spec.resolve().is_err());
    }
}
