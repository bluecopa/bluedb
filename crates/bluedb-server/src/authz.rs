//! Bearer-token → scopes authorization. In-memory static map (dev/config); the
//! identity provider is pluggable later. Enforced only when an `Authz` is
//! configured on `AppState` (open mode otherwise).
use std::collections::{HashMap, HashSet};

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
}

impl Authz {
    pub fn insert(&mut self, token: String, scopes: HashSet<Scope>) {
        self.tokens.insert(token, scopes);
    }

    /// `true` iff `token` is known and holds `required` (or `Superuser`).
    pub fn allows(&self, token: Option<&str>, required: Scope) -> bool {
        match token.and_then(|t| self.tokens.get(t)) {
            Some(scopes) => scopes.contains(&required) || scopes.contains(&Scope::Superuser),
            None => false,
        }
    }

    /// Parse `"tok=scope,scope;tok2=scope"`. Returns None if any scope token is
    /// unrecognized (fail-closed config).
    pub fn parse_env(raw: &str) -> Option<Authz> {
        let mut a = Authz::default();
        for entry in raw.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            let (tok, scopes) = entry.split_once('=')?;
            let set: Option<HashSet<Scope>> = scopes.split(',').map(Scope::parse).collect();
            a.insert(tok.trim().to_string(), set?);
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
}
