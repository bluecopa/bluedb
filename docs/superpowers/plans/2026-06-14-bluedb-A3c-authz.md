# bluedb A3c — Authz scope seam Implementation Plan

> REQUIRED SUB-SKILL: superpowers:subagent-driven-development.

**Goal:** A bearer-token → scopes authorization seam over the HTTP surface, with per-route required scopes. Ships the seam + an in-memory static-token impl (dev/config). **Enforced when configured; open when not** (so existing behavior/tests are unchanged until a token map is provided — production configures one).

**Architecture:** `AppState` gains `authz: Option<Arc<Authz>>` where `Authz` maps `token → HashSet<Scope>`. A `state.authorize(&headers, Scope) -> Result<(), AppError>` helper: `None` → Ok (open mode); `Some` → extract `Authorization: Bearer <t>`, look up scopes, allow iff `required ∈ scopes` OR `Superuser ∈ scopes`, else 401 (missing/unknown token) / 403 (insufficient scope). Each handler calls `authorize(&headers, <its scope>)` first. Scope per route: GET `/tables`→`DataRead`; POST/PATCH/DELETE `/tables`→`DataWrite`; `/sql`→`DataQuery`; `/schema/*`→`SchemaAdmin`; `/admin/sql` + `/admin/{promote,demote}`→`Superuser`; `/health` + `/admin/status`→public.

**Tech Stack:** Rust, axum (`HeaderMap` extractor), std collections.

---

## Task 1: authz core (`Scope`, `Authz`, `authorize`, env parse)

**Files:** new `crates/bluedb-server/src/authz.rs`; `lib.rs` (AppState field + builder + `authorize`); `main.rs` (env parse).

- [ ] **Step 1: failing unit test.** In `crates/bluedb-server/src/authz.rs` add tests:
```rust
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
        // exact scope grants
        assert!(a.allows(Some("rotoken"), Scope::DataRead));
        // superuser grants anything
        assert!(a.allows(Some("super"), Scope::SchemaAdmin));
        // insufficient scope
        assert!(!a.allows(Some("rotoken"), Scope::DataWrite));
        // unknown / missing token
        assert!(!a.allows(Some("nope"), Scope::DataRead));
        assert!(!a.allows(None, Scope::DataRead));
    }

    #[test]
    fn parse_env_builds_map() {
        // "tokenA=data:read,data:write;tokenB=superuser"
        let a = Authz::parse_env("tokenA=data:read,data:write;tokenB=superuser").unwrap();
        assert!(a.allows(Some("tokenA"), Scope::DataWrite));
        assert!(a.allows(Some("tokenB"), Scope::DataQuery)); // superuser
        assert!(!a.allows(Some("tokenA"), Scope::Superuser));
    }
}
```
Run `cargo test -p bluedb-server --lib authz` → FAIL.

- [ ] **Step 2: implement `authz.rs`.**
```rust
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
```

- [ ] **Step 3: AppState wiring + `authorize`.** In `lib.rs`: add `authz: Option<Arc<Authz>>` to `Inner` (default `None`); add `pub fn with_authz(self, authz: Authz) -> Self`. Add the method:
```rust
    /// Authorize `required` scope from the request's `Authorization: Bearer` token.
    /// Open mode (no authz configured) always allows. Composes with `require_active`.
    pub(crate) fn authorize(&self, headers: &axum::http::HeaderMap, required: authz::Scope) -> Result<(), AppError> {
        let Some(authz) = self.inner.authz.as_ref() else { return Ok(()); };
        let token = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if authz.allows(token, required) {
            Ok(())
        } else if token.is_none() {
            Err(AppError { status: StatusCode::UNAUTHORIZED, message: "missing or malformed bearer token".into() })
        } else {
            Err(AppError { status: StatusCode::FORBIDDEN, message: "insufficient scope".into() })
        }
    }
```
Add `mod authz;` to lib.rs. `main.rs`: if `BLUEDB_AUTHZ_TOKENS` is set, `state = state.with_authz(Authz::parse_env(&v).expect("invalid BLUEDB_AUTHZ_TOKENS"))`.

- [ ] **Step 4: run** `cargo test -p bluedb-server --lib authz` (pass), `cargo build -p bluedb-server` (clean). **Commit:**
```bash
git add crates/bluedb-server/src/authz.rs crates/bluedb-server/src/lib.rs crates/bluedb-server/src/main.rs
git commit -m "feat(server): authz scope core (Scope/Authz/authorize, open when unconfigured)"
```

---

## Task 2: enforce scopes in handlers + integration tests

**Files:** `lib.rs` (handlers + `schema.rs` handlers gain a `HeaderMap` + `authorize` call); `tests/authz.rs` (new).

- [ ] **Step 1: failing integration test.** Create `crates/bluedb-server/tests/authz.rs` (reuse api.rs's harness; add a way to build an app WITH an `Authz` map — e.g. extend the test helper to take an `Option<Authz>`). Assert, with authz configured (`rotoken`=data:read, `rwtoken`=data:write, `super`=superuser):
  - `GET /tables/t` with no `Authorization` → 401.
  - `GET /tables/t` with `Bearer rotoken` → not 401/403 (reaches the handler; may be 200 or a domain error, just assert it's not an auth rejection).
  - `POST /tables/t` (write) with `Bearer rotoken` → 403 (read-only token).
  - `POST /tables/t` with `Bearer rwtoken` → not 401/403.
  - `POST /admin/sql` with `Bearer rwtoken` → 403; with `Bearer super` → not 401/403.
  - **Open mode** (no authz configured) — an existing api.rs test already proves requests work tokenless; optionally assert one here too.
  Run → FAIL (handlers don't enforce yet).

- [ ] **Step 2: wire `authorize` into every non-public handler.** Each handler gains a `headers: axum::http::HeaderMap` extractor and calls `state.authorize(&headers, Scope::X)?` as its FIRST line (before `require_active`). Mapping:
  - `select` (GET /tables) → `Scope::DataRead`
  - `insert`, `update`, `delete_rows` (POST/PATCH/DELETE /tables) → `Scope::DataWrite`
  - `exec_sql` (POST /sql) → `Scope::DataQuery`
  - `admin_sql` (POST /admin/sql) → `Scope::Superuser` (in addition to the existing enable-flag check)
  - `schema::{create_table, drop_table, create_index, drop_index}` → `Scope::SchemaAdmin`
  - `admin_promote`, `admin_demote` → `Scope::Superuser`
  - `health`, `admin_status` → NO authorize (public)
  (axum extractors: add `headers: axum::http::HeaderMap` to each handler signature — order matters only that `State`/`Path`/`Json` extractor rules are respected; `HeaderMap` can go before the body extractor.)

- [ ] **Step 3: run.** `cargo test -p bluedb-server` (ALL pass — existing tests run in open mode so they're unaffected; the new authz tests prove enforcement). `cargo build -p bluedb-server 2>&1 | grep -i warn` (clean).

- [ ] **Step 4: doc + commit.** `//!` note in `main.rs`: `BLUEDB_AUTHZ_TOKENS` format + that authz is enforced only when set (recommend setting it in production); list the per-route scopes. Commit:
```bash
git add crates/bluedb-server
git commit -m "feat(server): enforce per-route authz scopes (bearer token); open when unconfigured"
```

## Self-Review
- Spec A authz coverage: scope seam (`data:read`/`data:write`/`data:query`/`schema:admin`/`superuser`) ✓; bearer-token static-map dev impl ✓; per-route enforcement ✓; composes with `require_active` ✓; identity provider out of scope ✓. Deviation: **open-when-unconfigured** (vs always-required) to land incrementally without breaking every test — documented; production sets `BLUEDB_AUTHZ_TOKENS`.
- Superuser is a global grant (satisfies any required scope) — intended.
- `parse_env` is fail-closed on unknown scope tokens.
