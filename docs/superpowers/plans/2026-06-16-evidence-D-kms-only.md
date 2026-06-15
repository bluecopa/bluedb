# Evidence D — KMS-only signing (no in-process key management)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** bluedb must not manage private key material in production. Drop `local` as a *config* signing mode — production signing is `off | vault` (Vault Transit; future KMS backends behind the same enum). `LocalSigner` stays only as a **test fixture**, reachable solely via a `#[doc(hidden)] pub` test seam (never from config). Also surface the **Vault key version** that signed each STH (`key_version`), the rotation hook.

**Decision (settled):** keep a test-only local signer (user's choice) — so `p256` + `LocalSigner` stay compiled but are unreachable from any config path.

**Branch:** `feat/evidence-kms-only` (off merged `dev` @ `3a22fae`). Commit-only, NEVER push. Co-author trailer `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. NO `cargo fmt`. Tests directly `2>&1` (no tail/grep pipes). lowercase "bluedb"/"bluecopa".

## Current state (exact touchpoints)
- `crates/bluedb-server/src/lib.rs`: `AppState.signer: Option<Arc<EvidenceSigner>>` (l.242); `pub(crate) fn with_signer(Option<Arc<EvidenceSigner>>)` (l.289); `pub fn with_evidence_signing(self) -> Result<Self, AppError>` (l.304) reads `BLUEDB_EVIDENCE_SIGNING=off|local|vault` — the `local` arm loads `BLUEDB_EVIDENCE_SIGNING_KEY_PEM_FILE` or falls back to `LocalSigner::ephemeral()`; `pub(crate) fn signer(&self)` (l.352).
- `crates/bluedb-server/src/signer.rs`: `enum EvidenceSigner { Local(LocalSigner), Vault(VaultTransitSigner) }`; `async fn sign(&self,payload)->Result<Vec<u8>>`, `fn key_id()->String`, `async fn public_key_pem()->Result<String>`, `fn alg()`; `LocalSigner { from_pem (allow dead_code), ephemeral, sign, public_key_pem }`; `pub fn verify_es256_der`.
- `crates/bluedb-server/src/signer/vault.rs`: `VaultTransitSigner`; `sign` (parses `vault:vN:<b64-DER>` via `parse_signature`), `public_key_pem` (via `parse_public_key`, reads `latest_version`).
- `crates/bluedb-server/src/evidence_api.rs`: `digest_signed`, `signing_key` handlers.
- `crates/bluedb-server/tests/signing.rs`: e2e currently sets `BLUEDB_EVIDENCE_SIGNING=local` + `with_evidence_signing()`.

---

## Task D1: config = off|vault; add the test-only local seam

**Files:** `crates/bluedb-server/src/lib.rs`, `crates/bluedb-server/src/signer.rs`

- [ ] **Step 1:** Rewrite `with_evidence_signing` so the only modes are `off` and `vault` (no `local`, no PEM env, no ephemeral fallback):
```rust
    /// Build the evidence digest signer from the environment. Production never
    /// holds key material: signing is `off` (default) or `vault` (HashiCorp
    /// Vault Transit — the private key stays in the KMS). The in-process
    /// `LocalSigner` is a TEST fixture only ([`with_local_signer_for_tests`]),
    /// never selectable from config.
    ///
    /// `BLUEDB_EVIDENCE_SIGNING` = `off` (default) | `vault`. For `vault`:
    /// `VAULT_ADDR`, `VAULT_TOKEN`, `BLUEDB_EVIDENCE_VAULT_MOUNT` (default
    /// `transit`), `BLUEDB_EVIDENCE_VAULT_KEY` (transit `ecdsa-p256` key).
    pub fn with_evidence_signing(self) -> Result<Self, AppError> {
        use signer::EvidenceSigner;
        let mode = std::env::var("BLUEDB_EVIDENCE_SIGNING").unwrap_or_else(|_| "off".to_string());
        let built: Option<EvidenceSigner> = match mode.as_str() {
            "off" => None,
            "vault" => {
                let addr = req_env("VAULT_ADDR")?;
                let token = req_env("VAULT_TOKEN")?;
                let mount = std::env::var("BLUEDB_EVIDENCE_VAULT_MOUNT").unwrap_or_else(|_| "transit".to_string());
                let key = req_env("BLUEDB_EVIDENCE_VAULT_KEY")?;
                Some(EvidenceSigner::Vault(signer::vault::VaultTransitSigner::new(addr, token, mount, key)?))
            }
            other => {
                return Err(AppError::internal(format!(
                    "BLUEDB_EVIDENCE_SIGNING: unknown mode '{other}' (want off|vault)"
                )));
            }
        };
        Ok(self.with_signer(built.map(Arc::new)))
    }
```
Add the small `req_env` helper near it (or inline): a missing required var → `AppError::internal("BLUEDB_EVIDENCE_SIGNING=vault requires <VAR>")`.
```rust
fn req_env(var: &str) -> Result<String, AppError> {
    std::env::var(var).map_err(|_| AppError::internal(format!("BLUEDB_EVIDENCE_SIGNING=vault requires {var}")))
}
```
- [ ] **Step 2:** Add the `#[doc(hidden)] pub` test seam (so integration tests in the separate `tests/` crate can inject a local signer; production never calls it):
```rust
    /// TEST ONLY: attach an in-process `LocalSigner` (ephemeral key). Not a
    /// production path — production signing is configured via
    /// [`with_evidence_signing`] (`off`/`vault`). Hidden from docs.
    #[doc(hidden)]
    pub fn with_local_signer_for_tests(self) -> Self {
        let s = signer::EvidenceSigner::Local(signer::LocalSigner::ephemeral());
        self.with_signer(Some(Arc::new(s)))
    }
```
- [ ] **Step 3:** In `signer.rs`, remove `LocalSigner::from_pem` (its only caller was the deleted `local`-PEM config arm). Keep `ephemeral` (used by the test seam), `sign`, `public_key_pem`. `LocalSigner`/`EvidenceSigner` stay `pub(crate)`; `verify_es256_der` stays `pub`.
- [ ] **Step 4:** `cargo build -p bluedb-server 2>&1` compiles. `cargo test -p bluedb-server signer 2>&1` — unit tests pass (they construct `LocalSigner::ephemeral()` directly — fine).
- [ ] **Step 5:** Commit `refactor(evidence): signing config is off|vault only (local is test-only)`.

## Task D2: surface the signing key version (`key_version`)

**Files:** `crates/bluedb-server/src/signer.rs`, `crates/bluedb-server/src/signer/vault.rs`, `crates/bluedb-server/src/evidence_api.rs`

- [ ] **Step 1:** Change `sign` to also return the key **version**, and `public_key_pem` to return `(version, pem)`:
  - `signer.rs` `EvidenceSigner`:
    - `async fn sign(&self, payload) -> Result<(u64, Vec<u8>), AppError>` (version, DER).
    - `async fn public_key(&self) -> Result<(u64, String), AppError>` (version, SPKI PEM) — rename from `public_key_pem`.
  - `LocalSigner`: `sign` returns `(1, der)`; add `fn public_key(&self) -> (1, pem)` (the local fixture has no real versioning — version `1`).
  - `vault.rs` `VaultTransitSigner`:
    - `parse_signature(&Value) -> Result<(u64, Vec<u8>), AppError>`: the `vault:v<N>:<b64>` string already encodes N — parse it (`split(':')` → `["vault","vN","<b64>"]`; strip the `v` from `vN`). Return `(N, der)`.
    - `sign` returns `(N, der)`.
    - `parse_public_key` already reads `data.latest_version` — return `(latest_version as u64, pem)`; `public_key` returns that.
- [ ] **Step 2:** `evidence_api.rs`:
  - `digest_signed`: `let (key_version, sig) = signer.sign(&payload).await?;` and add `"key_version": key_version` to the JSON.
  - `signing_key`: `let (key_version, pem) = signer.public_key().await?;` and return `"key_version": key_version` alongside `key_id`, `alg`, `public_key`.
- [ ] **Step 3:** Update the `signer.rs` + `vault.rs` unit tests for the new return shapes (the roundtrip test now binds `(_v, sig)`; the canned-JSON `parse_signature` test asserts the version too, e.g. `vault:v1:…` → version `1`).
- [ ] **Step 4:** `cargo test -p bluedb-server signer 2>&1` — PASS. `cargo build -p bluedb-server 2>&1`.
- [ ] **Step 5:** Commit `feat(evidence): surface signing key version (key_version) in signed digest + signing-key`.

## Task D3: update the e2e to the test seam

**Files:** `crates/bluedb-server/tests/signing.rs`

- [ ] **Step 1:** Replace the env-based local enable with the seam. In `promoted_signing()`, drop the `std::env::set_var("BLUEDB_EVIDENCE_SIGNING","local")` + `remove_var(...)` + `with_evidence_signing()` and instead:
```rust
    let state = AppState::new(store, "bluedb", writer("sign-node")).with_local_signer_for_tests();
    state.promote().await.expect("promote");
    build_app(state)
```
`promoted_no_signing()` stays as-is (no signer attached → 501).
- [ ] **Step 2:** Assert the new field: in `signed_digest_verifies_end_to_end_and_rejects_replay`, after fetching the signed digest, assert `sa["key_version"].is_i64()` (the local fixture reports `1`); and `key["key_version"]` present on the signing-key response. Keep the existing end-to-end verify (`verify_es256_der` over the rebuilt `sth_payload`) and the cross-chain-replay rejection + `signing_off_returns_501` exactly.
- [ ] **Step 3:** `cargo test -p bluedb-server --test signing 2>&1` — PASS.
- [ ] **Step 4:** Commit `test(evidence): signing e2e uses the test-only local signer seam`.

## Task D4: docs + workspace green

**Files:** `docs/evidence/chains.md`, `ROADMAP.md`

- [ ] **Step 1:** `docs/evidence/chains.md` — **Digest signing** section: change the config table to `off | vault` only; **remove** the `local` row and the `BLUEDB_EVIDENCE_SIGNING_KEY_PEM_FILE` row; state production signing is **KMS-only (Vault Transit)** and bluedb holds no private key material (only a Vault token + key name); note an in-process signer exists solely as a **test fixture**, not a config option. Add that the signed-digest / signing-key responses carry a `key_version` (the Vault key version that signed), so signatures are attributable across rotations. Trust-model wording: bluedb never holds keys; rotation is the KMS's responsibility.
- [ ] **Step 2:** `ROADMAP.md` evidence section — the hardening bullet's signing clause: note signing is **KMS-only (Vault Transit; no in-process keys)** and key rotation is delegated to the KMS (`key_version` surfaced).
- [ ] **Step 3:** `cargo test --workspace 2>&1` (green), `cargo clippy --workspace --all-targets 2>&1` (no NEW warnings — watch for now-unused imports after removing `from_pem`/the local config arm), and `mkdocs build --strict -d /tmp/kms-doccheck 2>&1` (exit 0, no anchor/warn; then remove the dir).
- [ ] **Step 4:** Commit `docs(evidence): KMS-only signing — config, trust model, roadmap`.

## Acceptance
- `BLUEDB_EVIDENCE_SIGNING` accepts only `off`/`vault`; `vault` without required env errors clearly; there is **no config path that creates an in-process key**.
- `LocalSigner` is reachable only via `with_local_signer_for_tests` (the e2e uses it); the full sign→verify-through-HTTP + cross-chain-replay + off→501 e2e still passes.
- Signed-digest and signing-key responses include `key_version`.
- `cargo test --workspace` green; no new clippy warnings; `mkdocs build --strict` clean; chains.md/ROADMAP reflect KMS-only.
