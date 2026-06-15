# Evidence C — KMS-backed digest signing (Signed Tree Heads)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development. Steps use `- [ ]`.

**Goal:** Close the v1 "unsigned digests" trust-model limitation (level 2: non-repudiation). bluedb signs a verified chain's digest as a **Signed Tree Head (STH)** — `{tenant, chain, size, root_hash, timestamp}` over ECDSA P-256 (ES256) — with the private key held in a **KMS (HashiCorp Vault Transit)**, never on the server. Consumers fetch the public key and verify. **Opt-in**, off by default.

**Supersedes** the "digest signing" non-goal in `docs/superpowers/specs/2026-06-16-evidence-hardening-design.md`. Equivocation/anchoring (level 3) remains out of scope.

**Branch:** `feat/evidence-hardening` (stacked on the O(log N) + parallel-frontier work). Commit-only, NEVER push. Co-author trailer `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`. NO `cargo fmt`. Run tests directly `2>&1` (no tail/grep pipes). lowercase "bluedb"/"bluecopa".

## Design / factoring

- **`bluedb-evidence`** exposes only the canonical STH byte framing (`pub fn sth_payload`). No new deps. This is also the consumer's verification contract (a Rust consumer rebuilds the same bytes and verifies).
- **`bluedb-server`** owns signing (KMS/keys/HTTP are server concerns): a `signer` module with an `EvidenceSigner` enum (`Local` for dev/tests, `Vault` for production), `p256` for the local signer + the e2e verify, `reqwest` for Vault. `AppState` holds `Option<Arc<EvidenceSigner>>` (None ⇒ signing off).
- **Endpoints** (in `evidence_api.rs`): `GET /evidence/{chain}/digest/signed` and `GET /evidence/signing-key`, both `data:read` + tenant.
- **ECDSA signatures are ASN.1 DER**, ES256 (SHA-256). The local signer uses RFC 6979 deterministic nonces (`p256` default); Vault uses random nonces — both verify identically. Never assert a fixed signature byte value; assert via *verification*.

## Existing facts
- `Evidence::digest(chain) -> Digest { size: i64, root: [u8;32] }` (already O(log N)). `require_verified` rejects plain chains with `NotVerified`.
- `evidence_api.rs`: handlers do `state.tenant(&headers)?` + `state.authorize(&headers, Scope::DataRead)?`; `map_evidence_err`; `hex32(&[u8;32])` helper already exists.
- `bluedb-server/Cargo.toml` already has `reqwest = { version = "0.12", default-features = false, features = ["http2"] }`.
- `AppState`/`build_app` in `bluedb-server/src/lib.rs`; `AppError::{bad_request,internal,service_unavailable,not_found}` exist; add a `not_implemented`/use `service_unavailable`/`501` as needed.

---

## Task C1: `bluedb_evidence::sth_payload` (canonical STH framing)

**Files:** new `crates/bluedb-evidence/src/sth.rs`; `crates/bluedb-evidence/src/lib.rs`

- [ ] **Step 1:** Create `sth.rs`:
```rust
//! Canonical byte framing for a Signed Tree Head (STH). The signer signs these
//! bytes; a consumer rebuilds them identically and verifies the signature with
//! the published public key. Domain-separated and length-delimited so no two
//! distinct (tenant, chain, size, root, ts) tuples ever frame to the same bytes.

const STH_DOMAIN: &[u8] = b"bluedb-evidence-sth-v1";

/// Canonical STH bytes for `(tenant, chain, size, root_hash, timestamp_ms)`.
/// Layout: frame(domain) ‖ frame(tenant) ‖ frame(chain) ‖ size:i64-be ‖
/// root_hash[32] ‖ timestamp_ms:i64-be, where frame(x) = (len:u64-be) ‖ x.
pub fn sth_payload(tenant: &str, chain: &str, size: i64, root_hash: &[u8; 32], timestamp_ms: i64) -> Vec<u8> {
    let mut out = Vec::new();
    for field in [STH_DOMAIN, tenant.as_bytes(), chain.as_bytes()] {
        out.extend_from_slice(&(field.len() as u64).to_be_bytes());
        out.extend_from_slice(field);
    }
    out.extend_from_slice(&size.to_be_bytes());
    out.extend_from_slice(root_hash);
    out.extend_from_slice(&timestamp_ms.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sth_payload_is_deterministic_and_field_sensitive() {
        let r = [9u8; 32];
        let base = sth_payload("acme", "c", 3, &r, 100);
        assert_eq!(base, sth_payload("acme", "c", 3, &r, 100));
        // Cross-chain / cross-tenant / size / root / ts all change the bytes.
        assert_ne!(base, sth_payload("acme", "c2", 3, &r, 100));
        assert_ne!(base, sth_payload("globex", "c", 3, &r, 100));
        assert_ne!(base, sth_payload("acme", "c", 4, &r, 100));
        assert_ne!(base, sth_payload("acme", "c", 3, &[1u8; 32], 100));
        assert_ne!(base, sth_payload("acme", "c", 3, &r, 101));
        // Length-prefixing prevents (tenant="ac",chain="me") == (tenant="a",chain="cme") collisions.
        assert_ne!(sth_payload("ac", "me", 1, &r, 1), sth_payload("a", "cme", 1, &r, 1));
    }
}
```
- [ ] **Step 2:** `lib.rs`: `mod sth;` + `pub use sth::sth_payload;`.
- [ ] **Step 3:** `cargo test -p bluedb-evidence sth 2>&1` — PASS.
- [ ] **Step 4:** Commit `feat(evidence): canonical Signed Tree Head payload framing`.

## Task C2: server `signer` module + `LocalSigner` (p256)

**Files:** new `crates/bluedb-server/src/signer.rs`; `crates/bluedb-server/src/lib.rs`; `crates/bluedb-server/Cargo.toml`

- [ ] **Step 1:** Add `p256` to `bluedb-server/Cargo.toml` deps: `p256 = { version = "0.13", features = ["ecdsa", "pem"] }` (the `pem`/`pkcs8` feature set is what exposes SPKI PEM encode/decode — adjust features until `VerifyingKey::to_public_key_pem` / `from_public_key_pem` and `SigningKey` resolve). Add `mod signer;` in `lib.rs`.
- [ ] **Step 2:** Write `signer.rs` with the enum + `LocalSigner` (Vault variant lands in C3):
```rust
//! Digest signing for evidence chains. The private key lives in a KMS; the
//! server only invokes a signing operation. ES256 (ECDSA P-256, ASN.1-DER sigs).
//! `EvidenceSigner` dispatches to a backend; `Local` is in-process (dev/tests),
//! `Vault` (C3) calls HashiCorp Vault Transit.

use std::sync::Arc;

use p256::ecdsa::{signature::Signer as _, Signature, SigningKey, VerifyingKey};
use p256::pkcs8::{DecodePublicKey, EncodePublicKey};

use crate::AppError;

pub(crate) enum EvidenceSigner {
    Local(LocalSigner),
    Vault(crate::signer::vault::VaultTransitSigner),
}

impl EvidenceSigner {
    /// Sign `payload` → ASN.1-DER ES256 signature bytes.
    pub(crate) async fn sign(&self, payload: &[u8]) -> Result<Vec<u8>, AppError> {
        match self {
            EvidenceSigner::Local(s) => s.sign(payload),
            EvidenceSigner::Vault(s) => s.sign(payload).await,
        }
    }
    /// Opaque key identifier (for rotation / audit).
    pub(crate) fn key_id(&self) -> String {
        match self {
            EvidenceSigner::Local(s) => s.key_id.clone(),
            EvidenceSigner::Vault(s) => s.key_id(),
        }
    }
    /// SPKI public key, PEM-encoded, for consumer verification.
    pub(crate) async fn public_key_pem(&self) -> Result<String, AppError> {
        match self {
            EvidenceSigner::Local(s) => Ok(s.public_key_pem()),
            EvidenceSigner::Vault(s) => s.public_key_pem().await,
        }
    }
    pub(crate) fn alg(&self) -> &'static str { "ES256" }
}

/// In-process ECDSA P-256 signer. Dev / tests / air-gapped self-host only — NOT
/// production trust (the key sits in process memory).
pub(crate) struct LocalSigner {
    key: SigningKey,
    pub(crate) key_id: String,
}

impl LocalSigner {
    /// Load from a PKCS#8 PEM private key file, or generate an ephemeral key.
    pub(crate) fn from_pem(pem: &str, key_id: String) -> Result<Self, AppError> {
        use p256::pkcs8::DecodePrivateKey;
        let key = SigningKey::from_pkcs8_pem(pem)
            .map_err(|e| AppError::internal(format!("invalid local signing key: {e}")))?;
        Ok(Self { key, key_id })
    }
    /// Generate an ephemeral key (dev only — not stable across restarts).
    pub(crate) fn ephemeral() -> Self {
        // Deterministic-enough for a single process; uses OS RNG.
        let key = SigningKey::random(&mut rand_core_os_rng());
        Self { key, key_id: "local-ephemeral".to_string() }
    }
    fn sign(&self, payload: &[u8]) -> Result<Vec<u8>, AppError> {
        let sig: Signature = self.key.sign(payload); // RFC 6979 deterministic; SHA-256
        Ok(sig.to_der().as_bytes().to_vec())
    }
    fn public_key_pem(&self) -> String {
        let vk: VerifyingKey = *self.key.verifying_key();
        vk.to_public_key_pem(Default::default()).expect("spki pem")
    }
}

// p256 0.13 re-exports an OS RNG via `rand_core`; if `SigningKey::random` needs a
// specific RNG type, use `p256::elliptic_curve::rand_core::OsRng`. Adjust import
// to whatever compiles (the goal: a CSPRNG-seeded ephemeral key).
fn rand_core_os_rng() -> impl p256::elliptic_curve::rand_core::CryptoRngCore {
    p256::elliptic_curve::rand_core::OsRng
}

/// Verify helper (used by tests + available to Rust consumers): ES256-DER over
/// `payload` against an SPKI-PEM public key.
pub(crate) fn verify_es256_der(public_key_pem: &str, payload: &[u8], der_sig: &[u8]) -> bool {
    use p256::ecdsa::signature::Verifier;
    let Ok(vk) = VerifyingKey::from_public_key_pem(public_key_pem) else { return false };
    let Ok(sig) = Signature::from_der(der_sig) else { return false };
    vk.verify(payload, &sig).is_ok()
}

pub(crate) mod vault; // C3
```
> **Implementer note:** the exact `p256` RNG/feature spelling varies by version — adjust imports/features until it compiles (the contract is: ES256 sign producing DER, SPKI-PEM public key out, DER verify). Create an empty `signer/vault.rs` stub now (`pub(crate) struct VaultTransitSigner;` with `unimplemented!()` bodies) so C2 compiles; C3 fills it. OR gate the `Vault` variant behind C3 — but the enum referencing `vault::VaultTransitSigner` needs the stub to compile, so create the stub.
- [ ] **Step 3:** Unit tests in `signer.rs`:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bluedb_evidence::sth_payload;

    #[test]
    fn local_signer_sign_then_verify_roundtrips() {
        let s = LocalSigner::ephemeral();
        let pem = s.public_key_pem();
        let payload = sth_payload("acme", "c", 5, &[7u8; 32], 1_700_000_000_000);
        let sig = s.sign(&payload).unwrap();
        assert!(verify_es256_der(&pem, &payload, &sig));
        // Tamper: a different payload must NOT verify against the same sig.
        let other = sth_payload("acme", "c", 6, &[7u8; 32], 1_700_000_000_000);
        assert!(!verify_es256_der(&pem, &other, &sig));
        // A different key's pub must NOT verify.
        let pem2 = LocalSigner::ephemeral().public_key_pem();
        assert!(!verify_es256_der(&pem2, &payload, &sig));
    }
}
```
- [ ] **Step 4:** `cargo test -p bluedb-server signer 2>&1` — PASS. `cargo build -p bluedb-server 2>&1` — compiles (with the vault stub).
- [ ] **Step 5:** Commit `feat(evidence): server signer module + local ES256 signer`.

## Task C3: `VaultTransitSigner` (HashiCorp Vault Transit)

**Files:** `crates/bluedb-server/src/signer/vault.rs`; `crates/bluedb-server/Cargo.toml`

- [ ] **Step 1:** Extend `bluedb-server`'s reqwest features for HTTPS+JSON: change the dep to `reqwest = { version = "0.12", default-features = false, features = ["http2", "json", "rustls-tls"] }` (additive — keeps existing usage working).
- [ ] **Step 2:** Implement `vault.rs`:
```rust
//! HashiCorp Vault Transit signing backend. The signing key is a Transit key of
//! type `ecdsa-p256`; the server holds only a Vault token + key name. Signing:
//! POST {addr}/v1/{mount}/sign/{key} with `input` = base64(payload). Public key:
//! GET {addr}/v1/{mount}/keys/{key} → latest version's SPKI PEM.

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use serde_json::Value;

use crate::AppError;

pub(crate) struct VaultTransitSigner {
    client: reqwest::Client,
    addr: String,   // e.g. https://vault.internal:8200
    token: String,
    mount: String,  // default "transit"
    key: String,    // transit key name
}

impl VaultTransitSigner {
    pub(crate) fn new(addr: String, token: String, mount: String, key: String) -> Result<Self, AppError> {
        let client = reqwest::Client::builder()
            .build()
            .map_err(|e| AppError::internal(format!("vault http client: {e}")))?;
        Ok(Self { client, addr, token, mount, key })
    }

    pub(crate) fn key_id(&self) -> String {
        format!("vault:{}:{}", self.mount, self.key)
    }

    pub(crate) async fn sign(&self, payload: &[u8]) -> Result<Vec<u8>, AppError> {
        let url = format!("{}/v1/{}/sign/{}", self.addr, self.mount, self.key);
        // ecdsa-p256 transit key: server hashes sha2-256 unless prehashed.
        let body = serde_json::json!({
            "input": B64.encode(payload),
            "hash_algorithm": "sha2-256",
            "signature_algorithm": "pkcs1v15", // ignored for ecdsa; marshaling below matters
            "marshaling_algorithm": "asn1"
        });
        let resp = self.client.post(&url)
            .header("X-Vault-Token", &self.token)
            .json(&body)
            .send().await
            .map_err(|e| AppError::internal(format!("vault sign request: {e}")))?;
        let v: Value = parse_ok(resp).await?;
        let sig = v.pointer("/data/signature").and_then(|s| s.as_str())
            .ok_or_else(|| AppError::internal("vault sign: missing data.signature"))?;
        // Format: "vault:v<version>:<base64(DER)>". Strip the prefix, decode.
        let b64 = sig.rsplit(':').next().unwrap_or_default();
        B64.decode(b64).map_err(|e| AppError::internal(format!("vault sig decode: {e}")))
    }

    pub(crate) async fn public_key_pem(&self) -> Result<String, AppError> {
        let url = format!("{}/v1/{}/keys/{}", self.addr, self.mount, self.key);
        let resp = self.client.get(&url).header("X-Vault-Token", &self.token)
            .send().await.map_err(|e| AppError::internal(format!("vault keys request: {e}")))?;
        let v: Value = parse_ok(resp).await?;
        // data.keys."<latest_version>".public_key (SPKI PEM). latest_version at data.latest_version.
        let latest = v.pointer("/data/latest_version").and_then(|x| x.as_i64())
            .ok_or_else(|| AppError::internal("vault keys: missing latest_version"))?;
        let pem = v.pointer(&format!("/data/keys/{latest}/public_key")).and_then(|s| s.as_str())
            .ok_or_else(|| AppError::internal("vault keys: missing public_key"))?;
        Ok(pem.to_string())
    }
}

async fn parse_ok(resp: reqwest::Response) -> Result<Value, AppError> {
    let status = resp.status();
    let body = resp.text().await.map_err(|e| AppError::internal(format!("vault read body: {e}")))?;
    if !status.is_success() {
        return Err(AppError::internal(format!("vault {status}: {body}")));
    }
    serde_json::from_str(&body).map_err(|e| AppError::internal(format!("vault json: {e}")))
}
```
> **Verify the exact transit field names against the Vault docs** as you implement (e.g. `marshaling_algorithm: "asn1"` so the signature is DER to match the local signer + p256 verify; the `signature_algorithm` field is RSA-only and ignored for ecdsa — drop it if Vault rejects it). The signature string is `vault:vN:<b64>`; for `asn1` marshaling the decoded bytes are ASN.1 DER (what `verify_es256_der` expects).
- [ ] **Step 3:** Unit tests with **canned Vault JSON** (no live Vault) — test the parse logic only:
```rust
#[cfg(test)]
mod tests {
    use serde_json::Value;
    // Re-implement the two pure parse steps as free fns OR factor them out of the
    // request methods so they can be unit-tested without HTTP. e.g. extract:
    //   fn parse_signature(v: &Value) -> Result<Vec<u8>, AppError>
    //   fn parse_public_key(v: &Value) -> Result<String, AppError>
    // and test them against canned responses:
    #[test]
    fn parses_transit_sign_and_keys_responses() {
        let sign: Value = serde_json::from_str(r#"{"data":{"signature":"vault:v1:MEUCIQ=="}}"#).unwrap();
        // assert parse_signature(&sign) == base64-decode("MEUCIQ==")
        let keys: Value = serde_json::from_str(r#"{"data":{"latest_version":2,"keys":{"2":{"public_key":"-----BEGIN PUBLIC KEY-----\nABC\n-----END PUBLIC KEY-----"}}}}"#).unwrap();
        // assert parse_public_key(&keys) == that PEM string
    }
}
```
Refactor `sign`/`public_key_pem` to call small pure `parse_signature(&Value)` / `parse_public_key(&Value)` helpers so the tests above exercise the real parse code.
- [ ] **Step 4:** `cargo test -p bluedb-server signer 2>&1` — PASS. `cargo build -p bluedb-server 2>&1`.
- [ ] **Step 5:** Commit `feat(evidence): Vault Transit signing backend`.

## Task C4: config wiring + HTTP endpoints

**Files:** `crates/bluedb-server/src/lib.rs`; `crates/bluedb-server/src/evidence_api.rs`

- [ ] **Step 1:** Build the signer from env at startup and store `Option<Arc<EvidenceSigner>>` on `AppState`. Add a helper that reads:
  - `BLUEDB_EVIDENCE_SIGNING` = `off` (default) | `local` | `vault`.
  - local: `BLUEDB_EVIDENCE_SIGNING_KEY_PEM_FILE` (PKCS#8 PEM path); if unset, `LocalSigner::ephemeral()` with a `WARN` log (dev only).
  - vault: `VAULT_ADDR`, `VAULT_TOKEN`, `BLUEDB_EVIDENCE_VAULT_MOUNT` (default `transit`), `BLUEDB_EVIDENCE_VAULT_KEY`.
  Wire it where `AppState` is constructed (mirror how other config like the lakehouse is read). Add `pub(crate) fn signer(&self) -> Option<Arc<EvidenceSigner>>`.
- [ ] **Step 2:** Handlers in `evidence_api.rs`:
```rust
/// `GET /evidence/{chain}/digest/signed` — ES256-signed STH for a verified chain.
pub async fn digest_signed(
    State(state): State<AppState>, headers: HeaderMap, Path(chain): Path<String>,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let tenant = state.tenant(&headers)?;
    let signer = state.signer().ok_or_else(|| AppError::not_implemented("digest signing is not enabled (set BLUEDB_EVIDENCE_SIGNING)"))?;
    let d = state.evidence(&tenant).await?.digest(&chain).await.map_err(map_evidence_err)?;
    let ts = now_millis();
    let payload = bluedb_evidence::sth_payload(&tenant, &chain, d.size, &d.root, ts);
    let sig = signer.sign(&payload).await?;
    Ok(Json(json!({
        "size": d.size,
        "root_hash": hex32(&d.root),
        "timestamp": ts,
        "alg": signer.alg(),
        "key_id": signer.key_id(),
        "signature": B64.encode(&sig),
    })))
}

/// `GET /evidence/signing-key` — the SPKI-PEM public key for verifying STHs.
pub async fn signing_key(
    State(state): State<AppState>, headers: HeaderMap,
) -> Result<Json<Value>, AppError> {
    state.authorize(&headers, Scope::DataRead)?;
    let _ = state.tenant(&headers)?; // tenant-scoped auth, though the key is shared
    let signer = state.signer().ok_or_else(|| AppError::not_implemented("digest signing is not enabled"))?;
    Ok(Json(json!({
        "key_id": signer.key_id(),
        "alg": signer.alg(),
        "public_key": signer.public_key_pem().await?,
    })))
}

fn now_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}
```
Add `AppError::not_implemented(msg)` (status `501`) in `lib.rs` if absent (mirror the other constructors). `B64` is already imported in `evidence_api.rs`.
- [ ] **Step 3:** Routes in `build_app`: `.route("/evidence/{chain}/digest/signed", get(evidence_api::digest_signed))` and `.route("/evidence/signing-key", get(evidence_api::signing_key))`.
- [ ] **Step 4:** `cargo build -p bluedb-server 2>&1` — compiles.
- [ ] **Step 5:** Commit `feat(evidence): signed-digest + signing-key HTTP endpoints + config`.

## Task C5: e2e + docs + green

**Files:** `crates/bluedb-server/tests/evidence.rs` (or a new `tests/signing.rs`); `docs/evidence/chains.md`

- [ ] **Step 1:** e2e with the **local** signer (set the test app to `BLUEDB_EVIDENCE_SIGNING=local` / construct `AppState` with a `LocalSigner`). Reuse the evidence test harness. Cover:
  - append a few entries to a verified chain; `GET /evidence/{chain}/digest/signed` → 200 with `size`, `root_hash` (64-hex), `timestamp`, `alg:"ES256"`, `key_id`, `signature`.
  - fetch `GET /evidence/signing-key` → `public_key` PEM; **verify the signature** end-to-end: rebuild `bluedb_evidence::sth_payload(tenant, chain, size, root, timestamp)`, base64-decode `signature`, assert `crate::signer::verify_es256_der(pem, &payload, &sig)` is true.
  - **cross-chain replay rejected:** the signature for chain A must NOT verify against chain B's STH bytes.
  - signing **off** (a second app with no signer) → `GET .../digest/signed` returns `501`.
  - (auth) without `data:read` → 403, mirroring the existing evidence auth tests if that harness exists; else assert the happy path under open mode.
  > To call `verify_es256_der` from the integration test it must be reachable — either make it `pub` in `signer.rs` or add a thin `#[cfg(test)]`-friendly path. Simplest: mark `verify_es256_der` `pub` (it's a legitimate verification helper for Rust consumers).
- [ ] **Step 2:** Update `docs/evidence/chains.md` **Trust model** + **Limitations**: digests can now be **signed** (ES256 Signed Tree Heads, key in a KMS — HashiCorp Vault Transit, or a local key for dev) via `GET /evidence/{chain}/digest/signed` + `GET /evidence/signing-key`; opt-in via `BLUEDB_EVIDENCE_SIGNING`. Reframe the limitation: signing gives **non-repudiation** (operator can't deny/retroactively rewrite what it signed; consumer retains signed digests + checks consistency); **equivocation/forking still needs external anchoring** (level 3, out of scope). Add a short config note (env vars).
- [ ] **Step 3:** `cargo test -p bluedb-server 2>&1`, `cargo test -p bluedb-evidence 2>&1` — PASS. `cargo clippy --workspace --all-targets 2>&1` — no NEW warnings (watch reqwest feature interactions). Do a final `cargo test --workspace 2>&1` — green.
- [ ] **Step 4:** Commit `feat(evidence): signed-digest e2e + trust-model docs`.

## Acceptance
- A LocalSigner-signed digest verifies end-to-end through HTTP (rebuild `sth_payload` + `verify_es256_der`); a cross-chain signature does not verify; signing-off → 501.
- Vault parse logic unit-tested against canned responses (live Vault = documented manual test).
- `bluedb-evidence` gains only `sth_payload` (no new deps); signing (p256 + reqwest) is contained in `bluedb-server`.
- `cargo test --workspace` green; no new clippy warnings; `chains.md` trust model updated.

## Manual Vault integration test (documented, not automated)
```
vault server -dev ; export VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=<root>
vault secrets enable transit ; vault write -f transit/keys/evidence type=ecdsa-p256
BLUEDB_EVIDENCE_SIGNING=vault BLUEDB_EVIDENCE_VAULT_KEY=evidence <run server>
curl .../evidence/c/digest/signed ; curl .../evidence/signing-key  # verify with the PEM
```
