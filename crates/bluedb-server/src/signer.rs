//! Digest signing for evidence chains. The private key lives in a KMS; the
//! server only invokes a signing operation. ES256 (ECDSA P-256, ASN.1-DER sigs).
//! `EvidenceSigner` dispatches to a backend; `Local` is in-process (dev/tests),
//! `Vault` (C3) calls HashiCorp Vault Transit.

use p256::ecdsa::{signature::Signer as _, Signature, SigningKey, VerifyingKey};
use p256::pkcs8::{DecodePublicKey, EncodePublicKey};

use crate::AppError;

pub(crate) enum EvidenceSigner {
    Local(LocalSigner),
    Vault(crate::signer::vault::VaultTransitSigner),
}

impl EvidenceSigner {
    /// Sign `payload` → `(key_version, ASN.1-DER ES256 signature bytes)`. The
    /// version identifies which key generation produced the signature, so it
    /// stays attributable across KMS rotations.
    pub(crate) async fn sign(&self, payload: &[u8]) -> Result<(u64, Vec<u8>), AppError> {
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
    /// `(latest_key_version, SPKI public key PEM)` for consumer verification.
    pub(crate) async fn public_key(&self) -> Result<(u64, String), AppError> {
        match self {
            EvidenceSigner::Local(s) => Ok(s.public_key()),
            EvidenceSigner::Vault(s) => s.public_key().await,
        }
    }
    pub(crate) fn alg(&self) -> &'static str {
        "ES256"
    }
}

/// In-process ECDSA P-256 signer. Dev / tests / air-gapped self-host only — NOT
/// production trust (the key sits in process memory).
pub(crate) struct LocalSigner {
    key: SigningKey,
    pub(crate) key_id: String,
}

impl LocalSigner {
    /// Generate an ephemeral key (dev only — not stable across restarts).
    pub(crate) fn ephemeral() -> Self {
        // CSPRNG-seeded; uses the OS RNG.
        let key = SigningKey::random(&mut p256::elliptic_curve::rand_core::OsRng);
        Self { key, key_id: "local-ephemeral".to_string() }
    }
    /// The local fixture has no real versioning — it always reports version `1`.
    fn sign(&self, payload: &[u8]) -> Result<(u64, Vec<u8>), AppError> {
        let sig: Signature = self.key.sign(payload); // RFC 6979 deterministic; SHA-256
        Ok((1, sig.to_der().as_bytes().to_vec()))
    }
    fn public_key(&self) -> (u64, String) {
        let vk: VerifyingKey = *self.key.verifying_key();
        (1, vk.to_public_key_pem(Default::default()).expect("spki pem"))
    }
}

/// Verify helper (used by tests + available to Rust consumers): ES256-DER over
/// `payload` against an SPKI-PEM public key.
pub fn verify_es256_der(public_key_pem: &str, payload: &[u8], der_sig: &[u8]) -> bool {
    use p256::ecdsa::signature::Verifier;
    let Ok(vk) = VerifyingKey::from_public_key_pem(public_key_pem) else {
        return false;
    };
    let Ok(sig) = Signature::from_der(der_sig) else {
        return false;
    };
    vk.verify(payload, &sig).is_ok()
}

pub(crate) mod vault; // C3

#[cfg(test)]
mod tests {
    use super::*;
    use bluedb_evidence::sth_payload;

    #[test]
    fn local_signer_sign_then_verify_roundtrips() {
        let s = LocalSigner::ephemeral();
        let (pv, pem) = s.public_key();
        assert_eq!(pv, 1, "local fixture reports version 1");
        let payload = sth_payload("acme", "c", 5, &[7u8; 32], 1_700_000_000_000);
        let (sv, sig) = s.sign(&payload).unwrap();
        assert_eq!(sv, 1, "local fixture signs as version 1");
        assert!(verify_es256_der(&pem, &payload, &sig));
        // Tamper: a different payload must NOT verify against the same sig.
        let other = sth_payload("acme", "c", 6, &[7u8; 32], 1_700_000_000_000);
        assert!(!verify_es256_der(&pem, &other, &sig));
        // A different key's pub must NOT verify.
        let (_v2, pem2) = LocalSigner::ephemeral().public_key();
        assert!(!verify_es256_der(&pem2, &payload, &sig));
    }
}
