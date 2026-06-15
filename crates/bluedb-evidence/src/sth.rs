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
