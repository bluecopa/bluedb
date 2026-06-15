//! HashiCorp Vault Transit signing backend. The signing key is a Transit key of
//! type `ecdsa-p256`; the server holds only a Vault token + key name. Signing:
//! POST {addr}/v1/{mount}/sign/{key} with `input` = base64(payload). Public key:
//! GET {addr}/v1/{mount}/keys/{key} → latest version's SPKI PEM.

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use serde_json::Value;

use crate::AppError;

pub(crate) struct VaultTransitSigner {
    client: reqwest::Client,
    addr: String,  // e.g. https://vault.internal:8200
    token: String,
    mount: String, // default "transit"
    key: String,   // transit key name
}

impl VaultTransitSigner {
    #[allow(dead_code)]
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
        // ecdsa-p256 transit key: Vault hashes sha2-256 unless `prehashed`.
        // `marshaling_algorithm: asn1` makes the signature ASN.1 DER (what the
        // local signer emits + `verify_es256_der` expects). `signature_algorithm`
        // is RSA-only (pss/pkcs1v15) and meaningless for ecdsa, so it is omitted.
        let body = serde_json::json!({
            "input": B64.encode(payload),
            "hash_algorithm": "sha2-256",
            "marshaling_algorithm": "asn1",
        });
        let resp = self
            .client
            .post(&url)
            .header("X-Vault-Token", &self.token)
            .json(&body)
            .send()
            .await
            .map_err(|e| AppError::internal(format!("vault sign request: {e}")))?;
        let v: Value = parse_ok(resp).await?;
        parse_signature(&v)
    }

    pub(crate) async fn public_key_pem(&self) -> Result<String, AppError> {
        let url = format!("{}/v1/{}/keys/{}", self.addr, self.mount, self.key);
        let resp = self
            .client
            .get(&url)
            .header("X-Vault-Token", &self.token)
            .send()
            .await
            .map_err(|e| AppError::internal(format!("vault keys request: {e}")))?;
        let v: Value = parse_ok(resp).await?;
        parse_public_key(&v)
    }
}

/// Decode a Transit `sign` response into ASN.1-DER signature bytes.
/// Response: `{"data":{"signature":"vault:v<N>:<base64(DER)>"}}`.
fn parse_signature(v: &Value) -> Result<Vec<u8>, AppError> {
    let sig = v
        .pointer("/data/signature")
        .and_then(|s| s.as_str())
        .ok_or_else(|| AppError::internal("vault sign: missing data.signature"))?;
    // Strip the "vault:v<N>:" prefix; the trailing field is base64(DER).
    let b64 = sig.rsplit(':').next().unwrap_or_default();
    B64.decode(b64).map_err(|e| AppError::internal(format!("vault sig decode: {e}")))
}

/// Extract the latest version's SPKI-PEM public key from a Transit `keys` response.
/// Response: `{"data":{"latest_version":N,"keys":{"N":{"public_key":"...PEM..."}}}}`.
fn parse_public_key(v: &Value) -> Result<String, AppError> {
    let latest = v
        .pointer("/data/latest_version")
        .and_then(|x| x.as_i64())
        .ok_or_else(|| AppError::internal("vault keys: missing latest_version"))?;
    let pem = v
        .pointer(&format!("/data/keys/{latest}/public_key"))
        .and_then(|s| s.as_str())
        .ok_or_else(|| AppError::internal("vault keys: missing public_key"))?;
    Ok(pem.to_string())
}

async fn parse_ok(resp: reqwest::Response) -> Result<Value, AppError> {
    let status = resp.status();
    let body = resp.text().await.map_err(|e| AppError::internal(format!("vault read body: {e}")))?;
    if !status.is_success() {
        return Err(AppError::internal(format!("vault {status}: {body}")));
    }
    serde_json::from_str(&body).map_err(|e| AppError::internal(format!("vault json: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn parses_transit_sign_and_keys_responses() {
        let sign: Value =
            serde_json::from_str(r#"{"data":{"signature":"vault:v1:MEUCIQ=="}}"#).unwrap();
        assert_eq!(parse_signature(&sign).unwrap(), B64.decode("MEUCIQ==").unwrap());

        let keys: Value = serde_json::from_str(
            r#"{"data":{"latest_version":2,"keys":{"2":{"public_key":"-----BEGIN PUBLIC KEY-----\nABC\n-----END PUBLIC KEY-----"}}}}"#,
        )
        .unwrap();
        assert_eq!(
            parse_public_key(&keys).unwrap(),
            "-----BEGIN PUBLIC KEY-----\nABC\n-----END PUBLIC KEY-----"
        );
    }

    #[test]
    fn parse_signature_rejects_missing_field() {
        let bad: Value = serde_json::from_str(r#"{"data":{}}"#).unwrap();
        assert!(parse_signature(&bad).is_err());
    }

    #[test]
    fn parse_public_key_rejects_missing_version() {
        // latest_version points at a key version that isn't present.
        let bad: Value =
            serde_json::from_str(r#"{"data":{"latest_version":3,"keys":{"2":{"public_key":"x"}}}}"#).unwrap();
        assert!(parse_public_key(&bad).is_err());
    }
}
