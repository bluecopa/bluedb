//! HashiCorp Vault Transit signing backend (filled in C3). Stub so the
//! `EvidenceSigner::Vault` variant compiles before the implementation lands.

use crate::AppError;

pub(crate) struct VaultTransitSigner;

#[allow(dead_code)]
impl VaultTransitSigner {
    pub(crate) fn key_id(&self) -> String {
        unimplemented!("VaultTransitSigner: C3")
    }
    pub(crate) async fn sign(&self, _payload: &[u8]) -> Result<Vec<u8>, AppError> {
        unimplemented!("VaultTransitSigner: C3")
    }
    pub(crate) async fn public_key_pem(&self) -> Result<String, AppError> {
        unimplemented!("VaultTransitSigner: C3")
    }
}
