//! TLS certificate loading for ringline listeners.

use crate::config::TlsConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::sync::Arc;

/// Load PEM certificate and key files from `tls` config and build a [`ringline::TlsConfig`].
pub fn load_server_config(tls: &TlsConfig) -> Result<ringline::TlsConfig, Box<dyn std::error::Error>> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&tls.cert)
        .map_err(|e| format!("failed to read cert file '{}': {e}", tls.cert))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("failed to parse certs from '{}': {e}", tls.cert))?;

    if certs.is_empty() {
        return Err(format!("no certificates found in '{}'", tls.cert).into());
    }

    let key = PrivateKeyDer::from_pem_file(&tls.key)
        .map_err(|e| format!("failed to parse private key from '{}': {e}", tls.key))?;

    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("invalid TLS certificate/key: {e}"))?;

    Ok(ringline::TlsConfig {
        server_config: Arc::new(server_config),
    })
}
