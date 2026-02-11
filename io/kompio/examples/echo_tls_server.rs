use std::io;
use std::sync::Arc;

use kompio::{Config, ConnToken, DriverCtx, EventHandler, TlsConfig};

struct EchoHandler {
    worker_id: usize,
    connections: u32,
}

impl EventHandler for EchoHandler {
    fn on_accept(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        self.connections += 1;
        eprintln!(
            "[worker {}] TLS connection accepted {:?} (total: {})",
            self.worker_id, conn, self.connections
        );
    }

    fn on_data(&mut self, ctx: &mut DriverCtx, conn: ConnToken, data: &[u8]) -> usize {
        if data.is_empty() {
            return 0;
        }

        // Handler sees plaintext — TLS is transparent.
        if let Err(e) = ctx.send(conn, data) {
            eprintln!("[worker {}] send error: {e}", self.worker_id);
        }
        data.len()
    }

    fn on_send_complete(
        &mut self,
        _ctx: &mut DriverCtx,
        _conn: ConnToken,
        result: io::Result<u32>,
    ) {
        if let Err(e) = result {
            eprintln!("[worker {}] send completed with error: {e}", self.worker_id);
        }
    }

    fn on_close(&mut self, _ctx: &mut DriverCtx, conn: ConnToken) {
        self.connections = self.connections.saturating_sub(1);
        eprintln!(
            "[worker {}] TLS connection {:?} closed (remaining: {})",
            self.worker_id, conn, self.connections
        );
    }

    fn create_for_worker(worker_id: usize) -> Self {
        eprintln!("[worker {worker_id}] starting");
        EchoHandler {
            worker_id,
            connections: 0,
        }
    }
}

fn load_tls_config() -> Arc<rustls::ServerConfig> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};

    let cert_pem = std::env::var("TLS_CERT").unwrap_or_else(|_| "cert.pem".into());
    let key_pem = std::env::var("TLS_KEY").unwrap_or_else(|_| "key.pem".into());

    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert_pem)
        .unwrap_or_else(|e| panic!("failed to read {cert_pem}: {e}"))
        .collect::<Result<Vec<_>, _>>()
        .unwrap_or_else(|e| panic!("failed to parse certs from {cert_pem}: {e}"));

    if certs.is_empty() {
        panic!("no certificates found in {cert_pem}");
    }

    let key = PrivateKeyDer::from_pem_file(&key_pem)
        .unwrap_or_else(|e| panic!("failed to parse private key from {key_pem}: {e}"));

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("invalid TLS certificate/key");

    Arc::new(config)
}

fn main() {
    let bind_addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:7879".to_string());

    let server_config = load_tls_config();

    let mut config = Config::default();
    config.worker.threads = 1;
    config.worker.pin_to_core = false;
    config.sq_entries = 128;
    config.recv_buffer.ring_size = 128;
    config.recv_buffer.buffer_size = 4096;
    config.max_connections = 1024;
    config.tls = Some(TlsConfig { server_config });

    eprintln!("starting TLS echo server on {bind_addr}");
    eprintln!("test with: openssl s_client -connect {bind_addr}");

    let (_shutdown, handles) =
        kompio::launch::<EchoHandler>(config, &bind_addr).expect("failed to launch workers");

    for handle in handles {
        if let Err(e) = handle.join().expect("worker thread panicked") {
            eprintln!("worker exited with error: {e}");
        }
    }
}
