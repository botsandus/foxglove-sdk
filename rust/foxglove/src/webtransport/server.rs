//! WebTransport server — QUIC endpoint accept loop and client lifecycle.

use std::net::SocketAddr;
use std::sync::{Arc, Weak};

use tokio::task::JoinSet;
use wtransport::tls::Identity;
use wtransport::{Endpoint, ServerConfig};

use crate::sink_channel_filter::SinkChannelFilter;
use crate::Context;

use super::compression::{Compressor, DEFAULT_COMPRESSION_LEVEL};
use super::connected_client::ConnectedClient;
use super::poller::Poller;

/// Default message backlog size per client.
const DEFAULT_MESSAGE_BACKLOG_SIZE: usize = 1024;

/// Default maximum QUIC datagram size.
const DEFAULT_MAX_DATAGRAM_SIZE: usize = 1200;

/// TLS identity for the WebTransport server.
///
/// QUIC mandates TLS 1.3 — there is no unencrypted mode.
#[derive(Clone, Debug)]
pub struct TlsIdentity {
    /// Filesystem path to a PEM-encoded x509 certificate chain.
    pub cert_path: String,
    /// Filesystem path to a PEM-encoded PKCS8 private key.
    pub key_path: String,
}

/// Options for creating a WebTransport server.
pub(crate) struct ServerOptions {
    pub host: String,
    pub port: u16,
    pub tls: TlsIdentity,
    pub compression_level: i32,
    pub message_backlog_size: usize,
    pub max_datagram_size: usize,
    pub datagram_topic_patterns: Vec<String>,
    pub channel_filter: Option<Arc<dyn SinkChannelFilter>>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".into(),
            port: 8766,
            tls: TlsIdentity {
                cert_path: String::new(),
                key_path: String::new(),
            },
            compression_level: DEFAULT_COMPRESSION_LEVEL,
            message_backlog_size: DEFAULT_MESSAGE_BACKLOG_SIZE,
            max_datagram_size: DEFAULT_MAX_DATAGRAM_SIZE,
            datagram_topic_patterns: Vec::new(),
            channel_filter: None,
        }
    }
}

/// A running WebTransport server.
pub struct ServerHandle {
    addr: SocketAddr,
    cancel: tokio_util::sync::CancellationToken,
}

impl ServerHandle {
    /// Returns the port the server is listening on.
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    /// Returns the address the server is bound to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stop the server.
    pub fn stop(&self) {
        self.cancel.cancel();
    }
}

/// Start the WebTransport server and return a handle.
pub(crate) async fn start_server(
    options: ServerOptions,
    context: Arc<Context>,
) -> Result<ServerHandle, crate::FoxgloveError> {
    // Build wtransport server config.
    let bind_addr: SocketAddr = format!("{}:{}", options.host, options.port)
        .parse()
        .map_err(|e| crate::FoxgloveError::ValueError(format!("Invalid bind address: {e}")))?;

    // Load TLS identity from PEM files.
    let identity = Identity::load_pemfiles(&options.tls.cert_path, &options.tls.key_path)
        .await
        .map_err(|e| crate::FoxgloveError::ValueError(format!("Failed to load TLS identity: {e}")))?;

    let config = ServerConfig::builder()
        .with_bind_address(bind_addr)
        .with_identity(identity)
        .build();

    let endpoint = Endpoint::server(config)
        .map_err(|e| crate::FoxgloveError::Unspecified(Box::new(e)))?;

    let addr = endpoint
        .local_addr()
        .map_err(|e| crate::FoxgloveError::Unspecified(Box::new(e)))?;

    tracing::info!("WebTransport server listening on {addr}");

    let cancel = tokio_util::sync::CancellationToken::new();
    let cancel_clone = cancel.clone();

    // Compile datagram topic patterns.
    let datagram_patterns: Vec<regex::Regex> = options
        .datagram_topic_patterns
        .iter()
        .filter_map(|p| {
            regex::Regex::new(p)
                .inspect_err(|e| tracing::warn!("Invalid datagram topic pattern '{p}': {e}"))
                .ok()
        })
        .collect();

    let compression_level = options.compression_level;
    let message_backlog_size = options.message_backlog_size;
    let max_datagram_size = options.max_datagram_size;
    let channel_filter = options.channel_filter;

    tokio::spawn(async move {
        let mut client_tasks = JoinSet::new();
        let server_handle: Weak<ServerHandle> = Weak::new(); // placeholder

        loop {
            tokio::select! {
                _ = cancel_clone.cancelled() => {
                    tracing::info!("WebTransport server shutting down");
                    break;
                }
                incoming = endpoint.accept() => {
                    let session_request = match incoming.await {
                        Ok(req) => req,
                        Err(err) => {
                            tracing::error!("Failed to accept connection: {err}");
                            continue;
                        }
                    };

                    let connection = match session_request.accept().await {
                        Ok(conn) => conn,
                        Err(err) => {
                            tracing::error!("Failed to accept session: {err}");
                            continue;
                        }
                    };

                    let label = connection.remote_address().to_string();

                    tracing::info!("WebTransport client connected: {label}");

                    // Create channels for this client.
                    let (data_tx, data_rx) = flume::bounded(message_backlog_size);
                    let (control_tx, control_rx) = flume::bounded(message_backlog_size);
                    let (dgram_tx, dgram_rx) = flume::bounded(message_backlog_size);
                    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

                    let poller = Poller::new(
                        connection,
                        data_rx.clone(),
                        control_rx,
                        dgram_rx.clone(),
                        shutdown_rx,
                    );

                    let compressor = Compressor::new(compression_level);
                    let patterns = datagram_patterns.clone();
                    let ctx_weak = Arc::downgrade(&context);
                    let filter = channel_filter.clone();

                    let client = ConnectedClient::new(
                        &ctx_weak,
                        &server_handle,
                        poller,
                        label.clone(),
                        filter,
                        compressor,
                        patterns,
                        max_datagram_size,
                        data_tx,
                        data_rx,
                        control_tx,
                        dgram_tx,
                        dgram_rx,
                        shutdown_tx,
                    );

                    // Register the client as a sink with the context.
                    context.add_sink(Arc::clone(&client) as Arc<dyn crate::Sink>);

                    // Spawn the client's I/O loop.
                    let client_clone = Arc::clone(&client);
                    client_tasks.spawn(async move {
                        if let Some(poller) = client_clone.take_poller() {
                            poller.run(&client_clone).await;
                        }
                    });
                }
            }
        }

        // Clean up: abort remaining client tasks.
        client_tasks.shutdown().await;
    });

    Ok(ServerHandle { addr, cancel })
}
