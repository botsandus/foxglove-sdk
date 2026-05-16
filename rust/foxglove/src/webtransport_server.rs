//! Public builder API for the WebTransport server.

use std::sync::Arc;

use crate::sink_channel_filter::{SinkChannelFilter, SinkChannelFilterFn};
use crate::webtransport::server::{self, ServerHandle, ServerOptions, TlsIdentity};
use crate::{ChannelDescriptor, Context, FoxgloveError};

/// A WebTransport server for compressed live visualization over QUIC.
///
/// After the server is started, a client (e.g. the `foxglove_transport_client` ROS node)
/// connects over WebTransport (HTTP/3) and receives zstd-compressed message data.
///
/// ### Compression
///
/// All binary message payloads are zstd-compressed before transmission. The compression
/// level is configurable (1 = fastest, 19 = best ratio). Level 1 is recommended for
/// real-time use and typically achieves ~3x compression on CDR data.
///
/// ### Datagrams
///
/// Topics matching the `datagram_topics` patterns are sent as QUIC datagrams (unreliable,
/// unordered). This avoids head-of-line blocking for high-frequency sensor data like
/// point clouds. Messages that exceed the QUIC datagram MTU after compression fall back
/// to reliable stream delivery.
///
/// ### TLS
///
/// QUIC mandates TLS 1.3. A certificate and private key must be provided.
#[must_use]
pub struct WebTransportServer {
    host: String,
    port: u16,
    tls: Option<TlsIdentity>,
    compression_level: i32,
    message_backlog_size: Option<usize>,
    max_datagram_size: Option<usize>,
    datagram_topic_patterns: Vec<String>,
    channel_filter: Option<Arc<dyn SinkChannelFilter>>,
    context: Arc<Context>,
}

impl Default for WebTransportServer {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".into(),
            port: 8766,
            tls: None,
            compression_level: 1,
            message_backlog_size: None,
            max_datagram_size: None,
            datagram_topic_patterns: Vec::new(),
            channel_filter: None,
            context: Context::get_default(),
        }
    }
}

impl WebTransportServer {
    /// Creates a new WebTransport server builder with default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the bind address and port.
    ///
    /// `port` may be 0, in which case an available port will be automatically selected.
    /// Default: `0.0.0.0:8766`.
    pub fn bind(mut self, host: impl Into<String>, port: u16) -> Self {
        self.host = host.into();
        self.port = port;
        self
    }

    /// Set the TLS identity (required).
    ///
    /// QUIC mandates TLS 1.3 — the server will not start without a certificate.
    pub fn tls(mut self, identity: TlsIdentity) -> Self {
        self.tls = Some(identity);
        self
    }

    /// Set the zstd compression level.
    ///
    /// Range: 1 (fastest) to 19 (best ratio). Default: 1.
    pub fn compression_level(mut self, level: i32) -> Self {
        self.compression_level = level.clamp(1, 19);
        self
    }

    /// Set the message backlog size per client.
    ///
    /// When the outbox fills, oldest messages are dropped.
    /// Default: 1024.
    pub fn message_backlog_size(mut self, size: usize) -> Self {
        self.message_backlog_size = Some(size);
        self
    }

    /// Set the maximum QUIC datagram size.
    ///
    /// Messages larger than this (after compression + 14-byte header) use reliable streams.
    /// Default: 1200 bytes.
    pub fn max_datagram_size(mut self, size: usize) -> Self {
        self.max_datagram_size = Some(size);
        self
    }

    /// Set topic patterns for datagram (unreliable) delivery.
    ///
    /// Topics matching any of these ECMAScript regex patterns will be sent as QUIC datagrams
    /// when the compressed message fits within the datagram MTU.
    ///
    /// Example: `["/lidars/.*", "/camera/.*/compressed"]`
    pub fn datagram_topics(mut self, patterns: impl IntoIterator<Item = String>) -> Self {
        self.datagram_topic_patterns = patterns.into_iter().collect();
        self
    }

    /// Set a channel filter for connected clients.
    pub fn channel_filter(mut self, filter: Arc<dyn SinkChannelFilter>) -> Self {
        self.channel_filter = Some(filter);
        self
    }

    /// Set a channel filter function for connected clients.
    pub fn channel_filter_fn(
        mut self,
        filter: impl Fn(&ChannelDescriptor) -> bool + Sync + Send + 'static,
    ) -> Self {
        self.channel_filter = Some(Arc::new(SinkChannelFilterFn(filter)));
        self
    }

    /// Set the context for this server.
    pub fn context(mut self, context: &Arc<Context>) -> Self {
        self.context = Arc::clone(context);
        self
    }

    /// Start the server asynchronously.
    ///
    /// Returns a handle that can be used to query the port and stop the server.
    pub async fn start(self) -> Result<WebTransportServerHandle, FoxgloveError> {
        let tls = self.tls.ok_or_else(|| {
            FoxgloveError::ValueError(
                "TLS identity is required for WebTransport (QUIC mandates TLS 1.3)".into(),
            )
        })?;

        let options = ServerOptions {
            host: self.host,
            port: self.port,
            tls,
            compression_level: self.compression_level,
            message_backlog_size: self.message_backlog_size.unwrap_or(1024),
            max_datagram_size: self.max_datagram_size.unwrap_or(1200),
            datagram_topic_patterns: self.datagram_topic_patterns,
            channel_filter: self.channel_filter,
        };

        let handle = server::start_server(options, self.context).await?;
        Ok(WebTransportServerHandle { inner: handle })
    }

    /// Start the server, blocking the current thread until it's ready.
    ///
    /// This is a convenience for use from synchronous code. It creates a tokio runtime
    /// if one is not already available.
    pub fn start_blocking(self) -> Result<WebTransportServerHandle, FoxgloveError> {
        let rt = crate::runtime::get_runtime_handle();
        rt.block_on(self.start())
    }
}

/// Handle to a running WebTransport server.
pub struct WebTransportServerHandle {
    inner: ServerHandle,
}

impl WebTransportServerHandle {
    /// Returns the port the server is listening on.
    pub fn port(&self) -> u16 {
        self.inner.port()
    }

    /// Stop the server.
    pub fn stop(&self) {
        self.inner.stop();
    }
}

impl std::fmt::Debug for WebTransportServerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebTransportServerHandle")
            .field("addr", &self.inner.addr())
            .finish()
    }
}
