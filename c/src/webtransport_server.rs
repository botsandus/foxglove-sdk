use std::ffi::c_void;
use std::mem::ManuallyDrop;
use std::sync::Arc;

use crate::channel_descriptor::FoxgloveChannelDescriptor;
use crate::sink_channel_filter::ChannelFilter;
use crate::{FoxgloveContext, FoxgloveError, FoxgloveString, result_to_c};

pub struct FoxgloveWebTransportServer(Option<foxglove::WebTransportServerHandle>);

impl FoxgloveWebTransportServer {
    fn as_ref(&self) -> Option<&foxglove::WebTransportServerHandle> {
        self.0.as_ref()
    }

    fn take(&mut self) -> Option<foxglove::WebTransportServerHandle> {
        self.0.take()
    }
}

/// Options for creating a WebTransport server.
///
/// QUIC mandates TLS 1.3 — `tls_cert_path` and `tls_key_path` are required.
#[repr(C)]
pub struct FoxgloveWebTransportServerOptions<'a> {
    /// `context` can be null, or a valid pointer to a context created via `foxglove_context_new`.
    /// If it's null, the server will be created with the default context.
    pub context: *const FoxgloveContext,

    /// Host address to bind to. If empty, defaults to "0.0.0.0".
    pub host: FoxgloveString,

    /// Port to bind to. May be 0 for automatic selection. Default: 8766.
    pub port: u16,

    /// Path to a PEM-encoded x509 certificate file. Required.
    pub tls_cert_path: FoxgloveString,

    /// Path to a PEM-encoded PKCS8 private key file. Required.
    pub tls_key_path: FoxgloveString,

    /// zstd compression level (1 = fastest, 19 = best ratio). Default: 1.
    pub compression_level: i32,

    /// Message backlog size per client. Default: 1024.
    pub message_backlog_size: usize,

    /// Maximum QUIC datagram payload size. Messages exceeding this (after compression)
    /// fall back to reliable streams. Default: 1200.
    pub max_datagram_size: usize,

    /// Topic patterns for unreliable datagram delivery (ECMAScript regex).
    /// Topics matching any pattern will use QUIC datagrams when the message fits.
    ///
    /// # Safety
    /// - If provided, must be a valid pointer to an array of `datagram_topic_patterns_count`
    ///   FoxgloveString elements.
    pub datagram_topic_patterns: *const FoxgloveString,
    /// Number of datagram topic patterns.
    pub datagram_topic_patterns_count: usize,

    /// Context provided to the `sink_channel_filter` callback.
    pub sink_channel_filter_context: *const c_void,

    /// Optional channel filter. Return false to exclude a channel from this sink.
    ///
    /// # Safety
    /// - If provided, must remain valid until the server is stopped.
    pub sink_channel_filter: Option<
        unsafe extern "C" fn(
            context: *const c_void,
            channel: *const FoxgloveChannelDescriptor,
        ) -> bool,
    >,

    /// Lifetime anchor for borrowed references.
    pub _phantom: Option<&'a ()>,
}

/// Create and start a WebTransport server.
///
/// Resources must later be freed by calling `foxglove_webtransport_server_stop`.
///
/// Returns 0 on success, or returns a FoxgloveError code on error.
///
/// # Safety
///
/// - `tls_cert_path` and `tls_key_path` must contain valid UTF-8 file paths.
/// - If `host` is supplied, it must contain valid UTF-8.
/// - If `datagram_topic_patterns` is supplied, all elements must contain valid UTF-8
///   and the array must have `datagram_topic_patterns_count` elements.
#[unsafe(no_mangle)]
#[must_use]
pub unsafe extern "C" fn foxglove_webtransport_server_start(
    options: &FoxgloveWebTransportServerOptions,
    server: *mut *mut FoxgloveWebTransportServer,
) -> FoxgloveError {
    unsafe {
        let result = do_foxglove_webtransport_server_start(options);
        result_to_c(result, server)
    }
}

unsafe fn do_foxglove_webtransport_server_start(
    options: &FoxgloveWebTransportServerOptions,
) -> Result<*mut FoxgloveWebTransportServer, foxglove::FoxgloveError> {
    let host = unsafe { options.host.as_utf8_str() }
        .map_err(|e| foxglove::FoxgloveError::Utf8Error(format!("host is invalid: {e}")))?;
    let cert_path = unsafe { options.tls_cert_path.as_utf8_str() }
        .map_err(|e| {
            foxglove::FoxgloveError::Utf8Error(format!("tls_cert_path is invalid: {e}"))
        })?;
    let key_path = unsafe { options.tls_key_path.as_utf8_str() }
        .map_err(|e| foxglove::FoxgloveError::Utf8Error(format!("tls_key_path is invalid: {e}")))?;

    if cert_path.is_empty() || key_path.is_empty() {
        return Err(foxglove::FoxgloveError::ValueError(
            "TLS certificate and key paths are required for WebTransport".to_string(),
        ));
    }

    let tls_identity = foxglove::webtransport::TlsIdentity {
        cert_path: cert_path.to_string(),
        key_path: key_path.to_string(),
    };

    let mut builder = foxglove::WebTransportServer::new()
        .tls(tls_identity)
        .compression_level(options.compression_level.max(1));

    if !host.is_empty() {
        builder = builder.bind(host, options.port);
    } else {
        builder = builder.bind("0.0.0.0", options.port);
    }

    if options.message_backlog_size > 0 {
        builder = builder.message_backlog_size(options.message_backlog_size);
    }
    if options.max_datagram_size > 0 {
        builder = builder.max_datagram_size(options.max_datagram_size);
    }

    if options.datagram_topic_patterns_count > 0 && !options.datagram_topic_patterns.is_null() {
        let patterns = unsafe {
            std::slice::from_raw_parts(
                options.datagram_topic_patterns,
                options.datagram_topic_patterns_count,
            )
        };
        let pattern_strings: Vec<String> = patterns
            .iter()
            .map(|p| unsafe { p.as_utf8_str() }.map(|s| s.to_string()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                foxglove::FoxgloveError::Utf8Error(format!(
                    "datagram topic pattern is invalid: {e}"
                ))
            })?;
        builder = builder.datagram_topics(pattern_strings);
    }

    if let Some(sink_channel_filter) = options.sink_channel_filter {
        builder = builder.channel_filter(Arc::new(ChannelFilter::new(
            options.sink_channel_filter_context,
            sink_channel_filter,
        )));
    }

    if !options.context.is_null() {
        let context = ManuallyDrop::new(unsafe { Arc::from_raw(options.context) });
        builder = builder.context(&context);
    }

    let handle = builder.start_blocking()?;
    Ok(Box::into_raw(Box::new(FoxgloveWebTransportServer(Some(
        handle,
    )))))
}

/// Get the port on which the WebTransport server is listening.
#[unsafe(no_mangle)]
pub extern "C" fn foxglove_webtransport_server_get_port(
    server: Option<&FoxgloveWebTransportServer>,
) -> u16 {
    let Some(server) = server else {
        tracing::error!("foxglove_webtransport_server_get_port called with null server");
        return 0;
    };
    let Some(server) = server.as_ref() else {
        tracing::error!("foxglove_webtransport_server_get_port called with closed server");
        return 0;
    };
    server.port()
}

/// Stop and shut down a WebTransport server and free its resources.
#[unsafe(no_mangle)]
pub extern "C" fn foxglove_webtransport_server_stop(
    server: Option<&mut FoxgloveWebTransportServer>,
) -> FoxgloveError {
    let Some(server) = server else {
        tracing::error!("foxglove_webtransport_server_stop called with null server");
        return FoxgloveError::ValueError;
    };
    let mut server = unsafe { Box::from_raw(server) };
    let Some(server) = server.take() else {
        tracing::error!("foxglove_webtransport_server_stop called with closed server");
        return FoxgloveError::SinkClosed;
    };
    server.stop();
    FoxgloveError::Ok
}
