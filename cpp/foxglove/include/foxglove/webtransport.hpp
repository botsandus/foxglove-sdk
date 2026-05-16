#pragma once

#include <foxglove/channel.hpp>
#include <foxglove/context.hpp>
#include <foxglove/error.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

struct foxglove_webtransport_server;

namespace foxglove {

/// @brief TLS identity for a WebTransport server (file paths to PEM files).
///
/// QUIC mandates TLS 1.3 — a certificate and private key are always required.
struct WebTransportTlsIdentity {
  /// @brief Path to a PEM-encoded x509 certificate file.
  std::string cert_path;
  /// @brief Path to a PEM-encoded PKCS8 private key file.
  std::string key_path;
};

/// @brief Options for a WebTransport server.
struct WebTransportServerOptions {
  friend class WebTransportServer;

  /// @brief The logging context for this server.
  Context context;

  /// @brief The host address to bind to. Default: "0.0.0.0".
  std::string host = "0.0.0.0";

  /// @brief The port to bind to. May be 0 for automatic selection. Default: 8766.
  uint16_t port = 8766;

  /// @brief TLS identity (required). Paths to PEM certificate and private key files.
  WebTransportTlsIdentity tls_identity;

  /// @brief zstd compression level (1 = fastest, 19 = best ratio). Default: 1.
  int32_t compression_level = 1;

  /// @brief Message backlog size per client. When the outbox fills, oldest messages are dropped.
  /// Default: 1024.
  size_t message_backlog_size = 1024;

  /// @brief Maximum QUIC datagram payload size in bytes. Messages exceeding this (after
  /// compression + 14-byte header) fall back to reliable streams. Default: 1200.
  size_t max_datagram_size = 1200;

  /// @brief Topic patterns for unreliable datagram delivery (ECMAScript regex).
  ///
  /// Topics matching any pattern will use QUIC datagrams when the compressed message fits
  /// within the datagram MTU. Example: {"/lidars/.*", "/camera/.*/compressed"}
  std::vector<std::string> datagram_topic_patterns;

  /// @brief Optional channel filter. Return false to exclude a channel from this sink.
  SinkChannelFilterFn sink_channel_filter;
};

/// @brief A WebTransport server for compressed live data over QUIC/HTTP3.
///
/// All binary message payloads are zstd-compressed before transmission.
/// Topics matching `datagram_topic_patterns` are sent as QUIC datagrams (unreliable, unordered)
/// when the compressed message fits within the datagram MTU.
///
/// @note WebTransportServer is fully thread-safe.
class WebTransportServer final {
public:
  /// @brief Create a new WebTransport server with the given options.
  static FoxgloveResult<WebTransportServer> create(WebTransportServerOptions&& options);

  /// @brief Get the port on which the server is listening.
  [[nodiscard]] uint16_t port() const;

  /// @brief Gracefully shut down the WebTransport server.
  FoxgloveError stop();

private:
  explicit WebTransportServer(
    foxglove_webtransport_server* server,
    std::unique_ptr<SinkChannelFilterFn> sink_channel_filter
  );

  std::unique_ptr<SinkChannelFilterFn> sink_channel_filter_;

  struct Destructor {
    void operator()(foxglove_webtransport_server* ptr) const noexcept;
  };
  std::unique_ptr<foxglove_webtransport_server, Destructor> server_;
};

}  // namespace foxglove
