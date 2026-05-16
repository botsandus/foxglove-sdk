#include <foxglove-c/foxglove-c.h>
#include <foxglove/channel.hpp>
#include <foxglove/context.hpp>
#include <foxglove/error.hpp>
#include <foxglove/webtransport.hpp>

namespace foxglove {

FoxgloveResult<WebTransportServer> WebTransportServer::create(
  WebTransportServerOptions&& options  // NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
) {
  foxglove_internal_register_cpp_wrapper();

  std::unique_ptr<SinkChannelFilterFn> sink_channel_filter;

  foxglove_webtransport_server_options c_options = {};
  c_options.context = options.context.getInner();
  c_options.host = {options.host.c_str(), options.host.length()};
  c_options.port = options.port;
  c_options.tls_cert_path = {
    options.tls_identity.cert_path.c_str(), options.tls_identity.cert_path.length()
  };
  c_options.tls_key_path = {
    options.tls_identity.key_path.c_str(), options.tls_identity.key_path.length()
  };
  c_options.compression_level = options.compression_level;
  c_options.message_backlog_size = options.message_backlog_size;
  c_options.max_datagram_size = options.max_datagram_size;

  std::vector<foxglove_string> c_patterns;
  c_patterns.reserve(options.datagram_topic_patterns.size());
  for (const auto& pattern : options.datagram_topic_patterns) {
    c_patterns.push_back({pattern.c_str(), pattern.length()});
  }
  c_options.datagram_topic_patterns = c_patterns.data();
  c_options.datagram_topic_patterns_count = c_patterns.size();

  if (options.sink_channel_filter) {
    sink_channel_filter = std::make_unique<SinkChannelFilterFn>(options.sink_channel_filter);

    c_options.sink_channel_filter_context = sink_channel_filter.get();
    c_options.sink_channel_filter =
      [](const void* context, const struct foxglove_channel_descriptor* channel) -> bool {
      try {
        if (!context) {
          return true;
        }
        const auto* filter_func = static_cast<const SinkChannelFilterFn*>(context);
        auto cpp_channel = ChannelDescriptor(channel);
        return (*filter_func)(cpp_channel);
      } catch (const std::exception& exc) {
        return false;
      }
    };
  }

  foxglove_webtransport_server* server = nullptr;
  foxglove_error error = foxglove_webtransport_server_start(&c_options, &server);
  if (error != foxglove_error::FOXGLOVE_ERROR_OK || server == nullptr) {
    return tl::unexpected(static_cast<FoxgloveError>(error));
  }

  return WebTransportServer(server, std::move(sink_channel_filter));
}

WebTransportServer::WebTransportServer(
  foxglove_webtransport_server* server, std::unique_ptr<SinkChannelFilterFn> sink_channel_filter
)
    : sink_channel_filter_(std::move(sink_channel_filter))
    , server_(server) {}

FoxgloveError WebTransportServer::stop() {
  foxglove_error error = foxglove_webtransport_server_stop(server_.release());
  return FoxgloveError(error);
}

uint16_t WebTransportServer::port() const {
  return foxglove_webtransport_server_get_port(server_.get());
}

void WebTransportServer::Destructor::operator()(
  foxglove_webtransport_server* ptr
) const noexcept {
  if (ptr) {
    foxglove_webtransport_server_stop(ptr);
  }
}

}  // namespace foxglove
