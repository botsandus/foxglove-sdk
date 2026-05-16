//! WebTransport poller — reads/writes QUIC streams and datagrams for a connected client.

use std::sync::Arc;

use bytes::Bytes;
use wtransport::Connection;
use wtransport::RecvStream;

use super::connected_client::{ConnectedClient, ShutdownReason};
use super::framing::{StreamOpCode, STREAM_FRAME_HEADER_SIZE, MAX_MESSAGE_SIZE};

/// The poller owns the WebTransport connection and drives the I/O loop.
pub(super) struct Poller {
    connection: Connection,
    data_plane_rx: flume::Receiver<Bytes>,
    control_plane_rx: flume::Receiver<Bytes>,
    datagram_rx: flume::Receiver<Bytes>,
    shutdown_rx: tokio::sync::oneshot::Receiver<ShutdownReason>,
}

impl Poller {
    pub fn new(
        connection: Connection,
        data_plane_rx: flume::Receiver<Bytes>,
        control_plane_rx: flume::Receiver<Bytes>,
        datagram_rx: flume::Receiver<Bytes>,
        shutdown_rx: tokio::sync::oneshot::Receiver<ShutdownReason>,
    ) -> Self {
        Self {
            connection,
            data_plane_rx,
            control_plane_rx,
            datagram_rx,
            shutdown_rx,
        }
    }

    /// Run the I/O loop for the connected client.
    ///
    /// This drives five concurrent tasks:
    /// 1. Write loop: drains control + data plane queues to a reliable bidirectional stream
    /// 2. Datagram loop: drains the datagram queue as QUIC datagrams (fire-and-forget)
    /// 3. Read loop: reads client messages from the server-opened bidirectional stream
    /// 4. Accept loop: accepts client-initiated bidi streams and reads messages from them
    /// 5. Shutdown: waits for the shutdown signal
    pub async fn run(self, client: &Arc<ConnectedClient>) {
        // Open a bidirectional stream for reliable framed messages.
        let opening = match self.connection.open_bi().await {
            Ok(stream) => stream,
            Err(err) => {
                tracing::error!("Failed to open bidirectional stream: {err}");
                return;
            }
        };
        let (mut send_stream, mut recv_stream) = match opening.await {
            Ok(streams) => streams,
            Err(err) => {
                tracing::error!("Failed to complete bidirectional stream opening: {err}");
                return;
            }
        };

        // Write loop: multiplex control and data plane onto the reliable stream.
        let control_rx = self.control_plane_rx;
        let data_rx = self.data_plane_rx;
        let tx_loop = async {
            loop {
                let frame = tokio::select! {
                    // Prefer control plane messages.
                    biased;
                    msg = control_rx.recv_async() => msg,
                    msg = data_rx.recv_async() => msg,
                };
                match frame {
                    Ok(data) => {
                        if let Err(err) = send_stream.write_all(&data).await {
                            tracing::error!("Stream write error: {err}");
                            break;
                        }
                    }
                    Err(_) => break, // channels closed
                }
            }
        };

        // Datagram loop: send unreliable datagrams.
        let dgram_rx = self.datagram_rx;
        let connection_ref = &self.connection;
        let dgram_loop = async {
            while let Ok(dgram) = dgram_rx.recv_async().await {
                // send_datagram is non-blocking; drops silently if QUIC congestion window is full.
                if let Err(err) = connection_ref.send_datagram(&dgram) {
                    tracing::debug!("Datagram send failed (congestion?): {err}");
                }
            }
        };

        // Read loop: parse framed messages from the client (server-initiated stream).
        let client_ref = Arc::clone(client);
        let rx_loop = async move {
            read_stream_frames(&client_ref, &mut recv_stream).await;
        };

        // Accept loop: handle client-initiated bidirectional streams.
        let client_accept = Arc::clone(client);
        let accept_loop = async {
            loop {
                match connection_ref.accept_bi().await {
                    Ok((_send, mut recv)) => {
                        tracing::info!("Accepted client-initiated bidi stream");
                        let client_inner = Arc::clone(&client_accept);
                        // Spawn a task to read from each accepted stream.
                        tokio::spawn(async move {
                            read_stream_frames(&client_inner, &mut recv).await;
                        });
                    }
                    Err(err) => {
                        tracing::debug!("accept_bi ended: {err}");
                        break;
                    }
                }
            }
        };

        // Wait for any task to complete or shutdown signal.
        tokio::select! {
            _ = tx_loop => {
                // Write stream failed — connection broken.
                client.shutdown(ShutdownReason::ClientDisconnected);
            },
            _ = dgram_loop => {},
            _ = rx_loop => {
                // Read stream ended — client disconnected.
                client.shutdown(ShutdownReason::ClientDisconnected);
            },
            _ = accept_loop => {
                // Connection closed — no more streams to accept.
                client.shutdown(ShutdownReason::ClientDisconnected);
            },
            reason = self.shutdown_rx => {
                match reason {
                    Ok(ShutdownReason::ControlPlaneQueueFull) => {
                        tracing::warn!("Disconnecting client: control plane queue full");
                    }
                    Ok(ShutdownReason::ServerStopped) => {
                        tracing::info!("Server stopping, disconnecting client");
                    }
                    Ok(ShutdownReason::ClientDisconnected) => {}
                    Err(_) => {}
                }
            },
        }

        tracing::info!("WebTransport client disconnected: {}", client.id());
    }
}

/// Handle a JSON message from the client.
///
/// Parses the `"op"` field and dispatches to the appropriate handler.
fn handle_client_json(client: &ConnectedClient, text: &str) {
    // Minimal JSON parsing — look for the "op" field.
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(err) => {
            tracing::warn!("Invalid JSON from client: {err}");
            return;
        }
    };

    let op = value.get("op").and_then(|v| v.as_str()).unwrap_or("");
    match op {
        "subscribe" => {
            if let Some(channels) = value.get("channels").and_then(|v| v.as_array()) {
                for ch in channels {
                    if let Some(id) = ch.get("id").and_then(|v| v.as_u64()) {
                        client.on_subscribe(id);
                    }
                }
            }
        }
        "unsubscribe" => {
            if let Some(ids) = value.get("channelIds").and_then(|v| v.as_array()) {
                for id in ids {
                    if let Some(id) = id.as_u64() {
                        client.on_unsubscribe(id);
                    }
                }
            }
        }
        _ => {
            tracing::debug!("Unhandled client op: {op}");
        }
    }
}

/// Read framed messages from a QUIC recv stream and dispatch them.
///
/// This is used for both the server-initiated bidi stream and client-initiated streams.
async fn read_stream_frames(client: &ConnectedClient, recv_stream: &mut RecvStream) {
    loop {
        // Read frame header: [opcode: u8][length: u32 LE]
        let mut header = [0u8; STREAM_FRAME_HEADER_SIZE];
        if recv_stream.read_exact(&mut header).await.is_err() {
            break; // stream closed
        }
        let opcode = header[0];
        let len = u32::from_le_bytes([header[1], header[2], header[3], header[4]]) as usize;

        if len > MAX_MESSAGE_SIZE {
            tracing::error!("Message too large ({len} bytes), closing stream");
            break;
        }

        let mut payload = vec![0u8; len];
        if recv_stream.read_exact(&mut payload).await.is_err() {
            break; // stream closed
        }

        match StreamOpCode::from_u8(opcode) {
            Some(StreamOpCode::Text) => {
                if let Ok(text) = std::str::from_utf8(&payload) {
                    handle_client_json(client, text);
                }
            }
            Some(StreamOpCode::Binary) | Some(StreamOpCode::CompressedBinary) => {
                tracing::debug!("Received binary message from client (opcode={opcode})");
            }
            None => {
                tracing::warn!("Unknown opcode {opcode} from client");
            }
        }
    }
}
