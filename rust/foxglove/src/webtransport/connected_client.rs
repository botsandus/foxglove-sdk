//! WebTransport connected client — implements the Sink trait.
//!
//! Each connected client owns a set of flume channels for message delivery:
//! - control plane: reliable JSON messages (advertise, serverInfo, etc.)
//! - data plane: reliable compressed binary data
//! - datagram plane: unreliable compressed binary data (point clouds, etc.)

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Weak};

use bimap::BiHashMap;
use bytes::Bytes;

use crate::metadata::Metadata;
use crate::protocol::v2::server::MessageData;
use crate::protocol::v2::BinaryMessage;
use crate::sink::SinkId;
use crate::sink_channel_filter::SinkChannelFilter;
use crate::{ChannelId, Context, FoxgloveError, RawChannel, Sink};

use super::compression::Compressor;
use super::framing::{
    encode_binary_frame, encode_compressed_binary_frame, encode_datagram_frame, encode_json_frame,
};
use super::poller::Poller;
use super::send_lossy::{self, MAX_SEND_RETRIES};
use super::server::ServerHandle;

use crate::protocol::common::server::advertise;
use crate::protocol::common::server::{Advertise, Unadvertise};

/// Unique client identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(u64);

impl ClientId {
    pub fn next() -> Self {
        use std::sync::atomic::AtomicU64;
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        Self(NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl std::fmt::Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Batch size for channel advertisements.
const ADVERTISE_CHANNEL_BATCH_SIZE: usize = 100;

/// Reason the client is being shut down.
#[derive(Debug)]
pub(super) enum ShutdownReason {
    ClientDisconnected,
    ServerStopped,
    ControlPlaneQueueFull,
}

/// A connected WebTransport client session.
pub(super) struct ConnectedClient {
    id: ClientId,
    label: String,
    weak_self: Weak<Self>,
    sink_id: SinkId,
    channel_filter: Option<Arc<dyn SinkChannelFilter>>,
    context: Weak<Context>,
    poller: parking_lot::Mutex<Option<Poller>>,
    /// Cache of channels for subscription management.
    channels: parking_lot::RwLock<HashMap<ChannelId, Arc<RawChannel>>>,
    /// Reliable data plane (compressed binary frames).
    data_plane_tx: flume::Sender<Bytes>,
    data_plane_rx: flume::Receiver<Bytes>,
    /// Control plane (JSON protocol messages).
    control_plane_tx: flume::Sender<Bytes>,
    /// Datagram plane (unreliable compressed binary frames).
    datagram_tx: flume::Sender<Bytes>,
    datagram_rx: flume::Receiver<Bytes>,
    /// Subscriptions from this client: channel_id <-> subscription counter.
    subscriptions: parking_lot::Mutex<BiHashMap<ChannelId, u64>>,
    /// Next subscription counter.
    next_subscription_id: AtomicU32,
    /// Per-channel datagram sequence numbers.
    datagram_sequences: parking_lot::Mutex<HashMap<u64, AtomicU32>>,
    /// Compressor for message payloads.
    compressor: Compressor,
    /// Regex patterns for topics that should use datagram delivery.
    datagram_patterns: Vec<regex::Regex>,
    /// Maximum QUIC datagram size.
    max_datagram_size: usize,
    /// Shutdown signal.
    shutdown_tx: parking_lot::Mutex<Option<tokio::sync::oneshot::Sender<ShutdownReason>>>,
    /// Weak reference to server.
    server: Weak<ServerHandle>,
}

impl std::fmt::Debug for ConnectedClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebTransportClient")
            .field("id", &self.id)
            .field("label", &self.label)
            .finish()
    }
}

impl Sink for ConnectedClient {
    fn id(&self) -> SinkId {
        self.sink_id
    }

    fn log(
        &self,
        channel: &RawChannel,
        msg: &[u8],
        metadata: &Metadata,
    ) -> Result<(), FoxgloveError> {
        let subscriptions = self.subscriptions.lock();
        if !subscriptions.contains_left(&channel.id()) {
            return Ok(());
        }

        // Encode the v2 MessageData (channel_id + log_time + raw data).
        let message_data = MessageData::new(channel.id().into(), metadata.log_time, msg);
        let encoded = message_data.to_bytes();

        // Try to zstd-compress the encoded binary message.
        if let Some(compressed) = self.compressor.compress(&encoded) {
            let is_datagram = self.is_datagram_channel(channel);

            if is_datagram {
                // Try datagram delivery first (fire-and-forget).
                let seq = self.next_datagram_seq(channel.id().into());
                if let Some(dgram) = encode_datagram_frame(
                    channel.id().into(),
                    seq,
                    &compressed,
                    self.max_datagram_size,
                ) {
                    let _ = self.datagram_tx.try_send(dgram);
                    return Ok(());
                }
                // Datagram too large after compression — fall through to reliable stream.
            }

            // Send compressed frame over reliable stream.
            let frame = encode_compressed_binary_frame(&compressed);
            send_lossy::send_lossy(
                &self.label,
                &self.data_plane_tx,
                &self.data_plane_rx,
                frame,
                MAX_SEND_RETRIES,
            );
        } else {
            // Compression didn't help — send uncompressed.
            let frame = encode_binary_frame(&message_data);
            send_lossy::send_lossy(
                &self.label,
                &self.data_plane_tx,
                &self.data_plane_rx,
                frame,
                MAX_SEND_RETRIES,
            );
        }

        Ok(())
    }

    fn add_channels(&self, channels: &[&Arc<RawChannel>]) -> Option<Vec<ChannelId>> {
        let filtered: Vec<_> = channels
            .iter()
            .filter(|ch| {
                self.channel_filter
                    .as_ref()
                    .map_or(true, |f| f.should_subscribe(ch.descriptor()))
            })
            .copied()
            .collect();

        for batch in filtered.chunks(ADVERTISE_CHANNEL_BATCH_SIZE) {
            self.advertise_channels(batch);
        }
        // Clients subscribe asynchronously via the protocol.
        None
    }

    fn remove_channel(&self, channel: &RawChannel) {
        self.subscriptions.lock().remove_by_left(&channel.id());
        self.unadvertise_channel(channel.id());
    }

    fn auto_subscribe(&self) -> bool {
        false
    }
}

impl ConnectedClient {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        context: &Weak<Context>,
        server: &Weak<ServerHandle>,
        poller: Poller,
        label: String,
        channel_filter: Option<Arc<dyn SinkChannelFilter>>,
        compressor: Compressor,
        datagram_patterns: Vec<regex::Regex>,
        max_datagram_size: usize,
        data_plane_tx: flume::Sender<Bytes>,
        data_plane_rx: flume::Receiver<Bytes>,
        control_plane_tx: flume::Sender<Bytes>,
        datagram_tx: flume::Sender<Bytes>,
        datagram_rx: flume::Receiver<Bytes>,
        shutdown_tx: tokio::sync::oneshot::Sender<ShutdownReason>,
    ) -> Arc<Self> {

        Arc::new_cyclic(|weak_self| Self {
            id: ClientId::next(),
            label,
            weak_self: weak_self.clone(),
            sink_id: SinkId::next(),
            context: context.clone(),
            channel_filter,
            poller: parking_lot::Mutex::new(Some(poller)),
            channels: parking_lot::RwLock::default(),
            data_plane_tx,
            data_plane_rx,
            control_plane_tx,
            datagram_tx,
            datagram_rx,
            subscriptions: parking_lot::Mutex::default(),
            next_subscription_id: AtomicU32::new(1),
            datagram_sequences: parking_lot::Mutex::default(),
            compressor,
            datagram_patterns,
            max_datagram_size,
            shutdown_tx: parking_lot::Mutex::new(Some(shutdown_tx)),
            server: server.clone(),
        })
    }

    pub fn id(&self) -> ClientId {
        self.id
    }

    pub fn sink_id(&self) -> SinkId {
        self.sink_id
    }

    /// Check if a channel should use datagram (unreliable) delivery.
    fn is_datagram_channel(&self, channel: &RawChannel) -> bool {
        let topic = channel.topic();
        self.datagram_patterns
            .iter()
            .any(|pattern| pattern.is_match(topic))
    }

    /// Get the next datagram sequence number for a channel.
    fn next_datagram_seq(&self, channel_id: u64) -> u32 {
        let mut seqs = self.datagram_sequences.lock();
        let counter = seqs
            .entry(channel_id)
            .or_insert_with(|| AtomicU32::new(0));
        counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Send a control-plane message (JSON, reliable).
    pub(super) fn send_control(&self, frame: Bytes) {
        if self.control_plane_tx.try_send(frame).is_err() {
            tracing::error!("Control plane full for client {}, disconnecting", self.label);
            self.shutdown(ShutdownReason::ControlPlaneQueueFull);
        }
    }

    /// Shutdown the client with a reason.
    pub(super) fn shutdown(&self, reason: ShutdownReason) {
        if let Some(tx) = self.shutdown_tx.lock().take() {
            let _ = tx.send(reason);
        }
    }

    /// Advertise channels to this client.
    fn advertise_channels(&self, channels: &[&Arc<RawChannel>]) {
        let advertise_channels: Vec<advertise::Channel<'_>> = channels
            .iter()
            .filter_map(|ch| {
                let mut cache = self.channels.write();
                cache.insert(ch.id(), Arc::clone(ch));
                advertise::Channel::try_from(ch.as_ref())
                    .inspect_err(|err| {
                        tracing::error!("Failed to build channel advertisement: {err}");
                    })
                    .ok()
            })
            .collect();

        if !advertise_channels.is_empty() {
            let msg = Advertise::new(advertise_channels);
            self.send_control(encode_json_frame(&msg));
        }
    }

    /// Unadvertise a channel.
    fn unadvertise_channel(&self, channel_id: ChannelId) {
        self.channels.write().remove(&channel_id);
        let msg = Unadvertise::new([channel_id.into()]);
        self.send_control(encode_json_frame(&msg));
    }

    /// Handle a subscribe request from the client.
    pub(super) fn on_subscribe(&self, channel_id: u64) {
        let cid = ChannelId::from(channel_id);
        let mut subs = self.subscriptions.lock();
        if subs.contains_left(&cid) {
            return; // already subscribed
        }
        let sub_id = self.next_subscription_id.fetch_add(1, Ordering::Relaxed) as u64;
        subs.insert(cid, sub_id);

        // Ask the context to subscribe this sink to the channel.
        if let Some(ctx) = self.context.upgrade() {
            ctx.subscribe_channels(self.sink_id, &[cid]);
        }
    }

    /// Handle an unsubscribe request from the client.
    pub(super) fn on_unsubscribe(&self, channel_id: u64) {
        let cid = ChannelId::from(channel_id);
        self.subscriptions.lock().remove_by_left(&cid);

        if let Some(ctx) = self.context.upgrade() {
            ctx.unsubscribe_channels(self.sink_id, &[cid]);
        }
    }

    /// Take the poller out for running in a spawned task.
    pub(super) fn take_poller(&self) -> Option<Poller> {
        self.poller.lock().take()
    }
}
