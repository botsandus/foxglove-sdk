//! Lossy bounded-channel send for the WebTransport data plane.

use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;

use crate::throttler::Throttler;

static THROTTLER: Mutex<Throttler> = Mutex::new(Throttler::new(Duration::from_secs(30)));

/// Result of a lossy send attempt.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)] // variants read in match arms; field used for logging
pub(crate) enum SendLossyResult {
    /// Message sent without dropping anything.
    Sent,
    /// Message sent after dropping `usize` older messages.
    SentLossy(usize),
    /// Could not send after exhausting all retries.
    ExhaustedRetries,
}

/// Maximum number of retries when the channel is full (drop oldest and retry).
pub(crate) const MAX_SEND_RETRIES: usize = 10;

/// Attempt to send a message on a bounded channel, dropping oldest messages if full.
///
/// This is the same pattern used by the WebSocket transport: if the channel is full, we pop the
/// oldest message and retry, up to `retries` times.
pub(crate) fn send_lossy(
    client_label: &str,
    tx: &flume::Sender<Bytes>,
    rx: &flume::Receiver<Bytes>,
    mut message: Bytes,
    retries: usize,
) -> SendLossyResult {
    let mut dropped = 0;
    loop {
        match (dropped, tx.try_send(message)) {
            (0, Ok(_)) => return SendLossyResult::Sent,
            (_, Ok(_)) => {
                if THROTTLER.lock().try_acquire() {
                    tracing::info!("outbox for client {client_label} full, dropped {dropped} messages");
                }
                return SendLossyResult::SentLossy(dropped);
            }
            (_, Err(flume::TrySendError::Disconnected(_))) => unreachable!("we're holding rx"),
            (_, Err(flume::TrySendError::Full(rejected))) => {
                if dropped >= retries {
                    return SendLossyResult::ExhaustedRetries;
                }
                // Drop oldest message
                let _ = rx.try_recv();
                dropped += 1;
                message = rejected;
            }
        }
    }
}
