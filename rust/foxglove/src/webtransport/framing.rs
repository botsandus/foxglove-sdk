//! Framing for WebTransport messages.
//!
//! Defines the wire format for both reliable stream frames and unreliable datagram frames.
//! Stream frames use the v2 byte-stream framing extended with a compressed binary opcode.
//! Datagram frames use a compact header for fire-and-forget delivery.

use bytes::{BufMut, Bytes};

use crate::protocol::v2::{BinaryMessage, JsonMessage};

/// Opcodes for the v2 byte-stream framing over WebTransport streams.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamOpCode {
    /// JSON-encoded protocol message (uncompressed).
    Text = 1,
    /// Binary-encoded protocol message (uncompressed).
    Binary = 2,
    /// Binary-encoded protocol message (zstd-compressed payload).
    CompressedBinary = 3,
}

impl StreamOpCode {
    /// Parse a u8 into a StreamOpCode.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Text),
            2 => Some(Self::Binary),
            3 => Some(Self::CompressedBinary),
            _ => None,
        }
    }
}

/// Size of the stream frame header: 1 byte opcode + 4 byte LE length.
pub const STREAM_FRAME_HEADER_SIZE: usize = 5;

/// Maximum message size for a single frame.
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16 MiB

/// Encodes a JSON message with the v2 byte-stream framing.
///
/// Wire format: `[opcode=1][u32 LE payload_len][JSON bytes]`
pub fn encode_json_frame(message: &impl JsonMessage) -> Bytes {
    let payload = message.to_string();
    let payload_bytes = payload.as_bytes();
    let mut buf = Vec::with_capacity(STREAM_FRAME_HEADER_SIZE + payload_bytes.len());
    buf.put_u8(StreamOpCode::Text as u8);
    buf.put_u32_le(payload_bytes.len() as u32);
    buf.put_slice(payload_bytes);
    Bytes::from(buf)
}

/// Encodes a binary message (uncompressed) with the v2 byte-stream framing.
///
/// Wire format: `[opcode=2][u32 LE msg_len][inner_opcode][payload]`
pub fn encode_binary_frame<'a>(message: &impl BinaryMessage<'a>) -> Bytes {
    let msg_len = message.encoded_len();
    let mut buf = Vec::with_capacity(STREAM_FRAME_HEADER_SIZE + msg_len);
    buf.put_u8(StreamOpCode::Binary as u8);
    buf.put_u32_le(msg_len as u32);
    message.encode(&mut buf);
    Bytes::from(buf)
}

/// Encodes a compressed binary message with the v2 byte-stream framing.
///
/// The `compressed_payload` is the zstd-compressed output of a `BinaryMessage::to_bytes()`.
///
/// Wire format: `[opcode=3][u32 LE compressed_len][zstd(inner_opcode + payload)]`
pub fn encode_compressed_binary_frame(compressed_payload: &[u8]) -> Bytes {
    let mut buf = Vec::with_capacity(STREAM_FRAME_HEADER_SIZE + compressed_payload.len());
    buf.put_u8(StreamOpCode::CompressedBinary as u8);
    buf.put_u32_le(compressed_payload.len() as u32);
    buf.put_slice(compressed_payload);
    Bytes::from(buf)
}

/// Header size for a datagram frame.
///
/// ```text
/// ┌──────────────┬──────────────┬──────────────┬─────────────────────────┐
/// │ channel_id   │ sequence     │ flags        │ compressed payload      │
/// │ u64 LE       │ u32 LE       │ u16 LE       │ variable                │
/// └──────────────┴──────────────┴──────────────┴─────────────────────────┘
/// ```
pub const DATAGRAM_HEADER_SIZE: usize = 8 + 4 + 2; // 14 bytes

/// Encodes a datagram frame for unreliable delivery.
///
/// Returns `None` if the total frame size exceeds `max_datagram_size`.
pub fn encode_datagram_frame(
    channel_id: u64,
    sequence: u32,
    compressed_payload: &[u8],
    max_datagram_size: usize,
) -> Option<Bytes> {
    let total_size = DATAGRAM_HEADER_SIZE + compressed_payload.len();
    if total_size > max_datagram_size {
        return None;
    }

    let mut buf = Vec::with_capacity(total_size);
    buf.put_u64_le(channel_id);
    buf.put_u32_le(sequence);
    buf.put_u16_le(0); // flags: reserved
    buf.put_slice(compressed_payload);
    Some(Bytes::from(buf))
}

/// Parsed datagram frame header.
#[derive(Debug, Clone, Copy)]
pub struct DatagramHeader {
    /// Channel ID.
    pub channel_id: u64,
    /// Sequence number for ordering/loss detection.
    pub sequence: u32,
    /// Reserved flags.
    pub flags: u16,
}

/// Parses a datagram frame, returning the header and the compressed payload.
pub fn parse_datagram_frame(data: &[u8]) -> Option<(DatagramHeader, &[u8])> {
    if data.len() < DATAGRAM_HEADER_SIZE {
        return None;
    }
    let channel_id = u64::from_le_bytes(data[0..8].try_into().ok()?);
    let sequence = u32::from_le_bytes(data[8..12].try_into().ok()?);
    let flags = u16::from_le_bytes(data[12..14].try_into().ok()?);
    let payload = &data[DATAGRAM_HEADER_SIZE..];
    Some((
        DatagramHeader {
            channel_id,
            sequence,
            flags,
        },
        payload,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datagram_roundtrip() {
        let payload = b"compressed data here";
        let frame = encode_datagram_frame(42, 7, payload, 1200).expect("should fit");
        let (header, decoded_payload) = parse_datagram_frame(&frame).expect("should parse");
        assert_eq!(header.channel_id, 42);
        assert_eq!(header.sequence, 7);
        assert_eq!(header.flags, 0);
        assert_eq!(decoded_payload, payload);
    }

    #[test]
    fn test_datagram_too_large() {
        let payload = vec![0u8; 1200];
        assert!(encode_datagram_frame(1, 0, &payload, 1200).is_none());
    }
}
