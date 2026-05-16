//! WebTransport transport module.
//!
//! Provides a QUIC/HTTP3-based transport for the Foxglove SDK with zstd compression
//! and unreliable datagram support for lossy channels.

pub mod compression;
pub mod framing;
pub(crate) mod connected_client;
pub(crate) mod poller;
pub mod server;
mod send_lossy;

pub use server::TlsIdentity;
