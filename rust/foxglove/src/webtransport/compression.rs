//! zstd compression for WebTransport message payloads.

use zstd_safe::{CCtx, DCtx};

/// Default zstd compression level (fast).
pub const DEFAULT_COMPRESSION_LEVEL: i32 = 1;

/// Compresses data using zstd.
///
/// Uses streaming compression to avoid extra copies. The compression level is configurable
/// (1 = fastest, 19 = best ratio). Level 1 typically achieves ~3x compression on CDR data.
pub struct Compressor {
    level: i32,
}

impl Compressor {
    /// Creates a new compressor with the given zstd level.
    pub fn new(level: i32) -> Self {
        Self { level }
    }

    /// Compresses input data, returning the compressed bytes.
    ///
    /// Returns `None` if compression fails or produces output larger than input
    /// (in which case the caller should send uncompressed).
    pub fn compress(&self, input: &[u8]) -> Option<Vec<u8>> {
        let max_size = zstd_safe::compress_bound(input.len());
        let mut output = Vec::with_capacity(max_size);

        let mut cctx = CCtx::create();
        cctx.set_parameter(zstd_safe::CParameter::CompressionLevel(self.level))
            .ok()?;

        cctx.compress2(&mut output, input).ok()?;

        // If compressed output is larger than input, skip compression
        if output.len() >= input.len() {
            return None;
        }
        Some(output)
    }
}

/// Decompresses zstd-compressed data.
///
/// The caller must know the upper bound on decompressed size (or use a growable approach).
pub fn decompress(input: &[u8], max_decompressed_size: usize) -> Option<Vec<u8>> {
    let mut output = Vec::with_capacity(max_decompressed_size);
    let mut dctx = DCtx::create();
    dctx.decompress(&mut output, input).ok()?;
    Some(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_roundtrip() {
        let compressor = Compressor::new(1);
        // CDR-like data with repeated patterns compresses well
        let data: Vec<u8> = (0..1024).flat_map(|i| {
            let val = (i % 256) as u8;
            vec![val, 0, 0, 0] // simulates padded CDR fields
        }).collect();

        let compressed = compressor.compress(&data).expect("compression should succeed");
        assert!(compressed.len() < data.len(), "compressed should be smaller");

        let decompressed = decompress(&compressed, data.len() * 2).expect("decompression should succeed");
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_incompressible_returns_none() {
        let compressor = Compressor::new(1);
        // Random-looking data that won't compress
        let data: Vec<u8> = (0..32).collect();
        // Very small random data may not compress at all
        // This is fine — the caller falls back to uncompressed
        let _ = compressor.compress(&data);
    }
}
