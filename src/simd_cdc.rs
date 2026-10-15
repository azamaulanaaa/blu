// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Cut a byte stream into content-defined chunks with SIMD.
//!
//! The scan uses the Gear hash from the `gearhash` crate. It runs `SSE4.2`
//! or `AVX2` on `x86_64` and `NEON` on `aarch64`. Other targets run scalar
//! code. Every path cuts the same bytes.
//!
//! Cuts differ from the `fastcdc` cuts of older releases. The table and the
//! scan changed. Old chunks still restore. New backups share less data with
//! old ones. Back up again to converge.
//!
//! DEPRECATED: the scalar `fastcdc` chunker is gone. `SimdStream` replaces
//! it. The store keeps chunk contents. It never kept cut positions.

use std::{
    fmt::Debug,
    io::{self, Read},
};

use gearhash::Hasher;

/// Smallest minimum chunk size the stream accepts, in bytes.
pub const MINIMUM_MIN: usize = 64;
/// Largest minimum chunk size the stream accepts, in bytes.
pub const MINIMUM_MAX: usize = 67_108_864;
/// Smallest average chunk size the stream accepts, in bytes.
pub const AVERAGE_MIN: usize = 256;
/// Largest average chunk size the stream accepts, in bytes.
pub const AVERAGE_MAX: usize = 268_435_456;
/// Smallest maximum chunk size the stream accepts, in bytes.
pub const MAXIMUM_MIN: usize = 1024;
/// Largest maximum chunk size the stream accepts, in bytes.
pub const MAXIMUM_MAX: usize = 16_777_216;

// Cut masks, one row per target size bucket.
//
// These values come from the `fastcdc` crate (MIT). Its 64 B through 128 KiB
// rows come from the destor C code. Its other rows come from restic-FastCDC.
// The paper spreads mask bits evenly. The table stays. Only the hash table
// changed, so the cut chance per byte stays the same.
//
// Rows 0 through 5 never run.
const MASKS: [u64; 26] = [
    0,                     // padding
    0,                     // padding
    0,                     // padding
    0,                     // padding
    0,                     // padding
    0x0000_0000_0180_4110, // padding, used only at level 3
    0x0000_0000_0180_3110, // 64 B
    0x0000_0001_1803_5100, // 128 B
    0x0000_0018_0003_5300, // 256 B
    0x0000_0190_0035_3000, // 512 B
    0x0000_5900_0353_0000, // 1 KiB
    0x0000_d900_0353_0000, // 2 KiB
    0x0000_d901_0353_0000, // 4 KiB
    0x0000_d903_0353_0000, // 8 KiB
    0x0000_d903_1353_0000, // 16 KiB
    0x0000_d90f_0353_0000, // 32 KiB
    0x0000_d903_0353_7000, // 64 KiB
    0x0000_d907_0353_7000, // 128 KiB
    0x0000_d907_0753_7000, // 256 KiB
    0x0000_d917_0753_7000, // 512 KiB
    0x0000_d917_4753_7000, // 1 MiB
    0x0000_d917_6753_7000, // 2 MiB
    0x0000_d937_6753_7000, // 4 MiB
    0x0000_d937_7753_7000, // 8 MiB
    0x0000_d937_7757_7000, // 16 MiB
    0x0000_db37_7757_7000, // padding, used only at level 3
];

/// Round to the closest base-2 logarithm.
///
/// The float holds every input exactly. Valid averages top out at 2^28.
/// Their logarithms top out at 28.
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)]
fn logarithm2(value: usize) -> u32 {
    (value as f64).log2().round() as u32
}

/// Pick the strict and relaxed masks for an average size.
///
/// The strict mask runs below the average. It holds more bits, so cuts land
/// less often. The relaxed mask runs above it. It holds fewer bits, so cuts
/// land more often. Together they pull cuts toward the average.
fn select_masks(avg_size: usize) -> (u64, u64) {
    // The clamp keeps indexes inside MASKS. `validate` rejects these sizes
    // first. The clamp only guards direct use.
    let bits = logarithm2(avg_size).clamp(1, 24);
    let strict = MASKS[usize::try_from(bits + 1).unwrap_or(MASKS.len() - 1)];
    let relaxed = MASKS[usize::try_from(bits - 1).unwrap_or(0)];
    (strict, relaxed)
}

/// One chunk of the scanned stream.
#[derive(Clone, PartialEq, Eq)]
pub struct SimdChunkData {
    /// Byte offset of the chunk in the stream.
    pub offset: u64,
    /// Length of the chunk in bytes.
    pub length: usize,
    /// Bytes of the chunk.
    pub data: Vec<u8>,
}

impl Debug for SimdChunkData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Contents stay out of logs. Length says enough.
        f.debug_struct("SimdChunkData")
            .field("offset", &self.offset)
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

/// Cut a `Read` into content-defined chunks.
///
/// The stream holds `max_size` bytes at most. It fills the buffer, cuts one
/// chunk, drains it, then reads on. Each chunk restarts the hash at zero, so
/// a cut never depends on bytes before the chunk.
pub struct SimdStream<R> {
    reader: R,
    buf: Vec<u8>,
    len: usize,
    eof: bool,
    processed: u64,
    min_size: usize,
    avg_size: usize,
    max_size: usize,
    mask_strict: u64,
    mask_relaxed: u64,
}

impl<R> Debug for SimdStream<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Buffer holds user bytes. Sizes describe the run.
        f.debug_struct("SimdStream")
            .field("len", &self.len)
            .field("eof", &self.eof)
            .field("processed", &self.processed)
            .field("min_size", &self.min_size)
            .field("avg_size", &self.avg_size)
            .field("max_size", &self.max_size)
            .finish_non_exhaustive()
    }
}

impl<R: Read> SimdStream<R> {
    /// Cut `reader` with `min_size`, `avg_size`, and `max_size`.
    ///
    /// Callers must check sizes first. This constructor trusts them.
    #[must_use]
    pub fn new(reader: R, min_size: usize, avg_size: usize, max_size: usize) -> Self {
        let (mask_strict, mask_relaxed) = select_masks(avg_size);
        Self {
            reader,
            buf: vec![0_u8; max_size],
            len: 0,
            eof: false,
            processed: 0,
            min_size,
            avg_size,
            max_size,
            mask_strict,
            mask_relaxed,
        }
    }

    /// Fill the buffer from the reader.
    fn fill(&mut self) -> io::Result<()> {
        while !self.eof && self.len < self.max_size {
            let read = self.reader.read(&mut self.buf[self.len..])?;
            if read == 0 {
                self.eof = true;
            } else {
                self.len += read;
            }
        }
        Ok(())
    }

    /// Find the cut point inside the filled buffer.
    ///
    /// Bytes below `min_size` warm the hash without cutting. A match there
    /// advances the scan instead of ending the chunk.
    fn cut_point(&self) -> usize {
        let window = &self.buf[..self.len];
        if window.len() <= self.min_size {
            return window.len();
        }
        let center = self.avg_size.min(window.len());
        let mut hasher = Hasher::default();
        let mut pos = 0;
        // The loop only exits through `None`: the strict region holds no cut
        // at or past `min_size`, and the hash state sits at `center`. A match
        // below `min_size` advances the scan instead of ending the chunk, so
        // `pos` never reaches `min_size` without returning.
        while pos < self.min_size {
            match hasher.next_match(&window[pos..center], self.mask_strict) {
                Some(found) if pos + found < self.min_size => pos += found,
                Some(found) => return pos + found,
                None => break,
            }
        }
        if let Some(found) = hasher.next_match(&window[center..], self.mask_relaxed) {
            return center + found;
        }
        window.len()
    }

    /// Drain `count` bytes from the buffer front.
    fn drain(&mut self, count: usize) -> Vec<u8> {
        let data = self.buf[..count].to_vec();
        self.buf.copy_within(count..self.len, 0);
        self.len -= count;
        data
    }

    /// Read and cut the next chunk.
    ///
    /// # Errors
    ///
    /// Returns an error when the reader fails.
    fn read_chunk(&mut self) -> io::Result<Option<SimdChunkData>> {
        self.fill()?;
        if self.len == 0 {
            return Ok(None);
        }
        let count = self.cut_point();
        let offset = self.processed;
        self.processed = self.processed.saturating_add(count as u64);
        let data = self.drain(count);
        let length = data.len();
        Ok(Some(SimdChunkData {
            offset,
            length,
            data,
        }))
    }
}

impl<R: Read> Iterator for SimdStream<R> {
    type Item = io::Result<SimdChunkData>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_chunk() {
            Ok(None) => None,
            Ok(Some(chunk)) => Some(Ok(chunk)),
            Err(error) => Some(Err(error)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::SimdStream;
    use std::io::{Cursor, Read};

    /// Fixed sizes for the tests. They match the default profile.
    const MIN: usize = 8 * 1024;
    /// Fixed sizes for the tests. They match the default profile.
    const AVG: usize = 16 * 1024;
    /// Fixed sizes for the tests. They match the default profile.
    const MAX: usize = 64 * 1024;

    /// Deterministic bytes without long runs.
    fn noisy(len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| {
                let mut x = u64::try_from(i)
                    .unwrap_or(0)
                    .wrapping_mul(0x9E37_79B9_7F4A_7C15);
                x ^= x >> 30;
                x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
                x ^= x >> 27;
                u8::try_from(x & 0xff).unwrap_or(0)
            })
            .collect()
    }

    /// Cut the bytes and return every chunk.
    fn cut(bytes: &[u8]) -> std::io::Result<Vec<super::SimdChunkData>> {
        let stream = SimdStream::new(Cursor::new(bytes.to_vec()), MIN, AVG, MAX);
        stream.collect()
    }

    /// Cuts cover every byte exactly once.
    #[test]
    fn cuts_cover_every_byte() {
        let bytes = noisy(300 * 1024);
        let chunks = cut(&bytes).unwrap();
        let mut offset = 0_u64;
        for chunk in &chunks {
            assert_eq!(chunk.offset, offset);
            offset = offset.saturating_add(u64::try_from(chunk.length).unwrap_or(0));
            let start = usize::try_from(chunk.offset).unwrap_or(bytes.len());
            assert_eq!(chunk.data.as_slice(), &bytes[start..start + chunk.length]);
        }
        assert_eq!(offset, u64::try_from(bytes.len()).unwrap_or(0));
    }

    /// Cuts respect the size bounds, except the final one.
    #[test]
    fn cuts_respect_size_bounds() {
        let chunks = cut(&noisy(300 * 1024)).unwrap();
        assert!(chunks.len() > 1);
        for chunk in &chunks[..chunks.len() - 1] {
            assert!(
                chunk.length >= MIN && chunk.length <= MAX,
                "chunk of {} bytes breaks [{MIN}, {MAX}]",
                chunk.length
            );
        }
    }

    /// Cuts repeat for the same input.
    #[test]
    fn cuts_repeat_for_same_input() {
        let bytes = noisy(300 * 1024);
        assert_eq!(cut(&bytes).unwrap(), cut(&bytes).unwrap());
    }

    /// Empty input cuts nothing.
    #[test]
    fn empty_input_cuts_nothing() {
        assert_eq!(cut(&[]).unwrap().len(), 0);
    }

    /// Short input cuts one chunk.
    #[test]
    fn short_input_cuts_one_chunk() {
        let chunks = cut(&noisy(1024)).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].length, 1024);
    }

    /// Average cut lands near the target average.
    #[test]
    fn average_cut_lands_near_target() {
        let bytes = noisy(1024 * 1024);
        let chunks = cut(&bytes).unwrap();
        let mean = bytes.len() / chunks.len();
        assert!(
            (AVG / 2..=AVG * 2).contains(&mean),
            "mean cut of {mean} strays from {AVG}"
        );
    }

    /// A reader that yields one byte per call.
    #[derive(Debug)]
    struct Dripper {
        bytes: Vec<u8>,
        pos: usize,
    }

    impl Read for Dripper {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.pos >= self.bytes.len() || buf.is_empty() {
                return Ok(0);
            }
            buf[0] = self.bytes[self.pos];
            self.pos += 1;
            Ok(1)
        }
    }

    /// Short reads cut the same bytes.
    #[test]
    fn short_reads_cut_same_bytes() {
        let bytes = noisy(300 * 1024);
        let dripper = Dripper {
            bytes: bytes.clone(),
            pos: 0,
        };
        let stream = SimdStream::new(dripper, MIN, AVG, MAX);
        let chunks = stream.collect::<std::io::Result<Vec<_>>>().unwrap();
        assert_eq!(chunks, cut(&bytes).unwrap());
    }
}
