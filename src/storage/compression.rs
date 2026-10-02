//! How record payloads are compressed on the way in — and, more often, not.
//!
//! The policy is handed to the writer and the reader never sees it: every record
//! names the codec it was written with, so a set written at mixed levels, or
//! with compression off, is still one store and still deduplicates. That is
//! what makes the fallback in `compress` safe, and on a real tree the fallback
//! is the common case — photos, archives and video are already entropy-coded,
//! and a frame around one comes out *larger* than the bytes it replaced.
//!
//! The measurements behind the default level are in the README, and
//! `tests/compression.rs` is what holds the store's other properties still true
//! across a codec byte.

use std::borrow::Cow;

use super::record::Codec;

/// How record payloads are compressed on the way in.
///
/// A write policy, not a format property: every record names the codec it was
/// written with and hashes the content rather than the stored bytes, so a set
/// written at mixed levels — or with compression off — reads back and
/// deduplicates correctly. The default is [`Compression::Zstd`] at level 1; the
/// measurements behind that are in the README.
/// A write policy, chosen per run and recorded per record.
///
/// # Example
///
/// `no_run`, unlike the examples beside it: opening a store creates a segment on
/// disk, so this one compiles but must not execute.
///
/// ```no_run
/// # async fn example() -> std::io::Result<()> {
/// use lepatch::storage::{BlobStore, Compression, SegmentLimit};
///
/// // The defaults: zstd level 1, four gibibytes per segment.
/// let store = BlobStore::open_for_writing(
///     "photos",
///     SegmentLimit::default(),
///     Compression::default(),
/// ).await?;
///
/// // Or not. Both are write policies, and a set written under one reads back
/// // under the other: a compressed record says so in its own header, and which
/// // records are compressed is a question each record answers for itself.
/// let store = BlobStore::open_for_writing(
///     "photos",
///     SegmentLimit::new(64 * 1024 * 1024),
///     Compression::Off,
/// ).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// Store payloads as they are. What [`Compression::from_level`] returns for 0.
    Off,
    /// Compress payloads as zstd frames at `level`.
    Zstd {
        /// The zstd compression level, 1 (fastest, default) to 9 (smallest).
        level: u8,
    },
}

/// The highest level offered. zstd goes further; see [`Compression`].
pub const MAX_COMPRESSION_LEVEL: u8 = 9;

impl Default for Compression {
    fn default() -> Self {
        Self::Zstd { level: 1 }
    }
}

impl Compression {
    /// zstd at `level`, clamped to 1..=[`MAX_COMPRESSION_LEVEL`].
    ///
    /// The only way to name a zstd policy. A level of 0 clamps to 1, not to
    /// [`Compression::Off`], because the caller asked for zstd and the two are
    /// different requests: `Off` is a decision to store payloads as they are, and
    /// silently taking it would be a store written at a policy nobody chose.
    /// [`Compression::from_level`] is how a user's 0 becomes `Off`, and it is a
    /// separate function because that is a separate question.
    #[must_use]
    pub fn zstd(level: u8) -> Self {
        Self::Zstd {
            level: level.clamp(1, MAX_COMPRESSION_LEVEL),
        }
    }

    /// The policy for a level the way a user counts them: 0 stores payloads as they
    /// are, and 1 to 9 are zstd levels.
    ///
    /// The difference from [`Compression::zstd`] is the `0` arm and nothing else,
    /// which is the whole reason they are two functions. One of them means "use
    /// zstd at this level" and the other means "this is a level from a command
    /// line, where 0 has always meant off". A single function would have to
    /// answer both questions, and it would answer the first one wrongly for
    /// anyone who passed 0 meaning zstd's floor.
    ///
    /// Out-of-range values are clamped rather than rejected, so a bad number gives
    /// a working policy rather than a panic. A command line rejects them instead,
    /// where the user can be told.
    #[must_use]
    pub fn from_level(level: u8) -> Self {
        if level == 0 {
            Self::Off
        } else {
            Self::zstd(level)
        }
    }

    /// The zstd level, or `None` when compression is off.
    #[must_use]
    pub fn level(self) -> Option<u8> {
        match self {
            Self::Off => None,
            Self::Zstd { level } => Some(level),
        }
    }

    /// Compress `raw`, returning the codec to record and the bytes to store.
    ///
    /// A payload becomes a zstd frame only when that frame is strictly smaller
    /// than the input; otherwise [`Codec::Raw`] and the bytes unchanged, so
    /// already-compressed content never grows by a frame header.
    ///
    /// The bytes are a [`Cow`] because three of the four answers are "the input,
    /// unchanged", and returning a [`Vec`] made each of them a full copy of a
    /// payload the caller was already holding. That is not a rare path either: it
    /// is every payload with compression off, and every payload zstd cannot make
    /// smaller — which is most of a tree of photos, archives or video, since a
    /// zstd frame on content like that comes out *larger* and takes the fallback.
    /// So the common case copied every byte of the tree an extra time to then hand
    /// back exactly what it was given.
    ///
    /// The copy for the fourth answer is unavoidable and worth it: a frame is
    /// built before it is known to be smaller than the input.
    pub(super) fn compress(self, raw: &[u8]) -> (Codec, Cow<'_, [u8]>) {
        let Some(level) = self.level() else {
            return (Codec::Raw, Cow::Borrowed(raw));
        };

        // A compressor failure is not an error: the fallback stores the bytes as
        // they are, which is a correct record and beats failing a backup.
        let Ok(encoded) = zstd::bulk::compress(raw, i32::from(level)) else {
            return (Codec::Raw, Cow::Borrowed(raw));
        };

        if encoded.len() >= raw.len() {
            return (Codec::Raw, Cow::Borrowed(raw));
        }

        (Codec::Zstd, Cow::Owned(encoded))
    }
}

#[cfg(test)]
mod compression_tests {
    use std::borrow::Cow;

    use super::{Codec, Compression};

    /// A payload the compressor cannot make smaller falls through untouched.
    ///
    /// The three "the input, unchanged" answers are the ones worth pinning, and
    /// pointer identity is how: the borrowed variant's data address is the input's
    /// address, so a copy cannot have happened. A timing comparison cannot say
    /// this — a backup here turns out to be bound by per-record filesystem work
    /// rather than by memory bandwidth, so removing a full copy of every payload
    /// measures as nothing. Whether the copy happens is a fact about the code, and
    /// this asserts it directly.
    ///
    /// Incompressible input is the realistic trigger, not a corner: photos, video
    /// and archives are most of a real tree, and a zstd frame around one is larger
    /// than the payload, so every one of them takes the fallback.
    #[test]
    fn a_payload_the_compressor_cannot_shrink_is_handed_back_untouched() {
        let raw = incompressible(64 * 1024);

        for (what, policy) in [
            ("compression off", Compression::Off),
            ("compression on", Compression::zstd(9)),
        ] {
            let (codec, stored) = policy.compress(&raw);

            assert_eq!(
                codec,
                Codec::Raw,
                "{what}: a payload that does not compress is stored as it is"
            );
            assert_eq!(
                stored.as_ref(),
                raw.as_slice(),
                "{what}: the fallback must return the bytes unchanged"
            );
            assert!(
                matches!(stored, Cow::Borrowed(_)),
                "{what}: the fallback copied {} bytes it was handed",
                raw.len()
            );
            assert_eq!(
                stored.as_ptr(),
                raw.as_ptr(),
                "{what}: the borrowed variant must point at the input, not a copy"
            );
        }
    }

    /// Compression that works still owns its buffer.
    ///
    /// The complement, so the previous test cannot be satisfied by never
    /// compressing anything: when a frame *is* smaller, it is a fresh allocation
    /// and does not alias the input.
    #[test]
    fn a_payload_that_compresses_is_an_owned_copy() {
        let raw = vec![b'a'; 64 * 1024];
        let (codec, stored) = Compression::zstd(3).compress(&raw);

        assert_eq!(codec, Codec::Zstd, "repetitive input must compress");
        assert!(
            stored.len() < raw.len(),
            "a zstd frame of {} identical bytes should be much smaller, got {}",
            raw.len(),
            stored.len()
        );
        assert!(
            matches!(stored, Cow::Owned(_)),
            "a compressed payload owns its frame"
        );
    }

    /// Bytes zstd cannot compress, without needing a fixture on disk.
    ///
    /// BLAKE3 of a counter, so the input has no structure to find but is not
    /// obviously patterned either — the shape that defeats a general-purpose
    /// compressor. Asserted here rather than assumed, because a test that
    /// *assumes* its input is incompressible passes for the wrong reason when it
    /// is not: it would be testing the compressed path and calling it the fallback.
    fn incompressible(len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut counter = 0u64;
        while out.len() < len {
            out.extend_from_slice(blake3::hash(&counter.to_le_bytes()).as_bytes());
            counter = counter.wrapping_add(1);
        }
        out.truncate(len);

        assert!(
            zstd::bulk::compress(&out, 19).map_or(true, |z| z.len() >= out.len()),
            "the fixture is supposed to be incompressible, and zstd made it {} bytes \
             from {} — this test would be exercising the compressed path",
            zstd::bulk::compress(&out, 19).map_or(0, |z| z.len()),
            out.len()
        );
        out
    }
}
