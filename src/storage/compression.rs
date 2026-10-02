//! How record payloads are compressed on the way in — and, more often, not.
//!
//! The policy is handed to the writer and the reader never sees it: every record
//! names the codec it was written with, so a set written at mixed levels, or
//! with compression off, is still one store and still deduplicates. That is
//! what makes the fallback in `compress` safe, and on a real tree the fallback
//! is the common case — photos, archives and video are already entropy-coded,
//! and a frame around one comes out *larger* than the bytes it replaced.
//!
//! [`Compressor`] and [`Decompressor`] are the two halves of that policy as a
//! seam, so a caller outside this crate can bring its own without the store
//! learning about it. [`Compression`] implements the first and [`Zstd`] the
//! second, which is the whole of what this crate ships.
//!
//! The measurements behind the default level are in the README, and
//! `tests/compression.rs` is what holds the store's other properties still true
//! across a codec byte.

use std::{borrow::Cow, io};

use super::record::Codec;

/// How a payload becomes the bytes a record stores.
///
/// The seam is *how a payload is written*, not *which codec*: an implementation is
/// handed a payload and answers with the bytes to write and the codec to record
/// beside them. Nothing here is about zstd, about a level, or about whether
/// compressing is a good idea for this payload — [`Compression`] is one
/// implementation and the one this crate ships, but a caller with its own policy
/// (a size threshold, a level of its own, a rule about which payloads are worth a
/// frame) implements this trait and hands it to
/// [`BlobStore::open_for_writing`](super::BlobStore::open_for_writing) instead,
/// without touching a line of the store.
///
/// What a caller cannot do through this trait is add a codec. [`Codec`] has two
/// arms and the store decodes one of them, so a policy is a choice about *which
/// payloads* get a frame, at what level, and whether one is built at all — and
/// the codec that comes back is advice, because the store records its own:
/// `Zstd` only for bytes it will keep, and `Raw` for the payload it hashed.
/// Naming a third codec would mean a new on-disk format, which is a larger
/// change than the seam is for.
///
/// # The fallback belongs to the writer, not to the trait
///
/// The one rule a payload must never break is **a frame is stored only when it is
/// strictly smaller than the payload it replaces**. It is documented on
/// [`Compressor::compress`] as an obligation and *not* left to the obligation:
/// [`super::BlobStore`] re-checks the answer and stores the payload untouched
/// whenever the returned bytes came back no smaller, or named a codec the store
/// cannot decode. So the guarantee is a property of the store rather than a
/// promise every implementor has to remember, and a third-party compressor
/// cannot grow a set by accident, or store bytes under a name that means
/// something else.
///
/// The alternative — leaving it to the implementor, and trusting it — was rejected
/// for the obvious reason: the cost of getting it wrong is paid by the user's
/// disk, not by the crate, and the mistake is invisible in review because a
/// compressor that returns a frame is *doing what it was asked*. A compressor that
/// is always allowed to answer is also a compressor that can always be wrong.
///
/// [`Compression`] applies the rule itself as well, which is not redundant: it is
/// what lets it hand back the input borrowed rather than an owned frame the store
/// would drop, and the frame has to be built either way to be measured against the
/// input. So the rule is stated once, where the size is compared, and enforced
/// once, where the bytes are written.
///
/// The lifetime is named rather than elided: with two input lifetimes, elision
/// would tie the returned borrow to `&self` rather than to `raw`, which is the
/// opposite of what every implementation wants — the answer usually borrows the
/// payload and never touches the policy.
pub trait Compressor: Send + Sync + 'static {
    /// The bytes to store for `raw`, and the codec to record beside them.
    ///
    /// Returning [`Codec::Raw`] means "store these bytes as they are", which is a
    /// legitimate answer for any payload and not a failure: the overwhelming
    /// majority of a real tree is already entropy-coded, and a frame around one
    /// comes out larger.
    ///
    /// The answer is not trusted to be an improvement, and not trusted to name a
    /// codec either — see the note on the trait — so the store stores `raw`
    /// untouched unless the returned bytes are strictly smaller and the codec is
    /// one it can decode. `Cow` rather than `Vec` for the reason [`Compression`]
    /// gives: most answers are the input, unchanged.
    fn compress<'a>(&self, raw: &'a [u8]) -> (Codec, Cow<'a, [u8]>);
}

impl Compressor for Compression {
    fn compress<'a>(&self, raw: &'a [u8]) -> (Codec, Cow<'a, [u8]>) {
        // Fully qualified, because the inherent method and this one are the same
        // operation under two names: the inherent one is what the tests in this
        // module call, and it is not going away.
        Compression::compress(*self, raw)
    }
}

/// How stored bytes become a payload again.
///
/// The mirror of [`Compressor`], and separate from it for the reason the two are
/// separate at all: a write policy is chosen by whoever opened the store, while a
/// read is answered by the codec byte in the record being read. A caller reading a
/// set written under a policy it has never heard of still has to decode it, so the
/// reader is not given a policy — it is given the codec, and this is what the
/// codec means.
///
/// [`Zstd`] is the only implementation, because [`Codec`] has only one
/// non-trivial arm. It is a trait rather than a call to `zstd::bulk::decompress`
/// because that call has more to it than the decode — it validates a frame against
/// the length its record claims, which is a trust decision about the file and not a
/// detail of the codec — and because the two places that decode were each doing
/// that validation for themselves.
///
/// Return-position `impl Future` for the `+ Send`, and for the same reason as
/// [`super::StorageGet::get`](super::StorageGet): a decode runs on the blocking
/// pool and its future is awaited from a `get`, which is spawned.
pub trait Decompressor: Send + Sync + 'static {
    /// Decode `stored` into the payload, which must come back exactly `raw_len`
    /// bytes long.
    ///
    /// `raw_len` is the record's claim, not a measurement: a record header carries a
    /// truncated hash rather than a MAC, so it is checked against the frame and
    /// never trusted. An implementation must refuse a frame that does not produce
    /// exactly that many bytes rather than hand back a short read — a short read
    /// looks like end of stream, and a restore that trusts one produces a file that
    /// is quietly incomplete.
    ///
    /// # Errors
    ///
    /// Returns an error if `stored` is not a frame this codec recognises, if the
    /// frame does not say how many bytes it decodes to, if that number is not
    /// `raw_len`, or if `raw_len` is not something this process can hold.
    fn decompress(
        &self,
        stored: Vec<u8>,
        raw_len: u64,
    ) -> impl Future<Output = io::Result<Vec<u8>>> + Send;
}

/// The only [`Decompressor`] this crate ships: zstd frames.
///
/// A unit struct rather than an enum because there is one thing to be, and a type
/// that says which is what an implementor of [`Decompressor`] needs to name. It
/// carries no level: decoding does not take one, and a frame records the level it
/// was written at in its own header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Zstd;

/// The body of [`Zstd::decompress`], split out so the trait method above can keep
/// its return-position `impl Future` — the `manual_async_fn` lint points at a
/// signature that names the future rather than an `async fn`, and here the naming
/// is the point: it is what says the future can cross a thread, which an `async
/// fn` in a trait does not.
impl Zstd {
    async fn decode(&self, stored: Vec<u8>, raw_len: u64) -> io::Result<Vec<u8>> {
        let expected = usize::try_from(raw_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("a payload of {raw_len} bytes does not fit in memory"),
            )
        })?;

        // `None` means the frame does not declare a size, which is what a
        // compressed *stream* looks like. Nothing here produces one, and
        // accepting one would mean decoding blind.
        let declared = zstd::zstd_safe::get_frame_content_size(&stored)
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("a compressed payload is not a readable zstd frame: {e}"),
                )
            })?
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "a compressed payload does not say how many bytes it decodes to",
                )
            })?;

        if usize::try_from(declared).unwrap_or(usize::MAX) != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "a compressed payload declares {declared} bytes but its record claims \
                     {expected}"
                ),
            ));
        }

        // Frame and header agree, so the decode can come up neither short nor long.
        let bytes = tokio::task::spawn_blocking(move || zstd::bulk::decompress(&stored, expected))
            .await
            .map_err(|join| io::Error::other(format!("decompression panicked: {join}")))??;

        if bytes.len() != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "a compressed payload decoded to {} bytes but its record claims {expected}",
                    bytes.len()
                ),
            ));
        }

        Ok(bytes)
    }
}

impl Decompressor for Zstd {
    fn decompress(
        &self,
        stored: Vec<u8>,
        raw_len: u64,
    ) -> impl Future<Output = io::Result<Vec<u8>>> + Send {
        self.decode(stored, raw_len)
    }
}

/// How record payloads are compressed on the way in.
///
/// A write policy, not a format property: every record names the codec it was
/// written with and hashes the content rather than the stored bytes, so a set
/// written at mixed levels — or with compression off — reads back and
/// deduplicates correctly. The default is [`Compression::Zstd`] at level 1; the
/// measurements behind that are in the README.
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
    /// This is the inherent form of [`Compressor::compress`], and it is what the
    /// store calls through the trait. It stays as a method because the unit tests
    /// below pin the fallback through it, and a test that went through a trait
    /// object to assert a pointer was not the clearest way to say what it meant.
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

    use super::{Codec, Compression, Decompressor, Zstd};

    /// Bytes that are not a frame at all: the first thing a damaged or
    /// hand-edited record can be.
    const NOT_A_FRAME: &[u8] = b"these bytes are not a zstd frame";

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

    /// The message a caller sees when the stored bytes are not a frame.
    ///
    /// The texts in [`Zstd`] are the store's report of a damaged record, and a
    /// restore is the only thing standing between a backup and a silently wrong
    /// file — so they are part of what this crate promises. Asserting them here
    /// is what stops that being true only because nobody has reworded them yet.
    ///
    /// `InvalidData` and the "not a readable zstd frame" wording are both
    /// asserted: the kind is what a caller can branch on, and zstd's own `{e}` is
    /// part of the message because it is what says which frame complaint this was.
    ///
    /// The panic path below it — a decode that panics inside `spawn_blocking` —
    /// is deliberately not covered, because it cannot be reached from a test
    /// without contrivance: it needs `zstd::bulk::decompress` itself to panic, and
    /// this API gives a caller no way to arrange that. It is mapped to
    /// `"decompression panicked: …"` so a panic cannot take the run down with it,
    /// not because a test gets there.
    #[tokio::test]
    async fn bytes_that_are_not_a_frame_are_reported_as_such() {
        let err = Zstd
            .decompress(NOT_A_FRAME.to_vec(), 1024)
            .await
            .unwrap_err();

        assert_eq!(
            err.kind(),
            std::io::ErrorKind::InvalidData,
            "a payload that is not a frame is damaged data, not an I/O failure"
        );
        assert!(
            err.to_string()
                .contains("a compressed payload is not a readable zstd frame:"),
            "the message should name what was wrong and with which codec, and says {:?}",
            err.to_string()
        );
    }

    /// A frame that decodes to the wrong number of bytes is refused, by name.
    ///
    /// The check this is, on a frame that is otherwise perfectly good: a header
    /// that lies about the payload's length would otherwise decode to something,
    /// and a restore would write it out as if it were the file.
    #[tokio::test]
    async fn a_frame_that_decodes_to_the_wrong_size_is_refused_by_name() {
        let raw = vec![b'a'; 64 * 1024];
        let frame = zstd::bulk::compress(&raw, 1).unwrap();
        let short = u64::try_from(raw.len() - 1).unwrap();

        let err = Zstd.decompress(frame, short).await.unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string()
                .contains("declares 65536 bytes but its record claims 65535"),
            "the message should say what the frame declares and what the record claims, \
             and says {:?}",
            err.to_string()
        );
    }

    /// A frame that declares no size at all is refused rather than decoded blind.
    ///
    /// A *streaming* frame is what looks like this: the encoder did not know the
    /// length up front, so the header does not carry it. Nothing this crate
    /// writes produces one, which is exactly why accepting one would mean decoding
    /// a record with nothing to check the answer against.
    #[tokio::test]
    async fn a_frame_that_declares_no_size_is_refused_by_name() {
        let mut frame = Vec::new();
        zstd::stream::copy_encode(
            &b"the quick brown fox jumps over the lazy dog. "[..],
            &mut frame,
            1,
        )
        .unwrap();

        let err = Zstd.decompress(frame, 44).await.unwrap_err();

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string()
                .contains("does not say how many bytes it decodes to"),
            "a frame with no declared size should be refused in those words, and says {:?}",
            err.to_string()
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
