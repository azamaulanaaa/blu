//! What a blob is named, and the short digest the formats check themselves with.
//!
//! Two things, both one line long and both BLAKE3: the full hash a chunk is
//! addressed by, and the first `N` bytes of one, which is what a record header
//! and a version index file hash over the bytes ahead of themselves.
//!
//! They live together because the record format needs both of them and nothing
//! else in the module needs either: a header carries a chunk's address and its
//! own checksum, and a checksum short enough to sit in a fixed-width field is
//! the same computation as the address, cut off.

/// Address of a blob: the BLAKE3 hash of its contents.
///
/// Both a chunk's identity in a snapshot and its key in the store's index.
///
/// This is [`blake3::Hash`] rather than a `[u8; 32]` of our own, which is what
/// the `traits-preview` feature on the `blake3` dependency has been providing
/// this whole time. The alias it replaced was not a newtype — `BlobId` and
/// `[u8; 32]` were the same type, interchangeable in both directions, so it
/// distinguished nothing — and it was spelled three ways across the crate:
/// `BlobId` in most places, `[u8; 32]` in `command::pipeline` and `metadata`,
/// and `[u8; HASH]` in `storage::record`, with `HASH` being 32.
///
/// `blake3::Hash` brings what this crate was writing by hand:
///
/// * [`Display`](std::fmt::Display) as lowercase hex, which is what every
///   message and log line in the crate wanted, and is why a `Hex` wrapper
///   existed until this landed;
/// * [`From<[u8; 32]>`](blake3::Hash::from_bytes) and
///   [`as_bytes`](blake3::Hash::as_bytes), so the record header and the
///   snapshot's chunk table keep writing the same 32 bytes;
/// * `PartialEq` against `[u8; 32]` and `[u8]`, both constant-time, so
///   comparing a computed hash to a decoded one needs no conversion;
/// * [`Copy`], [`Hash`] and [`Eq`], so it is usable as a map key as it was.
///
/// It deliberately has no `Ord`, no `AsRef<[u8]>` and no `Deref`: asking for the
/// bytes is how you get them, and a `Deref` to a byte array would hand out the
/// non-constant-time comparison that `Hash` exists to avoid.
///
/// [`from_slice`](blake3::Hash::from_slice) is `TryFrom<&[u8]>`, so a length
/// error is a type error rather than a check.
pub type BlobId = blake3::Hash;

/// The first `N` bytes of a BLAKE3 hash, for a checksum over fixed-size bytes.
///
/// The record header and the version index file both use it.
pub(super) fn truncated_hash<const N: usize>(bytes: &[u8]) -> [u8; N] {
    let mut out = [0u8; N];
    out.copy_from_slice(&blake3::hash(bytes).as_bytes()[..N]);
    out
}
