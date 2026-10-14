// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! What names a blob, and the short digest that the formats check themselves
//! with.
//!
//! Two things, both one line long and both BLAKE3. The first is the full hash
//! that addresses a chunk. The second is the first `N` bytes of that hash. A
//! record header and a version index file use those bytes to hash the bytes
//! ahead of themselves.
//!
//! They live together because the record format needs both of them, and nothing
//! else in the module needs either one. A header carries a chunk address and its
//! own checksum. A checksum short enough to sit in a fixed-width field is the
//! same computation as the address, cut off.

/// Address of a blob: the BLAKE3 hash of its contents.
///
/// Both a chunk's identity in a snapshot and its key in the store's index.
///
/// This is [`blake3::Hash`] rather than a `[u8; 32]` of our own. The
/// `traits-preview` feature on the `blake3` dependency has been providing this
/// type for the whole life of the crate. The alias it replaced was not a newtype.
/// `BlobId` and `[u8; 32]` were the same type, and they were interchangeable in
/// both directions, so the alias distinguished nothing. The crate also spelled
/// the type three ways. Most places use `BlobId`. `command::pipeline` and
/// `metadata` use `[u8; 32]`. `storage::record` uses `[u8; HASH]`, where
/// `HASH` is 32.
///
/// `blake3::Hash` brings what this crate was writing by hand:
///
/// * [`Display`](std::fmt::Display) as lowercase hex. Every message and log
///   line in the crate wanted that form. A `Hex` wrapper existed until this
///   change landed.
/// * [`From<[u8; 32]>`](blake3::Hash::from_bytes) and
///   [`as_bytes`](blake3::Hash::as_bytes), so the record header and the
///   snapshot's chunk table keep writing the same 32 bytes.
/// * `PartialEq` against `[u8; 32]` and `[u8]`, both constant-time, so
///   comparing a computed hash to a decoded one needs no conversion.
/// * [`Copy`], [`Hash`] and [`Eq`], so the type works as a map key.
///
/// It deliberately has no `Ord`, no `AsRef<[u8]>` and no `Deref`. Asking for
/// the bytes is how you get them. A `Deref` to a byte array would hand out the
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
