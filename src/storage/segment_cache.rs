//! A per-segment blob cache for sealed segments.
//!
//! The cache holds the blob map of a sealed segment.
//! It saves the memory and the time of building that map.
//! It does not skip the scan of a segment.
//! Verification still reads every header of every sealed segment.
//!
//! ## This is a cache
//!
//! Treat segment files as truth.
//! Treat cache files as hints.
//! Check every hint before use.
//! Rebuild hints after each write.
//!
//! ## Verification steps
//!
//! - Read the cache file.
//! - Check magic and versions.
//! - Check checksum over header.
//! - Check store header of segment.
//! - Check segment length matches cache.
//! - Scan commit records only.
//! - Compare the digest of the record headers with the cache.
//! - Compare commits with cache.
//! - Use cached blobs on match.
//! - Fall back on any mismatch.
//!
//! ## Writer rules
//!
//! - Cache sealed segments only.
//! - Scan the last segment fully.
//! - Write caches on writer open.
//! - Never write caches on reader open.
//! - Write caches atomically with rename.

use std::{collections::HashMap, fs, io, path::Path};

use crate::{
    layout::IndexSet,
    storage::{BlobId, record},
};

/// Magic at the start of every segment cache file.
///
/// Write this magic on encode.
/// Check this magic on decode.
/// Refuse files with other bytes.
const CACHE_MAGIC: [u8; 4] = *b"LSC1";

/// Version of the cache encoding.
///
/// Bump this version on format change.
/// Refuse caches with other versions.
/// Rebuild refused caches from segments.
///
/// **2** added the digest of the record headers.
/// A cache from version 1 cannot prove that its segment still holds the
/// headers it describes.
/// The store therefore refuses a file of version 1 and rebuilds the cache
/// from the segment.
const CACHE_VERSION: u32 = 2;

/// Bytes of cache header before counts.
///
/// Count magic and versions and segment and length and digest.
/// Keep header fixed width.
/// Parse counts after header.
const CACHE_HEADER: usize = 4 + 4 + 4 + 4 + 8 + 32;

/// Bytes of one cached blob entry.
///
/// Count hash and offsets and lengths and codec.
/// Keep entries fixed width.
/// Parse entries in a loop.
const BLOB_ENTRY: usize = 32 + 8 + 8 + 8 + 1;

/// Bytes of checksum at end of file.
///
/// Hash all bytes ahead of checksum.
/// Store first eight bytes.
/// Check hash on read.
const CHECK: usize = 8;

/// What a verified cache holds for one sealed segment.
///
/// Hold segment number for checks.
/// Hold segment length for checks.
/// Hold the digest of the record headers for checks.
/// Hold commits for checks.
/// Hold blobs for reuse.
#[derive(Debug)]
pub(super) struct CachedSegment {
    /// Which segment this cache describes.
    pub segment: u32,
    /// Bytes the segment file held on write.
    pub segment_len: u64,
    /// Digest of the record headers of the segment, in file order.
    pub digest: record::HeaderDigest,
    /// Snapshot hash of each commit in order.
    pub commits: Vec<BlobId>,
    /// Every blob in the segment by hash.
    pub blobs: HashMap<BlobId, record::Placement>,
}

/// Encode a sealed segment scan into bytes.
///
/// Take segment number as identity.
/// Take segment length as freshness.
/// Take the digest of the record headers as proof.
/// Take commits in log order.
/// Take blobs with placements.
/// Append checksum over all bytes.
fn encode(
    segment: u32,
    segment_len: u64,
    digest: record::HeaderDigest,
    commits: &[BlobId],
    blobs: &HashMap<BlobId, record::Placement>,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&CACHE_MAGIC);
    out.extend_from_slice(&CACHE_VERSION.to_le_bytes());
    out.extend_from_slice(&record::FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&segment.to_le_bytes());
    out.extend_from_slice(&segment_len.to_le_bytes());
    out.extend_from_slice(digest.as_bytes());
    let commit_count = u64::try_from(commits.len()).unwrap_or(u64::MAX);
    out.extend_from_slice(&commit_count.to_le_bytes());
    for id in commits {
        out.extend_from_slice(id.as_bytes());
    }
    // Sort entries for stable bytes.
    // Keep output deterministic across runs.
    // Simplify tests and debugging.
    let mut entries: Vec<(&BlobId, &record::Placement)> = blobs.iter().collect();
    entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let blob_count = u64::try_from(entries.len()).unwrap_or(u64::MAX);
    out.extend_from_slice(&blob_count.to_le_bytes());
    for (id, placement) in entries {
        out.extend_from_slice(id.as_bytes());
        out.extend_from_slice(&placement.offset.to_le_bytes());
        out.extend_from_slice(&placement.stored_len.to_le_bytes());
        out.extend_from_slice(&placement.raw_len.to_le_bytes());
        out.push(placement.codec.to_byte());
    }
    let guard: [u8; CHECK] = crate::storage::hash::truncated_hash(&out);
    out.extend_from_slice(&guard);
    out
}

/// The plain facts one cache header states.
///
/// Hold the segment number as identity.
/// Hold the segment length as freshness.
/// Hold the digest of the record headers as proof.
/// Hold the body end for the entry parsers.
#[derive(Debug, Clone, Copy)]
struct CacheHead {
    /// The segment this cache describes.
    segment: u32,
    /// The length the segment file had when the cache was written.
    segment_len: u64,
    /// The digest of the record headers of the segment this cache describes.
    digest: record::HeaderDigest,
    /// Bytes between the header and the checksum.
    body: usize,
}

/// Check the fixed part of one cache file.
///
/// Check the length first.
/// Check the magic and both versions next.
/// Check the checksum over the body last.
fn decode_head(bytes: &[u8]) -> io::Result<CacheHead> {
    if bytes.len() < CACHE_HEADER + 8 + 8 + CHECK {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a segment cache is too short to hold a header",
        ));
    }
    if bytes[..4] != CACHE_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a segment cache does not start with cache magic",
        ));
    }
    let cache_version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    if cache_version != CACHE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a segment cache uses an unknown cache version",
        ));
    }
    let record_version = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    if record_version != record::FORMAT_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a segment cache uses an unknown record version",
        ));
    }
    let mut len_bytes = [0u8; 8];
    len_bytes.copy_from_slice(&bytes[16..24]);
    let mut digest_bytes = [0u8; 32];
    digest_bytes.copy_from_slice(&bytes[24..56]);
    let head = CacheHead {
        segment: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
        segment_len: u64::from_le_bytes(len_bytes),
        digest: record::HeaderDigest::from_bytes(digest_bytes),
        body: bytes.len() - CHECK,
    };
    let mut guard = [0u8; CHECK];
    guard.copy_from_slice(&bytes[head.body..]);
    let expect: [u8; CHECK] = crate::storage::hash::truncated_hash(&bytes[..head.body]);
    if guard != expect {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a segment cache does not match its own checksum",
        ));
    }
    Ok(head)
}

/// Decode one cache's commit list.
///
/// Take the bytes, the list start, and the body end.
/// Bound every entry against the body.
fn decode_commits(bytes: &[u8], start: usize, body: usize) -> io::Result<(Vec<BlobId>, usize)> {
    let mut cursor = start;
    if cursor + 8 > body {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a segment cache ends before its commit count",
        ));
    }
    let mut count_bytes = [0u8; 8];
    count_bytes.copy_from_slice(&bytes[cursor..cursor + 8]);
    let commit_count = usize::try_from(u64::from_le_bytes(count_bytes)).unwrap_or(usize::MAX);
    cursor += 8;
    let mut commits = Vec::new();
    for _ in 0..commit_count {
        if cursor + 32 > body {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a segment cache ends inside its commit list",
            ));
        }
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes[cursor..cursor + 32]);
        commits.push(BlobId::from_bytes(hash));
        cursor += 32;
    }
    Ok((commits, cursor))
}

/// Decode one cache's blob list.
///
/// Take the bytes, the list start, the body end, and the segment.
/// Tag every placement with the segment.
fn decode_blobs(
    bytes: &[u8],
    start: usize,
    body: usize,
    segment: u32,
) -> io::Result<(HashMap<BlobId, record::Placement>, usize)> {
    let mut cursor = start;
    if cursor + 8 > body {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a segment cache ends before its blob count",
        ));
    }
    let mut count_bytes = [0u8; 8];
    count_bytes.copy_from_slice(&bytes[cursor..cursor + 8]);
    let blob_count = usize::try_from(u64::from_le_bytes(count_bytes)).unwrap_or(usize::MAX);
    cursor += 8;
    let mut blobs = HashMap::new();
    for _ in 0..blob_count {
        if cursor + BLOB_ENTRY > body {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a segment cache ends inside its blob list",
            ));
        }
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes[cursor..cursor + 32]);
        let mut eight = [0u8; 8];
        eight.copy_from_slice(&bytes[cursor + 32..cursor + 40]);
        let offset = u64::from_le_bytes(eight);
        eight.copy_from_slice(&bytes[cursor + 40..cursor + 48]);
        let stored_len = u64::from_le_bytes(eight);
        eight.copy_from_slice(&bytes[cursor + 48..cursor + 56]);
        let raw_len = u64::from_le_bytes(eight);
        let codec = record::Codec::from_byte(bytes[cursor + 56]).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "a segment cache names an unknown codec",
            )
        })?;
        blobs.insert(
            BlobId::from_bytes(hash),
            record::Placement {
                segment,
                offset,
                stored_len,
                raw_len,
                codec,
            },
        );
        cursor += BLOB_ENTRY;
    }
    Ok((blobs, cursor))
}

/// Decode cache bytes into a segment.
///
/// Check the header first.
/// Parse the commit list next.
/// Parse the blob list after that.
/// Refuse bytes past the blob list.
fn decode(bytes: &[u8]) -> io::Result<CachedSegment> {
    let head = decode_head(bytes)?;
    let (commits, at) = decode_commits(bytes, CACHE_HEADER, head.body)?;
    let (blobs, at) = decode_blobs(bytes, at, head.body, head.segment)?;
    if at != head.body {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a segment cache holds bytes past its blob list",
        ));
    }
    Ok(CachedSegment {
        segment: head.segment,
        segment_len: head.segment_len,
        digest: head.digest,
        commits,
        blobs,
    })
}

/// Write a cache file atomically.
///
/// Write to a temp file first.
/// Sync the temp file fully.
/// Rename temp into place.
/// Sync the parent directory.
fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut tmp = tempfile::NamedTempFile::new_in(dir).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("could not create temp file near {}", path.display()),
        )
    })?;
    {
        use std::io::Write as _;
        tmp.write_all(bytes)
            .map_err(|e| io::Error::new(e.kind(), format!("could not write {}", path.display())))?;
        tmp.as_file()
            .sync_all()
            .map_err(|e| io::Error::new(e.kind(), format!("could not sync {}", path.display())))?;
    }
    tmp.persist(path).map_err(|e| {
        io::Error::new(
            e.error.kind(),
            format!("could not rename temp file into {}", path.display()),
        )
    })?;
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        super::at(parent, "open to sync it", fs::File::open(parent))?.sync_all()?;
    }
    Ok(())
}

/// Store a sealed segment scan for the next open.
///
/// Encode the digest and the commits and the blobs.
/// Write bytes atomically.
/// Log failures at debug level.
/// Never fail the open.
pub(super) fn store(
    set: &IndexSet,
    segment: u32,
    segment_len: u64,
    digest: record::HeaderDigest,
    commits: &[BlobId],
    blobs: &HashMap<BlobId, record::Placement>,
) {
    let path = set.segment_cache_path(segment);
    let bytes = encode(segment, segment_len, digest, commits, blobs);
    if let Err(e) = write_atomic(&path, &bytes) {
        tracing::debug!(
            segment,
            path = %path.display(),
            error = %e,
            "skip writing segment cache"
        );
    }
}

/// Read one cache file and decode it, logging why it is unusable.
///
/// Treat a missing cache as a miss, not as damage.
/// Treat an unreadable or badly encoded cache as damage.
/// Treat a cache for another segment as damage.
fn decode_cache(cache_path: &Path, segment: u32) -> Option<CachedSegment> {
    let bytes = fs::read(cache_path)
        .inspect_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                tracing::debug!(
                    segment,
                    path = %cache_path.display(),
                    "miss the segment cache"
                );
            } else {
                tracing::warn!(
                    segment,
                    path = %cache_path.display(),
                    error = %e,
                    "cannot read the segment cache"
                );
            }
        })
        .ok()?;
    let cached = decode(&bytes)
        .inspect_err(|e| {
            tracing::warn!(
                segment,
                path = %cache_path.display(),
                error = %e,
                "drop a corrupt or unknown segment cache"
            );
        })
        .ok()?;
    if cached.segment != segment {
        tracing::warn!(
            segment,
            path = %cache_path.display(),
            cached_segment = cached.segment,
            "drop a segment cache for another segment"
        );
        return None;
    }
    Some(cached)
}

/// Check one segment against its cache.
///
/// - Open the segment read-only.
/// - Compare the file length with the cache.
/// - Compare the store header's segment number.
/// - Scan the commit records only.
/// - Refuse a scan that stops before the end.
/// - Compare the digest of the record headers.
/// - Compare the commit hashes.
/// - Return `Some` on a full match.
fn segment_agrees(segment_path: &Path, segment: u32, cached: &CachedSegment) -> Option<()> {
    let mut file = fs::File::open(segment_path)
        .inspect_err(|e| {
            tracing::warn!(
                segment,
                path = %segment_path.display(),
                error = %e,
                "drop a segment cache without its segment"
            );
        })
        .ok()?;
    let len = file
        .metadata()
        .inspect_err(|e| {
            tracing::warn!(
                segment,
                path = %segment_path.display(),
                error = %e,
                "drop a segment cache with an unreadable segment"
            );
        })
        .ok()?
        .len();
    if len != cached.segment_len {
        tracing::warn!(
            segment,
            len,
            cached_len = cached.segment_len,
            "drop a stale segment cache"
        );
        return None;
    }
    if super::blob::check_store_header(&mut file, segment_path).ok() != Some(segment) {
        tracing::warn!(
            segment,
            path = %segment_path.display(),
            "drop a segment cache whose segment claims another number"
        );
        return None;
    }
    // The commit-only scan reads every record header in the segment.
    // The digest of those headers must match the digest in the cache.
    // The end of that scan must match the file length.
    // A segment that passes this check holds the same headers, in the same
    // order, as the segment the cache came from. The cached placements
    // therefore name the bytes this segment holds.
    let fresh = record::scan(&mut file, segment, record::Blobs::Discard)
        .inspect_err(|e| {
            tracing::warn!(
                segment,
                path = %segment_path.display(),
                error = %e,
                "drop a segment cache that fails the commit scan"
            );
        })
        .ok()?;
    if fresh.end != len {
        tracing::warn!(
            segment,
            "drop a segment cache whose segment holds trailing bytes"
        );
        return None;
    }
    if fresh.digest != cached.digest {
        tracing::warn!(
            segment,
            "drop a segment cache whose record headers disagree with the segment"
        );
        return None;
    }
    if fresh.commits != cached.commits {
        tracing::warn!(segment, "drop a segment cache whose commits disagree");
        return None;
    }
    Some(())
}

/// Load a verified cache for one sealed segment.
///
/// Treat the cache as a hint.
/// Prove the hint against the segment.
/// Return `None` when anything disagrees.
pub(super) fn load_checked(
    set: &IndexSet,
    segment: u32,
    segment_path: &Path,
) -> Option<CachedSegment> {
    let cache_path = set.segment_cache_path(segment);
    let cached = decode_cache(&cache_path, segment)?;
    segment_agrees(segment_path, segment, &cached)?;
    tracing::debug!(
        segment,
        blobs = cached.blobs.len(),
        commits = cached.commits.len(),
        "reuse the verified segment cache"
    );
    Some(cached)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)] // helpers run outside `#[test]`

    use super::*;
    use crate::layout::IndexSet;
    use std::collections::HashMap;

    /// Build one fake placement for cache tests.
    ///
    /// Take an offset as input.
    /// Return a raw placement.
    /// Keep fields fixed otherwise.
    fn fake_placement(segment: u32, offset: u64) -> record::Placement {
        record::Placement {
            segment,
            offset,
            stored_len: 3,
            raw_len: 3,
            codec: record::Codec::Raw,
        }
    }

    /// Encode then decode and compare all fields.
    #[test]
    fn reuse_valid_cache_bytes_on_decode() {
        let mut blobs = HashMap::new();
        blobs.insert(BlobId::from_bytes([7u8; 32]), fake_placement(0, 12));
        blobs.insert(BlobId::from_bytes([9u8; 32]), fake_placement(0, 60));
        let commits = vec![BlobId::from_bytes([1u8; 32])];
        let digest = record::HeaderDigest::from_bytes([3u8; 32]);

        let bytes = encode(0, 100, digest, &commits, &blobs);
        let back = decode(&bytes).expect("fresh cache must decode");

        assert_eq!(back.segment, 0, "decode keeps segment number");
        assert_eq!(back.segment_len, 100, "decode keeps segment length");
        assert_eq!(back.digest, digest, "decode keeps the header digest");
        assert_eq!(back.commits, commits, "decode keeps commit list");
        assert_eq!(back.blobs.len(), 2, "decode keeps blob count");
    }

    /// Refuse edited bytes with a checksum error.
    #[test]
    fn drop_edited_cache_bytes_on_decode() {
        let mut blobs = HashMap::new();
        blobs.insert(BlobId::from_bytes([7u8; 32]), fake_placement(0, 12));
        let commits = vec![BlobId::from_bytes([1u8; 32])];

        let mut bytes = encode(0, 100, record::HeaderDigest::default(), &commits, &blobs);
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;

        assert!(decode(&bytes).is_err(), "edited cache must not decode");
    }

    /// Refuse caches from other record versions.
    #[test]
    fn drop_cache_with_wrong_record_version() {
        let blobs = HashMap::new();
        let commits = vec![];

        let mut bytes = encode(0, 12, record::HeaderDigest::default(), &commits, &blobs);
        bytes[8] ^= 0xff;

        // Fix checksum so version check runs first.
        // Keep test focused on version.
        // Recalculate guard over header.
        let body = bytes.len() - CHECK;
        let guard: [u8; CHECK] = crate::storage::hash::truncated_hash(&bytes[..body]);
        bytes[body..].copy_from_slice(&guard);

        assert!(
            decode(&bytes).is_err(),
            "wrong record version must not decode"
        );
    }

    /// Refuse a cache written before the digest existed.
    ///
    /// A file of the old version cannot prove what its segment holds.
    /// The version check refuses it before anything else reads it.
    #[test]
    fn drop_a_cache_from_an_older_format() {
        let blobs = HashMap::new();
        let commits = vec![];

        let mut bytes = encode(0, 12, record::HeaderDigest::default(), &commits, &blobs);
        bytes[4..8].copy_from_slice(&(CACHE_VERSION - 1).to_le_bytes());

        // Fix the checksum so the version check is what refuses the file.
        let body = bytes.len() - CHECK;
        let guard: [u8; CHECK] = crate::storage::hash::truncated_hash(&bytes[..body]);
        bytes[body..].copy_from_slice(&guard);

        assert!(
            decode(&bytes).is_err(),
            "a cache from an older format must not decode"
        );
    }

    /// Build high entropy payloads that resist compression.
    ///
    /// Take a seed as input.
    /// Take a length as input.
    /// Return bytes that zstd cannot shrink.
    fn noisy_payload(seed: u8, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut key = [0u8; 32];
        key[0] = seed;
        let chunks = len.div_ceil(64);
        for counter in 0..u64::try_from(chunks).unwrap_or(0) {
            let mut input = [0u8; 72];
            input[..32].copy_from_slice(&key);
            input[32..40].copy_from_slice(&counter.to_le_bytes());
            out.extend_from_slice(blake3::hash(&input).as_bytes());
        }
        out.truncate(len);
        out
    }

    /// Roll one store over several segments.
    ///
    /// Open one writer at a 4 KiB segment limit.
    /// Write eight payloads that resist compression.
    /// Sync the store before the block ends.
    /// Return the segment count and the blob ids.
    async fn roll_over_store(base: &Path) -> (usize, Vec<BlobId>) {
        use crate::storage::{BlobStore, SegmentLimit, StoragePut};
        use std::io::Cursor;

        let payloads: Vec<Vec<u8>> = (0..8).map(|i| noisy_payload(i, 900)).collect();
        {
            let store = BlobStore::builder(base)
                .segment_limit(SegmentLimit::new(4096))
                .open_writer()
                .await
                .expect("first open must succeed");
            for payload in &payloads {
                let id = blake3::hash(payload);
                let len = u64::try_from(payload.len()).unwrap_or(u64::MAX);
                store
                    .put(&id, Box::new(Cursor::new(payload.clone())), len)
                    .await
                    .expect("put must succeed");
            }
            store.sync_data().await.expect("sync must succeed");
        }
        let probe = BlobStore::builder(base)
            .open_reader()
            .await
            .expect("probe open must succeed");
        assert!(
            probe.segment_count() > 1,
            "writes must roll over two segments"
        );
        let ids = payloads.iter().map(|p| blake3::hash(p)).collect();
        (probe.segment_count(), ids)
    }

    /// Roll one store over several segments, and commit in the first one.
    ///
    /// Open one writer at a 4 KiB segment limit.
    /// Write four payloads that resist compression.
    /// Commit a snapshot in the first segment.
    /// Write four more payloads, which roll over to a second segment.
    /// Return the segment count.
    async fn roll_over_store_with_a_commit(base: &Path) -> usize {
        use crate::storage::{BlobStore, SegmentLimit, StoragePut};
        use std::io::Cursor;

        let payloads: Vec<Vec<u8>> = (0..8).map(|i| noisy_payload(i, 900)).collect();
        {
            let store = BlobStore::builder(base)
                .segment_limit(SegmentLimit::new(4096))
                .open_writer()
                .await
                .expect("first open must succeed");
            for payload in payloads.iter().take(4) {
                let id = blake3::hash(payload);
                let len = u64::try_from(payload.len()).unwrap_or(u64::MAX);
                store
                    .put(&id, Box::new(Cursor::new(payload.clone())), len)
                    .await
                    .expect("put must succeed");
            }
            // The commit lands in the first segment.
            // That segment therefore holds a commit list.
            let snapshot = blake3::hash(b"a snapshot this store never held");
            store
                .commit_with_time(&snapshot, crate::metadata::Timestamp::now())
                .await
                .expect("the commit must succeed");
            for payload in payloads.iter().skip(4) {
                let id = blake3::hash(payload);
                let len = u64::try_from(payload.len()).unwrap_or(u64::MAX);
                store
                    .put(&id, Box::new(Cursor::new(payload.clone())), len)
                    .await
                    .expect("put must succeed");
            }
            store.sync_data().await.expect("sync must succeed");
        }
        let probe = BlobStore::builder(base)
            .open_reader()
            .await
            .expect("probe open must succeed");
        assert!(
            probe.segment_count() > 1,
            "writes must roll over two segments"
        );
        probe.segment_count()
    }

    /// Rewrite the first blob header of a segment, and keep every other byte.
    ///
    /// The length of the segment stays the same.
    /// The commit list stays the same.
    /// The record headers do not stay the same.
    ///
    /// The new header uses the same encoder as the old one.
    /// The header therefore keeps its length and its own checksum.
    fn replace_the_first_blob_hash(bytes: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(bytes.len());
        out.extend_from_slice(&bytes[..record::STORE_HEADER]);
        let mut at = record::STORE_HEADER;
        while at < bytes.len() {
            let Some(header) =
                record::decode_header(&bytes[at..], u64::try_from(at).unwrap_or(u64::MAX))
            else {
                out.extend_from_slice(&bytes[at..]);
                break;
            };
            let header_len = usize::try_from(header.total_len).unwrap_or(usize::MAX);
            let end = (at + header_len + usize::try_from(header.stored_len).unwrap_or(usize::MAX))
                .min(bytes.len());
            if header.kind == record::Kind::Blob {
                let mut hash = header.hash;
                hash[0] ^= 0xff;
                let mut fresh = Vec::new();
                record::encode_header(
                    &mut fresh,
                    header.kind,
                    header.codec,
                    &hash,
                    header.stored_len,
                    header.raw_len,
                );
                out.extend_from_slice(&fresh);
                out.extend_from_slice(&bytes[at + header_len..end]);
                out.extend_from_slice(&bytes[end..]);
                return out;
            }
            out.extend_from_slice(&bytes[at..end]);
            at = end;
        }
        out
    }

    /// A cache does not survive a replaced segment.
    ///
    /// The replacement has the same length and the same commit list.
    /// Its record headers differ, so the digest of those headers differs.
    /// The cache describes the old segment, so it must be refused.
    #[tokio::test]
    async fn a_replaced_segment_refuses_its_cache() {
        use crate::storage::{BlobStore, SegmentLimit};

        let dir = tempfile::tempdir().expect("temp dir must open");
        let base = dir.path().join("store");
        roll_over_store_with_a_commit(&base).await;
        let set = IndexSet::new(&base);
        {
            let _store = BlobStore::builder(&base)
                .segment_limit(SegmentLimit::new(4096))
                .open_writer()
                .await
                .expect("cache building open must succeed");
        }
        assert!(
            set.segment_cache_path(0).is_file(),
            "the writer must write a cache for the sealed segment"
        );
        let path = set.segment_path(0);
        assert!(
            super::load_checked(&set, 0, &path).is_some(),
            "the fresh cache must pass its own verification"
        );

        let original = std::fs::read(&path).expect("the segment must be readable");
        let replaced = replace_the_first_blob_hash(&original);
        assert_eq!(
            replaced.len(),
            original.len(),
            "the replacement must keep the length of the segment"
        );
        std::fs::write(&path, &replaced).expect("the replacement must write");

        assert!(
            super::load_checked(&set, 0, &path).is_none(),
            "a cache must not survive a segment that was replaced"
        );
    }

    /// A writer open builds a verified cache for every sealed segment.
    ///
    /// The last segment stays uncached.
    /// The last segment always scans fully.
    #[tokio::test]
    async fn a_writer_open_caches_sealed_segments_only() {
        use crate::storage::{BlobStore, SegmentLimit};

        let dir = tempfile::tempdir().expect("temp dir must open");
        let base = dir.path().join("store");
        let (count, _) = roll_over_store(&base).await;
        let set = IndexSet::new(&base);
        let last = u32::try_from(count - 1).expect("the count fits a u32");
        assert!(
            !set.segment_cache_path(0).exists(),
            "no writer has run since the rollover, so no sealed cache is there yet"
        );

        let _store = BlobStore::builder(&base)
            .segment_limit(SegmentLimit::new(4096))
            .open_writer()
            .await
            .expect("second open must succeed");
        assert!(
            set.segment_cache_path(0).is_file(),
            "writer open must write a cache file"
        );
        assert!(
            !set.segment_cache_path(last).exists(),
            "writer open must never cache the last segment"
        );
        assert!(
            super::load_checked(&set, 0, &set.segment_path(0)).is_some(),
            "the fresh cache must pass its own verification"
        );
        for segment in 0..last {
            assert!(
                set.segment_cache_path(segment).is_file(),
                "each sealed segment gets a cache"
            );
            assert!(
                super::load_checked(&set, segment, &set.segment_path(segment)).is_some(),
                "each sealed cache verifies"
            );
        }
    }

    /// A reader open uses a verified cache and writes nothing.
    ///
    /// The reader locates every blob through the caches.
    /// With the caches gone, the same open still locates every blob.
    /// That open leaves no cache files behind.
    #[tokio::test]
    async fn a_reader_open_reads_without_writing_caches() {
        use crate::storage::{BlobStore, SegmentLimit};

        let dir = tempfile::tempdir().expect("temp dir must open");
        let base = dir.path().join("store");
        let (count, ids) = roll_over_store(&base).await;
        let sealed = u32::try_from(count - 1).expect("the count fits a u32");
        let set = IndexSet::new(&base);
        {
            let writer = BlobStore::builder(&base)
                .segment_limit(SegmentLimit::new(4096))
                .open_writer()
                .await
                .expect("cache building open must succeed");
            let placed: Vec<_> = ids
                .iter()
                .map(|id| writer.locate(id).expect("writer must locate blobs"))
                .collect();
            let store = BlobStore::builder(&base)
                .open_reader()
                .await
                .expect("reader open must succeed");
            for (id, want) in ids.iter().zip(placed.iter()) {
                assert_eq!(
                    store.locate(id).expect("reader must locate blobs"),
                    *want,
                    "reader and writer must agree on each placement"
                );
            }
        }
        for segment in 0..sealed {
            std::fs::remove_file(set.segment_cache_path(segment))
                .expect("the writer left a cache for every sealed segment");
        }
        let store = BlobStore::builder(&base)
            .open_reader()
            .await
            .expect("reader open without caches must succeed");
        for id in &ids {
            store.locate(id).expect("reader must locate blobs");
        }
        assert!(
            super::load_checked(&set, 0, &set.segment_path(0)).is_none(),
            "no cache may exist after a reader without caches"
        );
    }

    /// A damaged cache falls back to a full scan.
    ///
    /// The verification refuses garbage.
    /// The next writer rebuilds the cache and keeps every placement.
    #[tokio::test]
    async fn a_damaged_cache_falls_back_to_a_full_scan() {
        use crate::storage::{BlobStore, SegmentLimit};

        let dir = tempfile::tempdir().expect("temp dir must open");
        let base = dir.path().join("store");
        roll_over_store(&base).await;
        let set = IndexSet::new(&base);
        let ids: Vec<BlobId> = (0..8)
            .map(|i| noisy_payload(i, 900))
            .map(|p| blake3::hash(&p))
            .collect();
        let limit = SegmentLimit::new(4096);
        let seal_cache = set.segment_cache_path(0);
        {
            let store = BlobStore::builder(&base)
                .segment_limit(limit)
                .open_writer()
                .await
                .expect("cache building open must succeed");
            let placed = store.locate(&ids[0]).expect("writer must locate blobs");
            std::fs::write(&seal_cache, b"garbage").expect("damage must write");
            assert!(
                super::load_checked(&set, 0, &set.segment_path(0)).is_none(),
                "a damaged cache must fail its own verification"
            );
            let store = BlobStore::builder(&base)
                .segment_limit(limit)
                .open_writer()
                .await
                .expect("damaged cache must still open");
            let got = store.locate(&ids[0]).expect("fallback must locate blobs");
            assert_eq!(got, placed, "fallback must keep placements");
            assert!(
                super::load_checked(&set, 0, &set.segment_path(0)).is_some(),
                "the writer must rebuild a damaged cache"
            );
        }
    }
}
