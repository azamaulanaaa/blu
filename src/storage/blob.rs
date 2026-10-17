// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! A store of content-addressed blobs, in a sequence of append-only segments.
//!
//! Each segment is the journal for its own stretch of the log. A store header
//! comes first, then records (see [`super::record`]). A version of the backup set
//! is a *commit record* that names a snapshot. A version number is a position in
//! the sequence across all segments. Nothing outside the segments is needed to
//! read a set. Nothing outside the segments is trusted to say what the set holds.
//!
//! # Format guarantees
//!
//! * A segment is **sealed** once the next record would not fit. Only the last
//!   segment is written again. A record is never split across a boundary, so a
//!   segment can be verified on its own.
//! * A non-last segment with a tail that does not verify is *corruption*, not an
//!   unfinished run. The code refuses it rather than truncating it. Only the last
//!   segment's tail is truncated, back to the end of its last valid record. That
//!   is why a run may abandon a record mid-file. The scan stops at the hole, and
//!   everything after it goes with it.
//! * A file that does not begin with the store magic is refused, never truncated.
//!
//! # Commit protocol
//!
//! A run writes its chunk records, then its snapshot, then one commit record:
//!
//! 1. every record of the run is written, with no flush to the device.
//! 2. one `sync_data`, which makes all of it durable.
//! 3. the commit record, then one more `sync_data`.
//!
//! The protocol uses two syncs rather than one. Page write-back within a file is in
//! no particular order. A single sync after the commit record can therefore leave a
//! commit that names data that is not there. A digest attests to what a payload
//! *claims*, not to whether it is present. Two syncs cost nothing on a run that
//! takes seconds to hours.
//!
//! A crash before step 2 leaves a tail with no commit record, which the next open
//! discards. A crash after step 3 leaves a whole committed run. There is no third
//! outcome, which is the entire guarantee.
//!
//! # Recovery
//!
//! Opening for writing scans every segment. It then truncates the *last* one back
//! to the end of its last valid record. That reclaims the space a failed run left.
//! A store from another build is a backup that somebody cares about. A store that
//! does not begin with the store magic is therefore refused rather than scanned.
//!
//! # Sealed segment caches
//!
//! A writer open may leave a `.sidx` file beside each sealed segment. The file
//! holds that segment's blob placements and commit hashes. It is a cache, not a
//! source, and nothing in it is believed before it is proven. A cached segment
//! must carry the same length, the same store header, the same verified record
//! headers, and the same commit hashes as a scan finds. Any disagreement drops
//! the cache and falls back to a full scan. A reader open reads caches and
//! rebuilds none.
//!
//! # Remote backends
//!
//! Remote backends use the same record format. The local durability mechanics
//! do not apply. These mechanics stay local:
//!
//! - Reserved-offset appends.
//! - Tail truncation.
//! - File syncs.
//! - The file lock.
//!
//! A remote backend mirrors sealed segments as immutable objects. It mirrors
//! index files as immutable objects. It uploads each object whole. It stages
//! the mutable tail locally.
//!
//! See [`crate::storage::mirror`] for the copy in both directions.
//!
//! A blob placement names a segment, an offset, and lengths. A remote backend
//! reads the blob with a ranged fetch.
//!
//! The backend answers deduplication checks from a cached index. It loads the
//! index once. It does not make one round trip per chunk.

use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::Debug,
    io::{self, SeekFrom},
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{Mutex, PoisonError},
};

use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
};
use tracing::instrument;

use super::record::{self, Kind, Placement, STORE_HEADER};
use crate::{
    layout::IndexSet,
    metadata::{Snapshot, Timestamp},
    slice::{DecompressReader, SliceAsyncReader},
    storage::{
        self, BlobId, Codec, Compression, Compressor, Decompressor, SegmentLimit, StorageGet,
        StreamReadSeeker, VersionIndex, Zstd,
    },
};

/// Copy buffer for a payload: where it is hashed on its way through, and what
/// coalesces the short reads a streaming backend hands out.
const COPY_BUFFER: usize = 256 * 1024;

/// Stream payloads above this size through temp files.
///
/// - Keep small payloads in memory.
/// - Stage large payloads on disk.
/// - Bound memory during concurrent writes.
const STREAM_THRESHOLD: u64 = 4 * 1024 * 1024;

/// What a scan found, and what this handle may add to.
#[derive(Debug, Default)]
struct Contents {
    /// Every blob the store holds, by content hash.
    blobs: HashMap<BlobId, Placement>,
    /// The snapshot hash of each committed version, oldest first.
    commits: Vec<BlobId>,
    /// How many commit records each segment holds, by segment number.
    ///
    /// Seeded by the scan, grown by each commit. An index file's ordinal is
    /// counted against this, so a rollover must not reset it.
    commits_per_segment: HashMap<u32, u32>,
}

impl Contents {
    /// Lock the contents, ignoring poisoning.
    ///
    /// A panic during an update leaves a half-updated index. Unwinding through a
    /// backup aborts the run. The next open's scan then rebuilds the index from the
    /// files.
    fn lock(contents: &Mutex<Contents>) -> std::sync::MutexGuard<'_, Contents> {
        contents.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Where the next record goes, given as a `(segment, offset)` rather than a bare
/// offset, because the log spans several files. Taking a reservation is a short
/// critical section. The copy that follows happens outside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Reservation {
    /// Which segment the record goes in.
    segment: u32,
    /// Byte offset within that segment.
    offset: u64,
}

/// Whether a handle may append to its store.
///
/// A marker trait rather than the `bool` it replaced, so the type says *which*
/// instead of asking the reader to look up a convention. It carries the one
/// derived fact the code needs, which is [`WriteMode::WRITES`]. That constant is
/// read in exactly one place, the hand-written `Debug`. That is all it is for.
///
/// Not sealed, and deliberately. Nothing constructs a `BlobStore` except
/// [`StoreBuilder::open_writer`] and [`StoreBuilder::open_reader`]. Both of
/// them fix the mode. An implementation outside this crate therefore cannot
/// produce an instantiation that means anything. Sealing it would be a bound no
/// caller could satisfy and a restriction nothing needed.
pub trait WriteMode: Send + Sync + 'static {
    /// Whether a handle with this mode may append to its store.
    const WRITES: bool;
}

/// A store a run may append to. The yes to "may this handle write?".
///
/// Ask for one with [`BlobStore::builder`] plus `open_writer`, which requires the write
/// policies and so cannot be confused with the other open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Write;

impl WriteMode for Write {
    const WRITES: bool = true;
}

/// A store that may only be read. The no, and it is the type rather than a
/// promise, so a restore path holding one cannot append and the mistake will not
/// compile.
///
/// Ask for one with [`BlobStore::builder`] plus `open_reader`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Read;

impl WriteMode for Read {
    const WRITES: bool = false;
}

/// A store of content-addressed blobs.
///
/// `W` is a [`WriteMode`] so that a read-only handle cannot be used to write.
/// The distinction is part of the type. A restore path that only needs to read
/// therefore cannot accidentally append to the store.
///
/// It was `BlobStore<true>` and `BlobStore<false>`. That was the same guarantee
/// carrying no information. A `bool` in a type position has to be looked up in
/// whatever doc comment is nearby. `false` reads as "disabled" rather than
/// "read-only". [`Read`] and [`Write`] say which. The enforcement is unchanged and
/// is still structural. Only `impl StoragePut for BlobStore<Write>` exists, so
/// naming the mode and permitting the method are the same edit.
///
/// `C` is the [`Compressor`] this store writes payloads through, defaulting to
/// [`Compression`]. It is a parameter rather than a constructor argument type for
/// the same reason that `W` is. The policy is part of the handle, so a store cannot
/// be holding one policy while a caller believes it is writing under another. A
/// reader is not generic over the policy at all. The reader consults no policy,
/// because the codec byte in each record says how that record was written.
/// [`BlobStore::builder`] opens a reader with [`Compression`] whatever else exists.
/// That is the honest answer. There is nothing on the reading side to configure.
pub struct BlobStore<W: WriteMode, C: Compressor = Compression> {
    /// The set this store is, which is how its segments are named and found.
    set: IndexSet,
    /// Every segment on disk, in order. This list is fixed at open. The set only
    /// grows by appending a segment, and a reader holds this list for its lifetime.
    segments: Vec<PathBuf>,
    /// What the store holds, rebuilt by the scan that every open begins with.
    contents: Mutex<Contents>,
    /// Where the next record goes, and how full the last segment is. The value is
    /// seeded from the end of the last valid record.
    ///
    /// A failed record keeps the range it claimed. The next open's scan reclaims
    /// that range. Reclaiming it here would mean truncating past a writer that
    /// already promised the space.
    reserved: Mutex<Reservation>,
    /// Serialise rollovers so one thread wins each new segment.
    ///
    /// Hold this lock across file creation and publish.
    /// Never hold `reserved` across file creation.
    ///
    /// Lock order:
    ///
    /// - Lock `rollover` first.
    /// - Lock `reserved` next.
    /// - Lock `dirty` last.
    rollover: Mutex<()>,
    /// How large a segment may get. This is a write policy, not a format property.
    /// That is why it is not in the header. No record depends on where a boundary
    /// fell.
    limit: SegmentLimit,
    /// How record payloads are compressed. This is a write policy for the same
    /// reason. The reason is stronger here. Each record names its own codec.
    ///
    /// The writer applies the fallback rather than trusting it from here. See
    /// [`Compressor`]. This field is therefore whatever policy the caller brought.
    compression: C,
    /// Segments this run has written to, so `sync_data` knows which files to sync.
    ///
    /// Not persisted: a run that wrote nothing has nothing to sync, and one that
    /// did has synced by the time it commits.
    dirty: Mutex<Vec<u32>>,
    /// Reuse open segment files when reading stored payloads.
    ///
    /// Hold one file for each segment touched so far. Avoid opening the same file for each chunk.
    open_files: tokio::sync::Mutex<HashMap<u32, fs::File>>,
    /// The mode, which selects the methods and so appears in no field.
    ///
    /// A `const WRITE: bool` parameter needs no such field. That is the one thing
    /// the const generic was better at. `PhantomData<fn() -> W>` is the marker that
    /// does not claim ownership. It also does not affect auto-trait derivation.
    /// `BlobStore<Write>` therefore stays `Send` because its fields are. A `W`
    /// that is only ever a type parameter cannot make it `!Send` by existing.
    mode: PhantomData<fn() -> W>,
}

impl<W: WriteMode, C: Compressor> Debug for BlobStore<W, C> {
    /// Counts and paths, never the index.
    ///
    /// Hand-written because the derived version walks the whole in-memory
    /// contents. A `{:?}` of a store with a million blobs is a line of a million
    /// hashes. That line is unreadable *and* slow to produce. One stray `{:?}` in
    /// a log is a multi-megabyte message about a backup. A count says everything a
    /// diagnostic needs.
    ///
    /// The same reasoning appears in `reader::Chunk` for its contents and in
    /// `reader::Chunker` for its upstream library. This is the third place. It is
    /// written down here because the alternative is a third derivation that does
    /// not know about the other two.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let contents = Contents::lock(&self.contents);

        f.debug_struct("BlobStore")
            .field("write", &W::WRITES)
            .field("set", &self.set)
            .field("segments", &self.segments.len())
            .field("blobs", &contents.blobs.len())
            .field("commits", &contents.commits.len())
            .finish_non_exhaustive()
    }
}

impl<W: WriteMode, C: Compressor> BlobStore<W, C> {
    /// The path of segment `segment`.
    ///
    /// Every segment but the last is sealed, so this names the only one that a run
    /// writes to.
    #[must_use]
    pub fn segment_path(&self, segment: u32) -> PathBuf {
        self.set.segment_path(segment)
    }

    /// How many segments this store has, counting from zero.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Every version this store holds, oldest first.
    ///
    /// The commit records are the answer. The index files are not consulted,
    /// because a version whose index write was abandoned still exists.
    /// [`crate::layout::IndexSet::indexed_versions`] returns the file-name count.
    /// That count is a lower bound on this.
    ///
    /// # Errors
    ///
    /// Returns an error only if the version count does not fit in a `u32`.
    pub fn versions(&self) -> io::Result<Vec<u32>> {
        let commits = Contents::lock(&self.contents).commits.len();
        let count = u32::try_from(commits)
            .map_err(|_| io::Error::other("a store cannot hold more than u32::MAX versions"))?;

        Ok((1..=count).collect())
    }

    /// The newest committed version and the snapshot it names.
    #[must_use]
    pub fn latest(&self) -> Option<(u32, BlobId)> {
        let contents = Contents::lock(&self.contents);
        let version = u32::try_from(contents.commits.len()).ok()?;

        version
            .checked_sub(1)
            .and_then(|index| contents.commits.get(usize::try_from(index).ok()?))
            .copied()
            .map(|snapshot| (version, snapshot))
    }

    /// Where the blob hashing to `id` sits in this store.
    ///
    /// The index a scan builds, answered directly. This is what
    /// [`crate::storage::Plan`] resolves a snapshot against.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::NotFound`] if the store does not hold that blob,
    /// naming the hash it was asked for.
    pub fn locate(&self, id: &BlobId) -> io::Result<Placement> {
        Contents::lock(&self.contents)
            .blobs
            .get(id)
            .copied()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("this store does not hold a blob hashing to {id}"),
                )
            })
    }

    /// Read the stored payload of a blob.
    ///
    /// Reuse one open file per segment. Check the range against the file length. Return the payload bytes with the codec and the content length.
    ///
    /// # Errors
    ///
    /// Returns an error if the store misses the blob. Returns an error if the range overflows. Returns an error if the segment ends before the range. Returns an error if reading the payload fails.
    pub(crate) async fn read_stored_bytes(&self, id: &BlobId) -> io::Result<(Vec<u8>, Codec, u64)> {
        let placement = self.locate(id)?;
        let path = self.segment_path(placement.segment);
        let mut files = self.open_files.lock().await;
        let file = if let Some(file) = files.get_mut(&placement.segment) {
            file
        } else {
            let opened = crate::storage::at_async(&path, "open", fs::File::open(&path)).await?;
            files.insert(placement.segment, opened);
            files
                .get_mut(&placement.segment)
                .ok_or_else(|| io::Error::other("the open file cache misses its own entry"))?
        };
        let len = file.metadata().await?.len();
        let end = placement
            .offset
            .checked_add(placement.stored_len)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "a blob's range overflows")
            })?;
        if end > len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "blob at {} in segment {} needs bytes {}..{end} but that segment is \
                     only {len} bytes long",
                    path.display(),
                    placement.segment,
                    placement.offset
                ),
            ));
        }
        file.seek(SeekFrom::Start(placement.offset)).await?;
        let mut bytes = crate::storage::zeroed(
            placement.stored_len,
            &format!("chunk {id} as {} stores it", path.display()),
        )?;
        file.read_exact(&mut bytes).await?;
        Ok((bytes, placement.codec, placement.raw_len))
    }

    /// The snapshot a version names, taken from the per-version index file when
    /// that file verifies.
    ///
    /// The file is **checked, not believed**. The check covers three things. The set
    /// holds that version. A segment exists. A commit record in that segment has
    /// the hash the file claims. Anything else falls back to
    /// [`BlobStore::snapshot`], so a stale, edited or copied index costs a scan
    /// rather than correctness.
    ///
    /// This is not the fast path for a restore. A restore needs every chunk that a
    /// snapshot names. Those chunks live in the blob index that a scan builds. What
    /// this method removes is the scan for the *version list*.
    ///
    /// The code logs the fallback. A caller that asks this is trying to avoid a
    /// scan, and it needs to know that it did not get one. The code logs `debug`
    /// when no file exists. That is the ordinary case for a version written before
    /// the set's last run. The code logs `warn` when a file disagrees with the
    /// store.
    ///
    /// # Errors
    ///
    /// The error is the one that [`BlobStore::snapshot`] would give. A caller
    /// therefore cannot tell which path answered.
    ///
    /// The snapshot hash of every commit record in `segment`, in log order.
    ///
    /// In-memory twin of what [`resolve`](crate::storage::resolve) reads off the
    /// disk, and the same list. This store's open already scanned for the list, so
    /// answering from here costs nothing. Reading the file costs a walk of every
    /// record header in the segment.
    ///
    /// The result is empty for a segment that this store does not know.
    /// [`resolve_with`](crate::storage::resolve_with) treats an empty list as "no
    /// usable log".
    ///
    /// A `Vec` by value rather than a borrow, because the borrow would have to
    /// outlive a `MutexGuard`. The borrow would also be handed to a caller that
    /// cannot hold one. A segment's commit list is a handful of hashes, so the copy
    /// is not the thing worth avoiding. The file read is.
    #[must_use]
    pub fn segment_commits(&self, segment: u32) -> Vec<BlobId> {
        let contents = Contents::lock(&self.contents);

        // A segment's records start after every earlier segment's records. The
        // start is counted rather than kept. `commits_per_segment` is what the
        // scan built, and a second running offset would be a second thing to keep
        // in step with it.
        let start: u64 = contents
            .commits_per_segment
            .iter()
            .filter(|(other, _)| **other < segment)
            .map(|(_, count)| u64::from(*count))
            .sum();

        let Ok(first) = usize::try_from(start) else {
            return Vec::new();
        };
        let count = usize::try_from(
            contents
                .commits_per_segment
                .get(&segment)
                .copied()
                .unwrap_or(0),
        )
        .unwrap_or(0);
        let Some(end) = first.checked_add(count) else {
            return Vec::new();
        };

        contents
            .commits
            .get(first..end)
            .map_or_else(Vec::new, <[BlobId]>::to_vec)
    }

    /// The snapshot a version names, via the per-version index file when it
    /// verifies.
    ///
    /// The file is **checked, not believed**. The check covers three things. The set
    /// holds that version. A segment exists. A commit record in that segment has
    /// the hash the file claims. Anything else falls back to
    /// [`BlobStore::snapshot`], so a stale, edited or copied index costs a scan
    /// rather than correctness.
    ///
    /// This is not the fast path for a restore. A restore needs every chunk that a
    /// snapshot names. Those chunks live in the blob index that a scan builds. What
    /// this method removes is the scan for the *version list*. It also removes the
    /// read of the named segment, because the check takes that data from
    /// [`BlobStore::segment_commits`]. Resolving N versions against one segment
    /// used to read that segment N times. `blu versions` resolves one version
    /// at a time.
    ///
    /// The code logs the fallback. A caller that asks this is trying to avoid a
    /// scan, and it needs to know that it did not get one. The code logs `debug`
    /// when no file exists. That is the ordinary case for a version written before
    /// the set's last run. The code logs `warn` when a file disagrees with the
    /// store.
    ///
    /// The error is the one [`BlobStore::snapshot`] would give. A caller therefore
    /// cannot tell which path answered.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::NotFound`] if the store holds no such version. The error
    /// names both the version asked for and how many the store holds. That is the
    /// error the fallback gives. It is also the only error this method can give. A
    /// version that the set does not hold is a version that no index file can
    /// confirm.
    pub fn snapshot_via_index(&self, version: u32) -> io::Result<BlobId> {
        let versions = u32::try_from(Contents::lock(&self.contents).commits.len()).unwrap_or(0);

        // The lock is taken inside the callee rather than captured into the
        // closure, and that is not a style preference. A captured guard would be
        // held across `resolve_with`, which reads a file. A concurrent `put` would
        // then block on I/O rather than on the short critical section that the
        // guard was written for.
        let store = self;
        let resolved = storage::resolve_with(&self.set, version, versions, move |segment| {
            let commits = store.segment_commits(segment);
            (!commits.is_empty()).then_some(commits)
        });
        // One match, three arms, no panic.
        //
        // This used to ask `confirmed()` first and returned early on it. It then
        // matched on the same value again to log why it had *not* confirmed. The
        // `Confirmed` arm could not be written without an `unreachable!`. A crate
        // that denies `unwrap_used` and `expect_used` had a live panic because a
        // value was inspected twice.
        //
        // `reason` is the one place that says in a sentence why the file was not
        // used. That is why it is the message rather than a second wording of it
        // here.
        let why = resolved.reason(version, &self.set);

        match resolved {
            storage::Resolved::Confirmed { snapshot, .. } => return Ok(snapshot),
            storage::Resolved::Absent => tracing::debug!(version, reason = %why),
            storage::Resolved::Disagrees => tracing::warn!(version, reason = %why),
        }

        self.snapshot(version)
    }

    /// The snapshot a specific version names.
    ///
    /// From the in-memory index that the open's scan built, not the per-version
    /// file. This answer is always complete, because it comes from the store's own
    /// commit records.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::NotFound`] if the store has no such version,
    /// naming both the version asked for and how many it does hold.
    pub fn snapshot(&self, version: u32) -> io::Result<BlobId> {
        let contents = Contents::lock(&self.contents);

        let index = usize::try_from(version.checked_sub(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "versions start at 1, so 0 names no snapshot",
            )
        })?)
        .ok();

        index
            .and_then(|i| contents.commits.get(i).copied())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "version {version} does not exist in {}. It holds {} version(s)",
                        self.set.base().display(),
                        contents.commits.len()
                    ),
                )
            })
    }

    /// The stamp of a version, from its index file or its snapshot blob.
    ///
    /// Reads the index file where it names this version. Falls back to
    /// the snapshot blob where no usable index file exists.
    ///
    /// # Errors
    ///
    /// Returns an error when the set holds no such version. Returns an
    /// error when the snapshot blob cannot be read or decoded.
    pub async fn version_time(&self, version: u32) -> io::Result<Timestamp> {
        if let Some(index) = VersionIndex::read(&self.set.index_path(version)) {
            if index.version == version && self.snapshot(version).ok() == Some(index.snapshot) {
                return Ok(index.taken_at);
            }
            tracing::debug!(
                version,
                "the index file names another snapshot; reading the blob"
            );
        }

        let id = self.snapshot(version)?;
        self.snapshot_taken_at(&id).await
    }

    /// The stamp inside the snapshot blob named by `id`.
    ///
    /// Reads the blob whole. A snapshot fits in memory by design.
    async fn snapshot_taken_at(&self, id: &BlobId) -> io::Result<Timestamp> {
        let mut reader = self.get(id).await?;
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await?;
        Snapshot::try_from(buffer.as_slice())
            .map(|snapshot| snapshot.taken_at)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

/// Build a store open with chosen policies.
///
/// Call [`BlobStore::builder`] with the set path. Chain policy methods.
/// Call `open_writer` or `open_reader` to finish.
///
/// A reader ignores both policies. A read never rolls a segment.
/// A read decodes each record by its own codec byte.
#[derive(Debug, Clone)]
pub struct StoreBuilder<C = Compression> {
    path: PathBuf,
    limit: SegmentLimit,
    compression: C,
}

impl StoreBuilder<Compression> {
    /// Start a builder for the set at `path`.
    ///
    /// The builder holds default policies. Override them before opening.
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into(),
            limit: SegmentLimit::default(),
            compression: Compression::default(),
        }
    }
}

impl<C: Compressor> StoreBuilder<C> {
    /// Cap each segment at `limit` bytes.
    ///
    /// The limit shapes new segments only. Old segments keep their size.
    #[must_use]
    pub fn segment_limit(mut self, limit: SegmentLimit) -> Self {
        self.limit = limit;
        self
    }

    /// Encode new payloads with `compression`.
    ///
    /// The policy shapes new records only. Old records keep their codec.
    pub fn compression<N: Compressor>(self, compression: N) -> StoreBuilder<N> {
        StoreBuilder {
            path: self.path,
            limit: self.limit,
            compression,
        }
    }

    /// Open the set for writing.
    ///
    /// Create the set when it does not exist. Recover its tail when it does.
    ///
    /// # Errors
    ///
    /// Returns an error when the set cannot be created or read.
    /// Returns an error when a sealed segment holds a broken tail.
    #[instrument(skip_all, err)]
    pub async fn open_writer(self) -> io::Result<BlobStore<Write, C>> {
        let set = IndexSet::new(self.path);

        if let Some(parent) = set.base().parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent).await?;
        }

        let scan_set = set.clone();
        let scanned = tokio::task::spawn_blocking(move || recover(&scan_set))
            .await
            .map_err(|join| io::Error::other(format!("the store scan failed: {join}")))??;

        // A writing store always has an append point. `recover` creates segment 0
        // for a set with none, so there is always a last segment to carry on in.
        let reserved = scanned
            .append
            .ok_or_else(|| io::Error::other("a writing store has no segment to write into"))?;

        Ok(BlobStore {
            set,
            segments: scanned.segments,
            contents: Mutex::new(Contents {
                blobs: scanned.blobs,
                commits: scanned.commits,
                commits_per_segment: scanned.commits_per_segment,
            }),
            reserved: Mutex::new(reserved),
            rollover: Mutex::new(()),
            limit: self.limit,
            compression: self.compression,
            dirty: Mutex::new(Vec::new()),
            open_files: tokio::sync::Mutex::new(HashMap::new()),
            mode: PhantomData,
        })
    }

    /// Open the set for reading.
    ///
    /// Refuse a path that holds no set. A mistyped name never reads as empty.
    ///
    /// # Errors
    ///
    /// Returns an error when the set does not exist or cannot be read.
    /// Returns an error when a segment is missing or unreadable.
    #[instrument(skip_all, err)]
    pub async fn open_reader(self) -> io::Result<BlobStore<Read, Compression>> {
        let set = IndexSet::new(self.path);

        let scan_set = set.clone();
        let scanned = tokio::task::spawn_blocking(move || read_only_scan(&scan_set))
            .await
            .map_err(|join| io::Error::other(format!("the store scan failed: {join}")))??;

        Ok(BlobStore {
            set,
            segments: scanned.segments,
            contents: Mutex::new(Contents {
                blobs: scanned.blobs,
                commits: scanned.commits,
                commits_per_segment: scanned.commits_per_segment,
            }),
            // A reader has no append point, and never had one to record. The scan
            // says so rather than handing back a reservation built from a segment
            // number that does not exist.
            reserved: Mutex::new(scanned.append.unwrap_or(Reservation {
                segment: u32::MAX,
                offset: 0,
            })),
            rollover: Mutex::new(()),
            // Both fields are shared with the writing open, and only that
            // one consults them. Neither field has a value worth recording on a
            // read. A read never rolls over, and never compresses anything. The
            // compressor is [`Compression`] rather than a parameter for the reason
            // given on the type. Nothing on this side consults it. A reader decodes
            // what each record says it is.
            limit: SegmentLimit::default(),
            compression: Compression::default(),
            dirty: Mutex::new(Vec::new()),
            open_files: tokio::sync::Mutex::new(HashMap::new()),
            mode: PhantomData,
        })
    }
}

impl BlobStore<Write, Compression> {
    /// Start a builder for the set at `path`.
    ///
    /// The builder holds default policies. Override them before opening.
    /// Call `open_writer` to write. Call `open_reader` to read.
    pub fn builder<P: Into<PathBuf>>(path: P) -> StoreBuilder<Compression> {
        StoreBuilder::new(path)
    }
}

impl<W: WriteMode, C: Compressor> storage::StorageGet for BlobStore<W, C> {
    /// Open the blob with this content hash.
    ///
    /// The stream yields the blob's **content**. The stream yields that content
    /// decompressed if it is stored compressed. The stream is bounded to that
    /// content. A caller comparing against a length that the snapshot recorded is
    /// therefore comparing like with like.
    #[instrument(skip_all, err)]
    async fn get(&self, id: &BlobId) -> io::Result<StreamReadSeeker> {
        let placement = self.locate(id)?;

        // The list is fixed at open, but a run that rolled over holds blobs
        // in segments it opened itself. Those segments are real files with
        // real names, so the computed path serves where the list ends.
        let segment = self
            .segments
            .get(usize::try_from(placement.segment).unwrap_or(usize::MAX))
            .cloned()
            .unwrap_or_else(|| self.segment_path(placement.segment));

        let file = super::at_async(&segment, "open", fs::File::open(&segment)).await?;
        let len = file.metadata().await?.len();

        // The index is built by a scan and cannot be wrong unless a segment has
        // shrunk since. Only a hand-edited store can arrange that. The code checks
        // the range rather than trusting it, so the answer is a short read reported
        // here instead of a silent hole in a restored file.
        //
        // This is the *stored* length. It is the span that the record occupies on
        // disk, and a compressed record is shorter than its content.
        let end = placement
            .offset
            .checked_add(placement.stored_len)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "a blob's range overflows")
            })?;

        if end > len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "blob at {} in segment {} needs bytes {}..{end} but that segment is \
                     only {len} bytes long",
                    segment.display(),
                    placement.segment,
                    placement.offset
                ),
            ));
        }

        // No lock is taken. Records are only ever appended, so a blob handed out
        // now stays valid however much is written after it.
        let mut file = file;
        file.seek(SeekFrom::Start(placement.offset)).await?;

        match placement.codec {
            Codec::Raw => Ok(Box::new(SliceAsyncReader::new(file, placement.raw_len))),
            Codec::Zstd => {
                // The code reads the frame, then decodes it. The read is bounded to
                // the stored length, so a damaged record cannot pull in its
                // neighbour. The decode is told the expected size, so a frame that
                // does not produce it is refused rather than handed on short.
                let mut frame = storage::zeroed(
                    placement.stored_len,
                    &format!("the compressed frame of chunk {id}"),
                )?;
                file.read_exact(&mut frame).await?;

                // Through [`Zstd`] rather than through `DecompressReader::new`. That
                // leaves one implementation of "what a zstd frame means and what it
                // must produce" rather than two that can disagree. This is the
                // match on the codec byte. A read is answered by the record, not by
                // whatever policy wrote it.
                let bytes = Decompressor::decompress(&Zstd, frame, placement.raw_len).await?;

                Ok(Box::new(DecompressReader::from_decoded(bytes)))
            }
        }
    }
}

impl<C: Compressor> storage::StoragePut for BlobStore<Write, C> {
    /// Store `len` bytes whose content hashes to `id`.
    ///
    /// A blob the store already holds is not written again. The caller can hand
    /// over every chunk it read without tracking what it has seen.
    ///
    /// The payload is hashed on its way through and checked against `id`: a
    /// mismatch fails the run rather than storing content under the wrong name.
    #[instrument(skip_all, err)]
    async fn put(&self, id: &BlobId, reader: StreamReadSeeker, len: u64) -> io::Result<()> {
        if self.holds(id) {
            tracing::trace!(already = %id, "already stored");
            return Ok(());
        }

        // A large payload streams when the policy allows streaming. A small
        // payload, or a policy that maps to nothing, stays on the buffered path.
        if len > STREAM_THRESHOLD
            && let Some(policy) = self.compression.stream_policy()
        {
            return self.put_streaming(id, reader, len, policy).await;
        }

        let raw = read_whole(reader, len).await?;

        // Checked before anything is stored, so nothing is written under the wrong
        // name. Hashing the content is also what makes the codec a write policy.
        // The same bytes deduplicate whether they were stored raw or compressed.
        if blake3::hash(&raw) != *id {
            // Nothing is written yet, so the run simply fails and the store is as
            // it was.
            return Err(hash_mismatch(id));
        }

        // The code compresses, and stores the bytes as they are when that does not
        // help. A zstd frame carries a header of its own, so on content that it
        // cannot compress the frame comes out larger than the input.
        let (codec, stored) = self.compression.compress(&raw);
        // The code checks the answer, because the policy that produced it is not
        // this crate's code. That policy need not be this crate's careful. This is
        // the single place where the "only when strictly smaller" rule is
        // *enforced* rather than observed. An implementor who returns a frame
        // larger than its input therefore costs a copy of the frame and nothing
        // else. The payload is stored as it came in, and the record says `Raw`.
        // That is exactly what the reader would have done with it. See the note on
        // [`Compressor`].
        //
        // The code checks the codec as well, and by naming `Zstd` rather than by
        // asking the store's decoders. `Raw` in a record means "these are the bytes
        // that hashed to this id". A policy that paired it with anything else would
        // be naming bytes that the store never hashed. Nothing on the read path
        // checks what it hands back. The pairing is therefore worth enforcing where
        // it is written rather than assuming that a caller got it right. `Zstd` is
        // the fail-closed arm in exchange. Its decode validates the frame against
        // the `raw_len` that the header claims. That is why the store may keep a
        // frame on a policy's word that it decodes to exactly that.
        //
        // `Compression` never trips this, because it applies the rule itself and
        // hands back a borrow of `raw` when the frame did not win.
        let keep = matches!(codec, Codec::Zstd) && stored.len() < raw.len();
        let (codec, stored) = if keep {
            (Codec::Zstd, stored)
        } else {
            (Codec::Raw, Cow::Borrowed(raw.as_slice()))
        };

        let stored_len = u64::try_from(stored.len()).unwrap_or(u64::MAX);

        self.write_record(id, stored.as_ref(), stored_len, len, codec)
            .await
    }

    /// Store a payload that is already encoded, after checking what it decodes to.
    ///
    /// The decode is the whole of the verification and is not optional. See
    /// [`StoragePut::put_stored`](crate::storage::StoragePut::put_stored), which
    /// says why in the terms that a caller has. This is the part of that method
    /// which is specific to a store. The frame is kept as it came in only when it
    /// is strictly smaller than the content. The frame is kept only when it names a
    /// codec that the store can decode. Otherwise the content is stored raw. That
    /// is the same fallback that `put` takes, and for the same reason.
    #[instrument(skip_all, err)]
    async fn put_stored(
        &self,
        id: &BlobId,
        reader: StreamReadSeeker,
        stored_len: u64,
        raw_len: u64,
        codec: Codec,
    ) -> io::Result<()> {
        if self.holds(id) {
            tracing::trace!(already = %id, "already stored");
            return Ok(());
        }

        // A large payload streams. This path consults no policy: the codec and
        // the two lengths decide what is kept, and the store decides that in
        // `verify_staged` the same way the buffered path below does.
        if stored_len > STREAM_THRESHOLD {
            return self
                .put_stored_streaming(id, reader, stored_len, raw_len, codec)
                .await;
        }

        let mut stored = read_whole(reader, stored_len).await?;

        // Both lengths are the caller's claims and neither is measured here. The
        // comparison between them is this store's decision rather than the
        // caller's. A frame is kept only when it is strictly smaller. A frame is
        // kept only when this store can decode it. `Zstd` is named rather than
        // asked of the decoders for the reason that `put` gives. `Raw` in a record
        // means "these are the bytes that hashed to this id". `Zstd` is the
        // fail-closed arm in exchange, since its decode validates the frame
        // against `raw_len`.
        let keep = matches!(codec, Codec::Zstd) && stored_len < raw_len;

        // The code decodes through [`Zstd`] rather than through a bare decode call.
        // That leaves one implementation of what a frame must produce rather than
        // two that can disagree. `raw_len` is the record's claim about the size.
        // The claim is checked against the frame and never trusted. Which of the
        // three answers this takes is decided by the two rules above rather than
        // here. A `Raw` payload needs no decode at all, because it *is* the
        // content.
        let content: Vec<u8> = if keep {
            // The frame is written out again below, so the decode gets a copy of
            // it rather than the frame itself.
            Decompressor::decompress(&Zstd, stored.clone(), raw_len).await?
        } else if matches!(codec, Codec::Raw) {
            // Nothing to decode: a `Raw` payload *is* the content.
            std::mem::take(&mut stored)
        } else {
            // Handed over rather than copied. A frame that is not kept is never
            // written, so the decode can take the only copy of it.
            Decompressor::decompress(&Zstd, std::mem::take(&mut stored), raw_len).await?
        };

        // The identity check that `put` makes, on the same terms and for the same
        // reason. A chunk is named by the hash of its content. Bytes that are not
        // that content must not be stored under that name, however they arrived.
        // The check happens before anything is written, so a failure leaves the
        // store as it was.
        if blake3::hash(&content) != *id {
            return Err(hash_mismatch(id));
        }

        // The record's own content length is what was decoded. The value is not
        // the `raw_len` that the caller claimed. The frame was checked against that
        // claim above, so the two agree. A `Raw` payload whose two lengths disagree
        // is stored under the length it actually has.
        let content_len = u64::try_from(content.len()).unwrap_or(u64::MAX);

        if keep {
            self.write_record(id, stored.as_slice(), stored_len, content_len, Codec::Zstd)
                .await
        } else {
            self.write_record(id, content.as_slice(), content_len, content_len, Codec::Raw)
                .await
        }
    }

    /// Make every record written so far durable.
    ///
    /// Call this method once per run. Call it after the last data record. Call it before the commit record.
    ///
    /// The method syncs every dirty segment. It syncs the segments concurrently. It reuses cached handles where possible.
    ///
    /// The method follows these steps:
    ///
    /// - List dirty segments in order.
    /// - Reuse each cached handle.
    /// - Open each missing segment.
    /// - Sync all handles concurrently.
    /// - Drop synced segments from the dirty list.
    ///
    /// A failed segment stays dirty. A later call retries it. The two-sync protocol stays unchanged.
    ///
    /// Every segment that this run wrote to is synced, not just the last. A run
    /// that crossed a boundary has records in two files. Syncing only the newer one
    /// would leave the older one's pages unordered against it.
    async fn sync_data(&self) -> io::Result<()> {
        let segments = self.dirty_segments();
        if segments.is_empty() {
            return Ok(());
        }

        // Clone cached handles out from under the lock. A sync ignores
        // the file offset. A clone therefore cannot interleave payloads
        // the way a shared write handle could.
        let mut reused: HashMap<u32, fs::File> = HashMap::new();
        {
            let files = self.open_files.lock().await;
            for segment in &segments {
                if let Some(file) = files.get(segment)
                    && let Ok(clone) = file.try_clone().await
                {
                    reused.insert(*segment, clone);
                }
            }
        }

        let syncs = segments.iter().map(|segment| {
            let path = self.segment_path(*segment);
            let cached = reused.remove(segment);
            async move {
                let file = match cached {
                    Some(file) => file,
                    None => {
                        crate::storage::at_async(
                            &path,
                            "open",
                            fs::OpenOptions::new().write(true).open(&path),
                        )
                        .await?
                    }
                };
                crate::storage::at_async(&path, "sync", file.sync_data()).await?;
                Ok::<u32, io::Error>(*segment)
            }
        });
        let results = futures_util::future::join_all(syncs).await;

        // Drop only the segments that synced. Keep failed segments dirty.
        // Keep segments added while the sync ran. A later call retries them.
        let mut synced: Vec<u32> = Vec::new();
        let mut failure: Option<io::Error> = None;
        for (segment, result) in segments.iter().zip(results) {
            match result {
                Ok(_) => synced.push(*segment),
                Err(error) => {
                    if failure.is_none() {
                        failure = Some(error);
                    }
                }
            }
        }
        {
            let mut dirty = self.dirty.lock().unwrap_or_else(PoisonError::into_inner);
            dirty.retain(|segment| !synced.contains(segment));
        }

        if let Some(error) = failure {
            return Err(error);
        }

        Ok(())
    }

    /// Publish `snapshot` as the next version, durably.
    ///
    /// Read the stamp back from the snapshot blob.
    /// Prefer [`StoragePut::commit_with_time`](crate::storage::StoragePut::commit_with_time) when the caller holds the stamp.
    /// Keep this path for callers without the stamp.
    async fn commit(&self, snapshot: &BlobId) -> io::Result<u32> {
        let (version, segment, ordinal) = self.commit_record(snapshot).await?;

        // The code writes the file after the commit, and never lets its failure fail
        // the run. A version whose index write is lost still exists and still
        // restores. A run that reported failure here would have published a version
        // and told the caller that it had not.
        //
        // The stamp travels with the index file, so listings read no blob.
        // The snapshot was just stored, so the read below always hits.
        // A missed read skips the file rather than failing the run.
        match self.snapshot_taken_at(snapshot).await {
            Ok(taken_at) => {
                if let Err(e) = write_index(
                    &self.set,
                    VersionIndex {
                        version,
                        segment,
                        ordinal,
                        snapshot: *snapshot,
                        taken_at,
                    },
                ) {
                    tracing::debug!(
                        version,
                        error = %e,
                        "published a version but could not write its index file; a scan will \
                         find it instead"
                    );
                }
            }
            Err(e) => {
                tracing::debug!(
                    version,
                    error = %e,
                    "published a version but could not read its stamp; a scan will \
                     find it instead"
                );
            }
        }

        Ok(version)
    }

    /// Publish `snapshot` as the next version with a known stamp.
    ///
    /// Take the stamp from the caller:
    ///
    /// * Skip the snapshot read.
    /// * Write the index file from the given stamp.
    async fn commit_with_time(&self, snapshot: &BlobId, taken_at: Timestamp) -> io::Result<u32> {
        let (version, segment, ordinal) = self.commit_record(snapshot).await?;

        // The code writes the file after the commit, and never lets its failure fail
        // the run. A version whose index write is lost still exists and still
        // restores. A run that reported failure here would have published a version
        // and told the caller that it had not.
        //
        // The stamp travels with the index file, so listings read no blob.
        // The caller passed the stamp, so no read happens here.
        // A missed write skips the file rather than failing the run.
        if let Err(e) = write_index(
            &self.set,
            VersionIndex {
                version,
                segment,
                ordinal,
                snapshot: *snapshot,
                taken_at,
            },
        ) {
            tracing::debug!(
                version,
                error = %e,
                "published a version but could not write its index file; a scan will \
                 find it instead"
            );
        }

        Ok(version)
    }
}

/// Read exactly `len` bytes out of `reader`, and refuse a reader that does not
/// yield exactly that many.
///
/// Both failure modes are refusals rather than tolerances, for the reason that
/// [`storage::StoragePut::put`] gives. A short read leaves a blob that can never be
/// read back whole. A long one leaves bytes past the end of the record.
///
/// The code reads the payload in full before anything is written. That is a
/// requirement of the record layout rather than an implementation convenience. A
/// header cannot be written twice. The stored length is not known until the payload
/// has been compressed.
///
/// The buffer that this allocates is a copy of one that the caller already holds.
/// The chunker materialises each chunk before offering it. A run's peak is
/// therefore one chunk rather than a whole tree, either way.
async fn read_whole(mut reader: StreamReadSeeker, len: u64) -> io::Result<Vec<u8>> {
    let wanted = usize::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("a payload of {len} bytes does not fit in memory"),
        )
    })?;
    let mut bytes = Vec::with_capacity(wanted);

    {
        let mut source = BufReader::with_capacity(COPY_BUFFER, reader.as_mut().take(len));
        let read = tokio::io::AsyncReadExt::read_to_end(&mut source, &mut bytes).await?;
        if u64::try_from(read).unwrap_or(u64::MAX) != len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("declared {len} bytes but the reader yielded only {read}"),
            ));
        }
    }

    let mut trailing = [0u8; 1];
    if reader.read(&mut trailing).await? != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("the reader yielded more than the declared {len} bytes"),
        ));
    }

    Ok(bytes)
}

/// The error for content whose hash is not the name it was offered under.
///
/// Every write path checks this before anything is stored. Nothing is written
/// under a name its content does not own.
fn hash_mismatch(id: &BlobId) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("the content does not hash to {id}, so it was not stored"),
    )
}

/// Stage exactly `len` bytes to a temp file.
///
/// - Read the input in bounded chunks.
/// - Hash the bytes that are staged.
/// - Reject a short input with an error.
/// - Reject a long input with an error.
/// - Return the staged file and its hash.
///
/// The file is left at the end. Every reader of a staged file rewinds first.
async fn stage_raw_streaming(
    mut reader: StreamReadSeeker,
    len: u64,
) -> io::Result<(std::fs::File, blake3::Hash)> {
    let std_tmp = tempfile::tempfile()?;
    let mut tmp = fs::File::from_std(std_tmp);

    let mut hasher = blake3::Hasher::new();
    let mut remaining = len;
    let mut buf = vec![0u8; COPY_BUFFER];

    while remaining > 0 {
        let want = usize::try_from(remaining.min(COPY_BUFFER as u64)).unwrap_or(COPY_BUFFER);
        let read = reader.read(&mut buf[..want]).await?;
        if read == 0 {
            break;
        }
        tmp.write_all(&buf[..read]).await?;
        hasher.update(&buf[..read]);
        remaining -= u64::try_from(read).unwrap_or(u64::MAX);
    }

    if remaining != 0 {
        let read = len - remaining;
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!("declared {len} bytes but the reader yielded only {read}"),
        ));
    }

    let mut trailing = [0u8; 1];
    if reader.read(&mut trailing).await? != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("the reader yielded more than the declared {len} bytes"),
        ));
    }

    tmp.flush().await?;

    Ok((tmp.into_std().await, hasher.finalize()))
}

/// Compress a staged payload on a worker thread.
///
/// - Rewind the staged file.
/// - Build a frame at the level the policy names.
/// - Keep the frame only when it is strictly smaller.
/// - Fall back to raw when compression is off, fails, or does not win.
/// - Return the file a record writes, with its lengths and codec.
///
/// The frame is built with the content size pledged, so the record's frame
/// declares exactly `raw_len`. A frame without a declared size would be refused
/// by every later read, so the pledge is a correctness requirement here.
///
/// The returned file is left wherever the work left it. [`BlobStore::write_record`]
/// rewinds whatever it is handed.
fn compress_staged(
    raw: std::fs::File,
    raw_len: u64,
    policy: Compression,
) -> io::Result<(std::fs::File, u64, Codec)> {
    use std::io::Seek as _;

    let mut raw = raw;
    // `stage_raw_streaming` leaves the file at the end. The encoder reads from
    // the start.
    raw.seek(std::io::SeekFrom::Start(0))?;

    let Some(level) = policy.level() else {
        return Ok((raw, raw_len, Codec::Raw));
    };

    let Ok(dest) = tempfile::tempfile() else {
        return Ok((raw, raw_len, Codec::Raw));
    };

    let encoded: io::Result<(std::fs::File, u64)> = (|| {
        let mut encoder = match zstd::stream::Encoder::new(dest, i32::from(level)) {
            Ok(encoder) => encoder,
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("could not start zstd encoder: {e}"),
                ));
            }
        };
        if let Err(e) = encoder.set_pledged_src_size(Some(raw_len)) {
            return Err(io::Error::new(
                e.kind(),
                format!("could not pledge payload size: {e}"),
            ));
        }
        if let Err(e) = std::io::copy(&mut raw, &mut encoder) {
            return Err(io::Error::new(
                e.kind(),
                format!("could not feed payload to encoder: {e}"),
            ));
        }
        let finished = match encoder.finish() {
            Ok(finished) => finished,
            Err(e) => {
                return Err(io::Error::new(
                    e.kind(),
                    format!("could not finish zstd frame: {e}"),
                ));
            }
        };
        let frame_len = finished.metadata().map_or(u64::MAX, |m| m.len());
        Ok((finished, frame_len))
    })();

    match encoded {
        Ok((frame, frame_len)) if frame_len < raw_len => Ok((frame, frame_len, Codec::Zstd)),
        // The frame lost the comparison, or the encode failed. The frame is
        // dropped either way. The raw bytes are stored as they are, which is a
        // correct record however the encode went.
        _ => Ok((raw, raw_len, Codec::Raw)),
    }
}

/// What a verified staged payload is kept as.
///
/// A record writes the file the arm carries, and the arm's lengths are the
/// lengths the record claims. The three arms are the three answers the buffered
/// path can give, and they are given under the same rules.
enum Staged {
    /// The staged frame is kept, and it is strictly smaller than its content.
    Frame {
        /// The staged frame.
        file: std::fs::File,
        /// The content length the record claims.
        raw_len: u64,
    },
    /// The staged bytes are the content, and they are kept as they are.
    Content {
        /// The staged bytes.
        file: std::fs::File,
        /// The content length the record claims, which is the measured length.
        raw_len: u64,
    },
    /// The staged frame decoded into a second file, because its frame lost.
    Decoded {
        /// The decoded content.
        file: std::fs::File,
        /// The content length the record claims, which is the verified length.
        raw_len: u64,
    },
}

/// Verify a staged `put_stored` payload on a worker thread.
///
/// - Hash a raw payload as it stands.
/// - Check a frame's declared size against `raw_len`.
/// - Hash the content a frame decodes to.
/// - Refuse a frame that is not a single whole frame.
/// - Keep the frame only when it is strictly smaller.
/// - Decode into a second file when the frame is not kept.
/// - Return the file a record writes, with its lengths.
///
/// The buffered path decodes through [`Zstd::decompress`]. So does every later
/// read. This function checks the same things, in the same words, so a payload
/// it accepts is a payload a read accepts. A payload a read would refuse is
/// refused here, before the run can go on.
///
/// The staged file is left wherever the work left it.
/// [`BlobStore::write_record`] rewinds whatever it is handed.
fn verify_staged(
    stored: std::fs::File,
    stored_len: u64,
    raw_len: u64,
    codec: Codec,
    id: BlobId,
) -> io::Result<Staged> {
    use std::io::Seek as _;

    let mut stored = stored;
    // `stage_raw_streaming` leaves the file at the end.
    stored.seek(std::io::SeekFrom::Start(0))?;

    // A `Raw` payload *is* the content. There is no decode and no length to
    // check, so the hash is the whole of the verification. A `Raw` payload whose
    // two lengths disagree is stored under the length it actually has.
    if matches!(codec, Codec::Raw) {
        let hash = hash_staged(&mut stored)?;
        if hash != id {
            return Err(hash_mismatch(&id));
        }
        return Ok(Staged::Content {
            file: stored,
            raw_len: stored_len,
        });
    }

    // The frame must declare a size, and the size it declares must be `raw_len`.
    // The declared size is read from the frame header rather than taken from the
    // record, for the reason [`Zstd::decompress`] gives: the header is a claim
    // the frame carries about itself, and the record's `raw_len` is checked
    // against it rather than trusted by it.
    let declared = frame_content_size(&mut stored)?;
    if declared != raw_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "a compressed payload declares {declared} bytes but its record claims {raw_len}"
            ),
        ));
    }

    // A frame is kept only when it is strictly smaller than its content. The
    // decode runs either way: a kept frame is verified by it, and a frame that
    // loses is replaced by the content it decodes to.
    let keep = stored_len < raw_len;
    let mut out = if keep {
        None
    } else {
        Some(tempfile::tempfile()?)
    };
    let (hash, count, consumed) = decode_staged(&mut stored, out.as_mut())?;

    // The content a frame produces must be exactly `raw_len` bytes. A frame that
    // comes up short or long is a damaged record, however good its hash looks.
    if count != raw_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "a compressed payload decoded to {count} bytes but its record claims {raw_len}"
            ),
        ));
    }

    if hash != id {
        return Err(hash_mismatch(&id));
    }

    // The staged bytes must be exactly one frame. Bytes past the frame would be
    // stored with it and refused by every later read, so they are refused here.
    // The buffered path reaches the same refusal through the decode itself.
    if consumed != stored_len {
        return Err(io::Error::other(format!(
            "a compressed payload carries {} bytes past its frame",
            stored_len - consumed
        )));
    }

    match out {
        Some(file) => Ok(Staged::Decoded { file, raw_len }),
        None => Ok(Staged::Frame {
            file: stored,
            raw_len,
        }),
    }
}

/// Read the content size a staged frame declares, and rewind the file.
///
/// - Read the frame header from the start of the file.
/// - Rewind the file after the read.
/// - Refuse a frame whose header cannot be parsed.
/// - Refuse a frame that declares no size.
///
/// A frame header is at most 18 bytes. A staged payload here is longer, so the
/// read cannot come up short.
fn frame_content_size(stored: &mut std::fs::File) -> io::Result<u64> {
    use std::io::{Read as _, Seek as _};

    stored.seek(std::io::SeekFrom::Start(0))?;
    let mut header = [0u8; 18];
    stored.read_exact(&mut header)?;
    stored.seek(std::io::SeekFrom::Start(0))?;

    zstd::zstd_safe::get_frame_content_size(&header)
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
        })
}

/// Decode one frame from `stored`, hashing the content it produces.
///
/// - Read the frame through a bounded buffer.
/// - Write the content to `out` when one is given.
/// - Hash the content while it is produced.
/// - Return the content hash, its length, and the consumed byte count.
///
/// A decode error is the error zstd gives. The buffered path reports the same
/// errors, and for the same failures. The frame is read in single-frame mode, so
/// bytes past the frame are left unread for the caller to find.
fn decode_staged(
    stored: &mut std::fs::File,
    mut out: Option<&mut std::fs::File>,
) -> io::Result<(blake3::Hash, u64, u64)> {
    use std::io::{Read as _, Seek as _, Write as _};

    let buffered = std::io::BufReader::with_capacity(COPY_BUFFER, &mut *stored);
    let mut decoder = zstd::stream::read::Decoder::with_buffer(buffered)?.single_frame();

    let mut hasher = blake3::Hasher::new();
    let mut count = 0u64;
    let mut chunk = vec![0u8; COPY_BUFFER];

    loop {
        let read = decoder.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        hasher.update(&chunk[..read]);
        count += u64::try_from(read).unwrap_or(u64::MAX);
        if let Some(out) = out.as_mut() {
            out.write_all(&chunk[..read])?;
        }
    }

    let mut buffered = decoder.finish();
    // The decoder read ahead through the buffer it was given. Bytes the decoder
    // did not consume wait in that buffer, so the position the reader reports is
    // the span of the frame. That is what a `put_stored` caller must have offered.
    let consumed = buffered.stream_position()?;

    Ok((hasher.finalize(), count, consumed))
}

/// Hash a file whole, from its start.
///
/// - Read the file in bounded chunks.
/// - Hash the bytes that are read.
/// - Return the hash.
fn hash_staged(stored: &mut std::fs::File) -> io::Result<blake3::Hash> {
    use std::io::{Read as _, Seek as _};

    stored.seek(std::io::SeekFrom::Start(0))?;

    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; COPY_BUFFER];

    loop {
        let read = stored.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }

    Ok(hasher.finalize())
}

/// Write a version's index file, best-effort.
///
/// This function is never called on a path where its failure matters. That is the
/// point. The store's commit record is the version's existence. This file is a
/// faster way to find it.
fn write_index(set: &IndexSet, index: VersionIndex) -> io::Result<()> {
    index.write_atomic(&set.index_path(index.version))
}

impl<C: Compressor> BlobStore<Write, C> {
    /// Write the commit record and register the version number.
    ///
    /// Follow these steps:
    ///
    /// * Encode the commit header.
    /// * Append the record at the reserved offset.
    /// * Sync the file.
    /// * Register the version in memory.
    ///
    /// Return the version with its segment and ordinal.
    async fn commit_record(&self, snapshot: &BlobId) -> io::Result<(u32, u32, u32)> {
        // A commit's payload is the hash that it commits to. Its two lengths are
        // that hash's width. The value is `HASH`, not `snapshot.len()`. A `BlobId`
        // is a hash, and a hash is not an array. Asking it for a length is a
        // question about the wrong thing. That question compiled, because the value
        // used to be a `[u8; 32]`.
        let hash = snapshot.as_bytes();
        let hash_len = 32_u64;

        // Always raw: the scan requires a commit to be exactly one uncompressed
        // hash, so a compressed one would be invisible to the scan that finds it.
        let mut header = Vec::new();
        record::encode_header(
            &mut header,
            Kind::Commit,
            storage::Codec::Raw,
            hash,
            hash_len,
            hash_len,
        );

        let total = u64::try_from(header.len()).unwrap_or(u64::MAX) + hash_len;
        let reservation = self.reserve(total)?;
        let offset = reservation.offset;

        // Its own handle, for the same reason that `put` takes one. A clone shares
        // the offset that this write is aimed with.
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.segment_path(reservation.segment))
            .await?;

        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(&header).await?;
        file.write_all(snapshot.as_bytes()).await?;
        file.flush().await?;

        // The second sync, and the one that makes the run real.
        file.sync_data().await?;

        // The version, and which commit record within this segment it is. The
        // ordinal is counted rather than read back, because the run knows it. The
        // ordinal is what lets a reader confirm an index file from this segment
        // alone.
        let mut contents = Contents::lock(&self.contents);

        let version = u32::try_from(contents.commits.len() + 1)
            .map_err(|_| io::Error::other("a store cannot hold more than u32::MAX versions"))?;
        contents.commits.push(*snapshot);

        let count = contents
            .commits_per_segment
            .entry(reservation.segment)
            .or_insert(0);
        let ordinal = *count;
        *count = count.saturating_add(1);

        Ok((version, reservation.segment, ordinal))
    }

    /// Claim `total` bytes for one record.
    ///
    /// Return a range in the current segment when it fits.
    /// Start the next segment when it does not fit.
    /// Reject a record larger than the whole limit.
    ///
    /// Fast path:
    ///
    /// - Lock `reserved`.
    /// - Claim the range.
    /// - Release `reserved`.
    /// - Mark the segment dirty.
    ///
    /// Slow path:
    ///
    /// - Release `reserved`.
    /// - Lock `rollover`.
    /// - Recheck `reserved`.
    /// - Create the file outside `reserved`.
    /// - Publish the new segment under `reserved`.
    ///
    /// Lock order:
    ///
    /// - Lock `rollover` before `reserved`.
    /// - Release `reserved` before `dirty`.
    /// - Never hold `reserved` across file creation.
    ///
    /// Correctness notes:
    ///
    /// - Hold `rollover` across creation and publish.
    /// - Recheck `reserved` after acquiring `rollover`.
    /// - Publish the file before any writer uses it.
    /// - Cover this rule with `concurrent_writes_across_a_rollover_do_not_overlap`.
    ///
    /// Performance notes:
    ///
    /// - Keep the fast path short.
    /// - Run file creation outside `reserved`.
    /// - Let small writes proceed during a rollover.
    /// - Pay one file sync per rollover only.
    fn reserve(&self, total: u64) -> io::Result<Reservation> {
        loop {
            let full = {
                let mut reserved = self.reserved.lock().unwrap_or_else(PoisonError::into_inner);
                let limit = self.limit.bytes();
                let header = u64::try_from(STORE_HEADER).unwrap_or(u64::MAX);
                // `offset` starts past the store header.
                // It already counts those bytes against the file size.
                let end = reserved.offset.saturating_add(total);
                if end <= limit {
                    let reservation = Reservation {
                        segment: reserved.segment,
                        offset: reserved.offset,
                    };
                    reserved.offset = end;
                    drop(reserved);
                    self.note_segment(reservation.segment);
                    return Ok(reservation);
                }
                // Roll only a non-empty segment.
                // Report an oversized record instead of retrying forever.
                if reserved.offset <= header {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "a record of {total} bytes does not fit in a segment of {limit}, which is \
                             the limit this store was opened with; raise it to store this blob"
                        ),
                    ));
                }
                // Reject a record that cannot fit in a fresh segment.
                // Check here to avoid creating an empty file and then failing.
                if header.saturating_add(total) > limit {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "a record of {total} bytes does not fit in a segment of {limit}, which is \
                             the limit this store was opened with; raise it to store this blob"
                        ),
                    ));
                }
                reserved.segment
            };

            // Serialize rollovers.
            // Fast path writers continue to use the old segment meanwhile.
            let rollover = self.rollover.lock().unwrap_or_else(PoisonError::into_inner);

            let next = {
                let reserved = self.reserved.lock().unwrap_or_else(PoisonError::into_inner);
                let limit = self.limit.bytes();
                let header = u64::try_from(STORE_HEADER).unwrap_or(u64::MAX);
                let end = reserved.offset.saturating_add(total);
                if end <= limit || reserved.segment != full {
                    // Another thread already rolled or freed space.
                    // Retry through the fast path without holding `rollover`.
                    drop(reserved);
                    drop(rollover);
                    continue;
                }
                if reserved.offset <= header || header.saturating_add(total) > limit {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "a record of {total} bytes does not fit in a segment of {limit}, which is \
                             the limit this store was opened with; raise it to store this blob"
                        ),
                    ));
                }
                let next = reserved.segment.checked_add(1).ok_or_else(|| {
                    io::Error::other("a store cannot hold more than u32::MAX segments")
                })?;
                // A segment number uses zero-padded `SEGMENT_DIGITS`.
                // Padding never truncates.
                // One past the width would name a file that the parser rejects.
                if next > crate::layout::MAX_SEGMENT {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "this set has reached {MAX_SEGMENT} segments, which is as many as the \
                             naming scheme can name; a run that fills segment {MAX_SEGMENT} cannot be \
                             completed, so nothing was written",
                            MAX_SEGMENT = crate::layout::MAX_SEGMENT
                        ),
                    ));
                }
                // Drop `reserved` before file creation.
                // Keep `rollover` to keep one winner.
                drop(reserved);
                next
            };

            // Create the file before any writer uses it.
            // Hold `rollover` but not `reserved` here.
            create_segment(&self.segment_path(next), next)?;

            let header = u64::try_from(STORE_HEADER).unwrap_or(u64::MAX);
            let reservation = Reservation {
                segment: next,
                offset: header,
            };
            {
                let mut reserved = self.reserved.lock().unwrap_or_else(PoisonError::into_inner);
                // Only this thread holds `rollover`.
                // No other thread can publish a new segment now.
                // Fast path writes may have advanced the old offset meanwhile.
                // Discard that tail and seal the old segment.
                reserved.segment = next;
                reserved.offset = header.saturating_add(total);
            }
            drop(rollover);
            self.note_segment(next);
            return Ok(reservation);
        }
    }

    /// Record that `segment` was written to by this run.
    ///
    /// Call without holding `reserved`.
    /// Call without holding `rollover`.
    /// Let `sync_data` find the segment later.
    fn note_segment(&self, segment: u32) {
        let mut dirty = self.dirty.lock().unwrap_or_else(PoisonError::into_inner);

        if !dirty.contains(&segment) {
            dirty.push(segment);
        }
    }

    /// Whether this set already holds the blob named by `id`.
    ///
    /// Asked before a payload is read rather than after. A blob that the store holds
    /// is not written again, so the reader is never even drained. A caller that
    /// hands over every chunk it read can therefore stop tracking what the set
    /// has.
    fn holds(&self, id: &BlobId) -> bool {
        Contents::lock(&self.contents).blobs.contains_key(id)
    }

    /// Append one blob record, and index the blob once the payload is on disk.
    ///
    /// Every write path comes through here. `put` and `put_stored` differ in how
    /// they decide what the payload is. They never differ in how a record is
    /// written. There is one record layout, and one place that knows where a blob
    /// lands.
    ///
    /// `stored_len` is the length the payload occupies on disk. `raw_len` is the
    /// length of the *content*. That is what the header's second length carries.
    /// That is also what a reader compares a snapshot's lengths against.
    ///
    /// The payload is copied through a bounded buffer, so a record can hold a
    /// payload larger than this process would hold in memory. `payload` must
    /// yield exactly `stored_len` bytes, and it must stand at its start. A
    /// payload that ends early is an error.
    async fn write_record<R>(
        &self,
        id: &BlobId,
        mut payload: R,
        stored_len: u64,
        raw_len: u64,
        codec: Codec,
    ) -> io::Result<()>
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        // The copy buffer is sized to the payload, capped at [`COPY_BUFFER`]. A
        // small payload then pays for no oversized allocation.
        let chunk = COPY_BUFFER.min(usize::try_from(stored_len).unwrap_or(COPY_BUFFER));

        let mut header = Vec::with_capacity(
            usize::try_from(record::header_len(stored_len, raw_len)).unwrap_or(0),
        );
        record::encode_header(
            &mut header,
            Kind::Blob,
            codec,
            id.as_bytes(),
            stored_len,
            raw_len,
        );

        let total = u64::try_from(header.len()).unwrap_or(u64::MAX) + stored_len;
        let reservation = self.reserve(total)?;
        let offset = reservation.offset;

        // A handle of its own, never `try_clone`. A dup shares the file offset, so
        // concurrent puts would interleave headers into each other's payloads. The
        // store would then read back as a shorter prefix of itself.
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.segment_path(reservation.segment))
            .await?;

        // Positional, never append. `append` would redirect every write to wherever
        // the end of the file happened to be. That end is the offset that the code
        // just decided not to use.
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(&header).await?;

        let mut buf = vec![0u8; chunk];
        let mut remaining = stored_len;
        while remaining > 0 {
            let want = usize::try_from(remaining.min(chunk as u64)).unwrap_or(chunk);
            let read = payload.read(&mut buf[..want]).await?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!("the payload of {stored_len} bytes ended before it was copied whole"),
                ));
            }
            file.write_all(&buf[..read]).await?;
            remaining -= u64::try_from(read).unwrap_or(u64::MAX);
        }

        // Nothing is synced here. The run's single `sync_data` is the durability
        // point. Syncing per blob would pay for thousands of syncs.
        file.flush().await?;

        let payload_offset = offset + u64::try_from(header.len()).unwrap_or(u64::MAX);

        // The index receives the blob only once the payload is written, so nothing
        // ever points at bytes that are not there.
        Contents::lock(&self.contents).blobs.insert(
            *id,
            Placement {
                segment: reservation.segment,
                offset: payload_offset,
                stored_len,
                // The *content* length, which is why the two fields are named for
                // what they hold rather than for how Rust spells `Vec::len`.
                raw_len,
                codec,
            },
        );

        tracing::trace!(
            segment = reservation.segment,
            offset,
            raw_length = raw_len,
            stored_length = stored_len,
            ?codec,
            "stored blob"
        );

        Ok(())
    }

    /// Store a large payload without holding it whole.
    ///
    /// - Stage the content to a temp file.
    /// - Hash the content while it is staged.
    /// - Compress the staged content on a worker thread.
    /// - Write the staged record from the stored file.
    ///
    /// Nothing is written to the store before the hash is checked. The store is
    /// left as it was by a payload that does not hash to `id`.
    async fn put_streaming(
        &self,
        id: &BlobId,
        reader: StreamReadSeeker,
        len: u64,
        policy: Compression,
    ) -> io::Result<()> {
        let (raw_file, hash) = stage_raw_streaming(reader, len).await?;

        if hash != *id {
            return Err(hash_mismatch(id));
        }

        let staged = tokio::task::spawn_blocking(move || compress_staged(raw_file, len, policy))
            .await
            .map_err(|join| io::Error::other(format!("a compression task failed: {join}")))??;

        let (stored_file, stored_len, codec) = staged;

        // The staged file stands wherever the compression left it. The record is
        // written from its start.
        let mut staged = fs::File::from_std(stored_file);
        staged.seek(SeekFrom::Start(0)).await?;

        self.write_record(id, staged, stored_len, len, codec).await
    }

    /// Store a large already-encoded payload without holding it whole.
    ///
    /// - Stage the stored bytes to a temp file.
    /// - Verify the staged payload on a worker thread.
    /// - Write the record the verification answered for.
    ///
    /// The verification decodes the payload when it is a frame, and hashes the
    /// content either way. Nothing is written to the store before both checks
    /// pass, so a refused payload leaves the store as it was.
    async fn put_stored_streaming(
        &self,
        id: &BlobId,
        reader: StreamReadSeeker,
        stored_len: u64,
        raw_len: u64,
        codec: Codec,
    ) -> io::Result<()> {
        let (staged, _) = stage_raw_streaming(reader, stored_len).await?;
        let expected = *id;

        let verified = tokio::task::spawn_blocking(move || {
            verify_staged(staged, stored_len, raw_len, codec, expected)
        })
        .await
        .map_err(|join| io::Error::other(format!("a verification task failed: {join}")))??;

        match verified {
            Staged::Frame { file, raw_len } => {
                let mut frame = fs::File::from_std(file);
                frame.seek(SeekFrom::Start(0)).await?;
                self.write_record(id, frame, stored_len, raw_len, Codec::Zstd)
                    .await
            }
            Staged::Content { file, raw_len } => {
                let mut content = fs::File::from_std(file);
                content.seek(SeekFrom::Start(0)).await?;
                self.write_record(id, content, stored_len, raw_len, Codec::Raw)
                    .await
            }
            Staged::Decoded { file, raw_len } => {
                let mut decoded = fs::File::from_std(file);
                decoded.seek(SeekFrom::Start(0)).await?;
                self.write_record(id, decoded, raw_len, raw_len, Codec::Raw)
                    .await
            }
        }
    }
}

impl<C: Compressor> BlobStore<Write, C> {
    /// Write an index file for every version this store holds.
    ///
    /// Everything here is derived from the commit records. A set with no index files
    /// at all is therefore still a complete set. This method only brings the cache
    /// back up to date.
    ///
    /// The code counts a file that cannot be written as not written. That is not an
    /// error.
    ///
    /// # Errors
    ///
    /// Only if the version list cannot be read, which is the store's own in-memory
    /// index and so cannot really fail.
    pub async fn write_index_files(&self) -> io::Result<usize> {
        let mut written = 0;

        for version in self.versions()? {
            let Some(snapshot) = self.snapshot(version).ok() else {
                continue;
            };

            let Some((segment, ordinal)) = self.commit_position(version) else {
                continue;
            };

            let Ok(taken_at) = self.snapshot_taken_at(&snapshot).await else {
                continue;
            };

            if write_index(
                &self.set,
                VersionIndex {
                    version,
                    segment,
                    ordinal,
                    snapshot,
                    taken_at,
                },
            )
            .is_ok()
            {
                written += 1;
            }
        }

        Ok(written)
    }

    /// Which segment, and which commit record within it, a version was published
    /// by.
    ///
    /// Derived from the per-segment commit counts that the scan built. The value is
    /// therefore the same arithmetic that the writer used when it recorded the
    /// ordinal.
    fn commit_position(&self, version: u32) -> Option<(u32, u32)> {
        let counts = Contents::lock(&self.contents);

        let mut remaining = version.checked_sub(1)?;
        let mut segments: Vec<(&u32, &u32)> = counts.commits_per_segment.iter().collect();
        segments.sort_by_key(|(segment, _)| **segment);

        for (segment, count) in segments {
            if remaining < *count {
                return Some((*segment, remaining));
            }
            remaining -= *count;
        }

        None
    }

    /// List segments this run wrote to.
    ///
    /// Answer in segment order. Answer with numbers. Callers map numbers to paths.
    fn dirty_segments(&self) -> Vec<u32> {
        self.dirty
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// What scanning a whole set found, and where the log should carry on from.
struct Scanned {
    /// Every segment of the set, in order.
    segments: Vec<PathBuf>,
    /// Every blob across every segment, by content hash.
    blobs: HashMap<BlobId, Placement>,
    /// The snapshot hash of each committed version, in log order.
    commits: Vec<BlobId>,
    /// How many commit records each segment holds, by segment number.
    commits_per_segment: HashMap<u32, u32>,
    /// Where the log carries on from. The value is `None` for a store opened for
    /// reading, which has nowhere to write.
    ///
    /// This is an `Option` rather than a sentinel because a sentinel here was not
    /// reachable and did not say what it meant. `write_into` was documented as
    /// "`u32::MAX`, and a reader never uses it". The reader set it twice. It set it
    /// once while initialising the struct. It set it again at the end with a comment
    /// that explained that the value was never read. A `BlobStore<Read>` then held
    /// a `Reservation` built from a segment number that does not exist. It also held
    /// `SegmentLimit` and `Compression` fields defaulted to values that it ignores.
    append: Option<Reservation>,
}

impl Scanned {
    /// Add one segment's scan to what the set scan holds.
    ///
    /// Extend the blob index with the segment's blobs.
    /// Grow the commit log by the segment's commits.
    /// Record the per-segment commit count.
    fn absorb(&mut self, segment: u32, blobs: HashMap<BlobId, Placement>, commits: Vec<BlobId>) {
        self.blobs.extend(blobs);
        self.commits_per_segment
            .insert(segment, u32::try_from(commits.len()).unwrap_or(u32::MAX));
        self.commits.extend(commits);
    }
}

/// What a scan is allowed to do to the set.
///
/// The two halves of this were, for a long time, the same sixty lines written
/// twice. They differed in four places. Those four are the whole of the
/// difference, and they are not cosmetic.
///
/// * a writer creates segment 0 for a fresh set, and a reader reports that there
///   is no set.
/// * a writer opens the last segment for writing, and a reader opens every one
///   read-only.
/// * a writer *reclaims* an unverified tail from the last segment. It refuses
///   one in any other segment. A reader refuses one anywhere, because a read
///   does not repair a store.
/// * only a writer has somewhere to carry on from.
///
/// The first two are ordinary. The third is the one that matters. It is why this
/// is a parameter rather than a boolean or a pair of flags. Whether an unverified
/// tail is damage or an unfinished run depends entirely on whether this run is
/// going to write, and on nothing else. Spelling that out here means the two
/// behaviours cannot drift. A reader that started repairing would be a reader that
/// destroys evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scan {
    /// Opening for writing: repair what a run can safely repair.
    Recover,
    /// Opening for reading: change nothing.
    Inspect,
}

impl Scan {
    /// Whether this scan is going to write, and so may reclaim a tail.
    const fn repairs(self) -> bool {
        matches!(self, Self::Recover)
    }
}

/// Open a set for writing. The function creates the first segment if there is none,
/// scans every segment, and discards an unfinished tail from the last one.
///
/// Only the last segment is ever truncated. A segment before it was sealed, and a
/// sealed segment is never written again. A tail that does not verify there is
/// therefore damage rather than a run that stopped.
fn recover(set: &IndexSet) -> io::Result<Scanned> {
    scan_set(set, Scan::Recover)
}

/// Open a set for reading, without writing to it.
fn read_only_scan(set: &IndexSet) -> io::Result<Scanned> {
    scan_set(set, Scan::Inspect)
}

/// Check that `path` holds this set's segment `segment`, and prove it.
///
/// Check the store header first.
/// Compare the number the header claims with the slot the file fills.
/// Refuse every other file as [`not_a_store`].
fn check_segment_number(
    file: &mut (impl io::Read + io::Seek),
    path: &Path,
    segment: u32,
    mode: Scan,
) -> io::Result<()> {
    let claimed = check_store_header(file, path)?;
    if claimed != segment {
        return Err(not_a_store(
            path,
            &renamed_segment_message(claimed, segment, mode),
        ));
    }
    Ok(())
}

/// Say why a segment file cannot fill its slot in this set.
///
/// A writer must recover the set and cannot recover a renamed segment. That
/// writer names what the rename means. A reader refuses the same file in one
/// short sentence. Keep both messages here, so the two cannot drift apart.
fn renamed_segment_message(claimed: u32, segment: u32, mode: Scan) -> String {
    if mode.repairs() {
        format!(
            "its header says it is segment {claimed}, but it is named as segment \
             {segment}, so the set's segments have been moved around or renamed"
        )
    } else {
        format!(
            "its header says it is segment {claimed}, but it is named as segment \
             {segment}"
        )
    }
}

/// Scan one sealed segment, believing a cache that verifies and only that one.
///
/// - Try the verified cache first.
/// - Scan the whole segment on a miss.
/// - Refuse a tail that does not verify.
/// - Store the fresh scan when the open repairs.
///
/// The cache is a hint and carries no authority of its own. `load_checked`
/// proves it against the segment's headers and commit records. This function
/// therefore runs either a proven cache or a full scan, never both, and never
/// a half of either.
fn scan_sealed_segment(
    set: &IndexSet,
    segment: u32,
    path: &Path,
    mode: Scan,
) -> io::Result<record::Scan> {
    if let Some(cached) = super::segment_cache::load_checked(set, segment, path) {
        tracing::debug!(segment, path = %path.display(), "sealed segment read from cache");
        return Ok(record::Scan {
            blobs: cached.blobs,
            commits: cached.commits,
            end: cached.segment_len,
        });
    }
    let mut file = std::fs::File::open(path)?;
    check_segment_number(&mut file, path, segment, mode)?;

    let scan = record::scan(&mut file, segment, record::Blobs::Index)?;
    let len = file.metadata()?.len();
    if scan.end < len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{} is a sealed segment and has {} bytes that are not a valid record, so it \
                 is damaged rather than unfinished and nothing was truncated",
                path.display(),
                len - scan.end
            ),
        ));
    }
    if mode.repairs() {
        super::segment_cache::store(set, segment, len, &scan.commits, &scan.blobs);
    }
    Ok(scan)
}

/// Scan every segment of a set. See [`Scan`] for what the two callers differ in.
fn scan_set(set: &IndexSet, mode: Scan) -> io::Result<Scanned> {
    let mut segments = set.segments()?;

    if segments.is_empty() {
        if !mode.repairs() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no backup store at {}", set.base().display()),
            ));
        }
        let first = set.segment_path(0);
        create_segment(&first, 0)?;
        sync_parent(&first)?;
        segments.push(first);
    }

    let last = segments.len().saturating_sub(1);
    let mut out = Scanned {
        segments: segments.clone(),
        blobs: HashMap::new(),
        commits: Vec::new(),
        commits_per_segment: HashMap::new(),
        append: None,
    };

    for (index, path) in segments.iter().enumerate() {
        let segment = u32::try_from(index).unwrap_or(u32::MAX);
        let is_last = index == last;

        // Sealed segments read from a cache that verifies, else from a full
        // scan of their own. The last segment always scans fully, because a
        // cache would hide the tail that recovery is about to discard.
        if !is_last {
            let scan = scan_sealed_segment(set, segment, path, mode)?;
            out.absorb(segment, scan.blobs, scan.commits);
            continue;
        }

        // Only the last segment reaches here. Everything before it continued,
        // so this open takes the segment for writing whatever the set holds.
        let mut file = if mode.repairs() {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .truncate(false)
                .open(path)?
        } else {
            std::fs::File::open(path)?
        };

        check_segment_number(&mut file, path, segment, mode)?;

        let scan = record::scan(&mut file, segment, record::Blobs::Index)?;
        let len = file.metadata()?.len();

        if scan.end < len {
            if !mode.repairs() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{} has {} bytes past its last valid record, so it is damaged; a read does \
                         not repair a store",
                        path.display(),
                        len - scan.end
                    ),
                ));
            }

            // Bytes past the last valid record were never published, so they are
            // not data that anybody can lose. The code syncs before writing over the
            // space. A crash that brought the old tail back would put the scan and
            // the append pointer out of step.
            file.set_len(scan.end)?;
            file.sync_data()?;

            tracing::info!(
                path = %path.display(),
                reclaimed = len - scan.end,
                "discarded the tail of an unfinished run"
            );
        }

        out.absorb(segment, scan.blobs, scan.commits);

        // Recording the last segment rather than how many are sealed is what
        // makes a fresh set use the same arithmetic as a set of a thousand.
        out.append = Some(Reservation {
            segment,
            offset: scan.end,
        });
    }

    Ok(out)
}

/// Create segment `segment` and write its header.
///
/// `create_new` rather than `create`, so an existing segment is an error. The code
/// does not write a header over the top of records that are already there.
fn create_segment(path: &Path, segment: u32) -> io::Result<()> {
    use std::io::Write as _;

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;

    file.write_all(&record::encode_store_header(segment))?;
    file.sync_data()?;

    Ok(())
}

/// Check that `path` is a store segment, and return the number its header claims.
///
/// Check magic and version.
/// Return the segment number on a match.
/// Refuse every other file with an error.
/// Never repair.
/// Never truncate.
///
/// This function takes any `Read + Seek` source. A remote backend can check
/// header bytes it fetched. It needs no local file. The function finds the
/// length by seeking to the end. It does not read file metadata.
pub(super) fn check_store_header(
    file: &mut (impl io::Read + io::Seek),
    path: &Path,
) -> io::Result<u32> {
    let len = io::Seek::seek(file, io::SeekFrom::End(0))?;
    if len < STORE_HEADER as u64 {
        return Err(not_a_store(path, "it is too short to hold a store header"));
    }

    let mut header = [0u8; STORE_HEADER];

    // From the start, whatever the handle is positioned at. A handle that has just
    // written the header is left at the end of the file.
    io::Seek::seek(file, io::SeekFrom::Start(0))?;
    io::Read::read_exact(file, &mut header)?;

    record::decode_store_header(&header).map_err(|fault| {
        let why = match fault {
            record::HeaderFault::NotAStore => {
                "it does not begin with a blu store header, so it was not written by this format"
            }
            record::HeaderFault::UnsupportedVersion => {
                "it is a store format this build does not read"
            }
        };

        not_a_store(path, why)
    })
}

fn not_a_store(path: &Path, why: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{} is not a backup store this build can read: {why}",
            path.display()
        ),
    )
}

/// Flush the directory entry for `path`, so the file that it names survives a crash.
fn sync_parent(path: &Path) -> io::Result<()> {
    let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) else {
        return Ok(());
    };

    std::fs::File::open(parent)?.sync_all()
}

#[cfg(test)]
mod write_buffer_tests {
    use std::io::Cursor;

    use tokio::io::AsyncReadExt as _;

    use super::STREAM_THRESHOLD;
    use crate::storage::{BlobStore, Codec, StorageGet as _, StoragePut as _};

    /// Five mebibytes, in byte units.
    const MIB: u64 = 1024 * 1024;

    /// Pseudo-random bytes, generated without a fixture on disk.
    ///
    /// - Mix each 8-byte block from an incrementing seed.
    /// - Truncate the result to `len`.
    ///
    /// The stream has no structure for zstd to find. A frame around it is not
    /// smaller than it.
    fn noisy(len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut block = 0u64;
        while out.len() < len {
            let mut x = block.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            x ^= x >> 30;
            x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
            x ^= x >> 27;
            out.extend_from_slice(&x.to_le_bytes());
            block += 1;
        }
        out.truncate(len);
        out
    }

    /// Bytes that compress to about half their length.
    ///
    /// - Alternate a pseudo-random block with a fixed block.
    /// - Seed each random block by its index, so nothing repeats.
    ///
    /// A frame of this fixture is large. The tests use that to put a kept frame
    /// above the streaming threshold without a huge uncompressed payload.
    fn mixed(len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut block = 0u64;
        while out.len() < len {
            let want = out.len() + 16;
            let mut seed = block.wrapping_mul(0x5DEE_CE66);
            while out.len() < want {
                let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
                x ^= x >> 30;
                x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
                x ^= x >> 27;
                out.extend_from_slice(&x.to_le_bytes());
                seed += 1;
            }
            out.extend_from_slice(b"ABCDEFGHIJKLMNOP");
            block += 1;
        }
        out.truncate(len);
        out
    }

    /// Open a writer on a fresh set at `dir`.
    async fn writer(store: &std::path::Path) -> BlobStore<super::Write, super::Compression> {
        BlobStore::builder(store).open_writer().await.unwrap()
    }

    /// Open a reader on the set at `dir`.
    async fn reader_of(store: &std::path::Path) -> BlobStore<super::Read, super::Compression> {
        BlobStore::builder(store).open_reader().await.unwrap()
    }

    /// Read the content of a blob back whole.
    async fn content<W: super::WriteMode, C: super::Compressor>(
        store: &BlobStore<W, C>,
        id: &super::BlobId,
    ) -> Vec<u8> {
        let mut out = Vec::new();
        store
            .get(id)
            .await
            .unwrap()
            .read_to_end(&mut out)
            .await
            .unwrap();
        out
    }

    /// Stream a large compressible payload through temp files.
    ///
    /// - Write five mebibytes through the streaming path.
    /// - Check the record names Zstd.
    /// - Read the payload back byte for byte.
    #[tokio::test]
    async fn stream_large_compressible_payload_through_temp_file() {
        let len = 5 * MIB;
        let payload = vec![b'a'; usize::try_from(len).unwrap()];
        assert!(
            len > STREAM_THRESHOLD,
            "the fixture must sit above the streaming threshold"
        );
        let id = blake3::hash(&payload);

        let dir = tempfile::tempdir().unwrap();
        let store = writer(&dir.path().join("store")).await;
        store
            .put(&id, Box::new(Cursor::new(payload.clone())), len)
            .await
            .unwrap();

        let placement = store.locate(&id).unwrap();
        assert_eq!(
            placement.codec,
            Codec::Zstd,
            "a compressible payload is stored as a frame"
        );
        assert!(
            placement.stored_len < placement.raw_len,
            "a kept frame is smaller than its content"
        );

        let reader = reader_of(&dir.path().join("store")).await;
        assert_eq!(
            content(&reader, &id).await,
            payload,
            "the content is restored byte for byte"
        );
    }

    /// Stream a large incompressible payload through temp files.
    ///
    /// - Write five mebibytes of pseudo-random bytes.
    /// - Check the record names Raw.
    /// - Read the payload back byte for byte.
    #[tokio::test]
    async fn stream_large_incompressible_payload_as_raw() {
        let len = 5 * MIB;
        let payload = noisy(usize::try_from(len).unwrap());
        assert!(
            len > STREAM_THRESHOLD,
            "the fixture must sit above the streaming threshold"
        );
        let id = blake3::hash(&payload);

        let dir = tempfile::tempdir().unwrap();
        let store = writer(&dir.path().join("store")).await;
        store
            .put(&id, Box::new(Cursor::new(payload.clone())), len)
            .await
            .unwrap();

        let placement = store.locate(&id).unwrap();
        assert_eq!(
            placement.codec,
            Codec::Raw,
            "a payload zstd cannot shrink is stored as it came"
        );

        let reader = reader_of(&dir.path().join("store")).await;
        assert_eq!(
            content(&reader, &id).await,
            payload,
            "the content is restored byte for byte"
        );
    }

    /// Reject a large payload with a wrong hash.
    ///
    /// - Offer five mebibytes under another hash.
    /// - Expect an `InvalidData` error.
    /// - Index nothing under the wrong hash.
    #[tokio::test]
    async fn reject_large_payload_with_wrong_hash() {
        let len = 5 * MIB;
        let payload = vec![b'b'; usize::try_from(len).unwrap()];
        assert!(
            len > STREAM_THRESHOLD,
            "the fixture must sit above the streaming threshold"
        );
        let wrong = blake3::hash(b"other content for streaming path");

        let dir = tempfile::tempdir().unwrap();
        let store = writer(&dir.path().join("store")).await;
        let error = store
            .put(&wrong, Box::new(Cursor::new(payload)), len)
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::InvalidData,
            "a wrong hash is damaged data, not an I/O failure"
        );
        assert!(
            error.to_string().contains("does not hash to"),
            "the error names the hash the store was asked for"
        );
        assert!(
            store.locate(&wrong).is_err(),
            "the store indexes nothing under a hash it refused"
        );
    }

    /// Reject a large payload with a short reader.
    ///
    /// - Declare more bytes than the reader holds.
    /// - Expect an `UnexpectedEof` error.
    /// - Name both lengths in the error.
    #[tokio::test]
    async fn reject_large_payload_with_short_reader() {
        let declared = 5 * MIB + 1000;
        let payload = vec![b'c'; usize::try_from(declared - 1000).unwrap()];
        assert!(
            declared > STREAM_THRESHOLD,
            "the declared length must sit above the streaming threshold"
        );
        let id = blake3::hash(&payload);

        let dir = tempfile::tempdir().unwrap();
        let store = writer(&dir.path().join("store")).await;
        let error = store
            .put(&id, Box::new(Cursor::new(payload)), declared)
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::UnexpectedEof,
            "a short reader can never be read back whole"
        );
        assert!(
            error.to_string().contains("yielded only"),
            "the error names what was declared and what arrived"
        );
    }

    /// Reject a large payload with a long reader.
    ///
    /// - Declare fewer bytes than the reader holds.
    /// - Expect an `InvalidData` error.
    /// - Name the declared length in the error.
    #[tokio::test]
    async fn reject_large_payload_with_long_reader() {
        let declared = 5 * MIB;
        let mut payload = vec![b'd'; usize::try_from(declared).unwrap()];
        payload.extend_from_slice(&[b'e'; 1000]);
        assert!(
            declared > STREAM_THRESHOLD,
            "the declared length must sit above the streaming threshold"
        );
        let id = blake3::hash(&payload[..usize::try_from(declared).unwrap()]);

        let dir = tempfile::tempdir().unwrap();
        let store = writer(&dir.path().join("store")).await;
        let error = store
            .put(&id, Box::new(Cursor::new(payload)), declared)
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::InvalidData,
            "a reader that does not stop where it was told is damaged input"
        );
        assert!(
            error.to_string().contains("more than the declared"),
            "the error names the declared length"
        );
    }

    /// Stream a large frame that is kept as it stands.
    ///
    /// - Compress nine mebibytes to a frame above the threshold.
    /// - Offer the frame as already encoded, with both lengths.
    /// - Check the record keeps the frame.
    /// - Read the content back byte for byte.
    #[tokio::test]
    async fn stream_a_large_frame_that_is_kept_as_it_stands() {
        let raw = mixed(usize::try_from(9 * MIB).unwrap());
        let raw_len = u64::try_from(raw.len()).unwrap();
        let id = blake3::hash(&raw);

        let frame = zstd::bulk::compress(&raw, 1).unwrap();
        let stored_len = u64::try_from(frame.len()).unwrap();
        assert!(
            stored_len > STREAM_THRESHOLD && stored_len < raw_len,
            "the fixture must give a frame above {STREAM_THRESHOLD} and below {raw_len}, got {stored_len}"
        );

        let dir = tempfile::tempdir().unwrap();
        let store = writer(&dir.path().join("store")).await;
        store
            .put_stored(
                &id,
                Box::new(Cursor::new(frame)),
                stored_len,
                raw_len,
                Codec::Zstd,
            )
            .await
            .unwrap();

        let placement = store.locate(&id).unwrap();
        assert_eq!(
            placement.codec,
            Codec::Zstd,
            "a frame that wins is kept as it came"
        );
        assert_eq!(
            placement.stored_len, stored_len,
            "the frame is stored at the length it was offered"
        );
        assert_eq!(
            placement.raw_len, raw_len,
            "the content length is the length that was claimed"
        );

        let reader = reader_of(&dir.path().join("store")).await;
        assert_eq!(
            content(&reader, &id).await,
            raw,
            "the content is decoded back byte for byte"
        );
    }

    /// Store a large frame that cannot shrink its content as raw content.
    ///
    /// - Offer a frame no smaller than its content.
    /// - Check the record names Raw.
    /// - Check both lengths and the content.
    #[tokio::test]
    async fn store_a_large_losing_frame_as_raw_content() {
        let raw = noisy(usize::try_from(5 * MIB).unwrap());
        let raw_len = u64::try_from(raw.len()).unwrap();
        let id = blake3::hash(&raw);

        let frame = zstd::bulk::compress(&raw, 1).unwrap();
        let stored_len = u64::try_from(frame.len()).unwrap();
        assert!(stored_len >= raw_len, "the fixture must not compress");
        assert!(
            stored_len > STREAM_THRESHOLD,
            "the fixture must sit above the streaming threshold"
        );

        let dir = tempfile::tempdir().unwrap();
        let store = writer(&dir.path().join("store")).await;
        store
            .put_stored(
                &id,
                Box::new(Cursor::new(frame)),
                stored_len,
                raw_len,
                Codec::Zstd,
            )
            .await
            .unwrap();

        let placement = store.locate(&id).unwrap();
        assert_eq!(
            placement.codec,
            Codec::Raw,
            "a frame that does not win is replaced by its content"
        );
        assert_eq!(
            placement.raw_len, raw_len,
            "the content length is the length that was claimed"
        );
        assert_eq!(
            placement.stored_len, raw_len,
            "the stored length is the content length"
        );

        let reader = reader_of(&dir.path().join("store")).await;
        assert_eq!(
            content(&reader, &id).await,
            raw,
            "the content is decoded back byte for byte"
        );
    }

    /// Store a large raw payload under the length it actually has.
    ///
    /// - Claim a content length the payload does not have.
    /// - Check the record takes the measured length instead.
    /// - Read the payload back byte for byte.
    #[tokio::test]
    async fn store_a_large_raw_payload_under_the_length_it_actually_has() {
        let len = 5 * MIB;
        let payload = noisy(usize::try_from(len).unwrap());
        assert!(
            len > STREAM_THRESHOLD,
            "the fixture must sit above the streaming threshold"
        );
        let id = blake3::hash(&payload);

        let dir = tempfile::tempdir().unwrap();
        let store = writer(&dir.path().join("store")).await;
        store
            .put_stored(
                &id,
                Box::new(Cursor::new(payload.clone())),
                len,
                len + 5,
                Codec::Raw,
            )
            .await
            .unwrap();

        let placement = store.locate(&id).unwrap();
        assert_eq!(
            placement.codec,
            Codec::Raw,
            "a raw payload is stored as it came"
        );
        assert_eq!(
            placement.raw_len, len,
            "a raw payload is stored under its measured length"
        );

        let reader = reader_of(&dir.path().join("store")).await;
        assert_eq!(
            content(&reader, &id).await,
            payload,
            "the content is restored byte for byte"
        );
    }

    /// Reject a large stored payload that hashes to another name.
    ///
    /// - Offer five mebibytes under another hash.
    /// - Expect an `InvalidData` error.
    /// - Index nothing under the wrong hash.
    #[tokio::test]
    async fn reject_a_large_stored_payload_that_hashes_to_another_name() {
        let len = 5 * MIB;
        let payload = noisy(usize::try_from(len).unwrap());
        let wrong = blake3::hash(b"other content for the stored path");

        let dir = tempfile::tempdir().unwrap();
        let store = writer(&dir.path().join("store")).await;
        let error = store
            .put_stored(&wrong, Box::new(Cursor::new(payload)), len, len, Codec::Raw)
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::InvalidData,
            "a wrong hash is damaged data, not an I/O failure"
        );
        assert!(
            error.to_string().contains("does not hash to"),
            "the error names the hash the store was asked for"
        );
        assert!(
            store.locate(&wrong).is_err(),
            "the store indexes nothing under a hash it refused"
        );
    }

    /// Reject a large stored payload that ends early.
    ///
    /// - Declare more bytes than the reader holds.
    /// - Expect an `UnexpectedEof` error.
    /// - Name both lengths in the error.
    #[tokio::test]
    async fn reject_a_large_stored_payload_that_ends_early() {
        let declared = 5 * MIB + 1000;
        let payload = noisy(usize::try_from(declared - 1000).unwrap());
        assert!(
            declared > STREAM_THRESHOLD,
            "the declared length must sit above the streaming threshold"
        );
        let id = blake3::hash(&payload);

        let dir = tempfile::tempdir().unwrap();
        let store = writer(&dir.path().join("store")).await;
        let error = store
            .put_stored(
                &id,
                Box::new(Cursor::new(payload)),
                declared,
                declared,
                Codec::Raw,
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::UnexpectedEof,
            "a short reader can never be read back whole"
        );
        assert!(
            error.to_string().contains("yielded only"),
            "the error names what was declared and what arrived"
        );
    }

    /// Reject a large stored payload that carries extra bytes.
    ///
    /// - Declare fewer bytes than the reader holds.
    /// - Expect an `InvalidData` error.
    /// - Name the declared length in the error.
    #[tokio::test]
    async fn reject_a_large_stored_payload_that_carries_extra_bytes() {
        let declared = 5 * MIB;
        let mut payload = noisy(usize::try_from(declared).unwrap());
        payload.extend_from_slice(&[0xff; 1000]);
        assert!(
            declared > STREAM_THRESHOLD,
            "the declared length must sit above the streaming threshold"
        );
        let id = blake3::hash(&payload[..usize::try_from(declared).unwrap()]);

        let dir = tempfile::tempdir().unwrap();
        let store = writer(&dir.path().join("store")).await;
        let error = store
            .put_stored(
                &id,
                Box::new(Cursor::new(payload)),
                declared,
                declared,
                Codec::Raw,
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::InvalidData,
            "a reader that does not stop where it was told is damaged input"
        );
        assert!(
            error.to_string().contains("more than the declared"),
            "the error names the declared length"
        );
    }

    /// Reject a large frame that declares no content size.
    ///
    /// - Encode five mebibytes without a pledged size.
    /// - Expect an `InvalidData` error.
    /// - Name the missing size in the error.
    #[tokio::test]
    async fn reject_a_large_frame_that_declares_no_size() {
        let raw_len = 5 * MIB;
        let raw = noisy(usize::try_from(raw_len).unwrap());
        let id = blake3::hash(&raw);

        let mut frame = Vec::new();
        zstd::stream::copy_encode(&raw[..], &mut frame, 1).unwrap();
        let stored_len = u64::try_from(frame.len()).unwrap();
        assert!(
            stored_len > STREAM_THRESHOLD,
            "the frame must sit above the streaming threshold"
        );

        let dir = tempfile::tempdir().unwrap();
        let store = writer(&dir.path().join("store")).await;
        let error = store
            .put_stored(
                &id,
                Box::new(Cursor::new(frame)),
                stored_len,
                raw_len,
                Codec::Zstd,
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::InvalidData,
            "a frame without a declared size cannot be checked"
        );
        assert!(
            error
                .to_string()
                .contains("does not say how many bytes it decodes to"),
            "the error names the missing size"
        );
    }

    /// Reject a large frame whose declared size disagrees with the record.
    ///
    /// - Claim one byte less than the frame decodes to.
    /// - Expect an `InvalidData` error.
    /// - Name both sizes in the error.
    #[tokio::test]
    async fn reject_a_large_frame_whose_declared_size_disagrees() {
        let raw = noisy(usize::try_from(5 * MIB).unwrap());
        let raw_len = u64::try_from(raw.len()).unwrap();
        let id = blake3::hash(&raw);

        let frame = zstd::bulk::compress(&raw, 1).unwrap();
        let stored_len = u64::try_from(frame.len()).unwrap();
        assert!(
            stored_len > STREAM_THRESHOLD,
            "the frame must sit above the streaming threshold"
        );

        let dir = tempfile::tempdir().unwrap();
        let store = writer(&dir.path().join("store")).await;
        let error = store
            .put_stored(
                &id,
                Box::new(Cursor::new(frame)),
                stored_len,
                raw_len - 1,
                Codec::Zstd,
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::InvalidData,
            "a size the frame disagrees with is a damaged record"
        );
        assert!(
            error.to_string().contains("bytes but its record claims"),
            "the error names what the frame declares and what was claimed"
        );
    }

    /// Reject a large frame that carries bytes past its frame.
    ///
    /// - Append garbage to a frame, inside the stored length.
    /// - Expect an error of the kind the buffered path gives.
    /// - Name the extra bytes in the error.
    #[tokio::test]
    async fn reject_a_large_frame_that_carries_bytes_past_its_frame() {
        let raw = noisy(usize::try_from(5 * MIB).unwrap());
        let raw_len = u64::try_from(raw.len()).unwrap();
        let id = blake3::hash(&raw);

        let frame = zstd::bulk::compress(&raw, 1).unwrap();
        let mut stored = frame.clone();
        stored.extend_from_slice(b"TRAILING7");
        let stored_len = u64::try_from(stored.len()).unwrap();
        assert!(
            stored_len > STREAM_THRESHOLD,
            "the frame must sit above the streaming threshold"
        );

        let dir = tempfile::tempdir().unwrap();
        let store = writer(&dir.path().join("store")).await;
        let error = store
            .put_stored(
                &id,
                Box::new(Cursor::new(stored)),
                stored_len,
                raw_len,
                Codec::Zstd,
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::Other,
            "bytes past a frame are the refusal the decode gives"
        );
        assert!(
            error.to_string().contains("past its frame"),
            "the error names the bytes past the frame"
        );
    }
}
