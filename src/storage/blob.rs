//! A store of content-addressed blobs, in a sequence of append-only segments.
//!
//! Each segment is the journal for its own stretch of the log: a store header and
//! then records (see [`super::record`]). A version of the backup set is a *commit
//! record* naming a snapshot, and version numbers are positions in the sequence
//! across all segments. Nothing outside the segments is needed to read a set, and
//! nothing outside them is trusted to say what is in it.
//!
//! # Format guarantees
//!
//! * A segment is **sealed** once the next record would not fit, and only the last
//!   is written again. Records are never split across a boundary, so a segment can
//!   be verified on its own.
//! * A non-last segment with a tail that does not verify is *corruption*, not an
//!   unfinished run, and is refused rather than truncated. Only the last
//!   segment's tail is truncated, back to the end of its last valid record — which
//!   is why a run may abandon a record mid-file: the scan stops at the hole, and
//!   everything after it goes with it.
//! * A file that does not begin with the store magic is refused, never truncated.
//!
//! # Commit protocol
//!
//! A run writes its chunk records, then its snapshot, then one commit record:
//!
//! 1. every record of the run is written, with no flush to the device;
//! 2. one `sync_data`, which makes all of it durable;
//! 3. the commit record, then one more `sync_data`.
//!
//! Two syncs rather than one: page write-back within a file is in no particular
//! order, so a single sync after the commit record can leave a commit naming data
//! that is not there, and a digest attests to what a payload *claims*, not to
//! whether it is present. Two syncs on a run that takes seconds to hours is free.
//!
//! A crash before step 2 leaves a tail with no commit record, which the next open
//! discards. A crash after step 3 leaves a whole committed run. There is no third
//! outcome, which is the entire guarantee.
//!
//! # Recovery
//!
//! Opening for writing scans every segment, and truncates the *last* one back to
//! the end of its last valid record, reclaiming the space a failed run left. A
//! store from another build is a backup somebody cares about, so one that does
//! not begin with the store magic is refused rather than scanned.

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
    slice::{DecompressReader, SliceAsyncReader},
    storage::{
        self, BlobId, Codec, Compression, Compressor, Decompressor, SegmentLimit, StreamReadSeeker,
        VersionIndex, Zstd,
    },
};

/// Copy buffer for a payload: where it is hashed on its way through, and what
/// coalesces the short reads a streaming backend hands out.
const COPY_BUFFER: usize = 256 * 1024;

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
    /// A panic mid-update leaves a half-updated index, but unwinding through a
    /// backup aborts the run and the next open's scan rebuilds it from the files.
    fn lock(contents: &Mutex<Contents>) -> std::sync::MutexGuard<'_, Contents> {
        contents.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Where the next record goes: a `(segment, offset)` rather than a bare offset,
/// because the log spans several files. Taking one is a short critical section,
/// and the copy that follows happens outside it.
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
/// derived fact the code needs — [`WriteMode::WRITES`], read in exactly one
/// place, the hand-written `Debug` — and that is all it is for.
///
/// Not sealed, and deliberately. Nothing constructs a `BlobStore` except
/// [`BlobStore::open_for_writing`] and [`BlobStore::open_for_reading`], and both
/// fix the mode, so an implementation outside this crate cannot produce an
/// instantiation that means anything. Sealing it would be a bound no caller could
/// satisfy and a restriction nothing needed.
pub trait WriteMode: Send + Sync + 'static {
    /// Whether a handle with this mode may append to its store.
    const WRITES: bool;
}

/// A store a run may append to. The yes to "may this handle write?".
///
/// Ask for one with [`BlobStore::open_for_writing`], which requires the write
/// policies and so cannot be confused with the other constructor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Write;

impl WriteMode for Write {
    const WRITES: bool = true;
}

/// A store that may only be read. The no, and it is the type rather than a
/// promise, so a restore path holding one cannot append and the mistake will not
/// compile.
///
/// Ask for one with [`BlobStore::open_for_reading`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Read;

impl WriteMode for Read {
    const WRITES: bool = false;
}

/// A store of content-addressed blobs.
///
/// `W` is a [`WriteMode`] so that a read-only handle cannot be used to write: the
/// distinction is part of the type, so a restore path that only needs to read
/// cannot accidentally append to the store.
///
/// It was `BlobStore<true>` and `BlobStore<false>`, which is the same guarantee
/// carrying no information — a `bool` in a type position has to be looked up in
/// whatever doc comment is nearby, and `false` reads as "disabled" rather than
/// "read-only". [`Read`] and [`Write`] say which. The enforcement is unchanged and
/// is still structural: only `impl StoragePut for BlobStore<Write>` exists, so
/// naming the mode and permitting the method are the same edit.
///
/// `C` is the [`Compressor`] this store writes payloads through, defaulting to
/// [`Compression`]. It is a parameter rather than a constructor argument type for
/// the same reason `W` is: the policy is part of the handle, so a store cannot be
/// holding one policy while a caller believes it is writing under another, and a
/// reader — which consults no policy, because the codec byte in each record says
/// how that record was written — is not generic over it at all.
/// [`BlobStore::open_for_reading`] is a [`Compression`] store whatever else exists,
/// which is the honest answer: there is nothing on the reading side to configure.
pub struct BlobStore<W: WriteMode, C: Compressor = Compression> {
    /// The set this store is, which is how its segments are named and found.
    set: IndexSet,
    /// Every segment on disk, in order. Fixed at open: the set only grows by
    /// appending a segment, and a reader holds this list for its lifetime.
    segments: Vec<PathBuf>,
    /// What the store holds, rebuilt by the scan that every open begins with.
    contents: Mutex<Contents>,
    /// Where the next record goes, and how full the last segment is; seeded from
    /// the end of the last valid record.
    ///
    /// A failed record keeps the range it claimed — the next open's scan reclaims
    /// it, and reclaiming it here would mean truncating past a writer already
    /// promised that space.
    reserved: Mutex<Reservation>,
    /// How large a segment may get. A write policy, not a format property, which
    /// is why it is not in the header: no record depends on where a boundary fell.
    limit: SegmentLimit,
    /// How record payloads are compressed. A write policy for the same reason, and
    /// a stronger one: each record names its own codec.
    ///
    /// The fallback is applied by the writer rather than trusted from here — see
    /// [`Compressor`] — so this is whatever policy the caller brought.
    compression: C,
    /// Segments this run has written to, so `sync_data` knows which files to sync.
    ///
    /// Not persisted: a run that wrote nothing has nothing to sync, and one that
    /// did has synced by the time it commits.
    dirty: Mutex<Vec<u32>>,
    /// The mode, which selects the methods and so appears in no field.
    ///
    /// A `const WRITE: bool` parameter needs no such field, which is the one thing
    /// the const generic was better at. `PhantomData<fn() -> W>` is the marker
    /// that does not claim ownership and does not affect auto-trait derivation,
    /// so `BlobStore<Write>` stays `Send` because its fields are, and a `W` that is
    /// only ever a type parameter cannot make it `!Send` by existing.
    mode: PhantomData<fn() -> W>,
}

impl<W: WriteMode, C: Compressor> Debug for BlobStore<W, C> {
    /// Counts and paths, never the index.
    ///
    /// Hand-written because the derived version walks the whole in-memory
    /// contents: a `{:?}` of a store with a million blobs is a line of a million
    /// hashes, which is unreadable *and* slow to produce, and one stray `{:?}` in
    /// a log is a multi-megabyte message about a backup. A count says everything
    /// a diagnostic needs.
    ///
    /// The same reasoning `reader::Chunk` gives for its contents and
    /// `reader::Chunker` for its upstream library — this is the third place, and
    /// it is written down here because the alternative is a third derivation that
    /// does not know about the other two.
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
    /// Every segment but the last is sealed, so this names the only one a run
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
    /// The commit records are the answer; the index files are not consulted,
    /// because a version whose index write was abandoned still exists.
    /// [`crate::layout::IndexSet::indexed_versions`] is the file-name count, and
    /// a lower bound on this.
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

    /// The snapshot a version names, via the per-version index file when it
    /// verifies.
    ///
    /// The file is **checked, not believed**: a version this set holds, a segment
    /// that exists, and a commit record there with the hash the file claims.
    /// Anything else falls back to [`BlobStore::snapshot`], so a stale, edited or
    /// copied index costs a scan rather than correctness.
    ///
    /// This is not the fast path for a restore: a restore needs every chunk a
    /// snapshot names, and those live in the blob index a scan builds. What it
    /// removes is the scan for the *version list*.
    ///
    /// The fallback is logged, because a caller asking this is trying to avoid a
    /// scan and needs to know it did not get one: `debug` when no file exists, the
    /// ordinary case for a version written before the set's last run, and `warn`
    /// when one disagrees with the store.
    ///
    /// # Errors
    ///
    /// The error [`BlobStore::snapshot`] would give, so a caller cannot tell which
    /// path answered.
    /// The snapshot hash of every commit record in `segment`, in log order.
    ///
    /// The in-memory twin of what [`resolve`](crate::storage::resolve) reads off
    /// the disk, and the same list: this store's open already scanned for it, so
    /// answering from here costs nothing where reading the file costs a walk of
    /// every record header in the segment.
    ///
    /// Empty for a segment this store does not know, which is what
    /// [`resolve_with`](crate::storage::resolve_with) treats as "no usable log".
    ///
    /// A `Vec` by value rather than a borrow, because the borrow would have to
    /// outlive a `MutexGuard` and be handed to a caller that cannot hold one. A
    /// segment's commit list is a handful of hashes, so the copy is not the thing
    /// worth avoiding — the file read is.
    #[must_use]
    pub fn segment_commits(&self, segment: u32) -> Vec<BlobId> {
        let contents = Contents::lock(&self.contents);

        // A segment's records start after every earlier segment's. Counted rather
        // than kept: `commits_per_segment` is what the scan built, and a second
        // running offset would be a second thing to keep in step with it.
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
    /// The file is **checked, not believed**: a version this set holds, a segment
    /// that exists, and a commit record there with the hash the file claims.
    /// Anything else falls back to [`BlobStore::snapshot`], so a stale, edited or
    /// copied index costs a scan rather than correctness.
    ///
    /// This is not the fast path for a restore: a restore needs every chunk a
    /// snapshot names, and those live in the blob index a scan builds. What it
    /// removes is the scan for the *version list* — and, since it answers the
    /// check from [`BlobStore::segment_commits`], the read of the named segment
    /// that the check used to make. Resolving N versions against one segment used
    /// to read that segment N times, and `lepatch versions` resolves one version
    /// at a time.
    ///
    /// The fallback is logged, because a caller asking this is trying to avoid a
    /// scan and needs to know it did not get one: `debug` when no file exists, the
    /// ordinary case for a version written before the set's last run, and `warn`
    /// when one disagrees with the store.
    ///
    /// The error is the one [`BlobStore::snapshot`] would give, so a caller cannot
    /// tell which path answered.
    ///
    /// # Errors
    ///
    /// [`io::ErrorKind::NotFound`] if the store holds no such version, naming both
    /// the version asked for and how many it does hold — which is the error the
    /// fallback gives, and the only one this can give, since a version the set
    /// does not hold is a version no index file can confirm.
    pub fn snapshot_via_index(&self, version: u32) -> io::Result<BlobId> {
        let versions = u32::try_from(Contents::lock(&self.contents).commits.len()).unwrap_or(0);

        // The lock is taken inside the callee rather than captured into the
        // closure, and that is not a style preference: a captured guard would be
        // held across `resolve_with`, which reads a file, so a concurrent `put`
        // would block on I/O rather than on the short critical section it was
        // written for.
        let store = self;
        let resolved = storage::resolve_with(&self.set, version, versions, move |segment| {
            let commits = store.segment_commits(segment);
            (!commits.is_empty()).then_some(commits)
        });
        // One match, three arms, no panic.
        //
        // This used to ask `confirmed()` first, returned early on it, and then
        // matched on the same value again to log why it had *not* confirmed — so
        // the `Confirmed` arm could not be written without an `unreachable!`. A
        // crate that denies `unwrap_used` and `expect_used` had a live panic
        // because a value was inspected twice.
        //
        // `reason` is the one place that says in a sentence why the file was not
        // used, so it is the message rather than a second wording of it here.
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
    /// From the in-memory index the open's scan built, not the per-version file.
    /// Always complete, because it comes from the store's own commit records.
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
                        "version {version} does not exist in {}; it holds {} version(s)",
                        self.set.base().display(),
                        contents.commits.len()
                    ),
                )
            })
    }
}

impl<C: Compressor> BlobStore<Write, C> {
    /// Open a set for writing, creating its first segment and recovering it if
    /// necessary.
    ///
    /// Runs the recovery scan on a blocking thread: it is proportional to the
    /// record count, long enough on a large set to stall the async pool.
    ///
    /// # Errors
    ///
    /// Returns an error if the containing directory or a segment cannot be
    /// created, for instance because the path is a directory or the caller lacks
    /// permission; if a segment exists but is not a store this build understands,
    /// which is reported rather than repaired, because a store written by another
    /// version is a backup and not a broken file; if the set is missing a segment
    /// in the middle; or if a *sealed* segment has a tail that does not verify,
    /// which is corruption and is never truncated.
    ///
    /// `compression` is any [`Compressor`], so this is where a caller outside this
    /// crate brings its own write policy. Which one it is does not travel with the
    /// store: every record names the codec it was written with, so a set opened
    /// later under a different policy — or read by a build that has never heard of
    /// either — still reads back.
    #[instrument(skip_all, err)]
    pub async fn open_for_writing<P: Into<PathBuf> + Debug>(
        path: P,
        limit: SegmentLimit,
        compression: C,
    ) -> io::Result<Self> {
        let set = IndexSet::new(path.into());

        if let Some(parent) = set.base().parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent).await?;
        }

        let scan_set = set.clone();
        let scanned = tokio::task::spawn_blocking(move || recover(&scan_set))
            .await
            .map_err(|join| io::Error::other(format!("the store scan failed: {join}")))??;

        // A writing store always has an append point: `recover` creates segment 0
        // for a set with none, so there is always a last segment to carry on in.
        let reserved = scanned
            .append
            .ok_or_else(|| io::Error::other("a writing store has no segment to write into"))?;

        Ok(Self {
            set,
            segments: scanned.segments,
            contents: Mutex::new(Contents {
                blobs: scanned.blobs,
                commits: scanned.commits,
                commits_per_segment: scanned.commits_per_segment,
            }),
            reserved: Mutex::new(reserved),
            limit,
            compression,
            dirty: Mutex::new(Vec::new()),
            mode: PhantomData,
        })
    }
}

impl BlobStore<Read, Compression> {
    /// Open a set for reading.
    ///
    /// The set must already exist and must be a store: a path that is not one is
    /// an error, not an empty set, so a mistyped name cannot come back as "no
    /// versions".
    ///
    /// No `SegmentLimit`, unlike the writing constructor: a read never rolls over
    /// and no record depends on where a segment boundary fell.
    ///
    /// # Errors
    ///
    /// Returns an error if the set does not exist, cannot be read, is missing a
    /// segment, or was written by an incompatible build.
    #[instrument(skip_all, err)]
    pub async fn open_for_reading<P: Into<PathBuf> + Debug>(path: P) -> io::Result<Self> {
        let set = IndexSet::new(path.into());

        let scan_set = set.clone();
        let scanned = tokio::task::spawn_blocking(move || read_only_scan(&scan_set))
            .await
            .map_err(|join| io::Error::other(format!("the store scan failed: {join}")))??;

        Ok(Self {
            set,
            segments: scanned.segments,
            contents: Mutex::new(Contents {
                blobs: scanned.blobs,
                commits: scanned.commits,
                commits_per_segment: scanned.commits_per_segment,
            }),
            // A reader has no append point, and never had one to record. The
            // scan says so rather than handing back a reservation built from a
            // segment number that does not exist.
            reserved: Mutex::new(scanned.append.unwrap_or(Reservation {
                segment: u32::MAX,
                offset: 0,
            })),
            // Both fields are shared with the writing constructor, and only that
            // one consults them. Neither has a value worth recording on a read: a
            // read never rolls over, and never compresses anything. The
            // compressor is [`Compression`] rather than a parameter for the reason
            // given on the type: nothing on this side consults it, and a reader
            // decodes what each record says it is.
            limit: SegmentLimit::default(),
            compression: Compression::default(),
            dirty: Mutex::new(Vec::new()),
            mode: PhantomData,
        })
    }
}

impl<W: WriteMode, C: Compressor> storage::StorageGet for BlobStore<W, C> {
    /// Open the blob with this content hash.
    ///
    /// The stream yields the blob's **content**, decompressed if it is stored
    /// compressed, and is bounded to that content — so a caller comparing against a
    /// length the snapshot recorded is comparing like with like.
    #[instrument(skip_all, err)]
    async fn get(&self, id: &BlobId) -> io::Result<StreamReadSeeker> {
        let placement = self.locate(id)?;

        let segment = self.segments.get(usize::try_from(placement.segment).unwrap_or(usize::MAX))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "this store's index names segment {} for a blob, but it holds {} segment(s)",
                    placement.segment,
                    self.segments.len()
                ),
            )
        })?;

        let file = super::at_async(segment, "open", fs::File::open(segment)).await?;
        let len = file.metadata().await?.len();

        // The index is built by a scan and cannot be wrong unless a segment has
        // shrunk since, which only a hand-edited store can arrange. Checked
        // rather than trusted, so the answer is a short read reported here
        // instead of a silent hole in a restored file.
        //
        // This is the *stored* length: it is the span the record occupies on disk,
        // and a compressed record is shorter than its content.
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

        // No lock is taken: records are only ever appended, so a blob handed out
        // now stays valid however much is written after it.
        let mut file = file;
        file.seek(SeekFrom::Start(placement.offset)).await?;

        match placement.codec {
            Codec::Raw => Ok(Box::new(SliceAsyncReader::new(file, placement.raw_len))),
            Codec::Zstd => {
                // Read the frame, then decode it. The read is bounded to the
                // stored length so a damaged record cannot pull in its neighbour,
                // and the decode is told the expected size so a frame that does
                // not produce it is refused rather than handed on short.
                let mut frame = storage::zeroed(
                    placement.stored_len,
                    &format!("the compressed frame of chunk {id}"),
                )?;
                file.read_exact(&mut frame).await?;

                // Through [`Zstd`] rather than through `DecompressReader::new`, so
                // there is one implementation of "what a zstd frame means and
                // what it must produce" rather than two that can disagree. This is
                // the match on the codec byte: a read is answered by the record,
                // not by whatever policy wrote it.
                let bytes = Decompressor::decompress(&Zstd, frame, placement.raw_len).await?;

                Ok(Box::new(DecompressReader::from_decoded(bytes)))
            }
        }
    }
}

impl<C: Compressor> storage::StoragePut for BlobStore<Write, C> {
    /// Store `len` bytes whose content hashes to `id`.
    ///
    /// A blob the store already holds is not written again, so the caller can hand
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

        let raw = read_whole(reader, len).await?;

        // Checked before anything is stored, so nothing is written under the wrong
        // name. Hashing the content is also what makes the codec a write policy:
        // the same bytes deduplicate whether they were stored raw or compressed.
        if blake3::hash(&raw) != *id {
            // Nothing written yet, so the run simply fails and the store is as it was.
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the content does not hash to {id}, so it was not stored"),
            ));
        }

        // Compress, storing the bytes as they are when that does not help: a zstd
        // frame carries a header of its own, so on content it cannot compress it
        // comes out larger than the input.
        let (codec, stored) = self.compression.compress(&raw);

        // And check the answer, because the policy that produced it is not this
        // crate's code and need not be this crate's careful. This is the single
        // place the "only when strictly smaller" rule is *enforced* rather than
        // observed, so an implementor who returns a frame larger than its input
        // costs a copy of the frame and nothing else: the payload is stored as it
        // came in, and the record says `Raw`, which is exactly what the reader
        // would have done with it. See the note on [`Compressor`].
        //
        // The codec is checked as well, and by naming `Zstd` rather than by
        // asking the store's decoders: `Raw` in a record means "these are the
        // bytes that hashed to this id", and a policy that paired it with
        // anything else would be naming bytes the store never hashed. Nothing on
        // the read path checks what it hands back, so the pairing is worth
        // enforcing where it is written rather than assuming a caller got it
        // right. `Zstd` is the fail-closed arm in exchange — its decode
        // validates the frame against the `raw_len` the header claims — which is
        // why the store may keep a frame on a policy's word that it decodes to
        // exactly that.
        //
        // `Compression` never trips this, because it applies the rule itself and
        // hands back a borrow of `raw` when the frame did not win.
        let keep = matches!(codec, Codec::Zstd) && stored.len() < raw.len();
        let (codec, stored) = if keep {
            (Codec::Zstd, stored)
        } else {
            (Codec::Raw, Cow::Borrowed(raw.as_slice()))
        };

        self.write_record(id, &stored, len, codec).await
    }

    /// Store a payload that is already encoded, after checking what it decodes to.
    ///
    /// The decode is the whole of the verification and is not optional — see
    /// [`StoragePut::put_stored`](crate::storage::StoragePut::put_stored), which
    /// says why in the terms a caller has. This
    /// is the part of that method specific to a store: the frame is kept as it
    /// came in only when it is strictly smaller than the content and names a
    /// codec the store can decode, and otherwise the content is stored raw, which
    /// is the same fallback `put` takes and for the same reason.
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

        let mut stored = read_whole(reader, stored_len).await?;

        // Both lengths are the caller's claims and neither is measured here, but
        // the comparison between them is this store's decision rather than the
        // caller's: a frame is kept only when it is strictly smaller, and only
        // when this store can decode it. `Zstd` is named rather than asked of the
        // decoders for the reason `put` gives — `Raw` in a record means "these are
        // the bytes that hashed to this id" — and it is the fail-closed arm in
        // exchange, since its decode validates the frame against `raw_len`.
        let keep = matches!(codec, Codec::Zstd) && stored_len < raw_len;

        // Decoded through [`Zstd`] rather than by a bare decode call, so there is
        // one implementation of what a frame must produce rather than two that
        // can disagree; `raw_len` is the record's claim about the size and is
        // checked against the frame, never trusted. Which of the three answers
        // this takes is decided by the two rules above rather than here, and a
        // `Raw` payload needs no decode at all because it *is* the content.
        let content: Vec<u8> = if keep {
            // The frame is written out again below, so the decode gets a copy of
            // it rather than the thing itself.
            Decompressor::decompress(&Zstd, stored.clone(), raw_len).await?
        } else if matches!(codec, Codec::Raw) {
            // Nothing to decode: a `Raw` payload *is* the content.
            std::mem::take(&mut stored)
        } else {
            // Handed over rather than copied: a frame that is not kept is never
            // written, so the decode can take the only copy of it.
            Decompressor::decompress(&Zstd, std::mem::take(&mut stored), raw_len).await?
        };

        // The identity check `put` makes, on the same terms and for the same
        // reason: a chunk is named by the hash of its content, so bytes that are
        // not that content must not be stored under that name however they
        // arrived. Checked before anything is written, so a failure leaves the
        // store as it was.
        if blake3::hash(&content) != *id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the content does not hash to {id}, so it was not stored"),
            ));
        }

        // The record's own content length is what was decoded, rather than the
        // `raw_len` the caller claimed: the frame was checked against that claim
        // above, so the two agree, and a `Raw` payload whose two lengths disagree
        // is stored under the length it actually has.
        let content_len = u64::try_from(content.len()).unwrap_or(u64::MAX);

        if keep {
            self.write_record(id, &stored, content_len, Codec::Zstd)
                .await
        } else {
            self.write_record(id, &content, content_len, Codec::Raw)
                .await
        }
    }

    /// Make every record written so far durable.
    ///
    /// Called once per run, after the last data record and before the commit
    /// record (see the module docs).
    ///
    /// Every segment this run wrote to is synced, not just the last: a run that
    /// crossed a boundary has records in two files, and syncing only the newer
    /// would leave the older's pages unordered against it.
    async fn sync_data(&self) -> io::Result<()> {
        let dirty = self.dirty_segments()?;

        for segment in dirty {
            // A handle of its own per segment, for the same reason `put` takes one
            // per record: this is a standalone sync and nothing is seeking.
            let file = fs::OpenOptions::new().write(true).open(segment).await?;

            file.sync_data().await?;
        }

        Ok(())
    }

    /// Publish `snapshot` as the next version, durably.
    ///
    /// The caller must already have synced the run's data. This writes the one
    /// record that makes the run visible and syncs it, and only then is the
    /// version reported as taken.
    async fn commit(&self, snapshot: &BlobId) -> io::Result<u32> {
        // A commit's payload is the hash it commits to, and its two lengths are
        // that hash's width. `HASH`, not `snapshot.len()`: a `BlobId` is a hash
        // and a hash is not an array, so asking it for a length is a question
        // about the wrong thing — and it compiled, because it used to be a
        // `[u8; 32]`.
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

        // Its own handle, for the same reason `put` takes one: a clone shares the
        // offset this write is aimed with.
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
        // ordinal is counted rather than read back because the run knows it, and
        // it is what lets a reader confirm an index file from this segment alone.
        let (version, ordinal) = {
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

            (version, ordinal)
        };

        // Written after the commit, and never allowed to fail the run: a version
        // whose index write is lost still exists and still restores, and a run
        // that reported failure here would have published a version and told the
        // caller it had not.
        if let Err(e) = write_index(
            &self.set,
            VersionIndex {
                version,
                segment: reservation.segment,
                ordinal,
                snapshot: *snapshot,
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
/// Both failure modes are refusals rather than tolerances, for the reason
/// [`storage::StoragePut::put`] gives: a short read leaves a blob that can never
/// be read back whole, and a long one leaves bytes past the end of the record.
///
/// The payload is read in full before anything is written, which is a requirement
/// of the record layout rather than an implementation convenience: a header
/// cannot be written twice, and the stored length is not known until the payload
/// has been compressed.
///
/// The buffer this allocates is a copy of one the caller already holds — the
/// chunker materialises each chunk before offering it — so a run's peak is one
/// chunk rather than a whole tree either way.
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

/// Write a version's index file, best-effort.
///
/// Never called on a path where its failure matters, which is the point: the
/// store's commit record is the version's existence, and this is a faster way to
/// find it.
fn write_index(set: &IndexSet, index: VersionIndex) -> io::Result<()> {
    index.write_atomic(&set.index_path(index.version))
}

impl<C: Compressor> BlobStore<Write, C> {
    /// Claim `total` bytes for a record, rolling into a new segment if it will not
    /// fit in the last one.
    ///
    /// The lock is held only for the decision, and the copy that follows happens
    /// outside it. A record is never split across a boundary, so this either
    /// returns a range inside the current segment or starts the next one.
    ///
    /// A record larger than the whole limit is an error rather than a segment that
    /// quietly grows past the cap, and the error names the cap so the caller can
    /// raise it deliberately.
    ///
    /// # Why the `fsync` in here stays on the runtime thread
    ///
    /// `create_segment` opens a file, writes six bytes to it and `fsync`s, and all
    /// three are blocking. The audit flagged that for `spawn_blocking`, and it is
    /// the right instinct — `open` and `open_for_reading` both send their *scan*
    /// to a blocking thread, and the scan is what this module is careful about.
    ///
    /// It cannot be done here without giving up the property that makes this
    /// function correct. The lock is held across `create_segment` on purpose: the
    /// file has to exist before `reserved.segment` names it, or a rollover hands
    /// out an offset in a file that does not exist yet. Awaiting needs the guard
    /// dropped — a `std::sync::MutexGuard` held across an await is not `Send`, so
    /// the compiler rejects it — and dropping it means two threads can both
    /// decide to roll to segment *n*, both pick `offset = header`, and both write
    /// there. Tolerating `AlreadyExists` on the create does not fix that, because
    /// by then both have already chosen the same offset.
    ///
    /// What is left is to weigh it, and the weighing is arithmetic rather than a
    /// guess: this runs **once per segment rollover**, so a run against the
    /// default 4 GiB limit does it once per 4 GiB stored — a 150 MB backup does
    /// it zero times. A `fsync` on a freshly created six-byte header is not free,
    /// but it is not on any hot path either, and the alternative is losing the
    /// guarantee that `tests/segments.rs::concurrent_writes_across_a_rollover_do_not_overlap`
    /// exists to pin.
    ///
    /// A blocking pool and an async lock would make it both safe and off-thread,
    /// and both cost more than the problem does.
    fn reserve(&self, total: u64) -> io::Result<Reservation> {
        let mut reserved = self.reserved.lock().unwrap_or_else(PoisonError::into_inner);

        let limit = self.limit.bytes();
        let header = u64::try_from(STORE_HEADER).unwrap_or(u64::MAX);

        // `offset` starts past the store header, so it already counts those bytes
        // against the file's size; adding them again here would roll over early.
        let end = reserved.offset.saturating_add(total);

        if end <= limit {
            let reservation = Reservation {
                segment: reserved.segment,
                offset: reserved.offset,
            };

            reserved.offset = end;
            self.note_segment(reservation.segment);

            return Ok(reservation);
        }

        // Rolled only if something has been written to this segment. An empty
        // segment that cannot hold the record is a record bigger than the limit,
        // which is reported rather than retried forever against a fresh segment.
        if reserved.offset <= header {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "a record of {total} bytes does not fit in a segment of {limit}, which is \
                     the limit this store was opened with; raise it to store this blob"
                ),
            ));
        }

        let next = reserved
            .segment
            .checked_add(1)
            .ok_or_else(|| io::Error::other("a store cannot hold more than u32::MAX segments"))?;

        // A segment's number is zero-padded to `SEGMENT_DIGITS`, which pads and
        // never truncates. One past the width would be written as a five-digit
        // name that `IndexSet::segment_number` rejects, so the file would exist
        // while the set reported it missing: this run's commit record would go
        // into a segment no later run could read, taking a published version with
        // it, and the next `create_segment` would then fail `AlreadyExists` and
        // wedge the set. Refused here, where the message can name the cause.
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

        // The file is created here, before any writer is told it may use it, so a
        // rollover cannot hand out an offset in a file that does not exist yet.
        create_segment(&self.segment_path(next), next)?;

        let reservation = Reservation {
            segment: next,
            offset: header,
        };

        reserved.segment = next;
        reserved.offset = header + total;
        self.note_segment(next);

        Ok(reservation)
    }

    /// Record that `segment` was written to by this run, so `sync_data` knows
    /// which files to sync.
    fn note_segment(&self, segment: u32) {
        let mut dirty = self.dirty.lock().unwrap_or_else(PoisonError::into_inner);

        if !dirty.contains(&segment) {
            dirty.push(segment);
        }
    }

    /// Whether this set already holds the blob named by `id`.
    ///
    /// Asked before a payload is read rather than after: a blob the store holds is
    /// not written again, so the reader is never even drained. A caller that hands
    /// over every chunk it read can therefore stop tracking what the set has.
    fn holds(&self, id: &BlobId) -> bool {
        Contents::lock(&self.contents).blobs.contains_key(id)
    }

    /// Append one blob record holding `stored` verbatim, and index it.
    ///
    /// Shared by [`storage::StoragePut::put`] and
    /// [`storage::StoragePut::put_stored`], which differ in how they decide what
    /// `stored` is and never in how a record is written: one record layout, and
    /// one place that knows where a blob lands.
    ///
    /// `raw_len` is the length of the *content*, which is what the header's second
    /// length carries and what a reader compares a snapshot's lengths against.
    async fn write_record(
        &self,
        id: &BlobId,
        stored: &[u8],
        raw_len: u64,
        codec: Codec,
    ) -> io::Result<()> {
        let stored_len = u64::try_from(stored.len()).unwrap_or(u64::MAX);

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

        // A handle of its own, never `try_clone`: a dup shares the file offset, so
        // concurrent puts would interleave headers into each other's payloads and
        // the store would read back as a shorter prefix of itself.
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.segment_path(reservation.segment))
            .await?;

        // Positional, never append: `append` would redirect every write to
        // wherever the end of the file happened to be, which is the offset just
        // decided not to use.
        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(&header).await?;
        file.write_all(stored).await?;

        // Nothing is synced here: the run's single `sync_data` is the durability
        // point, and syncing per blob would pay for thousands of them.
        file.flush().await?;

        let payload_offset = offset + u64::try_from(header.len()).unwrap_or(u64::MAX);

        // Into the index only once the payload is written, so nothing ever
        // points at bytes that are not there.
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
}

impl<C: Compressor> BlobStore<Write, C> {
    /// Write an index file for every version this store holds.
    ///
    /// Everything here is derived from the commit records, so a set with no index
    /// files at all is still a complete set and this only brings the cache back up
    /// to date.
    ///
    /// A file that cannot be written is counted as not written, not an error.
    ///
    /// # Errors
    ///
    /// Only if the version list cannot be read, which is the store's own in-memory
    /// index and so cannot really fail.
    pub fn write_index_files(&self) -> io::Result<usize> {
        let mut written = 0;

        for version in self.versions()? {
            let Some(snapshot) = self.snapshot(version).ok() else {
                continue;
            };

            let Some((segment, ordinal)) = self.commit_position(version) else {
                continue;
            };

            if write_index(
                &self.set,
                VersionIndex {
                    version,
                    segment,
                    ordinal,
                    snapshot,
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
    /// Derived from the per-segment commit counts the scan built, so it is the
    /// arithmetic the writer used when it recorded the ordinal.
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

    /// The segments this run has written to, in order.
    fn dirty_segments(&self) -> io::Result<Vec<PathBuf>> {
        let dirty = self.dirty.lock().unwrap_or_else(PoisonError::into_inner);

        dirty.iter().map(|s| Ok(self.segment_path(*s))).collect()
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
    /// Where the log carries on from. `None` for a store opened for reading,
    /// which has nowhere to write.
    ///
    /// This is an `Option` rather than a sentinel because a sentinel here was
    /// not reachable and did not say what it meant: `write_into` was documented
    /// as "`u32::MAX`, and a reader never uses it", and the reader set it twice —
    /// once initialising the struct and once again at the end with a comment
    /// explaining that the value was never read. A `BlobStore<Read>` then held
    /// a `Reservation` built from a segment number that does not exist, plus
    /// `SegmentLimit` and `Compression` fields defaulted to values it ignores.
    append: Option<Reservation>,
}

/// What a scan is allowed to do to the set.
///
/// The two halves of this were, for a long time, the same sixty lines written
/// twice, differing in four places. Those four are the whole of the difference
/// and they are not cosmetic:
///
/// * a writer creates segment 0 for a fresh set, and a reader reports that
///   there is no set;
/// * a writer opens the last segment for writing, a reader opens every one
///   read-only;
/// * a writer *reclaims* an unverified tail from the last segment and refuses
///   one in any other, and a reader refuses one anywhere, because a read does
///   not repair a store;
/// * only a writer has somewhere to carry on from.
///
/// The first two are ordinary. The third is the one that matters, and it is why
/// this is a parameter rather than a boolean or a pair of flags: whether an
/// unverified tail is damage or an unfinished run depends entirely on whether
/// this run is going to write, and on nothing else. Spelling it out here means
/// the two behaviours cannot drift — a reader that started repairing would be a
/// reader that destroys evidence.
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

/// Open a set for writing: create the first segment if there is none, scan every
/// segment, and discard an unfinished tail from the last one.
///
/// Only the last segment is ever truncated. A segment before it was sealed, and a
/// sealed segment is never written again, so a tail that does not verify there is
/// damage rather than a run that stopped.
fn recover(set: &IndexSet) -> io::Result<Scanned> {
    scan_set(set, Scan::Recover)
}

/// Open a set for reading, without writing to it.
fn read_only_scan(set: &IndexSet) -> io::Result<Scanned> {
    scan_set(set, Scan::Inspect)
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

        let mut file = if mode.repairs() {
            std::fs::OpenOptions::new()
                .read(true)
                .write(is_last)
                .truncate(false)
                .open(path)?
        } else {
            std::fs::File::open(path)?
        };

        let claimed = check_store_header(&mut file, path)?;
        if claimed != segment {
            return Err(not_a_store(
                path,
                // The longer message is the writer's: a renamed or reordered
                // set is a recoverable misconfiguration for a read, and an
                // unrecoverable one for a write, so only the write explains it.
                &if mode.repairs() {
                    format!(
                        "its header says it is segment {claimed}, but it is named as segment \
                         {segment}, so the set's segments have been moved around or renamed"
                    )
                } else {
                    format!(
                        "its header says it is segment {claimed}, but it is named as segment \
                         {segment}"
                    )
                },
            ));
        }

        let scan = record::scan(&mut file, segment, record::Blobs::Index)?;
        let len = file.metadata()?.len();

        if scan.end < len {
            if !is_last {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{} is a sealed segment and has {} bytes that are not a valid record, so it \
                         is damaged rather than unfinished; nothing here was truncated",
                        path.display(),
                        len - scan.end
                    ),
                ));
            }

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
            // not data anybody can lose. Synced before anything is written over
            // the space: a crash that brought the old tail back would put the scan
            // and the append pointer out of step.
            file.set_len(scan.end)?;
            file.sync_data()?;

            tracing::info!(
                path = %path.display(),
                reclaimed = len - scan.end,
                "discarded the tail of an unfinished run"
            );
        }

        out.blobs.extend(scan.blobs);
        out.commits_per_segment.insert(
            segment,
            u32::try_from(scan.commits.len()).unwrap_or(u32::MAX),
        );
        out.commits.extend(scan.commits);

        if is_last {
            // Recording the last segment rather than how many are sealed is what
            // makes a fresh set use the same arithmetic as a set of a thousand.
            out.append = Some(Reservation {
                segment,
                offset: scan.end,
            });
        }
    }

    Ok(out)
}

/// Create segment `segment` and write its header.
///
/// `create_new` rather than `create`, so an existing segment is an error rather
/// than a header written over the top of records already there.
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
/// Never repairs and never truncates: a file that does not start with the store
/// magic is somebody's data this build does not understand.
fn check_store_header(file: &mut std::fs::File, path: &Path) -> io::Result<u32> {
    use std::io::{Read as _, Seek as _};

    let len = file.metadata()?.len();
    if len < STORE_HEADER as u64 {
        return Err(not_a_store(path, "it is too short to hold a store header"));
    }

    let mut header = [0u8; STORE_HEADER];

    // From the start, whatever the handle is positioned at: one that has just
    // written the header is left at the end of the file.
    file.seek(io::SeekFrom::Start(0))?;
    file.read_exact(&mut header)?;

    record::decode_store_header(&header).map_err(|fault| {
        let why = match fault {
            record::HeaderFault::NotAStore => {
                "it does not begin with a lepatch store header, so it was not written by this format"
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

/// Flush the directory entry for `path`, so the file it names survives a crash.
fn sync_parent(path: &Path) -> io::Result<()> {
    let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) else {
        return Ok(());
    };

    std::fs::File::open(parent)?.sync_all()
}
