//! A store of content-addressed blobs, in a sequence of append-only segments.
//!
//! Each segment is the journal for its own stretch of the log: a store header and
//! then records (see [`super::record`]). A version of the backup set is a *commit
//! record* naming a snapshot, and version numbers are positions in the sequence
//! across all segments. Nothing outside the segments is needed to read a set, and
//! nothing outside them is trusted to say what is in it.
//!
//! # Why segments, and what it costs
//!
//! A single file that grows without bound eventually meets a limit that is
//! somebody else's decision: a filesystem's maximum file size, a tool that
//! cannot read it, a transfer that has to succeed in one piece. Segments under a
//! fixed limit avoid all three, and a numbered segment can be moved, verified or
//! replaced on its own.
//!
//! The property that makes it safe is that **a full segment is sealed**. Nothing
//! writes to a sealed segment again, so a torn write can only ever be in the last
//! one. A segment that is not last and has a tail which does not verify is
//! therefore *corruption*, not an unfinished run, and is refused rather than
//! truncated — the previous design had no way to tell those apart, and treating
//! corruption as an unfinished run is how a damaged backup gets quietly trimmed
//! down to whatever still parses.
//!
//! Records are never split across a boundary. A segment is sealed when the next
//! record would not fit in it, so every record is whole within one file and a
//! segment can be verified on its own.
//!
//! # Commit protocol
//!
//! A run writes its chunk records, then its snapshot, then one commit record:
//!
//! 1. every record of the run is written, with no flush to the device;
//! 2. one `sync_data`, which makes all of it durable;
//! 3. the commit record, then one more `sync_data`.
//!
//! Two syncs rather than one, deliberately. A single sync after writing the
//! commit record would be cheaper and is *not* correct: pages within a file are
//! written back in no particular order, so the page holding the commit record
//! can reach the device while an earlier data page has not. A crash then leaves
//! a commit record naming data that is not there, and nothing in the store
//! detects it — a digest over the records attests to what the payload *claims*,
//! not to whether the payload is present. Ordering the two syncs is what makes
//! the claim true, and two syncs on a run that takes seconds to hours is free.
//!
//! A crash before step 2 leaves a tail with no commit record, which the next
//! open discards. A crash after step 3 leaves a whole committed run. There is
//! no third outcome, which is the entire guarantee.
//!
//! # Recovery
//!
//! Opening for writing scans every segment, and truncates the *last* one back to
//! the end of its last valid record. That reclaims the space a failed run left
//! behind — which the previous design could not do, because it had no way to tell
//! a committed byte from an abandoned one — and it is why a run may abandon a
//! record in the middle: nothing was committed, so the scan stops at the hole
//! and everything after it goes with it.
//!
//! A segment that does not begin with the store magic is *not* truncated. It is
//! refused, because a store from another build is a backup somebody cares about
//! and a recovery scan that decides it is garbage would destroy it.

use std::{
    collections::HashMap,
    fmt::Debug,
    io::{self, SeekFrom},
    path::{Path, PathBuf},
    sync::{Mutex, PoisonError},
};

use async_trait::async_trait;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
};
use tracing::instrument;

use super::record::{self, Kind, Placement, STORE_HEADER};
use crate::{
    index::IndexSet,
    slice::SliceAsyncReader,
    storage::{self, BlobId, SegmentLimit, StreamReadSeeker},
};

/// Copy buffer for a payload.
///
/// The payload is hashed on its way through, which is free for a chunk — that
/// hash is what deduplication is keyed on — and this is the buffer it is done
/// with. Also what coalesces the short reads a streaming backend hands out, so
/// the store is not written one small read at a time.
const COPY_BUFFER: usize = 256 * 1024;

/// What a scan found, and what this handle may add to.
#[derive(Debug, Default)]
struct Contents {
    /// Every blob the store holds, by content hash.
    blobs: HashMap<BlobId, Placement>,
    /// The snapshot hash of each committed version, oldest first.
    commits: Vec<BlobId>,
}

impl Contents {
    /// Lock the contents, ignoring poisoning.
    ///
    /// A panic while the lock was held would leave a half-updated index, but
    /// unwinding through a backup aborts the run and the recovery scan on the
    /// next open rebuilds the index from the files. The poison carries no
    /// information worth propagating.
    fn lock(contents: &Mutex<Contents>) -> std::sync::MutexGuard<'_, Contents> {
        contents.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Where the next record goes.
///
/// A reservation is a `(segment, offset)` rather than a bare offset, because the
/// log spans several files. Taking one is a short critical section — a couple of
/// comparisons and an addition — and the copy that follows happens outside it,
/// which is what keeps uploads running in parallel. A lock held across the copy
/// would serialise them, and that is the mistake the single-file design avoided
/// by using one `fetch_add`; a rollover decision is not expressible as one
/// atomic, so it takes a lock and that is the cost of segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Reservation {
    /// Which segment the record goes in.
    segment: u32,
    /// Byte offset within that segment.
    offset: u64,
    /// Bytes the record occupies, header included.
    len: u64,
}

/// A store of content-addressed blobs.
///
/// `WRITE` is a const generic rather than two types so that a read-only handle
/// cannot be used to write: the distinction is part of the type, so a restore
/// path that only needs to read cannot accidentally append to the store.
#[derive(Debug)]
pub struct BlobStore<const WRITE: bool> {
    /// The set this store is, which is how its segments are named and found.
    set: IndexSet,
    /// Every segment on disk, in order. Fixed at open: the set only grows by
    /// appending a segment, and a reader holds this list for its lifetime.
    segments: Vec<PathBuf>,
    /// What the store holds, rebuilt by the scan that every open begins with.
    contents: Mutex<Contents>,
    /// Where the next record goes, and how full the last segment is.
    ///
    /// Seeded from the end of the last valid record. A failed record keeps the
    /// range it claimed: reclaiming it would mean truncating past a writer that
    /// has already been promised that space, and the recovery scan on the next
    /// open reclaims the lot of them anyway.
    reserved: Mutex<Reservation>,
    /// How large a segment may get. A write policy, not a format property, which
    /// is why it is not recorded in the header: a set written under one limit
    /// reads back under any other, because no record depends on where a boundary
    /// fell.
    limit: SegmentLimit,
    /// Segments this run has written to, so `sync_data` knows which files to sync.
    ///
    /// Empty until the first record, and not persisted: a run that wrote nothing
    /// has nothing to sync, and a run that did has already synced by the time it
    /// commits.
    dirty: Mutex<Vec<u32>>,
}

impl<const WRITE: bool> BlobStore<WRITE> {
    /// The path of segment `segment`.
    ///
    /// The last segment is the only one a run writes to; everything before it is
    /// sealed. This is what a caller should name when it needs one file of a set
    /// rather than the whole of it.
    #[must_use]
    pub fn segment_path(&self, segment: u32) -> PathBuf {
        self.set.blob_path(segment)
    }

    /// How many segments this store has, counting from zero.
    #[must_use]
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// The paths of every segment, in order.
    #[must_use]
    pub fn segments(&self) -> &[PathBuf] {
        &self.segments
    }

    /// How large a segment may get before a new one is started.
    #[must_use]
    pub fn limit(&self) -> SegmentLimit {
        self.limit
    }

    /// Every version this store holds, oldest first.
    ///
    /// # Errors
    ///
    /// Returns an error only if the version count does not fit in a `u32`, which
    /// is four billion backups of one location.
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
    /// [`crate::storage::Plan`] resolves a snapshot against, and it is public
    /// because anything that walks a version — a pack writer, a differ, a
    /// uploader — needs locations and has no other way to get them.
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
                    format!(
                        "this store does not hold a blob hashing to {}",
                        storage::hex(id)
                    ),
                )
            })
    }

    /// The snapshot a specific version names.
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

impl BlobStore<true> {
    /// Open a set for writing, creating its first segment and recovering it if
    /// necessary.
    ///
    /// Runs the recovery scan on a blocking thread: it is proportional to the
    /// number of records in the set, which on a large one is long enough that
    /// holding up the async pool while it happens would be visible.
    ///
    /// # Errors
    ///
    /// Returns an error if the containing directory or a segment cannot be
    /// created, for instance because the path is a directory or the caller lacks
    /// permission; if a segment exists but is not a store this build understands,
    /// which is reported rather than repaired, because a store written by another
    /// version is a backup and not a broken file; if the set is missing a segment
    /// in the middle; or if a *sealed* segment has a tail that does not verify,
    /// which is corruption rather than an unfinished run and is never truncated.
    #[instrument(skip_all, err)]
    pub async fn open<P: Into<PathBuf> + Debug>(path: P, limit: SegmentLimit) -> io::Result<Self> {
        let set = IndexSet::new(path.into());

        if let Some(parent) = set.base().parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent).await?;
        }

        let scan_set = set.clone();
        let scanned = tokio::task::spawn_blocking(move || recover(&scan_set))
            .await
            .map_err(|join| io::Error::other(format!("the store scan failed: {join}")))??;

        Ok(Self {
            set,
            segments: scanned.segments,
            contents: Mutex::new(Contents {
                blobs: scanned.blobs,
                commits: scanned.commits,
            }),
            reserved: Mutex::new(Reservation {
                segment: scanned.write_into,
                offset: scanned.end,
                len: 0,
            }),
            limit,
            dirty: Mutex::new(Vec::new()),
        })
    }
}

impl BlobStore<false> {
    /// Open a set for reading.
    ///
    /// The set must already exist and must be a store: reading a path that is not
    /// one is an error rather than an empty backup set, so a mistyped name cannot
    /// come back as "no versions".
    ///
    /// `limit` is accepted and ignored. A read does not roll over, and no record
    /// depends on where a segment boundary fell, so the value a writer used is
    /// not needed to read what it wrote. Taking it keeps the two constructors
    /// interchangeable, which is what lets a caller pass one value to both.
    ///
    /// # Errors
    ///
    /// Returns an error if the set does not exist, cannot be read, is missing a
    /// segment, or was written by an incompatible build.
    #[instrument(skip_all, err)]
    pub async fn open<P: Into<PathBuf> + Debug>(path: P, limit: SegmentLimit) -> io::Result<Self> {
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
            }),
            reserved: Mutex::new(Reservation {
                segment: scanned.write_into,
                offset: scanned.end,
                len: 0,
            }),
            limit,
            dirty: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl<const WRITE: bool> storage::StorageGet for BlobStore<WRITE> {
    /// Open the blob with this content hash.
    ///
    /// The returned stream is bounded to the blob, so nothing downstream can
    /// read past it into its neighbour.
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

        let file = fs::File::open(segment).await?;
        let len = file.metadata().await?.len();

        // The index is built by a scan and cannot be wrong unless a segment has
        // shrunk since, which only a hand-edited store can arrange. Checked
        // rather than trusted, so the answer is a short read reported here
        // instead of a silent hole in a restored file.
        let end = placement.offset.checked_add(placement.len).ok_or_else(|| {
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

        Ok(Box::new(SliceAsyncReader::new(file, placement.len)))
    }
}

#[async_trait]
impl storage::StoragePut for BlobStore<true> {
    /// Store `len` bytes whose content hashes to `id`.
    ///
    /// A blob the store already holds is not written again, so the caller can
    /// hand over every chunk it read without tracking what it has seen: that is
    /// what makes deduplication independent of which version the last one was.
    ///
    /// The payload is hashed on its way through and checked against `id`. A
    /// mismatch fails the run rather than storing content under the wrong name,
    /// which would otherwise be a corruption that reads back perfectly and is
    /// only ever noticed by a full verify.
    #[instrument(skip_all, err)]
    async fn put(&self, id: &BlobId, mut reader: StreamReadSeeker, len: u64) -> io::Result<()> {
        if Contents::lock(&self.contents).blobs.contains_key(id) {
            tracing::trace!(already = %storage::hex(id), "already stored");
            return Ok(());
        }

        // A record header is a few dozen bytes by construction, so this cannot
        // overflow on any target that can address the file at all.
        let mut header = Vec::with_capacity(usize::try_from(record::header_len(len)).unwrap_or(0));
        record::encode_header(&mut header, Kind::Blob, id, len);

        let total = u64::try_from(header.len()).unwrap_or(u64::MAX) + len;
        let reservation = self.reserve(total)?;
        let offset = reservation.offset;

        // A handle of its own, not a clone of a shared one.
        //
        // `dup` — which is what `try_clone` is — shares the file offset between
        // the original and the copy, so every concurrent `put` would seek the
        // same cursor and interleave its header into somebody else's payload.
        // The records would still each be individually valid, and the store would
        // read back as a shorter prefix of itself: no error anywhere, just data
        // that quietly went missing. Each writer needs a descriptor with a
        // private offset, which means a separate open.
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

        let mut hasher = blake3::Hasher::new();
        let mut scratch = vec![0u8; COPY_BUFFER];
        let mut remaining = len;

        {
            let mut source = BufReader::with_capacity(COPY_BUFFER, reader.as_mut().take(len));

            while remaining > 0 {
                let want =
                    usize::try_from(remaining.min(scratch.len() as u64)).unwrap_or(scratch.len());

                let read = source.read(&mut scratch[..want]).await?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!(
                            "declared {len} bytes but the reader yielded only {}",
                            len - remaining
                        ),
                    ));
                }

                hasher.update(&scratch[..read]);
                file.write_all(&scratch[..read]).await?;
                remaining -= u64::try_from(read).unwrap_or(u64::MAX);
            }
        }

        let mut trailing = [0u8; 1];
        if reader.read(&mut trailing).await? != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the reader yielded more than the declared {len} bytes"),
            ));
        }

        if hasher.finalize().as_bytes() != id {
            // The header already names the content it was given, and it is
            // wrong. Leaving that header where it is would make the next open
            // index a record under a hash its payload does not have — a blob the
            // store would then hand out as if it were sound, and which only a
            // full verify would ever catch.
            //
            // So the record is made unreadable rather than merely unindexed, by
            // taking its magic away. A header that does not verify is where the
            // scan stops, which is the same signal a torn write leaves, and the
            // next run's recovery truncates from here. It is the documented way a
            // run abandons a record: nothing was committed, so everything after
            // this point goes with it.
            let hole = [0u8; 4];
            file.seek(SeekFrom::Start(offset)).await?;
            file.write_all(&hole).await?;
            file.flush().await?;

            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "the content does not hash to {}, so it was not stored",
                    storage::hex(id)
                ),
            ));
        }

        // `copy` returning is not the bytes having reached the file. Nothing is
        // synced here: the run's single `sync_data` is the durability point, and
        // syncing per blob would pay for thousands of them to protect a run that
        // publishes itself all at once.
        file.flush().await?;

        let payload_offset = offset + u64::try_from(header.len()).unwrap_or(u64::MAX);

        // Into the index only once the payload is written, so nothing ever
        // points at bytes that are not there.
        Contents::lock(&self.contents).blobs.insert(
            *id,
            Placement {
                segment: reservation.segment,
                offset: payload_offset,
                len,
            },
        );

        tracing::trace!(
            segment = reservation.segment,
            offset,
            length = len,
            "stored blob"
        );

        Ok(())
    }

    /// Make every record written so far durable.
    ///
    /// Called once per run, after the last of its data records and before the
    /// commit record. See the module docs for why the commit is a second sync
    /// rather than a continuation of this one.
    ///
    /// Every segment this run wrote to is synced, not just the last one. A run
    /// that crossed a boundary has records in two files, and syncing only the
    /// newer one would leave the older one's pages unordered against it — which is
    /// the failure the two syncs exist to prevent, one level up.
    async fn sync_data(&self) -> io::Result<()> {
        let dirty = self.dirty_segments()?;

        for segment in dirty {
            // A handle of its own per segment for the same reason `put` takes one
            // per record: this is a standalone sync, not a write, and nothing is
            // seeking.
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
        let hash = *blake3::hash(snapshot).as_bytes();

        let mut header = Vec::new();
        record::encode_header(
            &mut header,
            Kind::Commit,
            &hash,
            u64::try_from(snapshot.len()).unwrap_or(u64::MAX),
        );

        let total = u64::try_from(header.len()).unwrap_or(u64::MAX)
            + u64::try_from(snapshot.len()).unwrap_or(u64::MAX);
        let reservation = self.reserve(total)?;
        let offset = reservation.offset;

        // Its own handle, for the same reason `put` takes one: a clone shares its
        // offset with the original, and the offset is what this write is aimed
        // with. Nothing else is writing at this point — a commit is the end of a
        // run — but relying on that to make a shared cursor safe is the kind of
        // thing that stops being true the moment something is added.
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.segment_path(reservation.segment))
            .await?;

        file.seek(SeekFrom::Start(offset)).await?;
        file.write_all(&header).await?;
        file.write_all(snapshot).await?;
        file.flush().await?;

        // The second sync, and the one that makes the run real.
        file.sync_data().await?;

        let mut contents = Contents::lock(&self.contents);
        let version = u32::try_from(contents.commits.len() + 1)
            .map_err(|_| io::Error::other("a store cannot hold more than u32::MAX versions"))?;
        contents.commits.push(*snapshot);

        Ok(version)
    }
}

impl BlobStore<true> {
    /// Claim `total` bytes for a record, rolling into a new segment if it will not
    /// fit in the last one.
    ///
    /// The lock is held only for the decision. A record is never split across a
    /// boundary, so this either returns a range inside the current segment or
    /// starts the next one — and only the first of those is the common case.
    ///
    /// A record that is larger than the whole limit is an error rather than a
    /// segment that quietly grows past the cap. "No file over 4 GiB" is the
    /// reason segments exist; silently breaking it would defeat that, and the
    /// error names the cap so the caller can raise it deliberately.
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
                len: total,
            };

            reserved.offset = end;
            reserved.len = 0;
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

        // The file is created here, before any writer is told it may use it, so a
        // rollover cannot hand out an offset in a file that does not exist yet.
        create_segment(&self.segment_path(next), next)?;

        let reservation = Reservation {
            segment: next,
            offset: header,
            len: total,
        };

        reserved.segment = next;
        reserved.offset = header + total;
        reserved.len = 0;
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
}

impl BlobStore<true> {
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
    /// The segment a run writes into: the last one, which is the only one not
    /// sealed.
    ///
    /// Always a real index — segment 0 for a fresh set — so the reservation
    /// arithmetic is the same for a store with one segment and one with a
    /// thousand. A reader leaves it at `u32::MAX` and never uses it, because a
    /// reader has nowhere to write.
    write_into: u32,
    /// First byte past the last valid record in `write_into`.
    end: u64,
}

/// Open a set for writing: create the first segment if there is none, scan every
/// segment, and discard an unfinished tail from the last one.
///
/// Only the last segment is ever truncated. A segment before it was sealed, and a
/// sealed segment is never written again, so a tail that does not verify there is
/// damage rather than a run that stopped — and damage that this pass quietly
/// shortened away would be damage nobody ever heard about.
fn recover(set: &IndexSet) -> io::Result<Scanned> {
    let mut segments = set.segments()?;

    let created = segments.is_empty();
    if created {
        let first = set.blob_path(0);
        create_segment(&first, 0)?;
        sync_parent(&first)?;
        segments.push(first);
    }

    let last = u32::try_from(segments.len().saturating_sub(1)).unwrap_or(u32::MAX);
    let mut out = Scanned {
        segments: segments.clone(),
        blobs: HashMap::new(),
        commits: Vec::new(),
        write_into: u32::MAX,
        end: 0,
    };

    for (index, path) in segments.iter().enumerate() {
        let segment = u32::try_from(index).unwrap_or(u32::MAX);
        let is_last = index == segments.len() - 1;

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(is_last)
            .truncate(false)
            .open(path)?;

        let claimed = check_store_header(&mut file, path)?;
        if claimed != segment {
            return Err(not_a_store(
                path,
                &format!(
                    "its header says it is segment {claimed}, but it is named as segment {segment}, \
                     so the set's segments have been moved around or renamed"
                ),
            ));
        }

        let scan = record::scan(&mut file, segment)?;
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

            // A run that did not finish. The bytes past the last valid record
            // were never published, so they are not data anybody can lose, and the
            // file is shortened back over them. Synced, because the truncation has
            // to be durable before anything is written over the space: a crash
            // that brought the old tail back would put the scan and the append
            // pointer out of step with each other.
            file.set_len(scan.end)?;
            file.sync_data()?;

            tracing::info!(
                path = %path.display(),
                reclaimed = len - scan.end,
                "discarded the tail of an unfinished run"
            );
        }

        out.blobs.extend(scan.blobs);
        out.commits.extend(scan.commits);

        if is_last {
            // The last segment is the one a run writes into. Recording it rather
            // than "how many are sealed" is what makes a fresh set — one segment,
            // nothing sealed — use the same arithmetic as a set of a thousand.
            out.write_into = last;
            out.end = scan.end;
        }
    }

    Ok(out)
}

/// Open a set for reading, without writing to it.
fn read_only_scan(set: &IndexSet) -> io::Result<Scanned> {
    let segments = set.segments()?;
    if segments.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no backup store at {}", set.base().display()),
        ));
    }

    let mut out = Scanned {
        segments: segments.clone(),
        blobs: HashMap::new(),
        commits: Vec::new(),
        write_into: u32::MAX,
        end: 0,
    };

    for (index, path) in segments.iter().enumerate() {
        let segment = u32::try_from(index).unwrap_or(u32::MAX);
        let mut file = std::fs::File::open(path)?;
        let claimed = check_store_header(&mut file, path)?;

        if claimed != segment {
            return Err(not_a_store(
                path,
                &format!(
                    "its header says it is segment {claimed}, but it is named as segment {segment}"
                ),
            ));
        }

        // A read does not repair, so a segment with a tail that does not verify is
        // reported the same way the writing open reports damage in a sealed one:
        // the bytes are there, they are not readable, and pretending otherwise
        // would mean a restore that quietly omits whatever they held.
        let scan = record::scan(&mut file, segment)?;
        let len = file.metadata()?.len();

        if scan.end < len {
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

        out.blobs.extend(scan.blobs);
        out.commits.extend(scan.commits);
    }

    // A reader never writes, so it has no segment to write into and no end to
    // carry on from. Both are left saying so rather than pretending to be a
    // one-segment set.
    out.write_into = u32::MAX;
    out.end = 0;

    Ok(out)
}

/// Create segment `segment` and write its header.
///
/// `create_new` rather than `create`, so a segment that already exists is an error
/// instead of a header written over the top of records that are already there.
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
/// magic is somebody's data that this build does not understand, and the one
/// thing a recovery pass must not do is decide otherwise on its own.
fn check_store_header(file: &mut std::fs::File, path: &Path) -> io::Result<u32> {
    use std::io::{Read as _, Seek as _};

    let len = file.metadata()?.len();
    if len < STORE_HEADER as u64 {
        return Err(not_a_store(path, "it is too short to hold a store header"));
    }

    let mut header = [0u8; STORE_HEADER];

    // From the start, whatever the handle happens to be positioned at. A handle
    // that has just written the header is left at the end of the file, and
    // reading from there would be a read past the end of a 12-byte file rather
    // than a read of its header.
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
