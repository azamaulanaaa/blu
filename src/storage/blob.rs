//! A store of content-addressed blobs, in one append-only file.
//!
//! The file is the journal. It begins with a store header and is then a
//! sequence of records (see [`super::record`]); a version of the backup set is a
//! *commit record* naming a snapshot, and version numbers are positions in that
//! sequence. Nothing outside the file is needed to read it, and nothing outside
//! the file is trusted to say what is in it.
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
//! Opening for writing scans the records and truncates the file back to the end
//! of the last valid one. That reclaims the space a failed run left behind —
//! which the previous design could not do, because it had no way to tell a
//! committed byte from an abandoned one — and it is why a run may abandon a
//! record in the middle: nothing was committed, so the scan stops at the hole
//! and everything after it goes with it.
//!
//! A file that does not begin with the store magic is *not* truncated. It is
//! refused, because a store from an older build is a backup somebody cares
//! about and a recovery scan that decides it is garbage would destroy it.

use std::{
    collections::HashMap,
    fmt::Debug,
    io::{self, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
};

use async_trait::async_trait;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
};
use tracing::instrument;

use super::record::{self, FORMAT_VERSION, Kind, Placement, STORE_HEADER, STORE_MAGIC};
use crate::{
    slice::SliceAsyncReader,
    storage::{self, BlobId, StreamReadSeeker},
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
    /// next open rebuilds the index from the file. The poison carries no
    /// information worth propagating.
    fn lock(contents: &Mutex<Contents>) -> std::sync::MutexGuard<'_, Contents> {
        contents.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A store of content-addressed blobs.
///
/// `WRITE` is a const generic rather than two types so that a read-only handle
/// cannot be used to write: the distinction is part of the type, so a restore
/// path that only needs to read cannot accidentally append to the store.
#[derive(Debug)]
pub struct BlobStore<const WRITE: bool> {
    file_path: PathBuf,
    /// What the store holds, rebuilt by the scan that every open begins with.
    contents: Mutex<Contents>,
    /// First byte of the store not yet handed out to a record.
    ///
    /// A reservation counter rather than the file's length, so a writer claims
    /// its range and then copies into it while other writers claim theirs. Were
    /// the end taken from the file instead, two writers would both read the same
    /// length while the first was still writing and would pick the same offset.
    ///
    /// It is seeded from the end of the last valid record, and a failed record
    /// keeps the range it claimed: reclaiming it would mean truncating past a
    /// writer that has already been promised that space, and the recovery scan
    /// on the next open reclaims the lot of them anyway.
    reserved: AtomicU64,
    /// Handle the run's records are written through. Only when `WRITE` is set.
    writer: Option<fs::File>,
}

impl<const WRITE: bool> BlobStore<WRITE> {
    /// The path this store lives at.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.file_path
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
                        self.file_path.display(),
                        contents.commits.len()
                    ),
                )
            })
    }
}

impl BlobStore<true> {
    /// Open a store for writing, creating it and recovering it if necessary.
    ///
    /// Runs the recovery scan on a blocking thread: it is proportional to the
    /// number of records in the store, which on a large set is long enough that
    /// holding up the async pool while it happens would be visible.
    ///
    /// # Errors
    ///
    /// Returns an error if the containing directory or the store cannot be
    /// created, for instance because the path is a directory or the caller lacks
    /// permission, or if the file exists but is not a store this build
    /// understands — which is reported rather than repaired, because a store
    /// written by another version is a backup and not a broken file.
    #[instrument(skip_all, err)]
    pub async fn open<P: Into<PathBuf> + Debug>(path: P) -> io::Result<Self> {
        let file_path = path.into();

        if let Some(parent) = file_path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent).await?;
        }

        let scan_path = file_path.clone();
        let (scan, writer) = tokio::task::spawn_blocking(move || recover(&scan_path))
            .await
            .map_err(|join| io::Error::other(format!("the store scan failed: {join}")))??;

        Ok(Self {
            file_path,
            contents: Mutex::new(Contents {
                blobs: scan.blobs,
                commits: scan.commits,
            }),
            reserved: AtomicU64::new(scan.end),
            writer: Some(writer),
        })
    }
}

impl BlobStore<false> {
    /// Open a store for reading.
    ///
    /// The file must already exist and must be a store: reading a path that is
    /// not one is an error rather than an empty backup set, so a mistyped name
    /// cannot come back as "no versions".
    ///
    /// # Errors
    ///
    /// Returns an error if the store does not exist, cannot be read, or was
    /// written by an incompatible build.
    #[instrument(skip_all, err)]
    pub async fn open<P: Into<PathBuf> + Debug>(path: P) -> io::Result<Self> {
        let file_path = path.into();

        let scan_path = file_path.clone();
        let scan = tokio::task::spawn_blocking(move || read_only_scan(&scan_path))
            .await
            .map_err(|join| io::Error::other(format!("the store scan failed: {join}")))??;

        Ok(Self {
            file_path,
            contents: Mutex::new(Contents {
                blobs: scan.blobs,
                commits: scan.commits,
            }),
            reserved: AtomicU64::new(scan.end),
            writer: None,
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

        let file = fs::File::open(&self.file_path).await?;
        let len = file.metadata().await?.len();

        // The index is built by a scan and cannot be wrong unless the file has
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
                    "blob at {} needs bytes {}..{end} but the store is only {len} bytes long",
                    self.file_path.display(),
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

        let offset = self.reserved.fetch_add(
            u64::try_from(header.len()).unwrap_or(u64::MAX) + len,
            Ordering::Relaxed,
        );

        // A handle of its own, not a clone of the shared one.
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
            .open(&self.file_path)
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
                offset: payload_offset,
                len,
            },
        );

        tracing::trace!(offset, length = len, "stored blob");

        Ok(())
    }

    /// Make every record written so far durable.
    ///
    /// Called once per run, after the last of its data records and before the
    /// commit record. See the module docs for why the commit is a second sync
    /// rather than a continuation of this one.
    async fn sync_data(&self) -> io::Result<()> {
        self.writer
            .as_ref()
            .ok_or_else(|| io::Error::other("a read-only store cannot be synced"))?
            .sync_data()
            .await
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

        let offset = self.reserved.fetch_add(
            u64::try_from(header.len()).unwrap_or(u64::MAX)
                + u64::try_from(snapshot.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );

        // Its own handle, for the same reason `put` takes one: a clone shares its
        // offset with the original, and the offset is what this write is aimed
        // with. Nothing else is writing at this point — a commit is the end of a
        // run — but relying on that to make a shared cursor safe is the kind of
        // thing that stops being true the moment something is added.
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.file_path)
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

/// Open a store for writing, scanning it and discarding anything past the last
/// valid record.
///
/// Returns the scan and an open handle to write through.
fn recover(path: &Path) -> io::Result<(record::Scan, fs::File)> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;

    let created = file.metadata()?.len() == 0;

    if created {
        // A store with no header is not a store yet, and a zero-length file is
        // not an empty backup set. Writing it before anything else means every
        // later open can tell the two apart.
        use std::io::Write as _;

        let mut header = [0u8; STORE_HEADER];
        header[..4].copy_from_slice(&STORE_MAGIC);
        header[4..].copy_from_slice(&FORMAT_VERSION.to_le_bytes());

        file.write_all(&header)?;
        file.sync_data()?;
    }

    check_store_header(&mut file, path)?;

    let scan = record::scan(&mut file)?;
    let len = file.metadata()?.len();

    if scan.end < len {
        // A run that did not finish. The bytes past the last valid record were
        // never published, so they are not data anybody can lose, and the file is
        // shortened back over them. Synced, because the truncation has to be
        // durable before anything is written over the space: a crash that
        // brought the old tail back would put the scan and the append pointer
        // out of step with each other.
        file.set_len(scan.end)?;
        file.sync_data()?;

        tracing::info!(
            path = %path.display(),
            reclaimed = len - scan.end,
            "discarded the tail of an unfinished run"
        );
    }

    if created {
        // The file itself has to be in the directory, not just its contents, or
        // a power cut can leave a durable store that no longer exists.
        sync_parent(path)?;
    }

    let handle = fs::File::from_std(file);
    Ok((scan, handle))
}

/// Open a store for reading, without writing to it.
fn read_only_scan(path: &Path) -> io::Result<record::Scan> {
    let mut file = std::fs::File::open(path).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("no backup store at {}", path.display()),
            )
        } else {
            e
        }
    })?;

    check_store_header(&mut file, path)?;
    record::scan(&mut file)
}

/// Refuse a file that is not a store this build can read.
///
/// Never repairs and never truncates: a file that does not start with the store
/// magic is somebody's data that this build does not understand, and the one
/// thing a recovery pass must not do is decide otherwise on its own.
fn check_store_header(file: &mut std::fs::File, path: &Path) -> io::Result<()> {
    use std::io::{Read as _, Seek as _};

    let len = file.metadata()?.len();
    if len < STORE_HEADER as u64 {
        return Err(not_a_store(path, "it is too short to hold a store header"));
    }

    let mut header = [0u8; STORE_HEADER];

    // From the start, whatever the handle happens to be positioned at. The
    // caller that just created the store has written the header and left the
    // cursor at the end of the file, and reading from there would be a read past
    // the end of an 8-byte file rather than a read of its header.
    file.seek(io::SeekFrom::Start(0))?;
    file.read_exact(&mut header)?;

    if header[..4] != STORE_MAGIC {
        return Err(not_a_store(
            path,
            "it does not begin with a lepatch store header, so it was not written by this format",
        ));
    }

    let version = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    if version != FORMAT_VERSION {
        return Err(not_a_store(
            path,
            &format!("it is store format {version}, but this build reads {FORMAT_VERSION}"),
        ));
    }

    Ok(())
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
