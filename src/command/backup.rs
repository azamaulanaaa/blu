//! Recording what a directory tree contains, and handing the contents over to be
//! stored.
//! The run walks the tree, tells a file from a link from a directory, and writes
//! down what a restore needs.
//! It writes down what is needed to put the tree back.
//! What happens to the file contents once read belongs to [`super::pipeline`].

use std::{
    collections::HashMap,
    fmt::Debug,
    fs,
    io::{self, Cursor},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
};

use tracing::instrument;
use walkdir::WalkDir;

use crate::{command::pipeline, metadata, reader, storage};

/// Uploads to run at once when the caller has not asked for a specific number.
/// These uploads are I/O bound, so a little oversubscription keeps them from
/// stalling on round trips.
/// The cap stops a many-core host opening an unbounded number of blobs at a
/// remote store.
const DEFAULT_CONCURRENCY: usize = 8;

/// Hard ceiling on [`Options::concurrency`], whatever the caller asks for.
const MAX_CONCURRENCY: usize = 32;

/// Knobs for a single [`backup`] run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Options {
    /// Paths to leave out of the backup, compared after normalisation. The
    /// caller should exclude its own output artifacts: backing up a store that
    /// is being appended to while it is read corrupts the run.
    pub exclude: Vec<PathBuf>,
    /// Content-defined chunking parameters.
    pub chunker: reader::ChunkerConfig,
    /// How many chunks to upload at once.
    ///
    /// `1` runs the pipeline strictly one chunk at a time.
    /// Higher values overlap the round trips to the store, and do not change what
    /// comes out.
    /// Chunk order decides the snapshot, and it is the order chunks were *read* in,
    /// not the order they finished uploading.
    ///
    /// Values above 32 are treated as 32, and `0` as `1`, rather than rejected.
    pub concurrency: usize,
}

/// The command line's defaults.
///
/// `Default` *is* the command line's configuration, not a stand-in for it.
/// The binary's `--chunk-profile` resolves to one of the named
/// [`reader::ChunkerProfile`]s.
/// `default`, which is what a run gets without the flag, is exactly
/// [`reader::ChunkerConfig::default`].
/// Every backup this binary has ever written used that value.
/// A caller in a test that wants the same behaviour now says
/// `Options::default()` and gets it by construction.
/// The caller no longer restates three numbers and hopes they still match.
impl Default for Options {
    fn default() -> Self {
        Self {
            exclude: Vec::new(),
            chunker: reader::ChunkerConfig::default(),
            concurrency: default_concurrency(),
        }
    }
}

/// A version that has been committed to the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Committed {
    /// The version number the run was given.
    pub version: u32,
    /// Hash of the snapshot blob, which is how the store names this version.
    pub snapshot: storage::BlobId,
}

/// What a backup of a tree would do, worked out by reading the tree and by asking
/// the set what it already holds.
/// A plan writes nothing.
///
/// **Every figure here comes from the run a real backup would have made.**
/// It is not a second walk and an estimate beside it.
/// [`backup_plan`] drives the same walk, the same chunker and the same
/// deduplication map as [`backup`].
/// It uses a store that answers what it is asked and keeps none of it.
/// A plan that says two chunks are new describes a set that will grow by exactly
/// those two chunks.
/// A plan that says none are new describes a set that will not grow at all.
///
/// The dry run is a separate entry point rather than a field on [`Options`], and
/// the reason is the return type.
/// `backup` answers with a [`Committed`], whose version number is chosen by the
/// store at the moment it commits.
/// A run that writes nothing never reaches a commit, so there is no version for
/// it to report.
/// Asking one function for either answer means either making that field a lie or
/// inventing a report type a caller has to unwrap.
/// The plan is worth having in full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackupPlan {
    /// The version this run would commit, given the set as it stands.
    pub version: u32,
    /// Hash of the snapshot blob it would commit.
    pub snapshot: storage::BlobId,
    /// Regular files the walk found.
    pub files: u64,
    /// Directories the walk found.
    pub dirs: u64,
    /// Hard and symbolic links the walk found.
    pub links: u64,
    /// Distinct chunks the tree is cut into.
    pub chunks: u64,
    /// Of those, how many the set already holds.
    pub held: u64,
    /// Of those, how many would have to be written.
    pub fresh: u64,
    /// Bytes of chunk content that would be written.
    ///
    /// The content, before any compression.
    /// How large the records are on disk depends on the set's compression policy.
    /// The store chooses that policy when it is opened, so the size is not a
    /// property of the tree.
    pub bytes: u64,
}

/// What a walk found, which a plan reports and a committed run has no use for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct Entries {
    files: u64,
    dirs: u64,
    links: u64,
}

/// One blob a planning run was asked to store, and whether the set already had it.
struct Asked {
    id: storage::BlobId,
    len: u64,
    held: bool,
}

/// A store that answers what it is asked and keeps none of it.
///
/// The whole of a dry run.
/// It stands where the real store would.
/// The walk, the chunker, the deduplication map and the snapshot are built by
/// exactly the code a backup runs.
/// Nothing reaches the disk.
///
/// [`StoragePut::put`] asks the set whether it already holds the content instead
/// of writing it.
/// That is the one question a real store answers by consulting its own index and
/// returning without a byte.
/// A plan therefore costs the same lookup a deduplicating backup would have cost,
/// and nothing more.
/// It is not [`StoragePut`] itself that decides. See [`Planned`]'s
/// implementation.
/// The answer is recorded per blob so the report can count them afterwards.
struct Planned {
    /// The set, if there is one. `None` is the first backup of a location: no
    /// store, nothing held, and the whole tree new.
    store: Option<storage::BlobStore<storage::Read>>,
    /// Every distinct blob the run offered, and what became of it.
    asked: Arc<Mutex<Vec<Asked>>>,
}

// `async` on all three, and none of them awaits anything.
// That is what a store which writes nothing looks like.
// The trait is spelled in `async fn` because the two implementations that do
// real I/O are.
#[allow(clippy::unused_async_trait_impl)]
impl storage::StoragePut for Planned {
    /// Record what the run would have written, and drop the bytes.
    async fn put(
        &self,
        id: &storage::BlobId,
        _reader: storage::StreamReadSeeker,
        len: u64,
    ) -> io::Result<()> {
        self.record(id, len)
    }

    /// Record what the run would have written from a payload already encoded, and
    /// drop the bytes.
    ///
    /// The verbatim write path, and a plan of it is the same plan.
    /// What it would store is decided by the content hash, which both paths begin
    /// from.
    /// A record keyed by the content length is therefore the same record either
    /// way.
    async fn put_stored(
        &self,
        id: &storage::BlobId,
        _reader: storage::StreamReadSeeker,
        _stored_len: u64,
        raw_len: u64,
        _codec: storage::Codec,
    ) -> io::Result<()> {
        self.record(id, raw_len)
    }

    /// Nothing was written, so there is nothing to make durable.
    async fn sync_data(&self) -> io::Result<()> {
        Ok(())
    }

    /// The version this run *would* have committed.
    ///
    /// A plan publishes nothing, so there is no commit to report. The number is
    /// still worth handing back, and it is the one a real run would get: the set's
    /// own commit log, plus one.
    async fn commit(&self, _snapshot: &storage::BlobId) -> io::Result<u32> {
        let Some(store) = self.store.as_ref() else {
            return Ok(1);
        };

        Ok(u32::try_from(store.versions()?.len())
            .unwrap_or(u32::MAX)
            .saturating_add(1))
    }
}

impl Planned {
    /// Note what one blob would have been written as, and whether the set has it.
    ///
    /// The reader has already been dropped unread by the caller. Nothing
    /// downstream needs the bytes: the chunker has already hashed them, and the
    /// snapshot records the hashes.
    fn record(&self, id: &storage::BlobId, len: u64) -> io::Result<()> {
        let held = match self.store.as_ref() {
            // `locate` is the store's own index, so this is a lookup and not a
            // read of the chunk.
            // That is what keeps planning a large set the cost of a walk rather
            // than the cost of a backup.
            Some(store) => match store.locate(id) {
                Ok(_) => true,
                Err(e) if e.kind() == io::ErrorKind::NotFound => false,
                Err(e) => return Err(e),
            },
            None => false,
        };

        self.asked
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Asked { id: *id, len, held });

        Ok(())
    }
}

/// Uploads to run at once, bounded by what the machine can do.
fn default_concurrency() -> usize {
    super::concurrency(DEFAULT_CONCURRENCY)
}

/// Clamp a requested concurrency to something that can only help.
fn effective_concurrency(requested: usize) -> usize {
    requested.clamp(1, MAX_CONCURRENCY)
}

/// What the walk found, before any content was read.
struct Walked {
    /// Snapshot with its directories, links and file records filled in. Chunk
    /// and chunk-slice lists are still empty.
    snapshot: metadata::Snapshot,
    /// Each regular file's index in `snapshot.files`, by relative path.
    file_index: HashMap<PathBuf, u32>,
    /// The regular files' paths, in walk order, for the chunker to read.
    paths: Vec<PathBuf>,
}

/// Snapshot a directory tree and commit it to the store as a new version.
///
/// Deduplication is the store's business.
/// A chunk whose content the store already holds is not written again.
/// An unchanged file therefore costs nothing after the first time it is seen.
/// So does a file that was last seen in any version rather than only the most
/// recent.
///
/// # Errors
///
/// Returns an error if `root` is not a directory.
/// Returns an error if the tree cannot be walked.
/// Returns an error if a file changes while it is being read.
/// Returns an error if the store rejects a write, or if the run cannot be
/// committed.
/// A run that returns an error has written no commit record, so the store is
/// left exactly as it was. The next run's recovery reclaims the space.
/// The directory check is deliberately first.
/// A root that is not a directory would otherwise be recorded as an entry with
/// an empty path, which commits and is then unrestorable.
#[instrument(level = "info", skip(storage, options), err)]
pub async fn backup<P: AsRef<Path> + Debug, S: storage::StoragePut + 'static>(
    root: P,
    options: Options,
    storage: S,
) -> io::Result<Committed> {
    Ok(run(root, options, storage).await?.1)
}

/// Work out what a backup of `root` would write, and write none of it.
///
/// Everything a real backup does, in the same order and through the same code.
/// The run walks the tree, cuts it into chunks, hashes and deduplicates them.
/// The run builds and hashes the snapshot that would be committed.
/// The only difference is where the chunks go.
/// They go into a store that asks the set whether it holds them and keeps the
/// answers.
/// The figures are therefore the ones a real run would have printed rather than
/// an estimate made beside one.
///
/// `store` is `None` for a location with no set behind it.
/// That is the first backup of somewhere, and it is the case where a dry run has
/// to answer "everything is new".
/// A dry run must not fail the way [`storage::BlobStore::open_for_reading`] would.
/// Opening a set for writing to find out is not an option, because that *creates*
/// it.
///
/// Nothing is created, not even the directory the set would have lived in, and no
/// lock is taken.
/// A plan can therefore run against a set another backup is holding the lock on.
/// The cost is possibly planning against a set that moves under it.
///
/// # Errors
///
/// As [`backup`]: a tree that cannot be walked or read, or a file that changes
/// while it is being read.
/// The run also fails if a set cannot answer what it holds.
pub async fn backup_plan<P: AsRef<Path> + Debug>(
    root: P,
    options: Options,
    store: Option<storage::BlobStore<storage::Read>>,
) -> io::Result<BackupPlan> {
    let asked = Arc::new(Mutex::new(Vec::new()));
    let planned = Planned {
        store,
        asked: Arc::clone(&asked),
    };

    let (entries, committed) = run(root, options, planned).await?;

    // The snapshot is offered to the store like any other blob and is not a chunk
    // of anything.
    // It is dropped here rather than counted.
    // It is identified by the hash the run committed it under.
    // That hash is the one thing that run knows it is not.
    let asked = asked.lock().unwrap_or_else(PoisonError::into_inner);
    let mut chunks = 0u64;
    let mut held = 0u64;
    let mut bytes = 0u64;
    for ask in asked.iter().filter(|ask| ask.id != committed.snapshot) {
        chunks = chunks.saturating_add(1);
        if ask.held {
            held = held.saturating_add(1);
        } else {
            bytes = bytes.saturating_add(ask.len);
        }
    }

    Ok(BackupPlan {
        version: committed.version,
        snapshot: committed.snapshot,
        files: entries.files,
        dirs: entries.dirs,
        links: entries.links,
        chunks,
        held,
        fresh: chunks.saturating_sub(held),
        bytes,
    })
}

/// The run both entries to this module make, with or without a store underneath.
///
/// Split out rather than written twice because the thing that has to be shared is
/// the *whole* run.
/// A dry run that walked the tree by any other route could report a snapshot no
/// backup would ever commit.
/// That would be worse than no report.
#[instrument(level = "info", skip(storage, options), err)]
async fn run<P: AsRef<Path> + Debug, S: storage::StoragePut + 'static>(
    root: P,
    options: Options,
    storage: S,
) -> io::Result<(Entries, Committed)> {
    let Options {
        exclude,
        chunker: config,
        concurrency,
    } = options;

    let root = root.as_ref();
    let concurrency = effective_concurrency(concurrency);

    // The walk goes first and on its own.
    // The snapshot it records and the file index the pipeline needs have to exist
    // before the first chunk comes back.
    // The walk touches metadata only.
    let walk_root = root.to_path_buf();
    let excluded: Vec<PathBuf> = exclude.iter().map(|p| normalise(p)).collect();

    let Walked {
        mut snapshot,
        file_index,
        paths,
    } = tokio::task::spawn_blocking(move || walk(&walk_root, &excluded))
        .await
        .map_err(|join| io::Error::other(format!("the walk thread failed: {join}")))??;

    // Counted before the index moves into the pipeline, so the opening progress
    // line can report the file total before a single one is read.
    let files = u64::try_from(file_index.len()).unwrap_or(0);
    let entries = Entries {
        files,
        dirs: u64::try_from(snapshot.dirs.len()).unwrap_or(0),
        links: u64::try_from(snapshot.links.len()).unwrap_or(0),
    };

    let pipeline = pipeline::Pipeline::new(root.to_path_buf(), file_index, storage);

    Arc::clone(&pipeline)
        .run(paths, config, concurrency, files, &mut snapshot)
        .await?;

    // The chunker stats each file when it is registered and reads it again later.
    // A file that changed size in between would silently shift every subsequent
    // chunk into the wrong file.
    snapshot.verify_file_lengths("changed while it was being backed up")?;

    // Only fails where a path in the snapshot cannot be represented on this
    // platform, which on unix cannot happen.
    // Reported as invalid input rather than as damaged data.
    // Nothing about the snapshot is wrong. This build cannot write it.
    let buffer = snapshot.to_bytes().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("this tree cannot be recorded on this platform: {e}"),
        )
    })?;

    // The snapshot is a blob like any other, addressed by the hash of its own
    // bytes. The commit record names that hash.
    let id = blake3::hash(&buffer);

    let len = u64::try_from(buffer.len())
        .map_err(|_| io::Error::other("the snapshot is too large to store"))?;

    pipeline
        .storage
        .put(&id, Box::new(Cursor::new(buffer)), len)
        .await?;

    // The run's one data sync, before the commit record exists. See
    // `storage::blob` for why the commit is a second sync.
    pipeline.storage.sync_data().await?;

    let version = pipeline.storage.commit(&id).await?;

    Ok((
        entries,
        Committed {
            version,
            snapshot: id,
        },
    ))
}

/// Walk the tree, recording everything that is not file content.
fn walk(root: &Path, excluded: &[PathBuf]) -> io::Result<Walked> {
    // The walk records each entry relative to the root, so the root itself comes
    // back with an empty relative path and is skipped below.
    // That skip has to be unconditional.
    // Otherwise anything that is not a directory falls through it and is recorded
    // with an empty path.
    // Such a version commits and can then never be restored.
    // `restore` refuses an empty path before writing anything, and no subcommand
    // removes a version.
    //
    // `metadata` follows links, so a symlink to a directory passes.
    // That is what a user who typed `~/current-photos` means, and what a shell
    // would do.
    // The per-entry walk below still uses `symlink_metadata` and still records
    // links without following them.
    // Only the root is resolved, because a tree rooted at a link has to be
    // entered.
    if !fs::metadata(root)?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{} is not a directory. A backup is of a directory tree",
                root.display()
            ),
        ));
    }

    let mut snapshot = metadata::Snapshot::new(metadata::Timestamp::now());
    let mut inode_map: HashMap<FileId, PathBuf> = HashMap::new();

    // `io::Error::from(WalkFailure(..))` rather than a `?` on an `io::Result`
    // iterator: the walk has its own error type, and converting once here keeps
    // the conversion next to the reason it exists.
    let entries = WalkDir::new(root)
        .sort_by_file_name()
        .into_iter()
        .map(|entry| entry.map(walkdir::DirEntry::into_path).map_err(WalkFailure))
        .collect::<Result<Vec<_>, WalkFailure>>()
        .map_err(io::Error::from)?
        .into_iter();

    let paths = entries
        .filter(|path| !excluded.contains(&normalise(path)))
        .map(|path| {
            let meta = fs::symlink_metadata(&path)?;

            let relative_path = path
                .strip_prefix(root)
                .map_err(io::Error::other)?
                .to_path_buf();

            // The backup root is the destination on restore, so it is never
            // recorded as an entry of its own.
            // The root is known to be a directory by now, so the type test this
            // used to carry is not needed to justify the skip.
            if relative_path.as_os_str().is_empty() {
                return Ok(None);
            }

            // A symlink is recorded as a link and never followed, including one
            // that resolves to a directory.
            if meta.file_type().is_symlink() {
                snapshot.links.push(metadata::Link::Symbolic {
                    path: relative_path,
                    target: path.read_link()?,
                });
                return Ok(None);
            }

            // Recorded so empty ones and their permissions survive a restore.
            if meta.is_dir() {
                snapshot.dirs.push(metadata::Dir {
                    path: relative_path,
                    attrs: metadata::Attributes::of(&meta),
                });
                return Ok(None);
            }

            // A named pipe, socket or device node.
            // Reading one never returns.
            // Opening a FIFO for reading blocks until a writer appears.
            // These entries are therefore skipped rather than hanging the run.
            // The skip is logged, because a file in the tree that the backup does
            // not contain is what a user needs told.
            if !meta.is_file() {
                tracing::info!(
                    path = %relative_path.display(),
                    "skipping a named pipe, socket or device node",
                );
                return Ok(None);
            }

            // The first path to reference an inode carries the content.
            // Every later path sharing it becomes a hard link to that one.
            if let Some(file_id) = FileId::from_metadata(&meta) {
                if let Some(first) = inode_map.get(&file_id) {
                    snapshot.links.push(metadata::Link::Hard {
                        path: relative_path,
                        shares: first.clone(),
                    });

                    return Ok(None);
                }

                inode_map.insert(file_id, relative_path.clone());
            }

            snapshot.files.push(metadata::File {
                path: relative_path,
                len: meta.len(),
                attrs: metadata::Attributes::of(&meta),
            });

            Ok(Some(path))
        })
        .filter_map(Result::transpose)
        .collect::<io::Result<Vec<_>>>()?;

    // Indexed by relative path so a chunk source resolves in constant time
    // instead of relying on the chunker emitting files in order.
    let file_index = snapshot
        .files
        .iter()
        .enumerate()
        .map(|(index, file)| Ok((file.path.clone(), index_to_u32(index)?)))
        .collect::<io::Result<_>>()?;

    Ok(Walked {
        snapshot,
        file_index,
        paths,
    })
}

/// Best-effort normalisation used only for comparing excluded paths.
fn normalise(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// Narrow a collection index to the `u32` the snapshot format stores.
///
/// Wrapping instead of failing would point a file or chunk reference at a
/// different entry, so the limit is reported rather than crossed.
pub(super) fn index_to_u32(index: usize) -> io::Result<u32> {
    u32::try_from(index).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("the tree needs more than {} entries", u32::MAX),
        )
    })
}

/// A failure from walking the tree, with the walkdir error kept intact.
///
/// The flattening this replaces was `io::Error::other(format!("…: {e}"))`, which
/// threw away everything about the failure except its text.
/// The `ErrorKind` and the error itself went with it.
/// So an unreadable directory halfway down a tree came back as
/// `ErrorKind::Other` when the real answer was `PermissionDenied`.
/// Nothing could tell the two apart.
/// That included a caller of this library, which is not the CLI and does not have
/// a person reading a message.
///
/// The kind is recovered from `walkdir::Error::io_error`.
/// That value is `Some` for an I/O failure and `None` for the one case that has
/// no `io` error to speak of.
/// That case is a symlink loop.
/// `walkdir` names the offending path in its own `Display`, so the message gains
/// nothing by being longer.
struct WalkFailure(walkdir::Error);

impl std::fmt::Display for WalkFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "failed to walk backup root: {}", self.0)
    }
}

impl std::fmt::Debug for WalkFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

impl std::error::Error for WalkFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

impl From<WalkFailure> for io::Error {
    fn from(failure: WalkFailure) -> Self {
        // The inner error, not the message, so `get_ref` hands back the walkdir
        // error and `Display` still includes the path it names.
        // The kind comes from the same place.
        // It is `Other` only when walkdir had no `io` error to recover it from.
        let kind = failure
            .0
            .io_error()
            .map_or(io::ErrorKind::Other, io::Error::kind);
        io::Error::new(kind, failure)
    }
}

/// A file's identity on disk, for telling hard links apart.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct FileId {
    volume_id: u64,
    file_index: u64,
}

impl FileId {
    #[cfg(not(any(unix, windows)))]
    fn from_metadata(_meta: &fs::Metadata) -> Option<FileId> {
        None
    }

    // Uniform across platforms, though the unix variant always succeeds.
    // The others genuinely return `None` and callers should not have to care
    // which.
    #[cfg(unix)]
    #[allow(clippy::unnecessary_wraps)]
    fn from_metadata(meta: &fs::Metadata) -> Option<FileId> {
        use std::os::unix::fs::MetadataExt;
        Some(FileId {
            volume_id: meta.dev(),
            file_index: meta.ino(),
        })
    }

    #[cfg(all(windows, feature = "experimental"))]
    fn from_metadata(meta: &fs::Metadata) -> Option<FileId> {
        use std::os::windows::fs::MetadataExt;
        Some(FileId {
            volume_id: meta.volume_serial_number()?.into(),
            file_index: meta.file_index()?,
        })
    }

    #[cfg(all(windows, not(feature = "experimental")))]
    fn from_metadata(_meta: &fs::Metadata) -> Option<FileId> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{WalkDir, WalkFailure};
    use std::io;

    /// A walk failure keeps its `ErrorKind` and its error.
    ///
    /// Tested on the conversion rather than through a real unreadable directory,
    /// because provoking one needs a permission the test cannot take away from
    /// itself.
    /// This suite often runs as root, where `chmod 000` removes nothing.
    /// A test that only works for a non-root user is a test that is skipped in
    /// CI, silently.
    ///
    /// A path that is not there is a genuine walk failure and needs no special
    /// anything.
    /// walkdir yields the `io::Error` underneath, and the question is what comes
    /// out the other side.
    #[test]
    fn a_walk_failure_keeps_its_kind_and_its_error() {
        let missing = std::path::Path::new("/nonexistent-lepatch-walk-test");

        let failure = WalkDir::new(missing)
            .into_iter()
            .next()
            .expect("walking nothing should yield one error")
            .expect_err("and it should be an error");

        let error: io::Error = WalkFailure(failure).into();

        assert_eq!(
            error.kind(),
            io::ErrorKind::NotFound,
            "the kind should come from the error underneath, not default to Other: {error}"
        );

        // And the error itself survives, so `get_ref` is not `None` and
        // `Display` still carries the path walkdir named.
        assert!(
            error.get_ref().is_some(),
            "the walkdir error should still be reachable: {error}"
        );
        assert!(
            error.to_string().contains("nonexistent-lepatch-walk-test"),
            "and the path should still be in the message: {error}"
        );
        assert!(
            error.to_string().starts_with("failed to walk backup root:"),
            "with the context that says a walk is what failed: {error}"
        );
    }
}
