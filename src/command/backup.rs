//! Recording what a directory tree contains, and handing the contents over to be
//! stored: walking, telling a file from a link from a directory, and writing down
//! what a restore needs to put the tree back. What happens to the file contents
//! once read belongs to [`super::pipeline`].

use std::{
    collections::HashMap,
    fmt::Debug,
    fs,
    io::{self, Cursor},
    path::{Path, PathBuf},
    sync::Arc,
};

use tracing::instrument;
use walkdir::WalkDir;

use crate::{command::pipeline, metadata, reader, storage};

/// Uploads to run at once when the caller has not asked for a specific number.
/// I/O bound, so a little oversubscription keeps them from stalling on round
/// trips; the cap stops a many-core host opening an unbounded number of blobs at
/// a remote store.
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
    /// `1` runs the pipeline strictly one chunk at a time. Higher values overlap
    /// the round trips to the store, and do not change what comes out: chunk
    /// order, and therefore the snapshot, is decided by the order chunks were
    /// *read* in, not by the order they finished uploading.
    ///
    /// Values above 32 are treated as 32, and `0` as `1`, rather than rejected.
    pub concurrency: usize,
}

/// The command line's defaults.
///
/// `Default` *is* the command line's configuration, not a stand-in for it: the
/// binary's `--chunk-profile` resolves to one of the named
/// [`reader::ChunkerProfile`]s, and `default` — what a run gets without the flag —
/// is exactly [`reader::ChunkerConfig::default`], which is what every backup it
/// has ever written used. A caller in a test that wants the same behaviour now
/// says `Options::default()` and gets it by construction, rather than restating
/// three numbers and hoping they still match.
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
/// Deduplication is the store's business: a chunk whose content it already holds
/// is not written again, so an unchanged file costs nothing after the first time
/// it is seen, and so does one that was last seen in any version rather than
/// only the most recent.
///
/// # Errors
///
/// Returns an error if `root` is not a directory, if the tree cannot be walked,
/// if a file changes while it is being read, if the store rejects a write, or if
/// the run cannot be committed. A run that returns an error has written no commit
/// record, so the store is left exactly as it was — the space is reclaimed by the
/// next run's recovery. The directory check is deliberately first: a root that is
/// not a directory would otherwise be recorded as an entry with an empty path,
/// which commits and is then unrestorable.
#[instrument(level = "info", skip(storage, options), err)]
pub async fn backup<P: AsRef<Path> + Debug, S: storage::StoragePut + 'static>(
    root: P,
    options: Options,
    storage: S,
) -> io::Result<Committed> {
    let Options {
        exclude,
        chunker: config,
        concurrency,
    } = options;

    let root = root.as_ref();
    let concurrency = effective_concurrency(concurrency);

    // The walk goes first and on its own: the snapshot it records and the file
    // index the pipeline needs have to exist before the first chunk comes back,
    // and it touches metadata only.
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

    let pipeline = pipeline::Pipeline::new(root.to_path_buf(), file_index, storage);

    Arc::clone(&pipeline)
        .run(paths, config, concurrency, files, &mut snapshot)
        .await?;

    // The chunker stats each file when it is registered and reads it again later,
    // so a file that changed size in between would silently shift every
    // subsequent chunk into the wrong file.
    snapshot.verify_file_lengths("changed while it was being backed up")?;

    // Only fails where a path in the snapshot cannot be represented on this
    // platform, which on unix cannot happen. Reported as invalid input rather
    // than as damaged data: nothing about the snapshot is wrong, this build
    // cannot write it.
    let buffer = snapshot.to_bytes().map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("this tree cannot be recorded on this platform: {e}"),
        )
    })?;

    // The snapshot is a blob like any other, addressed by the hash of its own
    // bytes — which is what the commit record names.
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

    Ok(Committed {
        version,
        snapshot: id,
    })
}

/// Walk the tree, recording everything that is not file content.
fn walk(root: &Path, excluded: &[PathBuf]) -> io::Result<Walked> {
    // The walk records each entry relative to the root, so the root itself comes
    // back with an empty relative path and is skipped below. That skip has to be
    // unconditional, or anything that is not a directory falls through it and is
    // recorded with an empty path — a version that commits and can then never be
    // restored, since `restore` refuses an empty path before writing anything and
    // no subcommand removes a version.
    //
    // `metadata` follows links, so a symlink to a directory passes, which is what
    // a user who typed `~/current-photos` means and what a shell would do. The
    // per-entry walk below still uses `symlink_metadata` and still records links
    // without following them; only the root is resolved, because a tree rooted at
    // a link has to be entered.
    if !fs::metadata(root)?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{} is not a directory; a backup is of a directory tree",
                root.display()
            ),
        ));
    }

    let mut snapshot = metadata::Snapshot::new();
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
            // recorded as an entry of its own. The root is known to be a
            // directory by now, so the type test this used to carry is not
            // needed to justify the skip.
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

            // A named pipe, socket or device node. Reading one never returns —
            // opening a FIFO for reading blocks until a writer appears — so they
            // are skipped rather than hanging the run, and the skip is logged: a
            // file in the tree the backup does not contain is what a user needs
            // told.
            if !meta.is_file() {
                tracing::info!(
                    path = %relative_path.display(),
                    "skipping a named pipe, socket or device node",
                );
                return Ok(None);
            }

            // The first path to reference an inode carries the content; every
            // later path sharing it becomes a hard link to that one.
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
/// threw away everything about the failure except its text: the `ErrorKind` and
/// the error itself. So an unreadable directory halfway down a tree came back as
/// `ErrorKind::Other` when the real answer was `PermissionDenied`, and nothing
/// could tell the two apart — including a caller of this library, which is not
/// the CLI and does not have a person reading a message.
///
/// The kind is recovered from `walkdir::Error::io_error`, which is `Some` for an
/// I/O failure and `None` for the one case that has no `io` error to speak of (a
/// symlink loop). `walkdir` names the offending path in its own `Display`, so the
/// message gains nothing by being longer.
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
        // error and `Display` still includes the path it names. The kind comes
        // from the same place, and is `Other` only when walkdir had no `io` error
        // to recover it from.
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

    // Uniform across platforms, though the unix variant always succeeds: the
    // others genuinely return `None` and callers should not have to care which.
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
    /// itself — and this suite often runs as root, where `chmod 000` removes
    /// nothing. A test that only works for a non-root user is a test that is
    /// skipped in CI, silently.
    ///
    /// A path that is not there is a genuine walk failure and needs no special
    /// anything: walkdir yields the `io::Error` underneath, and the question is
    /// what comes out the other side.
    #[test]
    fn a_walk_failure_is_not_flattened_to_other() {
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
