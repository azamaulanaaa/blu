use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    fs,
    io::{self, Cursor},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
};

use tokio::{
    io::AsyncReadExt,
    sync::{OnceCell, mpsc},
    task::JoinSet,
};
use tracing::instrument;
use walkdir::WalkDir;

use crate::{fsattr, metadata, reader, storage};

/// Uploads to run at once when the caller has not asked for a specific number.
///
/// Uploads are I/O bound, so oversubscribing the machine a little is what keeps
/// them from stalling on round trips. The cap stops a many-core host from
/// opening an unbounded number of blobs at the store at once, which for a
/// remote backend is a way to get rate limited rather than to go faster.
const DEFAULT_CONCURRENCY: usize = 8;

/// Hard ceiling on [`Options::concurrency`], whatever the caller asks for.
const MAX_CONCURRENCY: usize = 32;

/// Knobs for a single [`backup`] run.
#[derive(Debug, Clone)]
pub struct Options {
    /// Key of a previous snapshot whose chunks may be reused instead of being
    /// uploaded again. Deduplication against this snapshot only.
    pub base_key: Option<String>,
    /// Paths to leave out of the backup, compared after normalisation. The
    /// caller should exclude its own output artifacts: backing up a blob store
    /// that is being appended to while it is read corrupts the chunk offsets.
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
    /// Values above 32 are treated as 32, and `0` as `1`, rather than being
    /// rejected — this is a tuning knob, not a correctness setting.
    pub concurrency: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            base_key: None,
            exclude: Vec::new(),
            chunker: reader::ChunkerConfig {
                min_size: 8 * 1024,
                avg_size: 16 * 1024,
                max_size: 64 * 1024,
            },
            concurrency: default_concurrency(),
        }
    }
}

/// Uploads to run at once when the caller has not asked for a specific number.
fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map_or(DEFAULT_CONCURRENCY, |n| n.get().min(DEFAULT_CONCURRENCY))
}

/// Clamp a requested concurrency to something that can only help.
fn effective_concurrency(requested: usize) -> usize {
    requested.clamp(1, MAX_CONCURRENCY)
}

/// A chunk that has been read and hashed, waiting to be stored.
struct Pending {
    /// Position of this chunk in the order the tree was read.
    seq: u64,
    hash: [u8; 32],
    sources: Vec<reader::ChunkSource>,
    data: Vec<u8>,
}

/// A chunk that has been stored, or found already stored.
struct Stored {
    seq: u64,
    hash: [u8; 32],
    /// Which chunk first turned up with this content. Equal to `seq` for the one
    /// that did the storing.
    first_seq: u64,
    /// The file slices this chunk covers.
    sources: Vec<reader::ChunkSource>,
    /// Where the content lives, once someone has put it there.
    chunk: metadata::Chunk,
}

/// One hash's entry in the run's deduplication map.
///
/// The cell is filled exactly once, by whichever chunk reaches it first. Every
/// later chunk with the same content waits on the same cell instead of storing
/// a second copy, which is what keeps deduplication exact now that chunks are
/// in flight at the same time.
struct Slot {
    first_seq: u64,
    stored: OnceCell<metadata::Chunk>,
}

/// Deduplication state shared by every upload in flight.
type Slots = Arc<Mutex<HashMap<[u8; 32], Arc<Slot>>>>;

/// Lock the deduplication map, ignoring poisoning.
///
/// A panic while the lock is held would leave a half-built map, but the only code
/// that runs under it is a hash insert, which cannot panic. Unwinding through
/// this function would abort the run anyway, so the poison carries no
/// information worth propagating.
fn lock_slots(slots: &Slots) -> std::sync::MutexGuard<'_, HashMap<[u8; 32], Arc<Slot>>> {
    slots.lock().unwrap_or_else(PoisonError::into_inner)
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

/// Snapshot a directory tree and return the storage key of the snapshot.
///
/// The returned snapshot can be passed back as [`Options::base_key`] on a later
/// run so that unchanged chunks are not uploaded again.
///
/// # Errors
///
/// Returns an error if the tree cannot be walked, if a file changes while it is
/// being read, or if the storage backend rejects a write.
#[instrument(level = "info", skip(storage, options), err)]
pub async fn backup<
    P: AsRef<Path> + Debug,
    S: storage::StoragePut + storage::StorageGet + 'static,
>(
    root: P,
    options: Options,
    storage: S,
) -> io::Result<String> {
    let Options {
        base_key,
        exclude,
        chunker: config,
        concurrency,
    } = options;

    let root = root.as_ref();
    let concurrency = effective_concurrency(concurrency);

    let base_chunks: Arc<HashMap<[u8; 32], metadata::Chunk>> = match base_key {
        Some(v) => {
            let mut reader = storage.get(&v).await?;
            let mut buffer = Vec::new();
            reader.read_to_end(&mut buffer).await?;
            let snapshot = metadata::Snapshot::from_bytes(buffer.as_slice())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            Arc::new(
                snapshot
                    .chunks
                    .into_iter()
                    .map(|chunk| (chunk.hash, chunk))
                    .collect(),
            )
        }
        None => Arc::new(HashMap::new()),
    };

    // Walking stats every entry in the tree, which blocks; reading and hashing
    // the contents blocks and burns CPU. Both halves of that are the producer's
    // job, run on blocking threads, while the only part that actually waits on
    // anything — storing a chunk — runs concurrently on the async pool.
    //
    // The walk goes first and on its own, because the snapshot it records and
    // the file index the committer needs have to exist before the first chunk
    // comes back. It touches metadata only, so it is cheap next to reading the
    // contents that follows it.
    let walk_root = root.to_path_buf();
    let excluded: Vec<PathBuf> = exclude.iter().map(|p| normalise(p)).collect();

    let Walked {
        mut snapshot,
        file_index,
        paths,
    } = tokio::task::spawn_blocking(move || walk(&walk_root, &excluded))
        .await
        .map_err(|join| io::Error::other(format!("the walk thread failed: {join}")))??;

    // A bounded channel between the two halves: a slow store throttles the
    // reader rather than letting the tree accumulate in memory.
    let (tx, mut rx) = mpsc::channel::<Pending>(concurrency * 2);

    let producer = tokio::task::spawn_blocking(move || produce(paths, config, tx));

    let slots: Slots = Arc::new(Mutex::new(HashMap::new()));

    let pipeline = Arc::new(Pipeline {
        root: root.to_path_buf(),
        file_index,
        base_chunks,
        storage: Arc::new(storage),
        slots,
    });

    let committed = commit(Arc::clone(&pipeline), &mut rx, concurrency, &mut snapshot).await;

    if let Err(e) = committed {
        // Dropping the receiver makes the producer's next send fail, so it stops
        // reading rather than working through a tree nobody wants.
        drop(rx);
        let _ = producer.await;
        return Err(e);
    }

    // The producer's own outcome is checked before the snapshot is trusted: it
    // is the one that knows why a file's contents could not be read, and its
    // error says so far more usefully than the length mismatch one would infer
    // further down.
    producer
        .await
        .map_err(|join| io::Error::other(format!("the reader thread failed: {join}")))??;

    // The chunker stats each file when it is registered and reads it again
    // later, so a file that changed size in between would silently shift every
    // subsequent chunk into the wrong file. Checking the reassembled lengths
    // against what the walk recorded turns that into a hard failure.
    verify_file_lengths(&snapshot)?;

    let buffer = snapshot
        .to_bytes()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let len = u64::try_from(buffer.len())
        .map_err(|_| io::Error::other("the snapshot is too large to store"))?;

    pipeline
        .storage
        .put(Box::new(Cursor::new(buffer)), len)
        .await
}

/// Read, chunk and hash the tree on a blocking thread, feeding `tx`.
///
/// This half is deliberately sequential: content-defined chunking finds each
/// boundary from the bytes before it, so the stream cannot be split across
/// threads. What it does overlap is the storing of chunks it has already handed
/// over.
fn produce(
    paths: Vec<PathBuf>,
    config: reader::ChunkerConfig,
    tx: mpsc::Sender<Pending>,
) -> io::Result<()> {
    // Built here rather than by the caller because this is the thread that
    // blocks on it: stat'ing every file is a syscall apiece.
    let chunker = reader::Chunker::new(paths, config)?;

    for (seq, chunk) in (0_u64..).zip(chunker) {
        let chunk = chunk?;

        // Hashed here, on the thread that already has the bytes in hand and
        // already blocks, rather than by the upload that consumes it: the hash
        // is needed for deduplication, whereas the store is the part that wants
        // the async pool to itself.
        let hash = *blake3::hash(&chunk.data).as_bytes();

        // `blocking_send`, because this is a blocking thread and the await it
        // would otherwise need is exactly what it must not do. It returns an
        // error once every receiver is gone, which is the signal that the run
        // has already failed and there is nothing left to read for.
        tx.blocking_send(Pending {
            seq,
            hash,
            sources: chunk.sources,
            data: chunk.data,
        })
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "every upload failed before the tree was read",
            )
        })?;
    }

    // Dropping the sender is how the other half learns the tree is finished.
    drop(tx);

    Ok(())
}

/// Everything the storing half of the pipeline needs, gathered so that passing it
/// around does not turn into a longer and longer argument list.
struct Pipeline<S> {
    /// Root the tree was walked from, to turn a chunk source's path back into the
    /// relative one the file index is keyed by.
    root: PathBuf,
    /// Each regular file's index, by relative path.
    file_index: HashMap<PathBuf, u32>,
    /// Chunks a previous snapshot already holds, so they are not stored again.
    base_chunks: Arc<HashMap<[u8; 32], metadata::Chunk>>,
    storage: Arc<S>,
    slots: Slots,
}

/// Drive the stores and fold their results into `snapshot`.
///
/// Results are committed strictly in the order the chunks were read, so the
/// snapshot a run produces does not depend on how the stores interleaved. That
/// is what lets [`Options::concurrency`] be tuned freely.
async fn commit<S: storage::StoragePut + 'static>(
    pipeline: Arc<Pipeline<S>>,
    rx: &mut mpsc::Receiver<Pending>,
    concurrency: usize,
    snapshot: &mut metadata::Snapshot,
) -> io::Result<()> {
    let mut in_flight = JoinSet::new();

    // Results that finished before the one they follow, keyed by read order.
    // Bounded by the number of stores running, so it cannot grow with the size
    // of the tree.
    let mut ready: BTreeMap<u64, Stored> = BTreeMap::new();
    let mut next_seq: u64 = 0;

    // Which chunk index each distinct content was given, so a later chunk with
    // the same content reuses it instead of being stored again.
    let mut index_of: HashMap<[u8; 32], u32> = HashMap::new();

    let mut producer_finished = false;

    loop {
        // Keep as many stores running as were asked for.
        while !producer_finished && in_flight.len() < concurrency {
            let Some(pending) = rx.recv().await else {
                producer_finished = true;
                break;
            };

            in_flight.spawn(store_chunk(pending, Arc::clone(&pipeline)));
        }

        if in_flight.is_empty() {
            if producer_finished {
                break;
            }
            continue;
        }

        match in_flight.join_next().await {
            Some(Ok(Ok(stored))) => {
                ready.insert(stored.seq, stored);
                commit_ready(
                    &pipeline,
                    &mut ready,
                    &mut next_seq,
                    &mut index_of,
                    snapshot,
                )?;
            }
            // The first failure ends the run; the rest of the stores are dropped
            // with it, and the receiver goes with them.
            Some(Ok(Err(e))) => return Err(e),
            Some(Err(join)) => {
                return Err(io::Error::other(format!("a store task failed: {join}")));
            }
            None => producer_finished = true,
        }
    }

    // Everything the producer sent has been committed: a result left over would
    // mean a chunk was stored but never referenced, silently wasting space.
    if let Some(&seq) = ready.keys().next() {
        return Err(io::Error::other(format!(
            "chunk {seq} was stored but never committed"
        )));
    }

    Ok(())
}

/// Fold in every result whose turn has come, in read order.
fn commit_ready<S>(
    pipeline: &Pipeline<S>,
    ready: &mut BTreeMap<u64, Stored>,
    next_seq: &mut u64,
    index_of: &mut HashMap<[u8; 32], u32>,
    snapshot: &mut metadata::Snapshot,
) -> io::Result<()> {
    while let Some(stored) = ready.remove(next_seq) {
        let chunk_index = if stored.first_seq == *next_seq {
            // First time this content has been seen: it gets the next index.
            let index = index_to_u32(snapshot.chunks.len())?;
            snapshot.chunks.push(stored.chunk);
            index_of.insert(stored.hash, index);
            index
        } else {
            // An earlier chunk already stored it, and was committed first
            // because it has the lower sequence number.
            *index_of.get(&stored.hash).ok_or_else(|| {
                io::Error::other(format!(
                    "chunk {} reuses content whose original was never committed",
                    stored.seq
                ))
            })?
        };

        let mut chunk_offset: u32 = 0;
        for source in stored.sources {
            // The file index is keyed by path relative to the backup root, while
            // a chunk source carries the path as the walk found it.
            let relative = source
                .path
                .strip_prefix(&pipeline.root)
                .map_err(io::Error::other)?;

            let Some(&index) = pipeline.file_index.get(relative) else {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "chunk source {} is not a file in the snapshot",
                        source.path.display()
                    ),
                ));
            };

            snapshot.file_chunks.push(metadata::FileChunk {
                chunk_index,
                file_index: index,
                chunk_offset,
                file_offset: source.offset,
                length: source.length,
            });

            chunk_offset += source.length;
        }

        *next_seq += 1;
    }

    Ok(())
}

/// Walk the tree, recording everything that is not file content.
fn walk(root: &Path, excluded: &[PathBuf]) -> io::Result<Walked> {
    let mut snapshot = metadata::Snapshot::new();
    let mut inode_map: HashMap<FileId, PathBuf> = HashMap::new();

    let entries = WalkDir::new(root)
        .sort_by_file_name()
        .into_iter()
        .map(|entry| {
            entry
                .map(walkdir::DirEntry::into_path)
                .map_err(|e| io::Error::other(format!("failed to walk backup root: {e}")))
        })
        .collect::<io::Result<Vec<_>>>()?;

    let paths = entries
        .into_iter()
        .filter(|path| !excluded.contains(&normalise(path)))
        .map(|path| {
            let meta = fs::symlink_metadata(&path)?;

            let relative_path = path
                .strip_prefix(root)
                .map_err(io::Error::other)?
                .to_path_buf();

            // The backup root itself is the destination on restore, so it is
            // never recorded as an entry of its own.
            if relative_path.as_os_str().is_empty() && meta.is_dir() {
                return Ok(None);
            }

            // A symlink is recorded as a link and never followed, including one
            // that resolves to a directory.
            if meta.file_type().is_symlink() {
                snapshot.links.push(metadata::Link {
                    path: relative_path,
                    target: path.read_link()?,
                    kind: metadata::LinkKind::Symbolic,
                });
                return Ok(None);
            }

            // Directories are recorded so that empty ones and their permissions
            // survive a restore.
            if meta.is_dir() {
                snapshot.dirs.push(metadata::Dir {
                    path: relative_path,
                    mode: fsattr::mode_of(&meta),
                    mtime: fsattr::mtime_of(&meta),
                });
                return Ok(None);
            }

            // The first path to reference an inode carries the content; every
            // later path sharing it becomes a hard link to that one.
            if let Some(file_id) = FileId::from_metadata(&meta) {
                if let Some(first) = inode_map.get(&file_id) {
                    snapshot.links.push(metadata::Link {
                        path: relative_path,
                        target: first.clone(),
                        kind: metadata::LinkKind::Hard,
                    });

                    return Ok(None);
                }

                inode_map.insert(file_id, relative_path.clone());
            }

            snapshot.files.push(metadata::File {
                path: relative_path,
                len: meta.len(),
                mode: fsattr::mode_of(&meta),
                mtime: fsattr::mtime_of(&meta),
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

/// Store a chunk, or wait for whichever chunk with the same content got there
/// first.
async fn store_chunk<S: storage::StoragePut + 'static>(
    pending: Pending,
    pipeline: Arc<Pipeline<S>>,
) -> io::Result<Stored> {
    let Pending {
        seq,
        hash,
        sources,
        data,
    } = pending;

    let slot = {
        let mut map = lock_slots(&pipeline.slots);
        Arc::clone(map.entry(hash).or_insert_with(|| {
            Arc::new(Slot {
                first_seq: seq,
                stored: OnceCell::new(),
            })
        }))
    };

    let chunk = slot
        .stored
        .get_or_try_init(|| async {
            // Content the base snapshot already holds is not stored again,
            // which is the whole point of an incremental backup.
            if let Some(base) = pipeline.base_chunks.get(&hash) {
                return Ok(base.clone());
            }

            let len = u64::try_from(data.len()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "chunk of {} bytes does not fit in a blob length",
                        data.len()
                    ),
                )
            })?;

            let location = pipeline
                .storage
                .put(Box::new(Cursor::new(data)), len)
                .await?;

            Ok::<metadata::Chunk, io::Error>(metadata::Chunk { hash, location })
        })
        .await?;

    Ok(Stored {
        seq,
        hash,
        first_seq: slot.first_seq,
        sources,
        chunk: chunk.clone(),
    })
}

/// Best-effort normalisation used only for comparing excluded paths.
fn normalise(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// Narrow a collection index to the `u32` the snapshot format stores.
///
/// Wrapping instead of failing would point a file or chunk reference at a
/// different entry, so the limit is reported rather than crossed. At the default
/// chunk size `u32::MAX` chunks is a quarter of a petabyte of data, and
/// `u32::MAX` files is unreachable in practice.
fn index_to_u32(index: usize) -> io::Result<u32> {
    u32::try_from(index).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("the tree needs more than {} entries", u32::MAX),
        )
    })
}

/// Assert that every file's chunk slices add up to the length recorded during
/// the walk, and that no slice claims space outside its file.
fn verify_file_lengths(snapshot: &metadata::Snapshot) -> io::Result<()> {
    let mut totals = vec![0u64; snapshot.files.len()];
    let mut extents: Vec<Option<(u64, u64)>> = vec![None; snapshot.files.len()];

    for slice in &snapshot.file_chunks {
        let index = slice.file_index as usize;
        let file = snapshot.files.get(index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("chunk references file {index}, which the snapshot does not contain"),
            )
        })?;

        let end = slice
            .file_offset
            .checked_add(slice.length.into())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("chunk range overflows in {}", file.path.display()),
                )
            })?;

        totals[index] += u64::from(slice.length);

        let extent = extents[index].get_or_insert((slice.file_offset, end));
        extent.0 = extent.0.min(slice.file_offset);
        extent.1 = extent.1.max(end);
    }

    for (index, file) in snapshot.files.iter().enumerate() {
        let covered = extents[index].map_or(0, |(start, end)| end - start);

        if totals[index] != file.len || covered != file.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} changed while it was being backed up: recorded {} bytes, chunked {} bytes covering {covered} bytes",
                    file.path.display(),
                    file.len,
                    totals[index]
                ),
            ));
        }
    }

    Ok(())
}

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

    // The signature is uniform across platforms even though the unix
    // implementation below always succeeds: the other `cfg` variants genuinely
    // return `None`, and callers should not have to care which one they got.
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
