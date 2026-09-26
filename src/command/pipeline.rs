//! Getting a walked tree's chunks stored, concurrently.
//!
//! Backup is two kinds of work wearing the same function. Reading the tree,
//! cutting it into chunks and hashing them all block or burn CPU, and only one
//! of them can happen at a time: content-defined chunking finds each boundary
//! from the bytes before it, so the stream cannot be split across threads.
//! Storing a chunk is the opposite — it does nothing but wait on the store —
//! and is worth doing several at once.
//!
//! So the reading half runs on a blocking thread, the storing half runs on the
//! async pool, and a bounded channel joins them. The bound matters: it means a
//! slow store throttles the reader rather than letting a whole tree accumulate
//! in memory waiting to be written.
//!
//! Deduplication has to stay exact while chunks are in flight together, which a
//! plain map lookup cannot manage on its own. Each hash gets a [`Slot`] holding
//! a `OnceCell`, and every chunk with that content fills the same one: the first
//! to arrive stores it, the rest wait and reuse the result. Nothing is stored
//! twice, however many stores happen to be running.
//!
//! What comes out does not depend on how the stores interleaved. Results are
//! committed in the order chunks were *read*, so a snapshot is a function of the
//! tree rather than of scheduling — which is what makes the concurrency a tuning
//! knob rather than a source of run-to-run differences.

use std::{
    collections::{BTreeMap, HashMap},
    io::{self, Cursor},
    path::PathBuf,
    sync::{Arc, Mutex, PoisonError},
};

use tokio::{
    sync::{OnceCell, mpsc},
    task::JoinSet,
};

use crate::{command::backup::index_to_u32, metadata, reader, storage};

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
pub(super) struct Pipeline<S> {
    /// Root the tree was walked from, to turn a chunk source's path back into the
    /// relative one the file index is keyed by.
    root: PathBuf,
    /// Each regular file's index, by relative path.
    file_index: HashMap<PathBuf, u32>,
    /// Chunks a previous snapshot already holds, so they are not stored again.
    base_chunks: Arc<HashMap<[u8; 32], metadata::Chunk>>,
    pub(super) storage: Arc<S>,
    slots: Slots,
}

impl<S: storage::StoragePut + storage::StorageGet + 'static> Pipeline<S> {
    pub(super) fn new(
        root: PathBuf,
        file_index: HashMap<PathBuf, u32>,
        base_chunks: Arc<HashMap<[u8; 32], metadata::Chunk>>,
        storage: S,
    ) -> Arc<Self> {
        Arc::new(Self {
            root,
            file_index,
            base_chunks,
            storage: Arc::new(storage),
            slots: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Read every path, store the chunks they turn into, and fold the results
    /// into `snapshot`.
    ///
    /// # Errors
    ///
    /// Returns an error if a file cannot be read, or if the store rejects a
    /// chunk. A failure on either side stops both: the reader is not left
    /// working through a tree whose chunks are going nowhere.
    pub(super) async fn run(
        self: Arc<Self>,
        paths: Vec<PathBuf>,
        config: reader::ChunkerConfig,
        concurrency: usize,
        snapshot: &mut metadata::Snapshot,
    ) -> io::Result<()> {
        // A bounded channel between the two halves: a slow store throttles the
        // reader rather than letting the tree accumulate in memory.
        let (tx, mut rx) = mpsc::channel::<Pending>(concurrency * 2);

        let producer = tokio::task::spawn_blocking(move || produce(paths, config, tx));

        let committed = commit(Arc::clone(&self), &mut rx, concurrency, snapshot).await;

        if let Err(e) = committed {
            // Dropping the receiver makes the producer's next send fail, so it
            // stops reading rather than working through a tree nobody wants.
            drop(rx);
            let _ = producer.await;
            return Err(e);
        }

        // The producer's own outcome is checked before the snapshot is trusted:
        // it is the one that knows why a file's contents could not be read, and
        // its error says so far more usefully than the length mismatch one would
        // infer further down.
        producer
            .await
            .map_err(|join| io::Error::other(format!("the reader thread failed: {join}")))??;

        Ok(())
    }
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
