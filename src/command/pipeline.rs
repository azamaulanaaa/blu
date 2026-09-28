//! Getting a walked tree's chunks stored, concurrently.
//!
//! Reading, chunking and hashing must be sequential: a content-defined boundary
//! depends on the bytes before it, so the reader cannot be split across threads.
//! Storing waits on the store and overlaps freely, so the reading half runs on a
//! blocking thread, the storing half on the async pool, and a bounded channel
//! joins them — which is what makes a slow store throttle the reader instead of
//! buffering a whole tree in memory.
//!
//! Each distinct hash gets a [`Slot`] filled once, so nothing is stored twice
//! however many stores are in flight. Results are committed in the order chunks
//! were *read*, so a snapshot is a function of the tree and not of scheduling.

use std::{
    collections::{BTreeMap, HashMap},
    io::{self, Cursor},
    path::PathBuf,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};

use tokio::{
    sync::{OnceCell, mpsc},
    task::JoinSet,
};

use crate::{command::backup::index_to_u32, metadata, progress::Progress, reader, storage};

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
    /// The file slices this chunk covers.
    sources: Vec<reader::ChunkSource>,
}

/// One hash's cell in the deduplication map, filled exactly once.
///
/// Every chunk of the same content fills the same cell: the first to arrive stores
/// it, the rest wait and reuse it. The store deduplicates too, but its index is
/// only updated once a blob is written, so without this a run hands it the same
/// content once per copy.
type Slot = Arc<OnceCell<()>>;

/// Deduplication state shared by every store in flight.
type Slots = Arc<Mutex<HashMap<[u8; 32], Slot>>>;

/// Lock the deduplication map, ignoring poisoning: the only code under it is a
/// hash insert, which cannot panic.
fn lock_slots(slots: &Slots) -> std::sync::MutexGuard<'_, HashMap<[u8; 32], Slot>> {
    slots.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Read, chunk and hash the tree on a blocking thread, feeding `tx`.
///
/// Sequential, because a content-defined boundary depends on the bytes before it.
/// What it does overlap is the storing of chunks it has already handed over.
///
/// Returns how long it spent blocked handing chunks over, which the committer
/// turns into the pipeline's timing line.
fn produce(
    paths: Vec<PathBuf>,
    config: reader::ChunkerConfig,
    tx: mpsc::Sender<Pending>,
) -> io::Result<Duration> {
    let mut blocked = Duration::ZERO;

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

        // `blocking_send`, because this is a blocking thread. An error means every
        // receiver is gone, so the run has already failed.
        let sending = Instant::now();
        let sent = tx.blocking_send(Pending {
            seq,
            hash,
            sources: chunk.sources,
            data: chunk.data,
        });
        blocked += sending.elapsed();

        sent.map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "every upload failed before the tree was read",
            )
        })?;
    }

    // Dropping the sender is how the other half learns the tree is finished.
    drop(tx);

    Ok(blocked)
}

/// Everything the storing half of the pipeline needs.
pub(super) struct Pipeline<S> {
    /// Root the tree was walked from, to turn a chunk source's path back into the
    /// relative one the file index is keyed by.
    root: PathBuf,
    /// Each regular file's index, by relative path.
    file_index: HashMap<PathBuf, u32>,
    pub(super) storage: Arc<S>,
    slots: Slots,
}

impl<S: storage::StoragePut + 'static> Pipeline<S> {
    pub(super) fn new(root: PathBuf, file_index: HashMap<PathBuf, u32>, storage: S) -> Arc<Self> {
        Arc::new(Self {
            root,
            file_index,
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
        files: u64,
        snapshot: &mut metadata::Snapshot,
    ) -> io::Result<()> {
        // A bounded channel between the two halves: a slow store throttles the
        // reader rather than letting the tree accumulate in memory.
        let (tx, mut rx) = mpsc::channel::<Pending>(concurrency * 2);

        let reader = tokio::task::spawn_blocking(move || produce(paths, config, tx));

        let committed = commit(Arc::clone(&self), &mut rx, concurrency, snapshot, files).await;

        let commit_busy = match committed {
            Ok(busy) => busy,
            Err(e) => {
                // Dropping the receiver makes the reader's next send fail, so it
                // stops reading rather than working through a tree nobody wants.
                drop(rx);
                let _ = reader.await;
                return Err(e);
            }
        };

        // The reader's own error, checked before the snapshot is trusted: it knows
        // why a file's contents could not be read, and says so better than the
        // length mismatch one would infer from it.
        let reader_blocked = reader
            .await
            .map_err(|join| io::Error::other(format!("the reader thread failed: {join}")))??;

        // `reader_blocked_ms` is how far ahead the reader got, not how slow the
        // stores were: a reader faster than the committer fills a bounded channel
        // and blocks on it repeatedly. The wall clock settles which it was.
        tracing::debug!(
            reader_blocked_ms = millis(reader_blocked),
            committer_ms = millis(commit_busy),
            "pipeline timings; a large reader_blocked_ms means the reader ran ahead \
             of the committer, which is usually but not always the stores being slow"
        );

        Ok(())
    }
}

/// Drive the stores and fold their results into `snapshot`.
///
/// Results are committed strictly in the order the chunks were read, so the
/// snapshot a run produces does not depend on how the stores interleaved. That
/// is what lets [`Options::concurrency`](crate::command::Options::concurrency) be
/// tuned freely.
async fn commit<S: storage::StoragePut + 'static>(
    pipeline: Arc<Pipeline<S>>,
    rx: &mut mpsc::Receiver<Pending>,
    concurrency: usize,
    snapshot: &mut metadata::Snapshot,
    files: u64,
) -> io::Result<Duration> {
    let mut commit_busy = Duration::ZERO;

    let mut in_flight = JoinSet::new();

    // Results that finished before the one they follow, keyed by read order.
    // Bounded by the number of stores running, so it cannot grow with the size
    // of the tree.
    let mut ready: BTreeMap<u64, Stored> = BTreeMap::new();
    let mut next_seq: u64 = 0;

    // Which chunk index each distinct content was given, so a later chunk with
    // the same content reuses it instead of being stored again.
    let mut index_of: HashMap<[u8; 32], u32> = HashMap::new();

    // The committer sees every chunk exactly once, so it is the one place that
    // can count them. The file count comes from the walk and is the only thing
    // known before any content has been read, so it is what the opening line
    // says.
    let mut progress = Progress::new("backup");
    progress.set("files", files);
    progress.report();

    let mut producer_finished = false;

    loop {
        // Keep as many stores running as were asked for.
        while !producer_finished && in_flight.len() < concurrency {
            let Some(pending) = rx.recv().await else {
                producer_finished = true;
                break;
            };

            progress.add("chunks", 1);
            progress.add("read", u64::try_from(pending.data.len()).unwrap_or(0));
            progress.report();

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

                let folding = Instant::now();
                commit_ready(
                    &pipeline,
                    &mut ready,
                    &mut next_seq,
                    &mut index_of,
                    snapshot,
                )?;
                commit_busy += folding.elapsed();

                // Distinct content is what costs storage, so it is the number
                // worth watching on a tree that deduplicates well.
                progress.set(
                    "distinct",
                    u64::try_from(snapshot.chunks.len()).unwrap_or(0),
                );
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

    // A result left over would mean a chunk was stored but never referenced.
    if let Some(&seq) = ready.keys().next() {
        return Err(io::Error::other(format!(
            "chunk {seq} was stored but never committed"
        )));
    }

    // How long this half spent folding results in, for the timing line.
    Ok(commit_busy)
}

/// Whole milliseconds, for a log field.
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
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
        // Uniqueness is decided here, by read order, and nowhere else. Stores
        // finish out of order, so a first-arrival claim is not good enough: every
        // chunk of the same content carries the same hash, so whichever result
        // reaches this point first allocates the index and the rest reuse it —
        // the same answer a sequential run gives, and the same one every time.
        let chunk_index = if let Some(&existing) = index_of.get(&stored.hash) {
            existing
        } else {
            let index = index_to_u32(snapshot.chunks.len())?;
            snapshot.chunks.push(stored.hash);
            index_of.insert(stored.hash, index);
            index
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
        Arc::clone(map.entry(hash).or_insert_with(|| Arc::new(OnceCell::new())))
    };

    // The store decides whether this content is already held, so a chunk seen in
    // *any* earlier version costs nothing: incremental backup is "nothing new to
    // store", however far back the content was last seen.
    slot.get_or_try_init(|| async {
        let len = u64::try_from(data.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "chunk of {} bytes does not fit in a blob length",
                    data.len()
                ),
            )
        })?;

        pipeline
            .storage
            .put(&hash, Box::new(Cursor::new(data)), len)
            .await
    })
    .await?;

    Ok(Stored { seq, hash, sources })
}
