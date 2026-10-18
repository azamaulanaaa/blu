// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! A byte-budgeted cache of whole chunks in front of a store.
//!
//! A store opens one chunk per [`StorageGet::get`] call.
//! A caller that names one chunk in several slices pays several reads.
//! This cache reads one missing chunk whole.
//! It keeps the bytes under the chunk's identity.
//! It serves every later request for that chunk from memory.
//!
//! # One read per chunk
//!
//! Two requests can name one missing chunk at the same time.
//! The first request then reads the store.
//! The other requests wait for those bytes.
//! One store read serves all of them.
//!
//! # Budget
//!
//! The cache holds at most a configured number of chunk bytes.
//! Every request moves its chunk to the most recent position.
//! A new chunk that does not fit drops the oldest chunks.
//! A chunk larger than the whole budget is not kept.
//! A budget of zero keeps nothing.
//! Every read then goes to the store.

use std::{
    collections::{BTreeMap, HashMap},
    io,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use bytes::Bytes;
use tokio::sync::OnceCell;

use super::{BlobId, StorageGet, StreamReadSeeker};

/// Bytes a [`ChunkCache::new`] cache holds: 64 MiB.
pub const DEFAULT_CACHE_BYTES: usize = 64 * 1024 * 1024;

/// A cache of whole chunk bytes in front of a store.
///
/// See the module documentation for the budget rules and for the one-read rule
/// on concurrent requests.
///
/// The cache is safe to share between tasks.
/// A shared cache lets concurrent requests for one chunk share one read.
#[derive(Debug)]
pub struct ChunkCache<S> {
    /// The store every missing chunk comes from.
    inner: S,
    /// The number of chunk bytes the cache may hold.
    budget: usize,
    /// The entries, the recency order, and the requests in flight.
    state: Mutex<State>,
}

impl<S> ChunkCache<S> {
    /// Wrap `store` with a cache of [`DEFAULT_CACHE_BYTES`] bytes.
    #[must_use]
    pub fn new(store: S) -> Self {
        Self::with_bytes(store, DEFAULT_CACHE_BYTES)
    }

    /// Wrap `store` with a cache of at most `bytes` bytes.
    ///
    /// A budget of zero keeps nothing.
    /// Every read then goes to the store.
    #[must_use]
    pub fn with_bytes(store: S, bytes: usize) -> Self {
        Self {
            inner: store,
            budget: bytes,
            state: Mutex::new(State::default()),
        }
    }
}

impl<S: StorageGet> ChunkCache<S> {
    /// Read one whole chunk from the store.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot open the chunk, or if the read
    /// fails.
    async fn read(&self, id: &BlobId) -> io::Result<Bytes> {
        use tokio::io::AsyncReadExt as _;

        let mut reader = self.inner.get(id).await?;
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await?;
        Ok(Bytes::from(bytes))
    }
}

impl<S: StorageGet> StorageGet for ChunkCache<S> {
    fn get(&self, id: &BlobId) -> impl Future<Output = io::Result<StreamReadSeeker>> + Send {
        let id = *id;

        async move {
            if self.budget == 0 {
                return self.inner.get(&id).await;
            }

            let flight = {
                let mut state = lock(&self.state);
                if let Some(bytes) = state.take(&id) {
                    return Ok(stream(bytes));
                }
                state.flight(&id)
            };

            let outcome = match flight
                .get_or_try_init(|| async { self.read(&id).await })
                .await
            {
                Ok(bytes) => Ok(bytes.clone()),
                Err(error) => Err(error),
            };

            lock(&self.state).finish(&id, &flight, self.budget, outcome.as_ref().ok());

            outcome.map(stream)
        }
    }
}

/// A stream over every byte of `bytes`, from its start.
fn stream(bytes: Bytes) -> StreamReadSeeker {
    Box::new(io::Cursor::new(bytes))
}

/// Lock the cache state, ignoring poisoning: the code under it only moves map
/// entries, which cannot panic.
fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The entries, the recency order, and the requests in flight.
#[derive(Debug, Default)]
struct State {
    /// The bytes held, by chunk identity.
    entries: HashMap<BlobId, Entry>,
    /// The recency order: the number of a use, by chunk identity.
    order: BTreeMap<u64, BlobId>,
    /// The requests in flight, by chunk identity.
    flights: HashMap<BlobId, Flight>,
    /// The number of the most recent use.
    seq: u64,
    /// The bytes the entries hold in total.
    used: usize,
}

/// One held chunk: its bytes and the number of its most recent use.
#[derive(Debug)]
struct Entry {
    /// The chunk's bytes.
    bytes: Bytes,
    /// The number of the most recent use.
    seq: u64,
}

/// One read of one missing chunk, shared by every request that waits for it.
type Flight = Arc<OnceCell<Bytes>>;

impl State {
    /// Take the bytes for `id` when they are held, and mark them most recent.
    fn take(&mut self, id: &BlobId) -> Option<Bytes> {
        self.seq += 1;
        let newest = self.seq;

        let entry = self.entries.get_mut(id)?;
        self.order.remove(&entry.seq);
        entry.seq = newest;
        self.order.insert(newest, *id);
        Some(entry.bytes.clone())
    }

    /// The request in flight for `id`, started when none runs.
    fn flight(&mut self, id: &BlobId) -> Flight {
        Arc::clone(self.flights.entry(*id).or_default())
    }

    /// End the request in flight for `id`, and keep its bytes when they fit.
    ///
    /// The flight is closed only when it is still the one in the map.
    /// A failed read leaves its cell empty.
    /// A request for the same chunk can then start its own read.
    fn finish(&mut self, id: &BlobId, flight: &Flight, budget: usize, bytes: Option<&Bytes>) {
        if self
            .flights
            .get(id)
            .is_some_and(|held| Arc::ptr_eq(held, flight))
        {
            self.flights.remove(id);
        }
        if let Some(bytes) = bytes {
            self.keep(id, budget, bytes);
        }
    }

    /// Hold `bytes` under `id`, and drop the oldest chunks until they fit.
    ///
    /// A chunk larger than the whole budget is not held.
    fn keep(&mut self, id: &BlobId, budget: usize, bytes: &Bytes) {
        if bytes.len() > budget {
            return;
        }

        self.seq += 1;
        if let Some(old) = self.entries.insert(
            *id,
            Entry {
                bytes: bytes.clone(),
                seq: self.seq,
            },
        ) {
            self.used = self.used.saturating_sub(old.bytes.len());
            self.order.remove(&old.seq);
        }
        self.order.insert(self.seq, *id);
        self.used = self.used.saturating_add(bytes.len());

        while self.used > budget {
            let Some((_, oldest)) = self.order.pop_first() else {
                break;
            };
            if let Some(entry) = self.entries.remove(&oldest) {
                self.used = self.used.saturating_sub(entry.bytes.len());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        io::{self, Cursor},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use tokio::{io::AsyncReadExt as _, sync::Notify};

    use super::{ChunkCache, DEFAULT_CACHE_BYTES};
    use crate::storage::{BlobId, StorageGet, StreamReadSeeker};

    /// One chunk of `len` bytes, all set to `seed`, and its identity.
    fn chunk(seed: u8, len: usize) -> (BlobId, Vec<u8>) {
        let bytes = vec![seed; len];
        (blake3::hash(&bytes), bytes)
    }

    /// A store of fixed chunks that counts every read.
    ///
    /// A holding store waits inside every read until [`Spy::release`].
    /// That holds two requests open at one time, which the one-read rule needs.
    struct Spy {
        /// The chunks the store holds, by identity.
        blobs: HashMap<BlobId, Vec<u8>>,
        /// How many reads the store was asked for.
        reads: AtomicUsize,
        /// Signals that a read has started.
        started: Notify,
        /// Holds every read until the test releases it.
        gate: Option<Notify>,
    }

    impl Spy {
        /// A store that answers at once.
        fn new(blobs: Vec<(BlobId, Vec<u8>)>) -> Self {
            Self {
                blobs: blobs.into_iter().collect(),
                reads: AtomicUsize::new(0),
                started: Notify::new(),
                gate: None,
            }
        }

        /// A store that waits inside every read until [`Spy::release`].
        fn holding(blobs: Vec<(BlobId, Vec<u8>)>) -> Self {
            Self {
                gate: Some(Notify::new()),
                ..Self::new(blobs)
            }
        }

        /// Let one held read finish.
        fn release(&self) {
            if let Some(gate) = &self.gate {
                gate.notify_one();
            }
        }

        /// How many reads the store was asked for.
        fn reads(&self) -> usize {
            self.reads.load(Ordering::SeqCst)
        }
    }

    impl StorageGet for Spy {
        fn get(&self, id: &BlobId) -> impl Future<Output = io::Result<StreamReadSeeker>> + Send {
            self.reads.fetch_add(1, Ordering::SeqCst);

            async move {
                self.started.notify_one();
                if let Some(gate) = &self.gate {
                    gate.notified().await;
                }

                let bytes = self.blobs.get(id).cloned().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "the spy holds no such chunk")
                })?;

                Ok(Box::new(Cursor::new(bytes)) as StreamReadSeeker)
            }
        }
    }

    /// Read a whole stream into a vector.
    async fn whole(mut stream: StreamReadSeeker) -> Vec<u8> {
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await.unwrap();
        bytes
    }

    #[test]
    fn the_default_budget_is_64_mib() {
        assert_eq!(DEFAULT_CACHE_BYTES, 64 * 1024 * 1024);
    }

    #[tokio::test]
    async fn a_repeated_chunk_is_read_once() {
        let (id, bytes) = chunk(1, 1024);
        let spy = Spy::new(vec![(id, bytes.clone())]);
        let cache = ChunkCache::new(&spy);

        let first = whole(cache.get(&id).await.unwrap()).await;
        let second = whole(cache.get(&id).await.unwrap()).await;

        assert_eq!(first, bytes);
        assert_eq!(second, bytes);
        assert_eq!(spy.reads(), 1);
    }

    #[tokio::test]
    async fn the_budget_evicts_the_least_recently_used_chunk() {
        let (a, a_bytes) = chunk(1, 1024);
        let (b, b_bytes) = chunk(2, 1024);
        let (c, c_bytes) = chunk(3, 1024);
        let spy = Spy::new(vec![(a, a_bytes), (b, b_bytes.clone()), (c, c_bytes)]);
        let cache = ChunkCache::with_bytes(&spy, 2 * 1024);

        cache.get(&a).await.unwrap();
        cache.get(&b).await.unwrap();
        cache.get(&a).await.unwrap();
        assert_eq!(spy.reads(), 2);

        // C fits only after the least recently used chunk leaves. That is B.
        cache.get(&c).await.unwrap();
        assert_eq!(spy.reads(), 3);

        // B left, so this reads the store again. A is still the most recent.
        assert_eq!(whole(cache.get(&b).await.unwrap()).await, b_bytes);
        assert_eq!(spy.reads(), 4);

        // C is still held, and B is now held in place of A.
        cache.get(&c).await.unwrap();
        cache.get(&b).await.unwrap();
        assert_eq!(spy.reads(), 4);
    }

    #[tokio::test]
    async fn a_chunk_larger_than_the_budget_is_not_kept() {
        let (id, bytes) = chunk(1, 2048);
        let spy = Spy::new(vec![(id, bytes.clone())]);
        let cache = ChunkCache::with_bytes(&spy, 1024);

        let first = whole(cache.get(&id).await.unwrap()).await;
        let second = whole(cache.get(&id).await.unwrap()).await;

        assert_eq!(first, bytes);
        assert_eq!(second, bytes);
        assert_eq!(spy.reads(), 2);
    }

    #[tokio::test]
    async fn a_zero_budget_passes_every_read_to_the_store() {
        let (id, bytes) = chunk(1, 1024);
        let spy = Spy::new(vec![(id, bytes)]);
        let cache = ChunkCache::with_bytes(&spy, 0);

        cache.get(&id).await.unwrap();
        cache.get(&id).await.unwrap();

        assert_eq!(spy.reads(), 2);
    }

    #[tokio::test]
    async fn two_concurrent_requests_read_the_store_once() {
        let (id, bytes) = chunk(1, 1024);
        let spy = Arc::new(Spy::holding(vec![(id, bytes.clone())]));
        let cache = Arc::new(ChunkCache::new(Arc::clone(&spy)));

        let first = tokio::spawn({
            let cache = Arc::clone(&cache);
            async move { whole(cache.get(&id).await.unwrap()).await }
        });

        // The first request is inside the store read and held there.
        spy.started.notified().await;

        let second = tokio::spawn({
            let cache = Arc::clone(&cache);
            async move { whole(cache.get(&id).await.unwrap()).await }
        });

        // Let the second request reach the flight before the first one ends.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        spy.release();

        assert_eq!(first.await.unwrap(), bytes);
        assert_eq!(second.await.unwrap(), bytes);
        assert_eq!(spy.reads(), 1);
    }
}
