//! The sets this server keeps, and the handles onto them.
//!
//! # Two problems, and they are not the same kind
//!
//! **Writing to a set needs a mutex, and not for politeness.** `BlobStore` holds
//! `Mutex<Reservation>` — where the next record goes in the last segment — inside
//! the handle. So two handles onto one set each believe they own the append
//! offset, and two concurrent merges would interleave records into the same
//! segment at the same place.
//!
//! `IndexSet::lock()` does not save it. It returns `AlreadyExists` rather than
//! blocking, so the second merge fails with "already locked" — confusing, and
//! wrong in a different way: it reports contention as an error about the world
//! rather than waiting for its turn.
//!
//! So writes take an in-process async mutex **first**, and the file lock second.
//! The file lock still does its job against another process and against
//! `lepatch merge` on the same set; the async mutex serialises this process's
//! own jobs.
//!
//! **Reading does not need a mutex, but it does need invalidation.**
//! `open_for_reading` runs a scan over every record header in every segment. That
//! is O(set), per request, which is the wrong shape for a server — so reads share
//! a handle. But a shared handle's `contents` map is built once at open and never
//! refreshed, so a handle cached across a merge would answer a version listing
//! that silently omits the version that was just pushed.
//!
//! [`Sets::evict`] is therefore called when a merge finishes. That is the one
//! place in this file where a missing call is a wrong answer rather than a slow
//! one, so `tests/serve_push.rs` pushes and then immediately lists on the same
//! server.

use std::{
    collections::{HashMap, HashSet},
    io,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
};

use tokio::sync::Mutex as AsyncMutex;

use crate::{
    layout::IndexSet,
    storage::{BlobStore, Compression, Read, Write},
};

/// Every set under a root, with a handle onto each.
///
/// Cloned into every request as `web::Data`. The inner maps are behind a
/// [`Mutex`] whose critical section is a hash lookup and an `Arc` clone, so the
/// lock is held for no I/O at all.
#[derive(Debug)]
pub struct Sets {
    /// The directory holding the sets.
    root: PathBuf,
    /// Write locks, one per set. Each holds a token rather than a store: the
    /// store is opened under the lock and dropped at the end of the merge, so
    /// two handles onto one set can never coexist.
    writes: Mutex<HashMap<String, Arc<AsyncMutex<()>>>>,
    /// Read handles, cached and evicted on merge.
    reads: Mutex<HashMap<String, Arc<BlobStore<Read, Compression>>>>,
}

impl Sets {
    /// A registry of the sets under `root`.
    #[must_use]
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            writes: Mutex::new(HashMap::new()),
            reads: Mutex::new(HashMap::new()),
        }
    }

    /// The set's base path, from a name off the wire.
    ///
    /// # Errors
    ///
    /// Returns an error if the name is empty, if it is anything but a single
    /// normal path component, or if the result would leave `root`.
    ///
    /// **This is the only thing standing between a request and the filesystem.**
    /// A set name arrives from a client and becomes a path that segments are
    /// named from, so `../../etc` must not survive it. Three checks, and each
    /// catches something the others do not: emptiness would name the root
    /// itself; a separator or a `..` would name something else; and the final
    /// containment check catches whatever combination the first two let through.
    ///
    /// The check is on the *name*, before the path is joined, so there is no
    /// window in which an unchecked name is on disk.
    pub fn base(&self, name: &str) -> io::Result<PathBuf> {
        if name.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a set name may not be empty",
            ));
        }

        let candidate = Path::new(name);
        let mut components = candidate.components();
        let only = components.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "a set name may not be empty")
        })?;

        if components.next().is_some() || only != Component::Normal(name.as_ref()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidFilename,
                format!("{name:?} is not a single name; a set is named by one path component"),
            ));
        }

        let base = self.root.join(name);

        // Belt and braces, and it is the only one of the three that compares
        // absolute against absolute. The two above are about the name's shape;
        // this one is about where the name lands.
        if !base.starts_with(&self.root) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("{name:?} names something outside {}", self.root.display()),
            ));
        }

        Ok(base)
    }

    /// The sets under the root, with what each one holds.
    ///
    /// A name that is not a set is not listed. So a stray file beside the sets
    /// does not turn into a `404` later on; it simply is not there, which is what
    /// a directory listing means.
    ///
    /// # Errors
    ///
    /// Returns an error if the root cannot be read.
    pub fn list(&self) -> io::Result<Vec<(String, usize, usize)>> {
        let mut out = Vec::new();

        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => entries,
            // A root that does not exist yet holds no sets. That is the state a
            // server starts in, so it is not an error and not a `404`.
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e),
        };

        // Deduplicated, because a set is named by its base and a directory holds
        // one file per segment. `docs` and `docs.0000.bin` are the same set —
        // `IndexSet` says so on purpose, because `photos.0000.bin` is the name a
        // person copies — so without this a set with three segments would be
        // listed three times.
        let mut seen: HashSet<String> = HashSet::new();

        for entry in entries {
            let entry = entry?;

            let Some(raw) = entry.file_name().to_str().map(str::to_string) else {
                continue;
            };

            // **`IndexSet` normalises the name**, which is what turns a segment's
            // file name back into the set's base. Taking the entry's name at face
            // value would list `docs.0000.bin` as a set called that, and every
            // request for it would then resolve to a different base than the one
            // the listing named.
            let set = IndexSet::new(self.root.join(&raw));
            let Some(name) = set.base().file_name().and_then(|name| name.to_str()) else {
                continue;
            };

            // Off the filesystem rather than off the wire, but the same rule: a
            // name the registry would refuse to serve is not listed.
            if self.base(name).is_err() || !seen.insert(name.to_string()) {
                continue;
            }

            let segments = set.segments()?;

            // No segments at all is a mistyped name, not an empty set. This is
            // the distinction `lepatch versions` draws, for the same reason:
            // answering "no versions" about a name that names nothing is a failure
            // a client acts on.
            if segments.is_empty() {
                continue;
            }

            // The index files, which is a lower bound — the caveat
            // `lepatch versions` carries, for the same reason. A version whose
            // index was never written still exists and still restores.
            let versions = set.indexed_versions()?.len();

            out.push((name.to_string(), versions, segments.len()));
        }

        out.sort();

        Ok(out)
    }

    /// A read handle for a set, opened on first use and then shared.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is not a usable set name, or if the set cannot
    /// be opened — which includes a name that names nothing, and a set that is
    /// damaged.
    pub async fn reader(&self, name: &str) -> io::Result<Arc<BlobStore<Read, Compression>>> {
        let key = name.to_string();

        if let Some(existing) = Self::locked(&self.reads, "reader")?.get(&key).cloned() {
            return Ok(existing);
        }

        let base = self.base(name)?;
        let store = Arc::new(BlobStore::open_for_reading(base).await?);

        let mut map = Self::locked(&self.reads, "reader")?;
        // Re-checked under the lock. Two requests for a set that is not yet
        // cached both open it, and the second open would double the scan. Taking
        // whichever landed first costs one lost handle.
        let store = map.entry(key).or_insert(store).clone();

        Ok(store)
    }

    /// The write lock for a set, held for the length of a merge.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is not a usable set name, or if the set cannot
    /// be opened.
    ///
    /// The handle is opened **while the mutex is held**, every time, and then
    /// dropped. That is deliberate, and it is cheaper than it sounds: the
    /// expensive part of `open_for_writing` is one scan, and a merge is one
    /// request. Caching a writable handle would trade a correctness argument for
    /// a scan that happens once per merge rather than once per request, and the
    /// correctness argument is the one this file exists to make:
    /// `open_for_writing` takes its reservation from the end of the last valid
    /// record, so two handles for the same set each believe they own the append
    /// offset and would interleave records into the same segment.
    ///
    /// Holding the mutex across the open is what makes "one handle at a time" a
    /// fact rather than an intention.
    pub async fn writing(&self, name: &str) -> io::Result<WriteGuard> {
        let key = name.to_string();
        let base = self.base(name)?;

        let entry = {
            let mut map = Self::locked(&self.writes, "writing")?;
            map.entry(key)
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone()
        };

        // Held for the whole merge, including the open. The file lock on the set
        // is taken by the caller inside this, against other processes.
        let held = Arc::clone(&entry).lock_owned().await;
        let store = Arc::new(
            BlobStore::open_for_writing(
                base,
                crate::storage::SegmentLimit::default(),
                Compression::default(),
            )
            .await?,
        );

        Ok(WriteGuard { store, _held: held })
    }

    /// Drop a set's cached read handle.
    ///
    /// Called after a merge publishes a version. Without it, the next listing
    /// served from the cache would answer from a `contents` map built before the
    /// push, and would omit the version the push created — a wrong answer, not a
    /// slow one.
    pub fn evict(&self, name: &str) {
        if let Ok(mut map) = self.reads.lock() {
            map.remove(name);
        }
    }

    /// Lock a map, turning a poisoned mutex into the error it deserves.
    ///
    /// A panic while one of these was held poisons it, and every later lock
    /// would fail. The map has no invariant that a panic could have broken — it
    /// is a cache and a set of handles — so the poison is recovered from rather
    /// than propagated as a failure to serve.
    fn locked<'a, T>(
        map: &'a Mutex<HashMap<String, T>>,
        what: &str,
    ) -> io::Result<std::sync::MutexGuard<'a, HashMap<String, T>>> {
        map.lock().map_err(|_| {
            io::Error::other(format!("the {what} lock was poisoned by an earlier panic"))
        })
    }
}

/// A set's write handle, and the lock that makes it the only one.
///
/// Holding this is what serialises merges into one set. Both halves matter and
/// they do different jobs. The `_held` guard keeps other merges in this process
/// out; the caller takes the set's own file lock inside, which keeps other
/// processes — and a `lepatch merge` on the same set — out.
#[derive(Debug)]
pub struct WriteGuard {
    /// The store to merge into.
    store: Arc<BlobStore<Write, Compression>>,
    /// The per-set lock, held for as long as this guard is.
    _held: tokio::sync::OwnedMutexGuard<()>,
}

impl WriteGuard {
    /// The store.
    #[must_use]
    pub fn store(&self) -> &Arc<BlobStore<Write, Compression>> {
        &self.store
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::Sets;

    fn registry() -> Sets {
        Sets::new(PathBuf::from("/srv/sets"))
    }

    #[test]
    fn an_ordinary_name_is_a_set() {
        // The whole normal case, including a name that looks odd but is legal:
        // the store names its files `<base>.<n>.bin`, so a dot is ordinary.
        let sets = registry();

        assert_eq!(sets.base("docs").unwrap(), Path::new("/srv/sets/docs"));
        assert_eq!(
            sets.base("docs.tar").unwrap(),
            Path::new("/srv/sets/docs.tar")
        );
        assert_eq!(
            sets.base("v2.0000.bin").unwrap(),
            Path::new("/srv/sets/v2.0000.bin")
        );
    }

    #[test]
    fn a_name_that_climbs_out_is_refused() {
        // The reason this function exists. A set name becomes the prefix its
        // segments are named from, so anything that is not one component is
        // either another set or a directory above the root.
        let sets = registry();

        for name in ["..", "../etc", "a/b", "/etc", "./docs", "a/../../b"] {
            assert!(sets.base(name).is_err(), "{name:?} must not name a set");
        }
    }

    #[test]
    fn a_climbing_name_says_what_it_wanted() {
        // An operator reading a `400` should learn the rule, not just that
        // something was wrong.
        let error = registry().base("../etc").unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidFilename);
        assert!(
            error.to_string().contains("single name"),
            "the message should name the rule: {error}"
        );
    }

    #[test]
    fn an_empty_name_is_refused_before_the_shape_check() {
        // A distinct case from a malformed one, and a distinct message. An empty
        // name is not "not a single component"; it is no name at all.
        let error = registry().base("").unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("empty"));
    }

    #[test]
    fn a_climbing_name_never_lands_inside_the_root() {
        // Stated as a property over every name, because the interesting failure
        // is the one nobody thought of.
        let sets = registry();

        for name in ["..", "....", "a/b", "a\\b", "x/../y"] {
            if let Ok(base) = sets.base(name) {
                assert!(
                    base.starts_with("/srv/sets"),
                    "{name:?} produced {base:?}, which is outside the root"
                );
                // And exactly one component below it, never the root itself.
                assert_eq!(
                    base.strip_prefix("/srv/sets").unwrap().components().count(),
                    1,
                    "{name:?} produced more than one component below the root"
                );
            }
        }
    }

    #[test]
    fn a_root_that_does_not_exist_yet_holds_no_sets() {
        // The state a server starts in. It is not an error and not a `404`, and
        // treating it as either would make a fresh `--root` unusable.
        let sets = Sets::new(PathBuf::from("/nonexistent/lepatch-root"));

        assert_eq!(sets.list().unwrap().len(), 0);
    }

    #[test]
    fn a_name_that_names_nothing_is_not_listed() {
        // A stray file beside the sets. Listing it would make it a `404` later,
        // which is a different claim from "there is no set called that".
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("notes.txt"), b"not a set").unwrap();

        let sets = Sets::new(dir.path().to_path_buf());

        assert_eq!(sets.list().unwrap().len(), 0);
    }
}
