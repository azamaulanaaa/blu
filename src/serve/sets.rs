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
//! one, so `tests/serve.rs` pushes and then immediately lists on the same
//! server.
//!
//! ## One store, one key
//!
//! **`docs` and `docs.0000.bin` are one set, and this registry keys them as
//! one.** [`layout::IndexSet`] cuts a segment tail off a base before anything
//! opens a store, so both spellings resolve to the same directory — and a
//! registry that keyed its locks and its read cache by the *raw* name would hold
//! two entries for one store. A merge through one spelling then evicted one
//! cache entry and left the other holding a `BlobStore` whose `contents` map
//! predated the commit, so a listing through that spelling read its version
//! numbers fresh off disk, failed to resolve the newest one against the stale
//! map, **skipped it**, and reported a set that had just lost a version.
//!
//! So [`Sets::base`] normalises, once, at the boundary, and every key this
//! registry keeps is derived from what it returns. `tests/serve.rs` merges and
//! then lists through both spellings, in both orders.

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
    /// **The name is normalised here, once.** `docs.0000.bin` is a spelling of
    /// `docs`, and it resolves to the same path — through
    /// [`IndexSet::new`], which is what every other part of the crate does with
    /// a set's name, and through [`Sets::list`], which already lists the base and
    /// not the segment file beside it. Accepting the tail rather than refusing
    /// it is the same trade [`IndexSet::new`] records: `photos.0000.bin` is the
    /// name a directory listing shows and a person copies, so it has to work
    /// wherever a set is named. The cost is that a set genuinely named
    /// `report.2024.bin` cannot be addressed, which is the cost the CLI already
    /// pays for the same name.
    ///
    /// Every key in this registry comes from what this returns, so both
    /// spellings share one write lock and one cache entry. See the module docs.
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
                format!("{name:?} is not a single name. A set is named by one path component"),
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

        // The one normaliser, and the only place this crate asks for one. A name
        // shaped like one of the set's own segments is unwrapped here, so what
        // leaves this function is the set rather than a spelling of it.
        Ok(IndexSet::new(base).base().to_path_buf())
    }

    /// The registry's key for `name`, and the base path it resolves to.
    ///
    /// **The key is the base's own file name, never the name as it arrived.**
    /// `docs` and `docs.0000.bin` are one store on disk, so they have to be one
    /// entry in the lock map and one entry in the read cache. Keyed by the raw
    /// name they were two of each, and the second merge into one spelling
    /// invalidated one cache entry and left the other serving a `contents` map
    /// built before the commit — which is how a listing came to omit the version
    /// that had just been published.
    ///
    /// # Errors
    ///
    /// Returns whatever [`Sets::base`] returns, plus an error for a base whose
    /// last component is not a UTF-8 name. A name off the wire is a `&str`, and
    /// normalising only ever cuts a tail off the end of one, so that arm is a
    /// shape this function cannot produce; it is here because this function
    /// returns a `String` and the conversion has to be a checked one.
    fn resolve(&self, name: &str) -> io::Result<(String, PathBuf)> {
        let base = self.base(name)?;

        let key = base
            .file_name()
            .and_then(|key| key.to_str())
            .map(str::to_string)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidFilename,
                    format!("{name:?} does not name a set this registry can key"),
                )
            })?;

        Ok((key, base))
    }

    /// The sets under the root, with what each one holds.
    ///
    /// A name that is not a set is not listed. So a stray file beside the sets
    /// does not turn into a `404` later on; it simply is not there, which is what
    /// a directory listing means.
    ///
    /// **Only under a name a request can be answered for.** Every listed name is
    /// a fixed point of the normalisation [`Sets::base`] applies, so the store a
    /// listing names and the store a request reaches are the same directory. A
    /// set whose own base is shaped like one of its segments — `report.2024.bin`
    /// — is addressable only through that clash, and is not listed at all; see
    /// the fixed-point check in the body.
    ///
    /// # Errors
    ///
    /// Returns an error if the root cannot be read.
    ///
    /// Returns an error if an entry of that directory cannot be read, or if the
    /// segments or index files of a listed set cannot be read. A set whose own
    /// figures cannot be read is not skipped: it is reported.
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
            let Ok(resolved) = self.base(name) else {
                continue;
            };

            // **And the name has to be a fixed point of the normalisation.**
            // `self.base` cuts a segment-shaped tail off a *second* time, so a
            // set whose own base is segment-shaped does not survive it:
            // `report.2024.bin`, whose files are `report.2024.bin.0000.bin`, is
            // listed under `report.2024.bin` and every request for that name
            // resolves to `<root>/report` — a different store, and a merge into
            // the wrong one. A name that cannot be requested back is not listed.
            //
            // No write in this build can produce one, because every write goes
            // through `IndexSet`; the realistic route is a root created or copied
            // from outside, which is the state the test builds.
            if resolved != set.base() || !seen.insert(name.to_string()) {
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
    ///
    /// Returns an error if the read cache's lock was poisoned by an earlier
    /// panic.
    pub async fn reader(&self, name: &str) -> io::Result<Arc<BlobStore<Read, Compression>>> {
        // Resolved before the lookup, so a name the registry would refuse is
        // refused rather than half-served, and so the key it caches under is the
        // set rather than a spelling of it.
        let (key, base) = self.resolve(name)?;

        if let Some(existing) = Self::locked(&self.reads, "reader")?.get(&key).cloned() {
            return Ok(existing);
        }

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
    /// **`compression` is the caller's, and it is a parameter rather than a
    /// default for that reason.** The store is what frames each record, so a
    /// merge that re-encodes chunks and a set opened under the wrong policy
    /// produce records at a level nobody asked for. The level a client sent is
    /// the level the set has to be opened at, and the only place that open
    /// happens is here. See [`crate::command::MergeCompression`], which says the
    /// same thing from the other end: a caller asking for a level opens the set
    /// with it.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is not a usable set name, or if the set cannot
    /// be opened.
    ///
    /// Returns an error if the write lock's lock was poisoned by an earlier
    /// panic.
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
    ///
    /// **`work` is a closure, and that is not a style choice.** This used to
    /// return a `WriteGuard`, which the caller held for as long as it wanted —
    /// so a caller could drop it early and let a second merge in, or keep it
    /// alive past the merge and block one. Both are the bug this lock exists to
    /// prevent, and both were possible. A closure makes the extent of the lock
    /// the closure's own body, so the only way to get the scope wrong is to move
    /// the call.
    pub async fn writing<F, Fut, T>(
        &self,
        name: &str,
        compression: Compression,
        work: F,
    ) -> io::Result<T>
    where
        F: FnOnce(Arc<BlobStore<Write, Compression>>) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let (key, base) = self.resolve(name)?;

        let entry = {
            let mut map = Self::locked(&self.writes, "writing")?;
            map.entry(key)
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone()
        };

        // Held across the open, and across `work`. The file lock on the set is
        // taken by `work`, against other processes.
        let _held = Arc::clone(&entry).lock_owned().await;

        let store = Arc::new(
            BlobStore::open_for_writing(base, crate::storage::SegmentLimit::default(), compression)
                .await?,
        );

        Ok(work(store).await)
    }

    /// Drop a set's cached read handle.
    ///
    /// Called after a merge publishes a version. Without it, the next listing
    /// served from the cache would answer from a `contents` map built before the
    /// push, and would omit the version the push created — a wrong answer, not a
    /// slow one.
    ///
    /// **The name is resolved the same way [`Sets::reader`] keys it**, so a merge
    /// through `docs.0000.bin` drops the entry a listing through `docs` reads.
    /// Evicting by the name as it arrived is what left a stale store behind the
    /// first time: the entry existed under both spellings and only one of them
    /// was dropped.
    ///
    /// A name that resolves to nothing is not an error here. This is called
    /// after the merge has already succeeded, so there is nothing left to fail
    /// and no set for the caller to be told about.
    pub fn evict(&self, name: &str) {
        let Ok((key, _)) = self.resolve(name) else {
            return;
        };

        if let Ok(mut map) = self.reads.lock() {
            map.remove(&key);
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
        map.lock()
            .map_err(|_| io::Error::other(format!("an earlier panic poisoned the {what} lock")))
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
        // A dot is ordinary; four digits and `.bin` after it is a segment file,
        // and that is a spelling of a base rather than a base of its own.
        assert_eq!(
            sets.base("docs.0000.bin").unwrap(),
            Path::new("/srv/sets/docs")
        );
        // Including one that names nothing on disk, because the tail is cut on
        // shape. A set that does not exist yet resolves like one that does.
        assert_eq!(sets.base("v2.0000.bin").unwrap(), Path::new("/srv/sets/v2"));
    }

    #[test]
    fn both_spellings_of_a_set_are_one_store_and_one_key() {
        // **The defect this file's second half is about.** `docs` and
        // `docs.0000.bin` are one directory, so a registry that keyed by the name
        // as it arrived held two write locks and two read handles onto it — and a
        // merge through one evicted one of the two. The listing then answered
        // from a `contents` map built before the commit and omitted the version
        // the merge had just published.
        let sets = registry();

        assert_eq!(
            sets.resolve("docs").unwrap(),
            sets.resolve("docs.0000.bin").unwrap()
        );

        // And the segment's own number makes no difference to the identity,
        // since no store has more than one segment zero.
        assert_eq!(
            sets.resolve("docs.0012.bin").unwrap(),
            sets.resolve("docs").unwrap()
        );

        // A name that is not shaped like a segment keeps its own identity.
        assert_ne!(
            sets.resolve("docs.tar").unwrap().0,
            sets.resolve("docs").unwrap().0
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

    /// Put one empty segment file for a set whose base is `base`, by hand.
    ///
    /// **The files are written directly rather than through a merge, because
    /// that is the only way to reach a set whose *own base* is shaped like a
    /// segment.** Every write in this crate goes through `IndexSet`, which strips
    /// such a tail off before anything is created, so a merge cannot produce one.
    /// The reachable route is a root copied in from outside.
    ///
    /// A segment lives *beside* its base rather than inside a directory named
    /// after it — [`IndexSet::segments`] reads the base's own parent — so this
    /// writes `<base>.0000.bin` into the root itself.
    fn store_with_one_segment(root: &Path, base: &str) {
        std::fs::write(root.join(format!("{base}.0000.bin")), b"").unwrap();
    }

    #[test]
    fn a_set_whose_own_base_is_shaped_like_a_segment_is_not_listed() {
        // **The listing named a store no request could reach.** `report.2024.bin`
        // is the base, and its segment is `report.2024.bin.0000.bin`. The listing
        // takes the name off `IndexSet`, which strips one tail and stops — so the
        // set is listed as `report.2024.bin`. But [`Sets::base`] strips a tail
        // again on the way in, and `.2024.bin` is shaped exactly like one, so a
        // request for `report.2024.bin` resolves to `<root>/report`. Listing it
        // would be advertising a set under a name that merges into a different
        // store.
        let dir = tempfile::tempdir().unwrap();
        store_with_one_segment(dir.path(), "report.2024.bin");

        let sets = Sets::new(dir.path().to_path_buf());

        assert!(
            sets.list().unwrap().is_empty(),
            "a set that cannot be requested back must not be listed"
        );

        // Stated the other way round as well, because the two directions are
        // different code: the name is refused *and* the path it would reach is
        // somebody else's.
        assert_eq!(
            sets.base("report.2024.bin").unwrap(),
            dir.path().join("report"),
            "the name resolves to a different store, which is why it is unlisted"
        );
        assert_ne!(
            sets.base("report.2024.bin").unwrap(),
            dir.path().join("report.2024.bin")
        );
    }

    #[test]
    fn an_ordinary_set_beside_an_unaddressable_one_is_still_listed() {
        // The check is per set, not a bail-out: one unclashable set in a root
        // must not take the rest of the listing with it. `photos.2024` is here
        // because it is the *near* miss — a dotted name that is not shaped like a
        // segment, and so is addressable.
        let dir = tempfile::tempdir().unwrap();
        store_with_one_segment(dir.path(), "report.2024.bin");
        store_with_one_segment(dir.path(), "docs");
        store_with_one_segment(dir.path(), "photos.2024");

        let listed = Sets::new(dir.path().to_path_buf()).list().unwrap();

        let names: Vec<&str> = listed.iter().map(|(name, _, _)| name.as_str()).collect();
        assert_eq!(names, ["docs", "photos.2024"]);
    }
}
