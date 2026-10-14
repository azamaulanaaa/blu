// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Objects behind a key, for backends a file path cannot name.
//!
//! A local directory is one backend. Memory is another, for tests. Remote
//! services arrive later through this same seam. The store never scans a
//! remote backend directly. It scans local staging. The mirror in
//! [`crate::storage::mirror`] moves finished files between the two.
//!
//! Keys are UTF-8 file names. A remote service needs UTF-8 keys. A local set
//! with a non-UTF-8 base name cannot mirror. The local backend reports such a
//! name as an error. It never skips the name quietly.

use std::{
    collections::BTreeMap,
    ffi::{OsStr, OsString},
    fs, io,
    path::{Path, PathBuf},
    sync::{Mutex, PoisonError},
};

/// Copy buffer for streaming imports and exports.
///
/// Bounded and fixed. A large segment crosses in chunks of this size. Memory
/// use stays flat whatever the object holds.
const COPY_BUFFER: usize = 256 * 1024;

/// A keyed object backend.
///
/// Six operations:
///
/// - Listing keys.
/// - Measuring one key.
/// - Reading a bounded range.
/// - Writing small bytes.
/// - Copying a large file in.
/// - Copying a large file out.
///
/// Large copies stream through a fixed buffer. They never hold the whole
/// object in memory. Small writes take bytes.
///
/// All methods are synchronous. The mirror runs them on a blocking thread. A
/// future async backend keeps this shape and moves the wait inside.
pub trait ObjectStore: Send + Sync {
    /// Every key this backend holds, in order.
    ///
    /// Names only. The caller filters them with the set grammar.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing fails. Returns an error if a local
    /// name is not UTF-8. A remote service needs UTF-8 keys.
    fn keys(&self) -> io::Result<Vec<String>>;

    /// Length of `key` in bytes, or `None` when the backend holds no such key.
    ///
    /// # Errors
    ///
    /// Returns an error if the measurement fails for any other reason.
    fn stat(&self, key: &str) -> io::Result<Option<u64>>;

    /// Exactly `len` bytes of `key` from `offset`.
    ///
    /// The caller bounds `len`. A scan refill is about a megabyte. A header
    /// check is twelve bytes. Nothing here reads a whole segment.
    ///
    /// # Errors
    ///
    /// Returns an error when `key` is missing. Returns an error when the
    /// range runs past the end. Returns an error when `len` does not fit in
    /// memory.
    fn read_range(&self, key: &str, offset: u64, len: u64) -> io::Result<Vec<u8>>;

    /// Store `data` under `key`, replacing what the key held.
    ///
    /// Small objects only: index files and descriptors, never segments. Large
    /// files travel through [`ObjectStore::import_file`].
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes cannot be stored.
    fn write_bytes(&self, key: &str, data: &[u8]) -> io::Result<()>;

    /// Stream the file at `path` into `key`, replacing what the key held.
    ///
    /// Copies with a fixed buffer. Never holds the whole file in memory.
    /// Returns the bytes written.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read. Returns an error if the
    /// object cannot be written.
    fn import_file(&self, key: &str, path: &Path) -> io::Result<u64>;

    /// Stream `key` into the file at `path`.
    ///
    /// Writes beside the target and renames into place. A crash leaves the
    /// old file, never a torn one. Creates parent directories. Returns the
    /// bytes written.
    ///
    /// # Errors
    ///
    /// Returns an error when `key` is missing. Returns an error if the copy
    /// fails.
    fn export_file(&self, key: &str, path: &Path) -> io::Result<u64>;
}

/// An [`ObjectStore`] backed by a directory.
///
/// Keys are file names in that directory. Subdirectories are not objects. The
/// mirror passes segment and index file names as keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalDirStore {
    /// The directory holding the objects.
    dir: PathBuf,
}

impl LocalDirStore {
    /// A store for the objects in `dir`.
    ///
    /// Creates nothing. Reads treat a missing directory as empty. Writes
    /// create parents on the way in.
    #[must_use]
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// The directory holding the objects.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Path of `key` in the directory.
    ///
    /// A key is a file name, never a path. Empty keys, `.`, `..`, and keys
    /// holding `/` or `\` are refused before they touch the filesystem.
    ///
    /// # Errors
    ///
    /// Returns an error when `key` is not a bare file name.
    fn path(&self, key: &str) -> io::Result<PathBuf> {
        if key.is_empty() || key == "." || key == ".." || key.contains('/') || key.contains('\\') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{key:?} is not an object key: a key is a file name, never a path"),
            ));
        }

        Ok(self.dir.join(key))
    }
}

impl ObjectStore for LocalDirStore {
    fn keys(&self) -> io::Result<Vec<String>> {
        let entries = match super::at(&self.dir, "list", fs::read_dir(&self.dir)) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let mut out = Vec::new();
        for entry in entries {
            let entry = entry?;
            if super::at(entry.path().as_path(), "inspect", entry.file_type())?.is_file() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "{} is not UTF-8, so it cannot mirror: remote keys are UTF-8",
                            entry.path().display()
                        ),
                    ));
                };
                out.push(name.to_string());
            }
        }

        out.sort();
        Ok(out)
    }

    fn stat(&self, key: &str) -> io::Result<Option<u64>> {
        let path = self.path(key)?;
        match super::at(&path, "measure", fs::metadata(&path)) {
            Ok(meta) => Ok(Some(meta.len())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn read_range(&self, key: &str, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        let path = self.path(key)?;
        let mut file = super::at(&path, "open", fs::File::open(&path))?;
        let actual = super::at(&path, "measure", file.metadata())?.len();

        let end = offset.checked_add(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("range {offset}..{offset}+{len} of object {key:?} overflows"),
            )
        })?;
        if end > actual {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "object {key:?} holds {actual} bytes, so range {offset}..{end} is past the end"
                ),
            ));
        }

        super::at(
            &path,
            "seek in",
            std::io::Seek::seek(&mut file, io::SeekFrom::Start(offset)).map(|_| ()),
        )?;
        let mut buf = super::zeroed(len, &format!("{len} bytes of object {key:?}"))?;
        super::at(
            &path,
            "read",
            std::io::Read::read_exact(&mut file, &mut buf),
        )?;
        Ok(buf)
    }

    fn write_bytes(&self, key: &str, data: &[u8]) -> io::Result<()> {
        write_atomic(&self.path(key)?, data)
    }

    fn import_file(&self, key: &str, path: &Path) -> io::Result<u64> {
        copy_file(path, &self.path(key)?)
    }

    fn export_file(&self, key: &str, path: &Path) -> io::Result<u64> {
        copy_file(&self.path(key)?, path)
    }
}

/// An [`ObjectStore`] held in memory.
///
/// For tests and dry runs. Keys map to bytes. Holds everything it is given,
/// so point it at fixtures, never at a real set.
///
/// Clones share the same objects. Two handles see one map.
#[derive(Clone, Default)]
pub struct MemoryStore {
    /// Objects by key, in key order.
    state: std::sync::Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
}

impl std::fmt::Debug for MemoryStore {
    /// Counts, never contents.
    ///
    /// Hand-written for the same reason [`crate::storage::BlobStore`] writes
    /// its own: a derived print of a store holding megabytes is a
    /// multi-megabyte log line about a test backend.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.state.lock() {
            Ok(state) => f
                .debug_struct("MemoryStore")
                .field("keys", &state.len())
                .field("bytes", &state.values().map(Vec::len).sum::<usize>())
                .finish_non_exhaustive(),
            Err(_) => f.debug_struct("MemoryStore").finish_non_exhaustive(),
        }
    }
}

impl MemoryStore {
    /// An empty store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Lock the objects, ignoring poisoning.
    ///
    /// A panic during a test leaves a half-updated map. The next operation
    /// still answers from what is there.
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Vec<u8>>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl ObjectStore for MemoryStore {
    fn keys(&self) -> io::Result<Vec<String>> {
        Ok(Self::lock(self).keys().cloned().collect())
    }

    fn stat(&self, key: &str) -> io::Result<Option<u64>> {
        Ok(Self::lock(self)
            .get(key)
            .map(|data| u64::try_from(data.len()).unwrap_or(u64::MAX)))
    }

    fn read_range(&self, key: &str, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        let state = Self::lock(self);
        let Some(data) = state.get(key) else {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("object {key:?} is not in this store"),
            ));
        };

        let start = usize::try_from(offset).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("offset {offset} of object {key:?} does not fit in memory"),
            )
        })?;
        let count = usize::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{len} bytes of object {key:?} do not fit in memory"),
            )
        })?;
        let end = start.checked_add(count).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("range {offset}..{offset}+{len} of object {key:?} overflows"),
            )
        })?;
        if end > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "object {key:?} holds {} bytes, so range {offset}..{end} is past the end",
                    data.len()
                ),
            ));
        }

        Ok(data[start..end].to_vec())
    }

    fn write_bytes(&self, key: &str, data: &[u8]) -> io::Result<()> {
        Self::lock(self).insert(key.to_string(), data.to_vec());
        Ok(())
    }

    fn import_file(&self, key: &str, path: &Path) -> io::Result<u64> {
        let data = super::at(path, "read", fs::read(path))?;
        let count = u64::try_from(data.len()).unwrap_or(u64::MAX);
        Self::lock(self).insert(key.to_string(), data);
        Ok(count)
    }

    fn export_file(&self, key: &str, path: &Path) -> io::Result<u64> {
        let data = Self::lock(self).get(key).cloned().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("object {key:?} is not in this store"),
            )
        })?;
        write_atomic(path, &data)?;
        Ok(u64::try_from(data.len()).unwrap_or(u64::MAX))
    }
}

/// An [`ObjectStore`] that does not block the runtime.
///
/// The same six operations, awaited. A future operator backend implements
/// this directly. Synchronous backends arrive through the blanket
/// implementation below, which moves each call onto a blocking thread.
///
/// Return-position `impl Future`, as in [`crate::storage::StorageGet`]: an
/// `async fn` says nothing about crossing threads, and the mirror awaits
/// these inside tasks that do. Spelled out, the future says it.
pub trait AsyncObjectStore: Send + Sync {
    /// Every key this backend holds, in order.
    ///
    /// # Errors
    ///
    /// Returns an error if the listing fails.
    fn keys(&self) -> impl Future<Output = io::Result<Vec<String>>> + Send;

    /// Length of `key` in bytes, or `None` when the backend holds no such key.
    ///
    /// # Errors
    ///
    /// Returns an error if the measurement fails for any other reason.
    fn stat(&self, key: &str) -> impl Future<Output = io::Result<Option<u64>>> + Send;

    /// Exactly `len` bytes of `key` from `offset`.
    ///
    /// # Errors
    ///
    /// Returns an error when `key` is missing, when the range runs past the
    /// end, or when `len` does not fit in memory.
    fn read_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> impl Future<Output = io::Result<Vec<u8>>> + Send;

    /// Store `data` under `key`, replacing what the key held.
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes cannot be stored.
    fn write_bytes(&self, key: &str, data: &[u8]) -> impl Future<Output = io::Result<()>> + Send;

    /// Stream the file at `path` into `key`. Returns the bytes written.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or the object cannot be
    /// written.
    fn import_file(&self, key: &str, path: &Path) -> impl Future<Output = io::Result<u64>> + Send;

    /// Stream `key` into the file at `path`. Returns the bytes written.
    ///
    /// # Errors
    ///
    /// Returns an error when `key` is missing or the copy fails.
    fn export_file(&self, key: &str, path: &Path) -> impl Future<Output = io::Result<u64>> + Send;
}

/// Every synchronous backend, bridged onto blocking threads.
///
/// Clones the backend per call, so the blocking thread owns what it touches
/// and the runtime thread never waits on a lock it cannot see. A backend
/// that speaks async natively implements [`AsyncObjectStore`] by hand
/// instead.
impl<T> AsyncObjectStore for T
where
    T: ObjectStore + Clone + Send + 'static,
{
    fn keys(&self) -> impl Future<Output = io::Result<Vec<String>>> + Send {
        let backend = self.clone();
        async move {
            tokio::task::spawn_blocking(move || backend.keys())
                .await
                .map_err(|join| io::Error::other(format!("a store task failed: {join}")))?
        }
    }

    fn stat(&self, key: &str) -> impl Future<Output = io::Result<Option<u64>>> + Send {
        let backend = self.clone();
        let key = key.to_string();
        async move {
            tokio::task::spawn_blocking(move || backend.stat(&key))
                .await
                .map_err(|join| io::Error::other(format!("a store task failed: {join}")))?
        }
    }

    fn read_range(
        &self,
        key: &str,
        offset: u64,
        len: u64,
    ) -> impl Future<Output = io::Result<Vec<u8>>> + Send {
        let backend = self.clone();
        let key = key.to_string();
        async move {
            tokio::task::spawn_blocking(move || backend.read_range(&key, offset, len))
                .await
                .map_err(|join| io::Error::other(format!("a store task failed: {join}")))?
        }
    }

    fn write_bytes(&self, key: &str, data: &[u8]) -> impl Future<Output = io::Result<()>> + Send {
        let backend = self.clone();
        let key = key.to_string();
        let data = data.to_vec();
        async move {
            tokio::task::spawn_blocking(move || backend.write_bytes(&key, &data))
                .await
                .map_err(|join| io::Error::other(format!("a store task failed: {join}")))?
        }
    }

    fn import_file(&self, key: &str, path: &Path) -> impl Future<Output = io::Result<u64>> + Send {
        let backend = self.clone();
        let key = key.to_string();
        let path = path.to_path_buf();
        async move {
            tokio::task::spawn_blocking(move || backend.import_file(&key, &path))
                .await
                .map_err(|join| io::Error::other(format!("a store task failed: {join}")))?
        }
    }

    fn export_file(&self, key: &str, path: &Path) -> impl Future<Output = io::Result<u64>> + Send {
        let backend = self.clone();
        let key = key.to_string();
        let path = path.to_path_buf();
        async move {
            tokio::task::spawn_blocking(move || backend.export_file(&key, &path))
                .await
                .map_err(|join| io::Error::other(format!("a store task failed: {join}")))?
        }
    }
}

/// Borrowed backends implement the sync trait too.
///
/// All six operations take `&self`, so a reference forwards without keeping
/// anything. Generic callers take `&impl ObjectStore` either way.
impl<T> ObjectStore for &T
where
    T: ObjectStore + ?Sized,
{
    fn keys(&self) -> io::Result<Vec<String>> {
        (**self).keys()
    }

    fn stat(&self, key: &str) -> io::Result<Option<u64>> {
        (**self).stat(key)
    }

    fn read_range(&self, key: &str, offset: u64, len: u64) -> io::Result<Vec<u8>> {
        (**self).read_range(key, offset, len)
    }

    fn write_bytes(&self, key: &str, data: &[u8]) -> io::Result<()> {
        (**self).write_bytes(key, data)
    }

    fn import_file(&self, key: &str, path: &Path) -> io::Result<u64> {
        (**self).import_file(key, path)
    }

    fn export_file(&self, key: &str, path: &Path) -> io::Result<u64> {
        (**self).export_file(key, path)
    }
}

/// Write `data` to `path` so a crash leaves the old file, never a torn one.
///
/// Writes beside the target and renames into place. Syncs the file and the
/// parent directory, so the rename survives a crash and not just the bytes.
fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        super::at(parent, "create", fs::create_dir_all(parent))?;
    }

    let mut sibling = path
        .file_name()
        .map_or_else(OsString::new, OsStr::to_os_string);
    sibling.push(".tmp");
    let temporary = path.with_file_name(sibling);

    {
        let mut file = super::at(&temporary, "create", fs::File::create(&temporary))?;
        super::at(
            &temporary,
            "write",
            std::io::Write::write_all(&mut file, data),
        )?;
        super::at(&temporary, "sync", file.sync_all())?;
    }

    super::at(path, "rename into place", fs::rename(&temporary, path))?;

    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        let dir = super::at(parent, "open to sync it", fs::File::open(parent))?;
        super::at(parent, "sync", dir.sync_all())?;
    }

    Ok(())
}

/// Copy the file at `from` to `to` through a fixed buffer.
///
/// Writes beside the target and renames into place, as in [`write_atomic`].
/// Returns the bytes copied.
fn copy_file(from: &Path, to: &Path) -> io::Result<u64> {
    if let Some(parent) = to.parent().filter(|p| !p.as_os_str().is_empty()) {
        super::at(parent, "create", fs::create_dir_all(parent))?;
    }

    let mut sibling = to
        .file_name()
        .map_or_else(OsString::new, OsStr::to_os_string);
    sibling.push(".tmp");
    let temporary = to.with_file_name(sibling);

    let count = {
        let source = super::at(from, "open", fs::File::open(from))?;
        let mut source = std::io::BufReader::with_capacity(COPY_BUFFER, source);
        let dest = super::at(&temporary, "create", fs::File::create(&temporary))?;
        let mut dest = std::io::BufWriter::with_capacity(COPY_BUFFER, dest);
        let count = super::at(
            from,
            "copy",
            std::io::copy(&mut source, &mut dest).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!(
                        "could not copy {} to {}: {e}",
                        from.display(),
                        temporary.display()
                    ),
                )
            }),
        )?;
        super::at(
            &temporary,
            "flush",
            std::io::Write::flush(&mut dest).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("could not flush {}: {e}", temporary.display()),
                )
            }),
        )?;
        super::at(
            &temporary,
            "sync",
            dest.into_inner()
                .map_err(|e| {
                    io::Error::other(format!("could not flush {}: {e}", temporary.display()))
                })?
                .sync_all(),
        )?;
        count
    };

    super::at(to, "rename into place", fs::rename(&temporary, to))?;

    if let Some(parent) = to.parent().filter(|p| !p.as_os_str().is_empty()) {
        let dir = super::at(parent, "open to sync it", fs::File::open(parent))?;
        super::at(parent, "sync", dir.sync_all())?;
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::{LocalDirStore, MemoryStore, ObjectStore};
    use std::io::ErrorKind;

    /// Unique scratch directory for one test, under the system temp dir.
    ///
    /// Small fixtures only. Each test removes its own directory on the way
    /// out, so a failed run leaves nothing behind.
    fn scratch(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "lepatch-object-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    /// Both backends answer listing, measuring, and bounded reads the same
    /// way. A few hundred bytes move. Nothing touches a real set.
    #[test]
    fn backends_agree_on_small_objects() {
        for backend in [
            Box::new(MemoryStore::new()) as Box<dyn ObjectStore>,
            Box::new(LocalDirStore::new(scratch("agree"))) as Box<dyn ObjectStore>,
        ] {
            assert_eq!(backend.keys().unwrap(), Vec::<String>::new());
            assert_eq!(backend.stat("a.bin").unwrap(), None);

            backend.write_bytes("a.bin", b"hello").unwrap();
            backend.write_bytes("b.bin", b"world!").unwrap();

            assert_eq!(backend.keys().unwrap(), vec!["a.bin", "b.bin"]);
            assert_eq!(backend.stat("a.bin").unwrap(), Some(5));
            assert_eq!(backend.read_range("a.bin", 1, 3).unwrap(), b"ell");

            let missing = backend.read_range("missing.bin", 0, 1).unwrap_err();
            assert_eq!(missing.kind(), ErrorKind::NotFound);
            let past_end = backend.read_range("a.bin", 4, 2).unwrap_err();
            assert_eq!(past_end.kind(), ErrorKind::UnexpectedEof);

            backend.write_bytes("a.bin", b"longer bytes").unwrap();
            assert_eq!(backend.stat("a.bin").unwrap(), Some(12));
        }

        std::fs::remove_dir_all(scratch("agree")).ok();
    }

    /// A key is a file name, never a path. Both backends refuse the same
    /// escapes. No filesystem assertions beyond the refusal.
    #[test]
    fn keys_cannot_escape_the_store() {
        let local = LocalDirStore::new(scratch("escape"));
        for key in ["", ".", "..", "a/b", "a\\b", "../x"] {
            assert!(local.stat(key).is_err(), "{key:?} must be refused");
            assert!(
                local.write_bytes(key, b"x").is_err(),
                "{key:?} must be refused"
            );
        }

        std::fs::remove_dir_all(scratch("escape")).ok();
    }

    /// Large files stream both ways without holding the whole object. The
    /// fixture is one megabyte, the largest thing this file moves.
    #[test]
    fn files_stream_in_and_out() {
        let dir = scratch("stream");
        let source = dir.join("source.bin");
        let dest = dir.join("nested").join("dest.bin");
        let bytes = vec![0xabu8; 1024 * 1024];
        std::fs::create_dir_all(dir.join("nested")).unwrap();
        std::fs::write(&source, &bytes).unwrap();

        let memory = MemoryStore::new();
        let written = memory.import_file("seg.bin", &source).unwrap();
        assert_eq!(written, bytes.len() as u64);

        let read = memory.export_file("seg.bin", &dest).unwrap();
        assert_eq!(read, bytes.len() as u64);
        assert_eq!(std::fs::read(&dest).unwrap(), bytes);

        let local = LocalDirStore::new(dir.join("store"));
        let written = local.import_file("seg.bin", &source).unwrap();
        assert_eq!(written, bytes.len() as u64);
        let second = dir.join("second.bin");
        let read = local.export_file("seg.bin", &second).unwrap();
        assert_eq!(read, bytes.len() as u64);
        assert_eq!(std::fs::read(&second).unwrap(), bytes);

        std::fs::remove_dir_all(dir).ok();
    }
}
