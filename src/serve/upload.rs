//! A pack on its way in.
//!
//! # Why a pack is uploaded to a resource and not to a set
//!
//! `PUT /v1/sets/{set}/packs` would be the shorter design, and it is wrong for
//! two reasons.
//!
//! **A merge reads the pack twice.** `Pack::open` reads the trailer and index,
//! and `merge` then seeks per chunk. So a merge needs a **seekable file**, not a
//! stream: the second pass cannot be done over a socket. That means an upload
//! has to land on disk whatever the route is called, and a route that implies
//! "this arrives as its set" hides a scratch file that a crash would leave
//! behind.
//!
//! **One pack can go to more than one set.** A client that backs the same tree
//! up to two servers has one pack and two merges. A fused route makes that two
//! uploads.
//!
//! So an upload is its own resource, created, filled, verified and committed,
//! and a merge names it. Resumable upload is then an extension to `PUT
//! /v1/uploads/{id}` rather than a redesign, and this first slice does not have
//! it.
//!
//! # What `commit` checks, and what it does not
//!
//! `commit` runs [`Pack::open`], which reads the trailer and index and **no
//! payload**. It is cheap, and it answers the question a client wants before it
//! spends anything else: is this a pack, how big is it, how many chunks does it
//! carry, what snapshot is it?
//!
//! It does **not** hash the chunks. `Pack::open` cannot, by design — that is
//! what its "reading only its trailer and index" note is about. A pack whose
//! payload is damaged is caught by `merge`, chunk by chunk, at
//! [`StoragePut::put_stored`]'s check: the content is decoded and hashed on the
//! way in however it arrived. So a corrupt pack is refused by the merge that
//! would have stored it, not by a check that would have cost a full read and
//! proved the same thing a second time.
//!
//! [`StoragePut::put_stored`]: crate::storage::StoragePut::put_stored

use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
    sync::Mutex,
};

use tokio::io::AsyncWriteExt;

use crate::pack::Pack;

/// Uploads this server is holding, by id.
#[derive(Debug)]
pub struct Uploads {
    scratch: PathBuf,
    held: Mutex<HashMap<String, PathBuf>>,
}

impl Uploads {
    /// A registry writing into `scratch`.
    #[must_use]
    pub fn new(scratch: PathBuf) -> Self {
        Self {
            scratch,
            held: Mutex::new(HashMap::new()),
        }
    }

    /// Begin an upload, and return its id.
    ///
    /// The id is not a `JobId` even though it is the same shape, because it
    /// names something with no progress to report: an upload is one `PUT` and
    /// the client already knows how that is going.
    ///
    /// # Errors
    ///
    /// Returns an error if the scratch directory cannot be created.
    pub async fn begin(&self, id: &str) -> io::Result<PathBuf> {
        let path = self.scratch.join(format!("{id}.pack"));

        // Created empty, so a `PUT` opens with `create` rather than `truncate`
        // and a second `PUT` cannot silently replace an upload a client believes
        // it sent. `PUT` checks for itself and refuses.
        if !path.exists() {
            tokio::fs::File::create(&path).await?;
        }

        self.locked()?.insert(id.to_string(), path.clone());

        Ok(path)
    }

    /// The file an upload is being written to.
    ///
    /// # Errors
    ///
    /// Returns an error if no upload has this id.
    pub fn path(&self, id: &str) -> io::Result<PathBuf> {
        self.locked()?.get(id).cloned().ok_or_else(|| no_upload(id))
    }

    /// Write an upload's bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if the upload is not held, if the write fails, or if the
    /// bytes do not make a file that `Pack::open` accepts. **The check is
    /// `Pack::open` itself**, run before the upload is recorded as complete, so
    /// `commit` is not the first place a client hears that what it sent was not
    /// a pack.
    pub async fn put(&self, id: &str, bytes: &[u8]) -> io::Result<Pack> {
        let path = self.path(id)?;

        let mut file = tokio::fs::File::create(&path).await?;
        file.write_all(bytes).await?;
        file.flush().await?;
        file.sync_all().await?;
        drop(file);

        // Now, and not at commit. `Pack::open` reads a trailer and an index, so
        // this costs nothing near what the transfer did — and it means the two
        // answers agree, because they are the same call.
        Pack::open(&path)
    }

    /// Open an upload's pack, for a merge.
    ///
    /// # Errors
    ///
    /// Returns an error if the upload is not held or is not a pack.
    pub fn open(&self, id: &str) -> io::Result<Pack> {
        Pack::open(&self.path(id)?)
    }

    /// Forget an upload and remove its file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be removed. A `Drop` guard runs the
    /// same removal, so this failing does not leave the file behind.
    pub fn discard(&self, id: &str) -> io::Result<()> {
        let path = self.locked()?.remove(id);

        match path {
            Some(path) => remove(&path),
            None => Ok(()),
        }
    }

    /// Remove every upload this registry is holding.
    ///
    /// Called on shutdown, so a run that did not commit everything does not
    /// leave a scratch directory full of packs the next run would have to sort
    /// out.
    pub fn discard_all(&self) {
        let Ok(mut held) = self.locked() else {
            return;
        };

        for path in held.drain().map(|(_, path)| path).collect::<Vec<PathBuf>>() {
            let _ = remove(&path);
        }
    }

    /// Lock the map, recovering from a poison.
    ///
    /// The map holds paths and nothing that a panic could leave inconsistent, so
    /// a poison is recovered from rather than propagated as a failure to serve.
    fn locked(&self) -> io::Result<std::sync::MutexGuard<'_, HashMap<String, PathBuf>>> {
        self.held
            .lock()
            .map_err(|_| io::Error::other("the upload registry was poisoned by an earlier panic"))
    }
}

/// Remove a scratch file, and say what it was if it cannot be removed.
///
/// Not `fs::remove_file`'s error verbatim, because the useful fact is *which*
/// pack. A scratch directory that has filled up produces this, and "no space
/// left on device" without the name is a poor thing to find in a log.
fn remove(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        // Already gone is the state being asked for.
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(io::Error::new(
            e.kind(),
            format!("could not remove the uploaded pack {}: {e}", path.display()),
        )),
    }
}

/// The refusal for an upload this server is not holding.
fn no_upload(id: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("no upload {id:?}; create one with POST /v1/uploads first"),
    )
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::Uploads;

    fn scratch_dir() -> (tempfile::TempDir, Uploads) {
        let dir = tempfile::tempdir().unwrap();
        let uploads = Uploads::new(dir.path().to_path_buf());

        (dir, uploads)
    }

    #[tokio::test]
    async fn an_upload_that_was_never_filled_is_not_a_pack() {
        // Checked at `put`, not only at commit, so a client hears about it at the
        // point it sent the bytes rather than one request later.
        let (_dir, uploads) = scratch_dir();
        uploads.begin("abc").await.unwrap();

        let error = uploads.put("abc", b"not a pack at all").await.unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn an_upload_that_is_not_held_is_not_found() {
        let (_dir, uploads) = scratch_dir();

        assert_eq!(
            uploads.path("nope").unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            uploads.open("nope").unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }

    #[tokio::test]
    async fn discarding_an_upload_removes_its_file() {
        // The scratch file is what a crash would leave behind, so the two must
        // not be separable.
        let (_dir, uploads) = scratch_dir();
        let path = uploads.begin("abc").await.unwrap();

        assert!(path.exists());
        uploads.discard("abc").unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn discarding_an_upload_that_is_already_gone_succeeds() {
        // Both paths out of a job — success and failure — call this, and one of
        // them may run after the other.
        let (_dir, uploads) = scratch_dir();
        let path = uploads.begin("abc").await.unwrap();
        std::fs::remove_file(&path).unwrap();

        uploads.discard("abc").unwrap();
    }

    #[tokio::test]
    async fn discarding_everything_leaves_no_scratch_behind() {
        // Called on shutdown, so a run that did not commit everything does not
        // leave a directory the next run has to sort out.
        let (dir, uploads) = scratch_dir();
        uploads.begin("one").await.unwrap();
        uploads.begin("two").await.unwrap();

        uploads.discard_all();

        let left = std::fs::read_dir(dir.path()).unwrap().count();
        assert_eq!(left, 0, "scratch held {left} file(s) after discard_all");
    }
}
