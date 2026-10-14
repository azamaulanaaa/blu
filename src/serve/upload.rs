// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
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
    ///
    /// It also returns an error if the upload's own file cannot be created there.
    /// The directory can exist and still refuse the file.
    ///
    /// Returns an error if the upload registry's lock was poisoned by an earlier
    /// panic.
    pub async fn begin(&self, id: &str) -> io::Result<PathBuf> {
        let path = self.scratch.join(format!("{id}.pack"));

        // Created empty, so the file exists before a `PUT` arrives and a `PUT`
        // against an id nobody began cannot invent one. `PUT` then opens it with
        // `create`, which truncates — see the note on `put` for why that is the
        // right answer to a second `PUT`.
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
    ///
    /// Returns an error if the upload registry's lock was poisoned by an earlier
    /// panic.
    pub fn path(&self, id: &str) -> io::Result<PathBuf> {
        self.locked()?.get(id).cloned().ok_or_else(|| no_upload(id))
    }

    /// Write an upload's bytes, streaming them to the scratch file.
    ///
    /// **A second `PUT` truncates and replaces; it does not refuse.** An earlier
    /// version of this comment claimed the opposite — that `begin` created the
    /// file precisely so that a second `PUT` could not silently replace an upload
    /// a client believed it had sent — and it was never implemented. It described
    /// a resumable upload that does not exist, so a reader checking the code
    /// against the comment would find they had the contract backwards.
    ///
    /// **Truncation is the right answer for the case that actually happens.** A
    /// backup client whose transfer was cut off re-sends the whole pack under the
    /// same upload id, and those bytes have to land. Refusing would leave it with
    /// no way forward at all: the only alternative this API offers is to abandon
    /// the upload and begin a new one, throwing away a scratch file the server has
    /// already accepted bytes into, which is exactly the cost a retry exists to
    /// avoid. And the case that *would* argue for a refusal — a second `PUT` over
    /// an upload something has already read — is not one this crate guards today
    /// in either direction, so refusing every second `PUT` would break the real
    /// case to defend against one that is already undefended.
    ///
    /// A short or interrupted body cannot be mistaken for a good one: `Pack::open`
    /// at the end of this function refuses it, and that check happens on every
    /// `PUT` rather than only at commit.
    ///
    /// **Streamed, not buffered, and the reason is that there is no ceiling on a
    /// pack's size.** This used to take `&[u8]`, which meant actix collected the
    /// whole body first — and actix's default payload limit is 256 KiB, so a pack
    /// over that was refused with a bare `413` and a backup tool that could not
    /// accept a 300 KiB pack was not a backup tool. Every test in the suite used
    /// a pack of about 20 KB, so nothing caught it.
    ///
    /// Streaming also fixes the other end of the same problem: a pack is derived
    /// output and can be larger than memory, so buffering one to write it out is
    /// a ceiling by another name.
    ///
    /// # Errors
    ///
    /// Returns an error if the upload is not held, if the client disconnects
    /// mid-transfer, or if the bytes do not make a file that `Pack::open`
    /// accepts. **The check is `Pack::open` itself**, run before the upload is
    /// recorded as complete, so `commit` is not the first place a client hears
    /// that what it sent was not a pack.
    ///
    /// Returns an error if the scratch file cannot be created, written, flushed
    /// or synced. A scratch directory that has filled up fails here.
    ///
    /// Returns an error if the upload arrived with no bytes at all. That case is
    /// named rather than left to the length error `Pack::open` reports on its
    /// own.
    pub async fn put<S, E>(&self, id: &str, payload: S) -> io::Result<Pack>
    where
        S: futures_util::Stream<Item = Result<actix_web::web::Bytes, E>> + Unpin,
        E: std::fmt::Display,
    {
        use futures_util::StreamExt as _;

        let path = self.path(id)?;
        let mut file = tokio::fs::File::create(&path).await?;
        let mut written = 0u64;

        let mut payload = std::pin::pin!(payload);
        while let Some(chunk) = payload.next().await {
            // A client that goes away mid-transfer leaves a partial file. It is not
            // a pack, so `Pack::open` below refuses it and nothing downstream can
            // mistake it for one. **A payload error is mapped, not lost**: a bare
            // `413` from a body limit and a bare disconnect are the two failures a
            // client cannot tell apart otherwise.
            let chunk = chunk.map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("the upload was cut short: {e}"),
                )
            })?;
            file.write_all(&chunk).await?;
            written = written.saturating_add(u64::try_from(chunk.len()).unwrap_or(0));
        }

        file.flush().await?;
        file.sync_all().await?;
        drop(file);

        // An empty body is not a pack, and saying so by name is more use than the
        // length error `Pack::open` would produce on its own.
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "the upload arrived empty. A pack has a header and an index at minimum",
            ));
        }

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
    ///
    /// Returns an error if the upload registry's lock was poisoned by an earlier
    /// panic.
    pub fn open(&self, id: &str) -> io::Result<Pack> {
        Pack::open(&self.path(id)?)
    }

    /// Forget an upload and remove its file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be removed. A `Drop` guard runs the
    /// same removal, so this failing does not leave the file behind.
    ///
    /// Returns an error if the upload registry's lock was poisoned by an earlier
    /// panic.
    pub fn discard(&self, id: &str) -> io::Result<()> {
        let path = self.locked()?.remove(id);

        match path {
            Some(path) => remove(&path),
            None => Ok(()),
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
        format!("no upload {id:?}. Create one with POST /v1/uploads first"),
    )
}

#[cfg(test)]
mod tests {
    use std::io;

    use actix_web::web::Bytes;
    use futures_util::Stream;

    use super::Uploads;

    fn scratch_dir() -> (tempfile::TempDir, Uploads) {
        let dir = tempfile::tempdir().unwrap();
        let uploads = Uploads::new(dir.path().to_path_buf());

        (dir, uploads)
    }

    /// A body arriving as the single chunk it was written as.
    ///
    /// `Uploads::put` takes the payload as a `Stream` because the real one is a
    /// socket read in pieces, and a test hands it over whole. One item over
    /// `stream::iter` is what "one chunk, then end of body" looks like as a
    /// stream, so a test that only cares about the bytes still reads as a `put`
    /// with a body.
    ///
    /// `stream::iter` rather than `stream::once` over an `async` block, because
    /// `put` pins its payload and an `async` block is not `Unpin`.
    fn one_chunk(body: &'static [u8]) -> impl Stream<Item = Result<Bytes, io::Error>> {
        futures_util::stream::iter([Ok(Bytes::from_static(body))])
    }

    #[tokio::test]
    async fn an_upload_that_was_never_filled_is_not_a_pack() {
        // Checked at `put`, not only at commit, so a client hears about it at the
        // point it sent the bytes rather than one request later.
        let (_dir, uploads) = scratch_dir();
        uploads.begin("abc").await.unwrap();

        let error = uploads
            .put("abc", one_chunk(b"not a pack at all"))
            .await
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn a_second_put_replaces_what_the_first_wrote() {
        // **The documented behaviour, and the one a retry needs.** A client whose
        // transfer was cut off re-sends the whole pack under the same id; the
        // bytes on disk afterwards have to be the ones it sent *last*, or the
        // retry silently produced a pack spliced out of two transfers.
        //
        // Both bodies here are refused by `Pack::open` — the assertion is about
        // which bytes survived, not about validity.
        let (_dir, uploads) = scratch_dir();
        let path = uploads.begin("abc").await.unwrap();

        uploads
            .put("abc", one_chunk(b"the first attempt, cut short"))
            .await
            .unwrap_err();
        uploads
            .put("abc", one_chunk(b"the whole pack, sent again"))
            .await
            .unwrap_err();

        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"the whole pack, sent again",
            "a second PUT must truncate, not append and not refuse"
        );
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

    /// Discarding an upload whose file is already gone removes nothing and does not
    /// fail, because both the success and the failure path of a merge call it and one
    /// may run after the other.
    #[tokio::test]
    async fn discarding_an_upload_whose_file_is_already_gone_does_not_fail() {
        // Both paths out of a job — success and failure — call this, and one of
        // them may run after the other.
        let (_dir, uploads) = scratch_dir();
        let path = uploads.begin("abc").await.unwrap();
        std::fs::remove_file(&path).unwrap();

        uploads.discard("abc").unwrap();
    }
}
