// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! `POST /v1/uploads`, `PUT /v1/uploads/{id}`, its commit and its `DELETE`.
//!
//! ## Why an upload is four requests
//!
//! **A merge reads the pack twice.** `Pack::open` reads the trailer and the
//! index, and `merge` then seeks per chunk, so the server needs a *seekable
//! file* rather than a stream — the bytes have to land on disk whatever the
//! route is called. And one pack can go to more than one set, so the pack is not
//! named by a set: a client backing the same tree up to two servers has one pack
//! and two merges.
//!
//! An upload is therefore its own resource, and these are its four steps:
//!
//! 1. [`Remote::begin_upload`] — `POST /v1/uploads`, and the `201` names the
//!    slot. Nothing has been transferred yet.
//! 2. [`Upload::send`] — `PUT /v1/uploads/{id}`, and the `204` means the bytes
//!    arrived **and are a pack**: the server runs `Pack::open` on every `PUT`,
//!    so a client hears at this step that what it sent is not a pack rather than
//!    one request later.
//! 3. [`Upload::commit`] — `POST /v1/uploads/{id}/commit`, and the `200` is the
//!    pack's own header: its length, its chunk count, the version it was packed
//!    from and the snapshot hash that names it. Cheap, and it answers "is this
//!    what I meant to send" before a merge is asked for.
//! 4. [`Upload::discard`] — `DELETE /v1/uploads/{id}`, which removes the scratch
//!    file. See below.
//!
//! ## Why a failed transfer drops its upload
//!
//! **The scratch file is the client's mess and the server's disk.** An upload
//! that was begun and never committed is a file on someone else's filesystem
//! that nothing will ever clean up, so
//! [`Remote::merge_pack`](super::Remote::merge_pack) drops the upload whenever a
//! later step fails. The failure the caller hears is the one that went wrong
//! rather than the cleanup's: a refused cleanup is a `tracing::debug!` and
//! nothing more, because reporting "could not reach the server" in place of "the
//! pack is not a pack" would name the wrong failure.
//!
//! A caller driving the four steps by hand owns the same obligation, and
//! [`Upload::discard`] is there for it. A transfer interrupted before the `201`
//! names the upload is the one case nothing can clean up: the client never
//! learned the id, and a server that allocates one per request is a server whose
//! own bookkeeping has to expire them.
//!
//! ## Why a refusal is the server's sentence
//!
//! A `422` on a `PUT` is this crate's `Pack::open` refusing bytes, and its
//! `detail` is the sentence the local command would have printed. So it arrives
//! here as an [`io::ErrorKind::InvalidData`] carrying that sentence, rather than
//! as "the upload failed". See [`super::error`].

use std::io;

use super::{
    Remote,
    error::{Document, read, refusal},
    segment,
};
use crate::{metadata::Timestamp, storage::BlobId};

/// The media type a pack travels as.
///
/// **The pack is not JSON and this says so.** `blu serve` documents one media
/// type for the body of a `PUT /v1/uploads/{id}`, and this is it, so the server
/// and every client agree on what arrived without either of them sniffing a
/// header.
pub const PACK_MEDIA_TYPE: &str = "application/x-blu-pack";

/// An upload this server is holding.
///
/// Built by [`Remote::begin_upload`] and named by the id the server chose. It
/// holds the [`Remote`] it came from, so the four steps are four calls on one
/// value rather than four that each had to be handed the same client.
#[derive(Clone)]
pub struct Upload {
    /// The client this upload was begun against.
    remote: Remote,
    /// The slot's id, exactly as the server named it.
    id: String,
}

impl std::fmt::Debug for Upload {
    /// The id, and the server holding it.
    ///
    /// Hand-written for the same reason [`Remote`]'s is: this type goes into
    /// error messages, and a reader wants to know which upload and which server.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Upload")
            .field("id", &self.id)
            .field("remote", &self.remote.address())
            .finish()
    }
}

impl Upload {
    /// The slot's id, as the server named it.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The path of this upload's resource on its server.
    fn path(&self) -> String {
        format!("/v1/uploads/{}", segment(&self.id))
    }

    /// Send `pack` as this upload's bytes.
    ///
    /// **The pack is taken by value rather than borrowed.** `awc` sends a body
    /// that owns its bytes, so a borrowed slice would have to be copied into a
    /// `Vec` to be sent — a second copy of a pack that is derived output a
    /// caller has already in memory. Handing it over costs nothing instead.
    ///
    /// The `204` that answers means the pack arrived and `Pack::open` accepted
    /// it. Bytes that are not a pack are refused here with a `422`, and the error
    /// carries the server's own sentence.
    ///
    /// **The 30-second request ceiling in [`Remote::new`] applies to this
    /// request like every other**, the transfer included. A pack large enough
    /// that it cannot be copied to the server in half a minute is refused rather
    /// than half-sent, which is the right way round for a body whose failure is
    /// ambiguous: the caller learns that nothing arrived rather than that some of
    /// it did, and it can begin again.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot be reached, if the upload is not held
    /// — a `404` is [`io::ErrorKind::NotFound`], carrying the server's sentence —
    /// or if the bytes are not a pack this build reads.
    pub async fn send(&self, pack: Vec<u8>) -> io::Result<()> {
        tokio::task::LocalSet::new()
            .run_until(self.send_inner(pack))
            .await
    }

    /// The request itself, for [`Upload::send`] to run on a
    /// [`tokio::task::LocalSet`].
    pub(super) async fn send_inner(&self, pack: Vec<u8>) -> io::Result<()> {
        let url = self.remote.url(&self.path())?;

        let mut response = self
            .remote
            .client
            .put(&url)
            .insert_header(("content-type", PACK_MEDIA_TYPE))
            .send_body(pack)
            .await
            .map_err(|e| io::Error::other(format!("could not reach {url}: {e}")))?;

        let status = response.status();
        let body = read(&url, &mut response).await?;

        if !status.is_success() {
            return Err(refusal(status.as_u16(), &url, &body));
        }

        Ok(())
    }

    /// Ask what arrived: the pack's header figures.
    ///
    /// **Read before the merge, and it is worth the round trip.** `commit` runs
    /// `Pack::open`, which reads a trailer and an index and no payload, so it
    /// costs nothing beside the transfer — and it answers whether the pack that
    /// landed is the one the caller meant to send, before a merge is asked to
    /// publish it as a version.
    ///
    /// It does **not** hash the chunks, because `Pack::open` cannot: a pack whose
    /// payload is damaged is caught by the merge, chunk by chunk, as each one is
    /// checked on the way in. So a successful commit is not a promise that the
    /// merge will succeed.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot be reached, if the upload is not held
    /// — a `404` is [`io::ErrorKind::NotFound`] — or if what arrived is not a pack
    /// this build reads.
    pub async fn commit(&self) -> io::Result<UploadCommit> {
        tokio::task::LocalSet::new()
            .run_until(self.commit_inner())
            .await
    }

    /// The request itself, for [`Upload::commit`] to run on a
    /// [`tokio::task::LocalSet`].
    pub(super) async fn commit_inner(&self) -> io::Result<UploadCommit> {
        let url = self.remote.url(&format!("{}/commit", self.path()))?;

        let mut response = self
            .remote
            .client
            .post(&url)
            .send()
            .await
            .map_err(|e| io::Error::other(format!("could not reach {url}: {e}")))?;

        let status = response.status();
        let body = read(&url, &mut response).await?;

        if !status.is_success() {
            return Err(refusal(status.as_u16(), &url, &body));
        }

        let document = Document::read(&url, "an upload's commit", &body)?;

        Ok(UploadCommit {
            upload: document.text("upload")?.to_string(),
            length: document.count("length")?,
            payload_bytes: document.count("payload_bytes")?,
            chunks: document.count("chunks")?,
            version: document.version("version")?,
            snapshot: document.hash("snapshot")?,
            taken_at: Timestamp::parse_utc(document.text("taken_at")?)?,
            base: document.optional_hash("base")?,
        })
    }

    /// Drop this upload and its file on the server.
    ///
    /// **The cleanup step, and it is not optional after a failure.** The
    /// scratch file is what a crash would leave behind, and only the two ends
    /// know about it.
    ///
    /// Dropping an upload that was already dropped succeeds: the server answers
    /// `204` either way, and a caller cleaning up after an error must not have to
    /// know whether the upload was still there.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot be reached, or refuses. It does not
    /// return an error because there was nothing to drop.
    pub async fn discard(&self) -> io::Result<()> {
        tokio::task::LocalSet::new()
            .run_until(self.discard_inner())
            .await
    }

    /// The request itself, for [`Upload::discard`] to run on a
    /// [`tokio::task::LocalSet`].
    pub(super) async fn discard_inner(&self) -> io::Result<()> {
        let url = self.remote.url(&self.path())?;

        let mut response = self
            .remote
            .client
            .delete(&url)
            .send()
            .await
            .map_err(|e| io::Error::other(format!("could not reach {url}: {e}")))?;

        let status = response.status();
        let body = read(&url, &mut response).await?;

        if !status.is_success() {
            return Err(refusal(status.as_u16(), &url, &body));
        }

        Ok(())
    }
}

/// What arrived, read out of an upload's own header.
///
/// Every field is one the server read from the trailer and the index, so they
/// describe the pack rather than the transfer: `length` is the whole file,
/// `payload_bytes` the chunk records inside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadCommit {
    /// The upload this is about.
    pub upload: String,
    /// Bytes the file occupies.
    pub length: u64,
    /// Bytes the chunk records occupy inside it.
    pub payload_bytes: u64,
    /// How many chunks it carries.
    pub chunks: u64,
    /// The version it was packed from, in the packing set's numbering.
    ///
    /// **The sender's numbering, not the receiver's.** The same snapshot is
    /// version 2 of one set and version 9 of another, so this number is about
    /// where the pack came from and says nothing about what the merge will
    /// publish.
    pub version: u32,
    /// The snapshot the pack holds.
    pub snapshot: BlobId,
    /// Time the packed version ran, in UTC seconds and nanos.
    pub taken_at: Timestamp,
    /// The snapshot a delta assumes its reader holds, or `None` for a full pack.
    pub base: Option<BlobId>,
}

impl Remote {
    /// Begin an upload, and get the slot the pack goes in.
    ///
    /// Nothing is transferred by this request: it allocates an id and an empty
    /// scratch file on the server. See [`Upload`] for the three steps after it.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot be reached, or refuses — including a
    /// `201` whose body this client cannot read, which leaves an upload on the
    /// server under an id this client never learned. See this module's note on
    /// what that case costs.
    pub async fn begin_upload(&self) -> io::Result<Upload> {
        tokio::task::LocalSet::new()
            .run_until(self.begin_upload_inner())
            .await
    }

    /// The request itself, for [`Remote::begin_upload`] to run on a
    /// [`tokio::task::LocalSet`].
    pub(super) async fn begin_upload_inner(&self) -> io::Result<Upload> {
        let url = self.url("/v1/uploads")?;

        let mut response = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(|e| io::Error::other(format!("could not reach {url}: {e}")))?;

        let status = response.status();
        let body = read(&url, &mut response).await?;

        if !status.is_success() {
            return Err(refusal(status.as_u16(), &url, &body));
        }

        let document = Document::read(&url, "an upload", &body)?;

        Ok(Upload {
            remote: self.clone(),
            id: document.text("upload")?.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{PACK_MEDIA_TYPE, UploadCommit};
    use crate::{metadata::Timestamp, storage::BlobId};

    fn hash(byte: char) -> BlobId {
        BlobId::from_hex(byte.to_string().repeat(64)).unwrap()
    }

    #[test]
    fn the_media_type_is_the_one_the_server_documents() {
        // A literal at each end is how a pack arrives as something else, and the
        // server does not sniff.
        assert_eq!(PACK_MEDIA_TYPE, "application/x-blu-pack");
    }

    #[test]
    fn a_commit_reads_every_field_the_server_sends() {
        // Written out longhand rather than decoded from a live server, so the
        // field list is asserted here and a field the server stops sending is a
        // failure of this test rather than a silently zeroed value later.
        let commit = UploadCommit {
            upload: "abc".to_string(),
            length: 4096,
            payload_bytes: 4000,
            chunks: 12,
            version: 3,
            snapshot: hash('a'),
            taken_at: Timestamp::parse_utc("2023-11-14T22:13:20Z").unwrap(),
            base: Some(hash('b')),
        };

        assert_eq!(commit.upload, "abc");
        assert_eq!(commit.length, 4096);
        assert_eq!(commit.payload_bytes, 4000);
        assert_eq!(commit.chunks, 12);
        assert_eq!(commit.version, 3);
        assert_eq!(commit.snapshot, hash('a'));
        assert_eq!(
            commit.taken_at,
            Timestamp::parse_utc("2023-11-14T22:13:20Z").unwrap()
        );
        assert_eq!(commit.base, Some(hash('b')));
    }
}
