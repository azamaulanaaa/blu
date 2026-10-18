// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Starting work on a server, and waiting for it to finish.
//!
//! ## Why work is a job
//!
//! **A merge of a large pack is measured in minutes and a pack request walks a
//! version.** So `POST /v1/sets/{set}/merges` and `POST /v1/sets/{set}/packs`
//! answer `202` with a job to follow rather than holding a connection open for
//! the length of the work.
//!
//! [`Remote::start_merge`] and [`Remote::start_pack`] therefore hand back the
//! **job's id** — the `job` field of the `202` body, which is the same id the
//! `Location` header names — and [`Remote::await_job`] is what turns an id into
//! an answer. Splitting them is what lets a caller report progress between the
//! two, which is the whole point of a job existing.
//!
//! ## Why a failed job is an error rather than a job
//!
//! **This is the bug `9ab42a7` fixed on the server side, and it has a client
//! half.** A terminal job whose state is `failed` is a failure that has already
//! happened: the merge did not publish a version, and no later poll will change
//! that. Returning it as `Ok(job)` and letting the caller check a field is how a
//! backup tool reports success for a backup that did not happen — the same lie
//! the server stopped telling, one hop further on.
//!
//! So [`Remote::await_job`] returns [`io::ErrorKind::Other`] carrying the
//! server's own `error` sentence — the `Err` the work returned, verbatim, which
//! is the sentence a local `blu merge` would have printed — and a
//! `cancelled` job is refused the same way, because a cancelled merge published
//! nothing either.
//!
//! ## How progress reaches a caller
//!
//! **A poller reads the same counters the server's event stream carries**, as
//! running totals under the names `blu progress` prints them under: `files`,
//! `chunks`, `distinct`, `checked`, `done`, `read`, `written`.
//! [`Remote::await_job_with`] is handed each poll's counters *when they have
//! moved since the last one*, so a caller can print one line per change rather
//! than one per poll — the server reports every 300 ms and a poll is 200 ms, so
//! without that a terminal would be asked to redraw the same line for as long as
//! a merge runs.
//!
//! The counters arrive as a `Vec<(String, u64)>` rather than as an enum because
//! the names are the server's: a client that had to name them itself would have
//! to be changed when the tool grows a counter, and a client that renders them
//! needs no such change. [`Remote::await_job`] is the same call with a callback
//! that prints nothing, for a caller that does not want progress.
//!
//! ## What a job's result is
//!
//! `result` is one object whose shape depends on `kind`, which is a closed enum
//! on both sides of this API, so the client reads it as one of two typed answers
//! rather than as a free-form [`serde_json::Value`]: [`Merged`] for a merge,
//! [`PackSummary`] for a pack request. A document whose `kind` says `merge` and
//! whose `result` is a pack summary fails on the field the pack summary does not
//! carry, rather than decoding into a merge that never happened.

use std::{io, path::Path};

use super::{
    Remote,
    error::{Document, drift, read, refusal},
    segment, set_path,
    uploads::Upload,
};
use crate::{
    metadata::Timestamp,
    storage::{BlobId, MAX_COMPRESSION_LEVEL},
};

/// How long to wait between two polls of a job.
///
/// Shorter than the server's 300 ms reporting interval, so a poll is usually
/// looking at counters that have moved since the last one, and long enough that a
/// merge reporting every 300 ms is not polled four times for each line it prints.
const POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(200);

/// The write policy a merge request names.
///
/// **The wire's own vocabulary, which is not either of this crate's.** `keep`
/// stores what the pack carries, frame and all; a level from `0` to `9` asks the
/// server to decode every chunk and re-encode it at that zstd level.
///
/// [`crate::command::MergeCompression`] is the same choice as a *command* takes
/// it, where `Reencode` means "under the set's own policy" and carries no level —
/// the level belongs to the store the merge opens, which a client asking for a
/// remote merge does not choose. And [`crate::storage::Compression`] is a store's
/// write policy, where `Off` means raw rather than *whatever the sender sent*.
/// Two different questions, three types, none of them interchangeable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeCompression {
    /// `keep`: store each chunk exactly as the pack carries it.
    Keep,
    /// A zstd level from `0` to [`MAX_COMPRESSION_LEVEL`], and the server
    /// re-encodes every chunk under it.
    ///
    /// **The level reaches the server's set, not just its answer.** A server
    /// merges by opening the receiving set under a write policy, so this is the
    /// level those records are written at; `0` stores each payload as it is.
    /// The levels a merge can actually use are the ones the server can open a
    /// store at, which is what `checked` refuses outside.
    Reencode(u8),
}

impl MergeCompression {
    /// The word this policy is on the wire as.
    ///
    /// **A string either way.** The server reads this field as text, so a level
    /// is `"3"` and not `3`.
    fn wire(self) -> String {
        match self {
            Self::Keep => "keep".to_string(),
            Self::Reencode(level) => level.to_string(),
        }
    }

    /// Refuses a level this crate's storage layer would not accept.
    ///
    /// **Checked before the request rather than left to the server's `400`**, so
    /// the refusal names the level as the mistake in the command line that named
    /// it. The server would refuse it too, with the same words, but only after
    /// the client had packed a tree to find out.
    fn checked(self) -> io::Result<Self> {
        match self {
            Self::Reencode(level) if u64::from(level) > u64::from(MAX_COMPRESSION_LEVEL) => {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "{level} is not a compression level. A level is from 0, which means \
                         as-is, through {MAX_COMPRESSION_LEVEL}"
                    ),
                ))
            }
            other => Ok(other),
        }
    }
}

/// What a job is doing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Answering `POST /v1/sets/{set}/packs`: a pack of a version.
    Pack,
    /// Answering `POST /v1/sets/{set}/merges`: a pack merged into a set.
    Merge,
}

impl Kind {
    /// The kind the server named.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidData`] for a word that is neither, naming
    /// the address. A closed enum read as a `String` is where a mistyped name
    /// would become a job of no particular kind.
    fn read(url: &str, said: &str) -> io::Result<Self> {
        match said {
            "pack" => Ok(Self::Pack),
            "merge" => Ok(Self::Merge),
            other => Err(drift(
                url,
                "a job",
                &format!("`kind` is {other:?}, which is not a job kind this client reads"),
            )),
        }
    }
}

/// Where a job is in its life.
///
/// The server's five words, as a closed enum. See [`crate::serve::State`], which
/// is the same set on the other side of the socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Accepted, not started.
    Queued,
    /// Running.
    Running,
    /// Finished, and produced a result.
    Succeeded,
    /// Finished, and did not. [`Remote::await_job`] refuses this.
    Failed,
    /// Stopped by a `DELETE`. Also refused: nothing was published.
    Cancelled,
}

impl State {
    /// Whether this is a state a job will not leave.
    #[must_use]
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    /// The state the server named.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidData`] for a word this enum does not
    /// have. **An unrecognised state is not "still running".** A server that
    /// added one would otherwise be polled forever by a client that reports no
    /// progress and never stops.
    fn read(url: &str, said: &str) -> io::Result<Self> {
        match said {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(drift(
                url,
                "a job",
                &format!("`state` is {other:?}, which is not a job state this client reads"),
            )),
        }
    }
}

/// A job, as a client sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct Job {
    /// Which job this is, as the server named it.
    pub id: String,
    /// What it is doing.
    pub kind: Kind,
    /// Where it is.
    pub state: State,
    /// When it started, in nanoseconds since the epoch.
    pub started: u64,
    /// When it finished, or `None` while it has not.
    pub finished: Option<u64>,
    /// Why it failed, or `None`.
    ///
    /// **Read it only on a job that failed**, which
    /// [`Remote::await_job`] refuses before a caller can. It is here because
    /// [`Remote::job`] hands back a failed job rather than hiding it, and a
    /// caller that asks for one wants the sentence.
    pub error: Option<String>,
    /// What it produced, or `None` while it has not.
    pub result: Option<Outcome>,
    /// Its running totals, by the name the tool reports them under.
    pub progress: Vec<(String, u64)>,
}

/// What a finished job produced.
///
/// One variant per [`Kind`], because `kind` decides the shape and both are closed
/// enums. A caller matches on it rather than reaching into a JSON object.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// A merge's answer.
    Merged(Merged),
    /// A pack request's answer: where the pack is and how big it is.
    Packed(PackSummary),
}

/// What a merge published.
///
/// **Two shapes, because the server sends two.** A merge that wrote a version
/// reports the plan it committed; a merge of a pack the set already holds
/// reports only which version holds it and writes nothing — which is a success,
/// so a retried transfer is answerable. Reading that second shape as the first
/// would mean eight fields that are not there, and the numbers a caller prints
/// would be zeros that mean nothing.
#[derive(Debug, Clone, PartialEq)]
pub enum Merged {
    /// The merge published a new version.
    Committed(MergedCommitted),
    /// The set already held this pack, so nothing was written.
    Already(MergedAlready),
}

impl Merged {
    /// The set the merge was into.
    #[must_use]
    pub fn set(&self) -> &str {
        match self {
            Self::Committed(committed) => &committed.set,
            Self::Already(already) => &already.set,
        }
    }

    /// The version of that set the merge left holding this pack.
    #[must_use]
    pub fn version(&self) -> u32 {
        match self {
            Self::Committed(committed) => committed.version,
            Self::Already(already) => already.version,
        }
    }
}

/// A merge that published a version.
///
/// Every figure is the server's, read from its own plan at commit time. **There
/// is no `already` field**, because this variant is what the server sends as
/// `already: false` — the flag is the choice between this and
/// [`MergedAlready`], not a boolean a caller has to check and then trust.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergedCommitted {
    /// The set it was merged into.
    pub set: String,
    /// The version the set allocated for it.
    pub version: u32,
    /// The snapshot that version restores to.
    pub snapshot: BlobId,
    /// The version of this set holding the pack's base, or `None` for a full pack.
    pub base: Option<u32>,
    /// Chunks the version needs.
    pub chunks: u64,
    /// How many of those had to be written.
    pub fresh: u64,
    /// How many the set already held.
    pub held: u64,
    /// Bytes of chunk content stored, before compression.
    pub bytes: u64,
}

/// A merge of a pack the set already held.
///
/// Success with nothing written, so that a transfer retried after a lost answer
/// is answerable rather than an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergedAlready {
    /// The set that already held it.
    pub set: String,
    /// The version of that set which holds this snapshot.
    pub version: u32,
}

/// A pack request's answer: where the pack is and how big it is.
///
/// **Geometry rather than bytes.** Nothing has been transferred: this says which
/// pack the server will serve and how long it is, which is what a client needs
/// before it can plan a range fetch against `GET /v1/packs/{id}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackSummary {
    /// The set it was packed from.
    pub set: String,
    /// The version it was packed from.
    pub version: u32,
    /// Bytes the whole pack occupies.
    pub length: u64,
    /// Where the chunk records begin, just after the header.
    pub payload_start: u64,
    /// Bytes the chunk records occupy.
    pub payload_bytes: u64,
    /// How many chunks it carries.
    pub chunks: u64,
    /// The snapshot it holds, which is what names it.
    pub snapshot: BlobId,
    /// Time the packed version ran, in UTC seconds and nanos.
    pub taken_at: Timestamp,
}

impl Remote {
    /// Ask for a merge of `upload` into `set`, and get the job's id.
    ///
    /// The upload must already have been sent and committed — see
    /// [`Remote::merge_pack`], which is this request with the three before it.
    /// The server opens the upload at this request so a bad id is refused here,
    /// with a `404`, rather than by a job that fails afterwards.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot be reached, if it refuses — no such
    /// set or upload is a `404`, a compression value that is neither `keep` nor
    /// a level is a `400` — or if the `202` does not name a job this client can
    /// ask about later.
    pub async fn start_merge(
        &self,
        set: &str,
        upload: &str,
        compression: MergeCompression,
    ) -> io::Result<String> {
        tokio::task::LocalSet::new()
            .run_until(self.start_merge_inner(set, upload, compression))
            .await
    }

    /// The request itself, for [`Remote::start_merge`] to run on a
    /// [`tokio::task::LocalSet`].
    pub(super) async fn start_merge_inner(
        &self,
        set: &str,
        upload: &str,
        compression: MergeCompression,
    ) -> io::Result<String> {
        let compression = compression.checked()?;

        let url = self.url(&format!("/v1/sets/{}/merges", set_path(set)?))?;
        let body = serde_json::json!({
            "upload": upload,
            "compression": compression.wire(),
        });

        self.start(&url, &body).await
    }

    /// Ask for a pack of `version` in `set`, and get the job's id.
    ///
    /// `version` of `None` asks for the newest version that resolves, which is
    /// what a client with no version in mind means. `base` of `Some` asks for a
    /// delta: only the chunks a reader holding that version does not have. Both
    /// must resolve in the same set.
    ///
    /// **There is no compression argument, and that is the API's shape rather
    /// than an omission.** A re-encode cannot be served from a set's bytes —
    /// they are not in the set — so this endpoint accepts `keep` and refuses
    /// anything else with a `422`, and an argument that could only hold one value
    /// would be a way to be wrong.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot be reached, if it refuses, or if the
    /// `202` does not name a job this client can ask about later.
    pub async fn start_pack(
        &self,
        set: &str,
        version: Option<u32>,
        base: Option<u32>,
    ) -> io::Result<String> {
        tokio::task::LocalSet::new()
            .run_until(self.start_pack_inner(set, version, base))
            .await
    }

    /// The request itself, for [`Remote::start_pack`] to run on a
    /// [`tokio::task::LocalSet`].
    async fn start_pack_inner(
        &self,
        set: &str,
        version: Option<u32>,
        base: Option<u32>,
    ) -> io::Result<String> {
        let url = self.url(&format!("/v1/sets/{}/packs", set_path(set)?))?;
        let mut body = serde_json::Map::new();

        // Absent rather than `null`, which the server reads the same way, and
        // which reads as "the one that resolves" in a log.
        if let Some(version) = version {
            body.insert("version".to_string(), serde_json::json!(version));
        }
        if let Some(base) = base {
            body.insert("base".to_string(), serde_json::json!(base));
        }

        self.start(&url, &serde_json::Value::Object(body)).await
    }

    /// A `202` that names a job, and the id it names.
    ///
    /// The body of an accepted request is `{"job": "<id>"}` — the same id its
    /// `Location` header names. **Not a whole job document**: the work has not
    /// started, so there is no state, no counters and no result to report, and a
    /// job built from this would be eight fields of nothing.
    async fn start(&self, url: &str, body: &serde_json::Value) -> io::Result<String> {
        let mut response = self
            .authorize(self.client.post(url))
            .send_json(body)
            .await
            .map_err(|e| io::Error::other(format!("could not reach {url}: {e}")))?;

        let status = response.status();
        let answer = read(url, &mut response).await?;

        if !status.is_success() {
            return Err(refusal(status.as_u16(), url, &answer));
        }

        let document = Document::read(url, "an accepted job", &answer)?;

        Ok(document.text("job")?.to_string())
    }

    /// Upload `pack` and merge it into `set`, and get the merge job's id.
    ///
    /// `pack` is taken by value, as [`Upload::send`] documents.
    ///
    /// **The four upload steps in the order they have to happen**, so a caller
    /// backing a tree up to a server does not have to know that they are four:
    /// begin, send, commit, and then [`Remote::start_merge`]. The commit is
    /// asked for and its answer thrown away here, because it is the step that
    /// proves the pack landed and this crate reads it strictly; a caller that
    /// wants to look at the figures should drive the steps itself through
    /// [`Remote::begin_upload`].
    ///
    /// **A failure after the upload is begun drops the upload.** The scratch file
    /// a half-finished upload leaves behind is on the server's disk and nobody
    /// else's job to clean, so it is removed here; the caller hears the failure
    /// that went wrong rather than the cleanup's. See
    /// [`Upload`] for why a `DELETE` is here at all.
    ///
    /// **A success leaves the upload where it is, deliberately.** This hands back
    /// at the `202`, and the merge job has not run yet — the server opens the
    /// upload again inside the job, so a `DELETE` sent now could pull the pack
    /// out from under a merge that had been accepted. A caller that wants the
    /// scratch file gone calls [`Upload::discard`] once the job has finished, or
    /// leaves it to the server, which clears its scratch directory when it stops.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the four steps fails — the server's own
    /// sentence, mapped to a kind as `blulib::client`'s documentation says — or
    /// if `compression` names a level this crate does not use.
    pub async fn merge_pack(
        &self,
        set: &str,
        pack: Vec<u8>,
        compression: MergeCompression,
    ) -> io::Result<String> {
        tokio::task::LocalSet::new()
            .run_until(self.merge_pack_inner(set, pack, compression))
            .await
    }

    /// The request itself, for [`Remote::merge_pack`] to run on a
    /// [`tokio::task::LocalSet`].
    async fn merge_pack_inner(
        &self,
        set: &str,
        pack: Vec<u8>,
        compression: MergeCompression,
    ) -> io::Result<String> {
        let upload = self.begin_upload_inner().await?;

        let started = match self.pushed(set, &upload, pack, compression).await {
            Ok(started) => started,
            Err(failure) => {
                self.discard_after_failure(&upload).await;

                return Err(failure);
            }
        };

        Ok(started)
    }

    /// Push the file `pack`, and ask a server to merge it.
    ///
    /// **The same four steps as [`Remote::merge_pack`], with the pack read
    /// from disk.** The client streams the file to the server's scratch space
    /// through [`Upload::send_file`] instead of holding it, so this is the
    /// entry point for a pack that may be larger than memory. The commit is
    /// asked for and its answer thrown away here for the same reason
    /// [`Remote::merge_pack`] gives.
    ///
    /// **A failure after the upload is begun drops the upload**, as
    /// [`Remote::merge_pack`] documents, and that covers a file this machine
    /// cannot read as well as a server that refused the bytes.
    ///
    /// # Errors
    ///
    /// Returns an error if `pack` cannot be read, if any of the four steps
    /// fails — the server's own sentence, mapped to a kind as
    /// `blulib::client`'s documentation says — or if `compression` names a
    /// level this crate does not use.
    pub async fn merge_pack_file(
        &self,
        set: &str,
        pack: &Path,
        compression: MergeCompression,
    ) -> io::Result<String> {
        tokio::task::LocalSet::new()
            .run_until(self.merge_pack_file_inner(set, pack, compression))
            .await
    }

    /// The request itself, for [`Remote::merge_pack_file`] to run on a
    /// [`tokio::task::LocalSet`].
    async fn merge_pack_file_inner(
        &self,
        set: &str,
        pack: &Path,
        compression: MergeCompression,
    ) -> io::Result<String> {
        let upload = self.begin_upload_inner().await?;

        let started = match self.pushed_file(set, &upload, pack, compression).await {
            Ok(started) => started,
            Err(failure) => {
                self.discard_after_failure(&upload).await;

                return Err(failure);
            }
        };

        Ok(started)
    }

    /// Drop an upload after a failed step, and keep the failure that matters.
    ///
    /// **The cleanup the server's disk is owed.** A half-finished upload
    /// leaves a scratch file on the server, so it is removed here. The failure
    /// the caller hears is the one that went wrong rather than the cleanup's:
    /// a refused cleanup is a `debug!` and nothing more, because reporting
    /// "could not reach the server" in place of the real failure would send
    /// the reader after the wrong problem.
    async fn discard_after_failure(&self, upload: &Upload) {
        if let Err(cleanup) = upload.discard_inner().await {
            tracing::debug!(
                upload = upload.id(),
                server = self.address(),
                error = %cleanup,
                "could not drop an upload after a failed push"
            );
        }
    }

    /// Send, commit and merge, for [`Remote::merge_pack`].
    ///
    /// **The `*_inner` requests rather than the public ones**, because this all
    /// runs inside the one [`tokio::task::LocalSet`] [`Remote::merge_pack`]
    /// entered. Every public method here enters a `LocalSet` of its own, so a
    /// composition written out of the public methods would nest one per step.
    async fn pushed(
        &self,
        set: &str,
        upload: &Upload,
        pack: Vec<u8>,
        compression: MergeCompression,
    ) -> io::Result<String> {
        upload.send_inner(pack).await?;
        upload.commit_inner().await?;

        self.start_merge_inner(set, upload.id(), compression).await
    }

    /// Send, commit and merge, for [`Remote::merge_pack_file`].
    ///
    /// The file variant of [`Remote::pushed`](Self::pushed): the same three
    /// steps inside the one [`tokio::task::LocalSet`] the caller entered, with
    /// the pack streamed from disk rather than held.
    async fn pushed_file(
        &self,
        set: &str,
        upload: &Upload,
        pack: &Path,
        compression: MergeCompression,
    ) -> io::Result<String> {
        upload.send_file_inner(pack).await?;
        upload.commit_inner().await?;

        self.start_merge_inner(set, upload.id(), compression).await
    }

    /// One job, as it stands now.
    ///
    /// **This is the raw answer, and it hands back a failed job rather than
    /// refusing one.** It is the poll a caller drives itself; [`Remote::await_job`]
    /// is the one that waits, and it is the one that turns a failure into an
    /// error.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot be reached, if there is no such job
    /// — a `404` is [`io::ErrorKind::NotFound`] — or if the answer is not the
    /// shape of a job.
    pub async fn job(&self, id: &str) -> io::Result<Job> {
        tokio::task::LocalSet::new()
            .run_until(self.job_inner(id))
            .await
    }

    /// The request itself, for [`Remote::job`] to run on a
    /// [`tokio::task::LocalSet`].
    pub(super) async fn job_inner(&self, id: &str) -> io::Result<Job> {
        let url = self.url(&format!("/v1/jobs/{}", segment(id)))?;

        let mut response = self
            .authorize(self.client.get(&url))
            .send()
            .await
            .map_err(|e| io::Error::other(format!("could not reach {url}: {e}")))?;

        let status = response.status();
        let body = read(&url, &mut response).await?;

        if !status.is_success() {
            return Err(refusal(status.as_u16(), &url, &body));
        }

        let document = Document::read(&url, "a job", &body)?;

        let kind = Kind::read(&url, document.text("kind")?)?;

        // `kind` decides the shape, and a document whose two halves disagree
        // fails on the field the other shape does not carry rather than
        // decoding into a job of the wrong kind with plausible zeros in it.
        let result = if document.has("result") {
            let result = document.object("result")?;

            Some(match kind {
                Kind::Merge => Outcome::Merged(merged(&result)?),
                Kind::Pack => Outcome::Packed(packed(&result)?),
            })
        } else {
            None
        };

        Ok(Job {
            id: document.text("id")?.to_string(),
            kind,
            state: State::read(&url, document.text("state")?)?,
            started: document.count("started")?,
            finished: document.optional_count("finished")?,
            error: document.optional_text("error")?.map(str::to_string),
            result,
            progress: document.counters("progress")?,
        })
    }

    /// Wait for `id` to finish, and return the job it became.
    ///
    /// **Polls `GET /v1/jobs/{id}` every 200 ms** until the state is terminal,
    /// and then returns that job.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot be reached, if the job is not there,
    /// or if the job **failed or was cancelled** — with the server's own sentence
    /// as the message. A job that succeeded is the only terminal state that is an
    /// `Ok`, and a `failed` one is the same lie the server stopped telling: see
    /// this module's documentation.
    pub async fn await_job(&self, id: &str) -> io::Result<Job> {
        self.await_job_with(id, |_| {}).await
    }

    /// Wait for `id`, handing each poll's counters to `on_progress`.
    ///
    /// `on_progress` is called with the job's running totals **when they have
    /// moved since the last call**, and not otherwise. The first call is the
    /// first poll that has any; a job that reports nothing is never reported, so
    /// a caller does not have to distinguish "no progress" from "nothing new".
    ///
    /// The closure is called on this task rather than on a thread of its own, and
    /// it is not required to be `Send`: this client's futures are not `Send`
    /// either, which [`Remote::versions`] documents why.
    ///
    /// # Errors
    ///
    /// As [`Remote::await_job`], and with the same insistence: a `failed` job is
    /// an error carrying the server's sentence, never an `Ok` a caller has to
    /// check.
    pub async fn await_job_with<F>(&self, id: &str, mut on_progress: F) -> io::Result<Job>
    where
        F: FnMut(&[(String, u64)]),
    {
        tokio::task::LocalSet::new()
            .run_until(self.await_job_inner(id, &mut on_progress))
            .await
    }

    /// The loop itself, for [`Remote::await_job_with`] to run on a
    /// [`tokio::task::LocalSet`].
    ///
    /// [`Remote::job_inner`] rather than [`Remote::job`], for the reason
    /// [`Remote::pushed`] names.
    async fn await_job_inner<F>(&self, id: &str, on_progress: &mut F) -> io::Result<Job>
    where
        F: FnMut(&[(String, u64)]),
    {
        // Compared against the last report, so a poll that found nothing new
        // prints nothing. `None` rather than an empty vector so that a job whose
        // first poll has counters is reported on that first poll.
        let mut reported: Option<Vec<(String, u64)>> = None;

        loop {
            let job = self.job_inner(id).await?;

            if reported.as_ref() != Some(&job.progress) && !job.progress.is_empty() {
                reported = Some(job.progress.clone());
                on_progress(&job.progress);
            }

            match job.state {
                State::Succeeded => return Ok(job),
                // **The refusal this whole module exists for.** `failed` and
                // `cancelled` are terminal, so no later poll changes the answer,
                // and a merge in either of them published nothing. The sentence
                // is the server's own, which is the `Err` its work returned.
                State::Failed | State::Cancelled => {
                    return Err(io::Error::other(failure(&job)));
                }
                State::Queued | State::Running => {
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        }
    }
}

/// The sentence a terminal job that did not succeed is an error about.
///
/// **The server's own, verbatim.** A job's `error` is the `Err` the work
/// returned, which is the sentence a local command would have printed, so it is
/// carried through unchanged rather than reworded. A terminal job with no
/// `error` is a server that has said a job failed without saying why, and the
/// state and the id are then the only facts there are.
fn failure(job: &Job) -> String {
    match (job.state, job.error.as_deref()) {
        (_, Some(error)) => error.to_string(),
        (State::Cancelled, None) => format!("job {} was cancelled, and published nothing", job.id),
        (State::Failed, None) => format!("job {} failed, and said nothing about why", job.id),
        // Unreachable for any other state: only terminal ones are refused.
        (other, None) => format!("job {} is {other:?}", job.id),
    }
}

/// A merge's answer, read strictly.
fn merged(result: &Document) -> io::Result<Merged> {
    // `already` is required by both shapes and is what tells them apart: the
    // server sends it as `false` on a committed merge and `true` on one that
    // wrote nothing.
    if result.flag("already")? {
        return Ok(Merged::Already(MergedAlready {
            set: result.text("set")?.to_string(),
            version: result.version("version")?,
        }));
    }

    Ok(Merged::Committed(MergedCommitted {
        set: result.text("set")?.to_string(),
        version: result.version("version")?,
        snapshot: result.hash("snapshot")?,
        base: result.optional_version("base")?,
        chunks: result.count("chunks")?,
        fresh: result.count("fresh")?,
        held: result.count("held")?,
        bytes: result.count("bytes")?,
    }))
}

/// A pack request's answer, read strictly.
fn packed(result: &Document) -> io::Result<PackSummary> {
    Ok(PackSummary {
        set: result.text("set")?.to_string(),
        version: result.version("version")?,
        length: result.count("length")?,
        payload_start: result.count("payload_start")?,
        payload_bytes: result.count("payload_bytes")?,
        chunks: result.count("chunks")?,
        snapshot: result.hash("snapshot")?,
        taken_at: Timestamp::parse_utc(result.text("taken_at")?)?,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        Document, Job, Kind, MAX_COMPRESSION_LEVEL, MergeCompression, Merged, Outcome, PackSummary,
        State, failure, merged, packed,
    };
    use crate::{metadata::Timestamp, storage::BlobId};

    /// A job document, as the server writes one for a merge that has published
    /// a version.
    const MERGE_JOB: &str = r#"{
        "id":"aaaa","kind":"merge","state":"succeeded","started":1,"finished":2,
        "error":null,
        "result":{"set":"docs","version":2,"already":false,
            "snapshot":"0101010101010101010101010101010101010101010101010101010101010101",
            "base":null,"chunks":9,"fresh":3,"held":6,"bytes":4096},
        "progress":{"chunks":9,"written":4096}
    }"#;

    fn read(body: &str) -> Job {
        let url = "http://127.0.0.1:8480/v1/jobs/aaaa";
        // The parse is `Remote::job_inner`'s, and this reaches it through the
        // only door it has: the shape is checked here rather than against a live
        // server, so a field the server stops sending fails here.
        let document = Document::read(url, "a job", body.as_bytes()).unwrap();
        let kind = Kind::read(url, document.text("kind").unwrap()).unwrap();

        let result = document.object("result").unwrap();
        let result = match kind {
            Kind::Merge => Outcome::Merged(merged(&result).unwrap()),
            Kind::Pack => Outcome::Packed(packed(&result).unwrap()),
        };

        Job {
            id: document.text("id").unwrap().to_string(),
            kind,
            state: State::read(url, document.text("state").unwrap()).unwrap(),
            started: document.count("started").unwrap(),
            finished: document.optional_count("finished").unwrap(),
            error: document.optional_text("error").unwrap().map(str::to_string),
            result: Some(result),
            progress: document.counters("progress").unwrap(),
        }
    }

    #[test]
    fn a_merged_job_carries_the_plan_the_server_committed() {
        let job = read(MERGE_JOB);

        assert_eq!(job.kind, Kind::Merge);
        assert_eq!(job.state, State::Succeeded);
        assert!(job.state.is_terminal());
        assert_eq!(job.started, 1);
        assert_eq!(job.finished, Some(2));
        assert_eq!(job.error, None);
        assert_eq!(
            job.progress,
            vec![("chunks".to_string(), 9), ("written".to_string(), 4096)]
        );

        let Outcome::Merged(Merged::Committed(committed)) = job.result.unwrap() else {
            panic!("a merge of a new pack committed a version");
        };

        assert_eq!(committed.set, "docs");
        assert_eq!(committed.version, 2);
        assert_eq!(committed.snapshot.to_hex().to_string(), "01".repeat(32));
        assert_eq!(committed.base, None);
        assert_eq!(committed.chunks, 9);
        assert_eq!(committed.fresh, 3);
        assert_eq!(committed.held, 6);
        assert_eq!(committed.bytes, 4096);
    }

    #[test]
    fn a_pack_job_carries_the_geometry_and_not_a_merge() {
        let body = r#"{"id":"aaaa","kind":"pack","state":"succeeded","started":1,"finished":2,
            "error":null,
            "result":{"set":"docs","version":3,"length":8192,"payload_start":64,
                "payload_bytes":8000,"chunks":12,
                "snapshot":"0202020202020202020202020202020202020202020202020202020202020202",
                "taken_at":"2023-11-14T22:13:20Z"},
            "progress":{}}"#;

        let job = read(body);

        let Outcome::Packed(summary) = job.result.unwrap() else {
            panic!("a pack job produced a pack summary");
        };

        assert_eq!(
            summary,
            PackSummary {
                set: "docs".to_string(),
                version: 3,
                length: 8192,
                payload_start: 64,
                payload_bytes: 8000,
                chunks: 12,
                snapshot: BlobId::from_hex("02".repeat(32)).unwrap(),
                taken_at: Timestamp::parse_utc("2023-11-14T22:13:20Z").unwrap(),
            }
        );
    }

    #[test]
    fn a_merge_of_a_pack_the_set_already_held_is_three_fields_and_a_variant() {
        // The server sends `set`, `version` and `already` and nothing else. A
        // parser that wanted the other eight would have to invent them, and every
        // one of the invented numbers would be a zero that reads as a count.
        let url = "http://127.0.0.1:8480/v1/jobs/aaaa";
        let body = br#"{"set":"docs","version":2,"already":true}"#;
        let document = Document::read(url, "a job", body).unwrap();

        assert_eq!(
            merged(&document).unwrap(),
            Merged::Already(super::MergedAlready {
                set: "docs".to_string(),
                version: 2,
            })
        );
    }

    #[test]
    fn a_result_whose_shape_does_not_match_its_kind_is_an_error() {
        // `kind` says `pack` and the object is a merge. Reading it as a pack
        // summary would fail on `length`; reading it as a merge would fail on
        // `already`. Neither is allowed to produce zeros.
        let url = "http://127.0.0.1:8480/v1/jobs/aaaa";
        let body = br#"{"set":"docs","version":2,"already":false,"chunks":9}"#;
        let document = Document::read(url, "a job", body).unwrap();

        assert!(packed(&document).is_err());
        assert!(
            merged(&document).is_err(),
            "`snapshot` is missing from this one"
        );
    }

    #[test]
    fn a_failed_job_is_an_error_carrying_the_servers_own_sentence() {
        // **The point of the whole module**, asserted on the value the error is
        // built from: the sentence is the work's own `Err`, not a rewording.
        let job = Job {
            id: "aaaa".to_string(),
            kind: Kind::Merge,
            state: State::Failed,
            started: 1,
            finished: Some(2),
            error: Some("the set at /srv/docs is locked by another merge".to_string()),
            result: None,
            progress: Vec::new(),
        };

        assert!(job.state.is_terminal());
        assert_eq!(
            failure(&job),
            "the set at /srv/docs is locked by another merge"
        );
    }

    #[test]
    fn a_cancelled_job_is_refused_too_because_it_published_nothing() {
        let job = Job {
            id: "aaaa".to_string(),
            kind: Kind::Merge,
            state: State::Cancelled,
            started: 1,
            finished: Some(2),
            error: None,
            result: None,
            progress: Vec::new(),
        };

        assert!(
            failure(&job).contains("cancelled"),
            "a cancelled merge is not a published version: {}",
            failure(&job)
        );
    }

    /// A job that has stopped is terminal whichever way it stopped, and only the
    /// two states that have not stopped are not. Named for the property rather
    /// than for the states it lists: an agent choosing what to run reads the
    /// property, and the inverse of an earlier name of this test was the one that
    /// sent it past the guard against reporting a failed job as a success.
    #[test]
    fn is_terminal_is_true_for_every_stopped_state_and_false_for_a_running_one() {
        // The two that are still running.
        assert!(!State::Queued.is_terminal());
        assert!(!State::Running.is_terminal());
        // The three that have stopped, whichever way they stopped.
        assert!(State::Succeeded.is_terminal());
        assert!(State::Failed.is_terminal());
        assert!(State::Cancelled.is_terminal());
    }

    #[test]
    fn a_word_this_client_does_not_read_is_an_error_rather_than_a_guess() {
        let url = "http://127.0.0.1:8480/v1/jobs/aaaa";

        assert_eq!(State::read(url, "running").unwrap(), State::Running);
        assert!(State::read(url, "exploded").is_err());
        assert!(Kind::read(url, "merge").is_ok());
        assert!(Kind::read(url, "restore").is_err());
    }

    #[test]
    fn a_compression_level_outside_the_range_is_refused_before_it_is_sent() {
        assert_eq!(
            MergeCompression::Reencode(0).checked().unwrap(),
            MergeCompression::Reencode(0)
        );
        assert_eq!(
            MergeCompression::Reencode(MAX_COMPRESSION_LEVEL)
                .checked()
                .unwrap(),
            MergeCompression::Reencode(MAX_COMPRESSION_LEVEL)
        );
        assert!(MergeCompression::Keep.checked().is_ok());

        let error = MergeCompression::Reencode(MAX_COMPRESSION_LEVEL + 1)
            .checked()
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            error.to_string().contains("not a compression level"),
            "the refusal names the level: {error}"
        );
    }

    #[test]
    fn a_compression_policy_is_a_word_on_the_wire() {
        // A level as a JSON number would be read by the server as the wrong
        // type: the field is text, and `keep` is not a number.
        assert_eq!(MergeCompression::Keep.wire(), "keep");
        assert_eq!(MergeCompression::Reencode(3).wire(), "3");
    }
}
