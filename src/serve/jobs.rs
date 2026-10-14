// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Long work, and how a client finds out how it is going.
//!
//! A merge of a large pack and a pack of a large version are both measured in
//! minutes, and both are I/O-bound. Neither suits an HTTP request that waits for
//! the answer: a proxy will time it out, a client cannot show progress, and
//! there is no way to stop it.
//!
//! So an operation that does not finish quickly **returns immediately with a job
//! id**, and the client asks about that job. That is one endpoint to create
//! work, one to read a job, and one to watch it.
//!
//! ## Why three ways to watch, and not one
//!
//! * `GET /v1/jobs/{id}` — a poll. Every client can do it, it survives anything,
//!   and it costs the server nothing when nobody is watching.
//! * `GET /v1/jobs/{id}/events` — Server-Sent Events. For a client that wants to
//!   show a progress bar, so it does not have to choose a poll interval.
//! * `DELETE /v1/jobs/{id}` — a cancel, for a client that changed its mind.
//!
//! They are three views of one [`JobEvent`] stream, not three mechanisms. A
//! client that polls and a client that subscribes see the same transitions in
//! the same order, because both read from the same broadcast.
//!
//! ## What a cancel actually does
//!
//! `DELETE` aborts the tokio task. Aborting drops its future, which drops its
//! locals, which runs their destructors — so an upload's temporary file is
//! removed and a set's claim file is dropped, with no unwinding code of our own.
//!
//! **And it needs no rollback, which is the property that makes this safe.** A
//! store is append-only with one commit record at the end of a run. An aborted
//! merge published no version, and left a torn tail that the next
//! `open_for_writing` reclaims. So a cancelled merge leaves no half-version and
//! nothing to undo.
//!
//! One honest limit, repeated from the oracle's analysis because it is the kind
//! of thing that is easy to promise and hard to deliver: `open_for_writing` and
//! `open_for_reading` each scan inside `spawn_blocking`, and aborting a task
//! does not cancel a blocking task. A cancel issued during the open takes effect
//! when the scan returns. The scan is over record *headers*, so it is bounded by
//! record count rather than by set size.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime},
};

use tokio::{sync::broadcast, task::JoinHandle};

use super::Json;

/// How long a finished job is kept before the oldest is dropped.
const RETENTION: Duration = Duration::from_secs(3600);

/// How many finished jobs are kept, whichever bound bites first.
const RETAINED: usize = 256;

/// How many events a late subscriber can still be given.
///
/// A client that connects to `/events` after a job has been running gets the
/// state it missed rather than an empty stream that looks like a job with no
/// progress. Bounded, because a job that ran for an hour has more events than
/// anyone needs replayed.
const REPLAY: usize = 64;

/// How many events one job's live channel holds before a slow reader lags.
///
/// **Four times [`REPLAY`], and that ratio is the point.** The history is the
/// last `REPLAY` events of one job, so a reader that falls behind by more than
/// this bound has already fallen behind by more than the crate promises to
/// replay. One channel held every job's events before, so a busy job pushed an
/// unrelated subscriber past a bound it never spent.
const CHANNEL: usize = 4 * REPLAY;

/// A job's identity.
///
/// BLAKE3 over the process id, the time the process started and a counter. Three
/// things rather than one because an id only has to be unique among the jobs this
/// process is running, and it has to stay unique when two requests arrive in the
/// same nanosecond. A random id would need a dependency for no gain; a counter
/// alone would repeat across a restart and a stale bookmark would name a
/// different job.
///
/// 24 bytes, hex. The same length and alphabet as a [`crate::storage::BlobId`],
/// which is what a client will already have machinery for.
/// Not `Ord`, and that is [`blake3::Hash`]'s doing: a hash has deliberately
/// no order, because hash order is not content order and pretending otherwise
/// would be a lie a caller could come to rely on. Jobs are listed by when they
/// started, not by their id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JobId(blake3::Hash);

impl JobId {
    /// A fresh id.
    #[must_use]
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};

        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let mut hasher = blake3::Hasher::new();
        hasher.update(&std::process::id().to_le_bytes());
        hasher.update(&nanos().to_le_bytes());
        hasher.update(&COUNTER.fetch_add(1, Ordering::Relaxed).to_le_bytes());

        Self(hasher.finalize())
    }

    /// The id as hex, which is how a client passes it back.
    #[must_use]
    pub fn hex(&self) -> String {
        self.0.to_hex().to_string()
    }

    /// An id from the hex a client sent.
    ///
    /// # Errors
    ///
    /// Returns an error if `text` is not 64 hex characters. Reported as
    /// `InvalidFilename`, because it is the same class of thing as a bad set
    /// name: an identifier that is not one.
    pub fn parse(text: &str) -> std::io::Result<Self> {
        let bytes = <blake3::Hash>::from_hex(text).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidFilename,
                format!("{text:?} is not a job id. A job id is 64 hexadecimal characters"),
            )
        })?;

        Ok(Self(bytes))
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.hex())
    }
}

/// Nanoseconds since the epoch, saturating rather than panicking.
///
/// The clock can go backwards. An id does not care, and a job's timestamps are
/// only ever compared with each other, so a wrapped subtraction is not an error
/// worth propagating.
fn nanos() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

/// The id an unset handle would carry.
///
/// Not read anywhere, and required: `clippy::new_without_default` is in the `all`
/// group this crate denies, so a `new()` without a `Default` is a build failure.
/// That is the same constraint that removed `assert_eq!(x, [])` from four tests
/// when `serde_json` arrived — a lint the author does not control deciding the
/// shape of the tree.
impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

/// Where a job is in its life.
///
/// A closed enum for the same reason [`crate::progress::Phase`] is one: the set
/// is small, and a `String` here would be a place a caller could misspell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    /// Accepted, not started. Present for as long as the task takes to be
    /// spawned, which is short, and which a client polling in a loop may well
    /// see.
    Queued,
    /// Running.
    Running,
    /// Finished, and produced a result.
    Succeeded,
    /// Finished, and did not.
    Failed,
    /// Stopped by a `DELETE`.
    Cancelled,
}

impl State {
    /// The word this service reports, in lower case.
    ///
    /// `pub(crate)` rather than private because
    /// [`crate::telemetry::metrics::record_job`] reports the same word as an
    /// attribute. Two spellings of one state is a dashboard that splits a
    /// failure count in two, so both are this.
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Whether this is a state a job will not leave.
    ///
    /// A cancel against a finished job is a `409`, not a `202`: a client that
    /// lost the race deserves a definite answer, and "it already succeeded" is
    /// more use than a `202` for something that will not happen.
    const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

/// Every [`JobEvent`] variant, declared once.
///
/// **The enum, its wire names, and one example of it all come from this one
/// list.** The three used to be written out in three files. The compiler forced
/// one edit of them and no other, so a new variant could reach the wire with a
/// name that no list held and no document described, and every test passed. A
/// probe did exactly that: it added a `note` variant with its render arm, left
/// `EVENT_NAMES` at four names, and both the render test and the document test
/// stayed green.
///
/// Rust cannot list an enum's variants at run time, so a list is what lists
/// them. One arm per variant, and the arm carries the payload type, the `event:`
/// name, and a value of that type. Adding a variant now costs one arm and
/// cannot leave the sample list behind.
///
/// **The sample is the payload, not a built event, and that is the part that
/// makes it a guard.** An earlier version took a whole `JobEvent` expression,
/// so an arm could name one variant and sample another and still compile. The
/// render arm for that variant would then never run in any test. Writing
/// `Self::$variant($value)` puts the two in the same type: a payload that does
/// not fit the variant is a compile error, and no sample can reach for a
/// different variant.
///
/// `render_event` in `handlers.rs` still matches on the enum and still writes
/// the name a second time. That match is exhaustive, so the compiler demands the
/// new arm, and `every_rendered_event_is_a_named_one` checks that the two names
/// agree. A macro cannot write a payload, so it cannot remove that repetition.
///
/// **`strum` would list these variants too, and it is the rejected option
/// rather than the chosen one.** `strum::VariantNames` gives a derive that
/// emits a `VARIANTS` array. It trades a dependency and a proc-macro for the
/// list above, and it buys nothing here: this list has to carry a payload value
/// and a wire name per arm, and neither has anywhere to go in a derive. The
/// crate rejects `utoipa` and a `build.rs` emitter for the `OpenAPI` document
/// for the same reason it rejects this: a generated answer is harder to read in
/// a diff than the thing it replaced.
macro_rules! job_events {
    (
        $(
            $(#[doc = $doc:literal])*
            $variant:ident($payload:ty) => $name:literal, $value:expr;
        )*
    ) => {
        /// One thing that changed about a job.
        #[derive(Debug, Clone, PartialEq)]
        pub enum JobEvent {
            $(
                $(#[doc = $doc])*
                $variant($payload),
            )*
        }

        impl JobEvent {
            /// Every `event:` name the SSE renderer can put on the wire.
            ///
            /// Read from the declaration above, so it cannot name a variant that
            /// does not exist and cannot miss one that does.
            ///
            /// Crate-private on purpose. `EVENT_NAMES` in `handlers.rs` is the
            /// export the document test reads, and this is where it comes from.
            pub(crate) const NAMES: &'static [&'static str] = &[$($name),*];

            /// One event of every variant, in declaration order.
            ///
            /// A test renders each and reads the name off the frame. Nothing
            /// written by hand decides what "every" means.
            #[cfg(test)]
            #[must_use]
            pub(crate) fn samples() -> Vec<Self> {
                vec![$( Self::$variant($value) ),*]
            }
        }
    };
}

job_events! {
    /// The job's state changed.
    State(State) => "state", State::Running;

    /// The job's counters moved.
    ///
    /// Integers keyed by the name [`crate::progress::Counter`] reports under, so
    /// a client renders a progress bar without knowing this crate's enum.
    Progress(Vec<(String, u64)>) => "progress", vec![("chunks".to_string(), 3)];

    /// The job finished, and this is its result.
    Result(Json) => "result", Json::Null;

    /// The job finished, and this is why it did not.
    ///
    /// **The counterpart to [`JobEvent::Result`], and it exists because a
    /// watcher otherwise cannot learn anything.** `Result` was broadcast on
    /// success only, so a client on `/events` was sent `state: failed` and then
    /// nothing: the `Err` was stored on the job and was reachable only by polling
    /// `GET /v1/jobs/{id}`, which is the endpoint a client falls back to precisely
    /// when the stream has not told it what happened. The sentence is the same one
    /// the poll reports — the `Err` the work returned, verbatim, not a rewording —
    /// so the two cannot disagree.
    Error(String) => "error", "a reason".to_string();
}

/// A job, as a client sees it.
#[derive(Debug, Clone)]
pub struct Job {
    /// Which job this is.
    pub id: JobId,
    /// What it is doing: `pack` or `merge`.
    pub kind: &'static str,
    /// Where it is.
    pub state: State,
    /// When it started, in nanoseconds since the epoch.
    pub started: u64,
    /// When it finished, or `None` while it has not.
    pub finished: Option<u64>,
    /// Why it failed, or `None`.
    pub error: Option<String>,
    /// What it produced, or `None`.
    pub result: Option<Json>,
    /// Its counters, by the name `Counter` reports under.
    pub progress: Vec<(String, u64)>,
}

impl Job {
    /// The job as the JSON a client reads.
    #[must_use]
    pub fn to_json(&self) -> Json {
        Json::object(vec![
            ("id", Json::string(self.id.hex())),
            ("kind", Json::string(self.kind)),
            ("state", Json::string(self.state.name())),
            ("started", Json::int(self.started)),
            ("finished", Json::opt(self.finished.map(Json::int))),
            ("error", Json::opt(self.error.clone().map(Json::Str))),
            ("result", Json::opt(self.result.clone())),
            (
                "progress",
                Json::Object(
                    self.progress
                        .iter()
                        .map(|(name, value)| (name.clone(), Json::int(*value)))
                        .collect(),
                ),
            ),
        ])
    }
}

/// Every job this process is running or has run.
///
/// Cloned into every request as `web::Data`; the map is behind an
/// [`Arc`], so a clone shares rather than copies.
#[derive(Debug, Clone)]
pub struct Jobs {
    inner: Arc<Inner>,
}

/// An empty registry.
///
/// Also `clippy::new_without_default`. See [`JobId::default`].
impl Default for Jobs {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct Inner {
    jobs: Mutex<HashMap<JobId, Job>>,
    handles: Mutex<HashMap<JobId, JoinHandle<()>>>,
    /// The live event channel of each job, one per job.
    ///
    /// **One channel per job, not one for the process.** One channel sent a
    /// subscriber the events of every other job, and [`JobEvent`] carries no job
    /// id, so the consumer had nothing to filter them out on. The same channel
    /// also shared one capacity, so a busy job made an unrelated subscriber lag
    /// and `handlers.rs` ended that stream.
    ///
    /// Taken **after** [`Inner::replay`], never before, so the two locks are
    /// always held in one order.
    events: Mutex<HashMap<JobId, broadcast::Sender<JobEvent>>>,
    /// Recent events per job, so a late subscriber is not told nothing.
    ///
    /// **This lock also covers the `send` in `broadcast`.** The history and the
    /// live stream are two views of one event, and they can only be taken
    /// consistently if both the sending and the reading happen under one lock.
    /// See [`Jobs::subscribe`].
    replay: Mutex<HashMap<JobId, Vec<JobEvent>>>,
}

impl Inner {
    /// The live sender of one job, made if the job has not broadcast yet.
    ///
    /// **Called with `replay` already held, and the receiver taken here.** That
    /// is what keeps each event exactly once: [`Jobs::subscribe`] creates the
    /// receiver and reads the history under one guard, so an event either lands
    /// before the receiver exists or after it, never between the two.
    fn sender(&self, id: JobId) -> broadcast::Sender<JobEvent> {
        // A poisoned map must not silence the live stream, so a poisoned lock
        // still yields the sender it holds.
        let mut events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        events.entry(id).or_insert_with(new_channel).clone()
    }
}

/// A new live channel, for one job that has not broadcast yet.
fn new_channel() -> broadcast::Sender<JobEvent> {
    broadcast::channel(CHANNEL).0
}

impl Jobs {
    /// A registry with no jobs in it.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                jobs: Mutex::new(HashMap::new()),
                handles: Mutex::new(HashMap::new()),
                events: Mutex::new(HashMap::new()),
                replay: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Start `work` as a job, and return its id.
    ///
    /// `work` is handed the job so it can report progress. The task is spawned
    /// immediately, so a job is `Queued` only for as long as the scheduler takes.
    ///
    /// **`work` returns a `Result`, and the `Err` is what makes the job fail.**
    ///
    /// This used to return the `Json` outright, with a failure carried as an
    /// `{"error": ...}` object inside it, and `spawn` set `Succeeded` either way.
    /// So a pack that could not be resolved produced
    ///
    /// ```text
    /// { "state": "succeeded",
    ///   "error": "no usable version in /srv/docs",
    ///   "result": { "error": "no usable version in /srv/docs" } }
    /// ```
    ///
    /// which is worse than a plain wrong answer, because `state` and `error`
    /// disagree and the field a client checks first is the false one. A second
    /// escape hatch — `JobHandle::fail`, which set `error` without touching
    /// `state` — existed for the same reason and is gone with this.
    ///
    /// **There is no cleanup path to get wrong**: dropping the future on a cancel
    /// runs the destructors inside it.
    ///
    /// ## What a panic in the work does
    ///
    /// Nothing observes the [`JoinHandle`], so a panic inside `work` unwinds the
    /// task and the job is left `running` for ever with its handle still in
    /// `handles`. **That is left as it is deliberately**, and the reason is that
    /// the alternative is a watchdog task per job — a second task per job, and a
    /// second state machine for the abort race — bought for a failure this
    /// codebase does not produce. `unwrap` and `expect` are denied crate-wide, so
    /// a job future's ways of going wrong are the `Err` values it returns, and a
    /// panic here is a bug that belongs in a log rather than one the server
    /// papers over with a sentence it invented.
    ///
    /// It is not stranded either. `handles` is left holding the handle, so
    /// `DELETE /v1/jobs/{id}` finds it, aborts it (a no-op on a task that has
    /// already finished) and marks the job `cancelled`.
    pub fn spawn<F, Fut>(&self, kind: &'static str, work: F) -> JobId
    where
        F: FnOnce(JobHandle) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<Json, String>> + Send + 'static,
    {
        let id = JobId::new();
        let started = nanos();

        self.inner.jobs.lock().map_or_else(
            |_| {
                // A poisoned map means an earlier panic. The job is still
                // reported as accepted, because refusing to accept work after an
                // unrelated panic would be a worse answer than running it
                // untracked.
                tracing::error!("the job registry was poisoned by an earlier panic");
            },
            |mut jobs| {
                jobs.insert(
                    id,
                    Job {
                        id,
                        kind,
                        state: State::Queued,
                        started,
                        finished: None,
                        error: None,
                        result: None,
                        progress: Vec::new(),
                    },
                );
            },
        );

        // `Running` is **announced** here and **written** at the top of the task,
        // once it has actually been scheduled. So `Queued` lasts as long as the
        // scheduler takes, which is what `State::Queued` claims, and a poller
        // stops reading `queued` for the whole length of the work — which is the
        // lie this fixes: the write used not to happen at all.
        self.broadcast(id, JobEvent::State(State::Running));

        let this = self.clone();
        let handle = tokio::spawn(async move {
            // **The span the progress layer matches on.** It lives here rather
            // than in the handler because the id does not exist until `spawn` has
            // run, and because putting it here makes it impossible for a new kind
            // of job to forget it: a job with no span reports no progress and
            // nothing fails.
            //
            // `blulib::progress` events emitted inside this span belong to this
            // job. The same events from another process, or from a `blu
            // restore` running in this one, are in no job span and are dropped.
            let job_span = tracing::info_span!(
                super::observe::SPAN,
                job = %id,
                kind = kind,
            );
            let _entered = job_span.enter();

            // **The state is written, not only broadcast.** It used to be
            // announced from `spawn` and never stored, so the only write that ever
            // set `job.state` set a terminal one: a client that polls — the
            // endpoint that has to keep working when a stream has been dropped —
            // read `"state": "queued"` for the entire length of a merge measured
            // in minutes.
            //
            // Written here rather than beside the broadcast in `spawn`, so
            // `Queued` is a state a client can actually observe: it lasts until
            // the task is first polled, which is what `State::Queued` promises.
            //
            // **Conditional, for the same reason `cancel`'s write is.** A
            // `DELETE` that arrived before the task was first polled has already
            // earned the job a terminal state, and a cancelled job turning
            // running afterwards would be the same class of lie this whole change
            // is about.
            if let Ok(mut jobs) = this.inner.jobs.lock()
                && let Some(job) = jobs.get_mut(&id)
                && !job.state.is_terminal()
            {
                job.state = State::Running;
            }

            let handle = JobHandle {
                id,
                jobs: this.clone(),
            };

            let outcome = work(handle).await;

            let finished = nanos();

            // One lock, one decision. `state`, `error` and `result` are set
            // together so no reader can observe a combination that never happened.
            let (state, result) = match &outcome {
                Ok(value) => (State::Succeeded, Some(value.clone())),
                Err(_) => (State::Failed, None),
            };

            if let Ok(mut jobs) = this.inner.jobs.lock()
                && let Some(job) = jobs.get_mut(&id)
            {
                job.state = state;
                job.finished = Some(finished);
                job.error = outcome.as_ref().err().cloned();
                job.result = result;

                // **Beside the write, not after it.** This is the only place a
                // job earns `succeeded` or `failed`, so a count taken from the
                // branch rather than from the write would be counting a terminal
                // state the job never had.
                crate::telemetry::metrics::record_job(kind, state.name());
            }

            if let Ok(mut handles) = this.inner.handles.lock() {
                handles.remove(&id);
            }

            this.broadcast(id, JobEvent::State(state));
            match outcome {
                Ok(value) => this.broadcast(id, JobEvent::Result(value)),
                // **The reason, on the stream.** The state write above already
                // carries it for a poller; without this a watcher was told
                // `failed` and nothing else.
                Err(reason) => this.broadcast(id, JobEvent::Error(reason)),
            }
        });

        if let Ok(mut handles) = self.inner.handles.lock() {
            handles.insert(id, handle);
        }

        self.retire();

        id
    }

    /// One job.
    #[must_use]
    pub fn get(&self, id: JobId) -> Option<Job> {
        self.inner
            .jobs
            .lock()
            .ok()
            .and_then(|jobs| jobs.get(&id).cloned())
    }

    /// Every job, oldest first.
    #[must_use]
    pub fn list(&self) -> Vec<Job> {
        let Ok(jobs) = self.inner.jobs.lock() else {
            return Vec::new();
        };

        let mut out: Vec<Job> = jobs.values().cloned().collect();
        out.sort_by_key(|job| job.started);
        out
    }

    /// Stop a job that has not finished.
    ///
    /// # Errors
    ///
    /// Returns an error if no job has this id, or if the job has already
    /// finished. Both are reported as [`std::io::ErrorKind::AlreadyExists`], which
    /// is a `409` on the wire: a client that lost the race deserves a definite
    /// answer, and neither case is a job that can still be stopped.
    ///
    /// Returns an error if the handle registry's lock was poisoned by an
    /// earlier panic. That one is not `AlreadyExists` and becomes a `500`.
    pub fn cancel(&self, id: JobId) -> std::io::Result<()> {
        if self
            .inner
            .jobs
            .lock()
            .map_or(true, |jobs| match jobs.get(&id) {
                None => true,
                Some(job) => job.state.is_terminal(),
            })
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("job {id} has already finished. There is nothing to cancel"),
            ));
        }

        let handle = self
            .inner
            .handles
            .lock()
            .map_err(|_| {
                std::io::Error::other("the job registry was poisoned by an earlier panic")
            })?
            .remove(&id);

        let Some(handle) = handle else {
            return Ok(());
        };

        handle.abort();

        // **Re-checked under the same lock that writes.** The check above and this
        // write are not one atomic step, so a job that finished in between would
        // otherwise be relabelled `cancelled` after it had succeeded — the same
        // class of lie as the one this whole change is about, one race along.
        // A job already in a terminal state keeps the state it earned.
        let mut cancelled_kind = None;
        if let Ok(mut jobs) = self.inner.jobs.lock()
            && let Some(job) = jobs.get_mut(&id)
            && !job.state.is_terminal()
        {
            job.state = State::Cancelled;
            job.finished = Some(nanos());
            cancelled_kind = Some(job.kind);
        }

        if let Some(kind) = cancelled_kind {
            self.broadcast(id, JobEvent::State(State::Cancelled));
            // Beside the broadcast, and inside the same branch: a cancel that
            // lost the race against a job that had already finished marked
            // nothing above, and must count nothing here either.
            crate::telemetry::metrics::record_job(kind, State::Cancelled.name());
        }

        Ok(())
    }

    /// Record that a job's counters moved, from outside.
    ///
    /// This is the seam [`observe`](super::observe) uses, and it is one line
    /// rather than a second event type: the layer has a job id and a bag of
    /// counters, which is exactly what a job document shows, so it updates both
    /// the stored figures and the broadcast through here. That is what makes a
    /// client that polls and a client that watches agree.
    pub(crate) fn emit_progress(&self, id: JobId, counters: Vec<(String, u64)>) {
        if let Ok(mut jobs) = self.inner.jobs.lock()
            && let Some(job) = jobs.get_mut(&id)
        {
            for (name, value) in &counters {
                match job
                    .progress
                    .iter_mut()
                    .find(|(existing, _)| existing == name)
                {
                    Some(slot) => slot.1 = *value,
                    None => job.progress.push((name.clone(), *value)),
                }
            }
        }

        self.broadcast(id, JobEvent::Progress(counters));
    }

    /// A receiver for this job's events, plus what a late subscriber missed.
    ///
    /// **Both, from one call, and that is the point.** Separating them made the
    /// caller check existence separately, which is two locks and two clones for
    /// one answer.
    ///
    /// ## Each event exactly once, which took one lock
    ///
    /// The receiver and the history are both taken **under the same lock that
    /// records and sends**. So an event either lands entirely before this
    /// returns — in which case it is in
    /// the history and was sent before the receiver existed, and it is replayed
    /// once — or entirely after, and it is on the receiver and not in the
    /// history. The two cannot disagree.
    ///
    /// Taking the receiver first and snapshotting the history second left a
    /// window between them with two ways to be wrong at once: an event broadcast
    /// inside it arrived on **both** and a client was told it twice, and an event
    /// sent just before the `subscribe` but pushed to the history just after the
    /// snapshot was on **neither** and reached nobody at all.
    ///
    /// What is still bounded, and is not claimed here to be more: the history is
    /// the last `REPLAY` events, and the receiver has its own job's finite
    /// channel. A subscriber that falls further behind than that is told the
    /// stream lagged and can poll `GET /v1/jobs/{id}` for what it missed. The
    /// channel belongs to one job, so only that job's events can make it lag.
    ///
    /// # Errors
    ///
    /// Returns an error if no job has this id.
    pub fn subscribe(
        &self,
        id: JobId,
    ) -> std::io::Result<(broadcast::Receiver<JobEvent>, Vec<JobEvent>)> {
        let known = self
            .inner
            .jobs
            .lock()
            .is_ok_and(|jobs| jobs.contains_key(&id));

        if !known {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no job {id}"),
            ));
        }

        // **The pair, under one lock, in this order.** The receiver is created
        // first and the history read second, but nothing can run between the two
        // while this guard is held — which is the whole of the guarantee above.
        let Ok(replay) = self.inner.replay.lock() else {
            // A poisoned history is still worth a live receiver: a subscriber
            // gets the events from here on and loses only the replay.
            return Ok((self.inner.sender(id).subscribe(), Vec::new()));
        };

        let receiver = self.inner.sender(id).subscribe();
        let history = replay.get(&id).cloned().unwrap_or_default();

        Ok((receiver, history))
    }

    /// Record an event, and remember the last few for a late subscriber.
    ///
    /// **One lock across the send and the record**, and that is what makes
    /// [`subscribe`](Self::subscribe) able to promise each event exactly once.
    /// The two operations are only atomic with respect to each other while the
    /// same guard covers both.
    fn broadcast(&self, id: JobId, event: JobEvent) {
        let Ok(mut replay) = self.inner.replay.lock() else {
            // A poisoned history must not also silence the live stream, so the
            // send still happens — it is only the replay that is lost.
            let _ = self.inner.sender(id).send(event);
            return;
        };

        // A send with nobody listening is not a failure. Every event here may
        // have no subscriber at all, which is the normal case for a job whose
        // client polls.
        let _ = self.inner.sender(id).send(event.clone());

        // `entry().or_default()` rather than `get_mut()`. `get_mut` returns
        // `None` for a job that has not recorded an event yet, so the **first**
        // event of every job was dropped and the history began at the second — and
        // a late subscriber was never told a job had started.
        let history = replay.entry(id).or_default();
        history.push(event);
        if history.len() > REPLAY {
            let excess = history.len() - REPLAY;
            history.drain(..excess);
        }
    }

    /// Drop finished jobs that are too old or too many.
    fn retire(&self) {
        let now = nanos();
        let Ok(mut jobs) = self.inner.jobs.lock() else {
            return;
        };

        // Kept: anything still running, whatever its age. A job that started an
        // hour ago and has not finished is not garbage — a merge of a very large
        // pack is still a merge that is going.
        //
        // Dropped: a *finished* job older than the retention. The age is measured
        // from `started`, not from `finished`, because `started` is set once and
        // never rewritten, and a long job must not become un-droppable by
        // finishing late.
        let window = u64::try_from(RETENTION.as_nanos()).unwrap_or(u64::MAX);
        jobs.retain(|_, job| !job.state.is_terminal() || now.saturating_sub(job.started) < window);

        // The count bound, applied only if the age bound left too many. Both are
        // constants rather than flags on purpose: a job registry that can be
        // configured into unbounded growth is not a knob worth having.
        let mut by_age: Vec<JobId> = jobs
            .iter()
            .filter(|(_, job)| job.state.is_terminal())
            .map(|(id, _)| *id)
            .collect();

        if by_age.len() > RETAINED {
            by_age.sort_by_key(|id| jobs[id].started);

            // **The ids being removed, collected as they go.** An earlier version
            // swept `jobs.keys()` *after* the removals, which is the set of jobs
            // that SURVIVED — so retiring one job destroyed the replay history of
            // every other job and leaked the retired one. It read as cleanup and
            // was the opposite.
            let mut retired = Vec::new();
            for id in &by_age[..by_age.len() - RETAINED] {
                retired.push(*id);
                jobs.remove(id);
            }

            if !retired.is_empty()
                && let Ok(mut replay) = self.inner.replay.lock()
            {
                for id in &retired {
                    replay.remove(id);
                }

                // **Beside the replay, under the same guard.** A retired job's
                // channel goes with the job and its history, so the map of
                // channels does not grow for ever.
                //
                // **At retirement and not at a terminal state.** Dropping the
                // channel when the job finishes would make `subscribe` answer
                // `NotFound` for a finished job and lose the replay a client
                // reads after the fact.
                let mut events = self
                    .inner
                    .events
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                for id in &retired {
                    events.remove(id);
                }
            }
        }
    }
}

/// What a running job is handed to report through.
#[derive(Debug, Clone)]
pub struct JobHandle {
    id: JobId,
    jobs: Jobs,
}

impl JobHandle {
    /// Which job this is.
    #[must_use]
    pub fn id(&self) -> JobId {
        self.id
    }

    /// Record that a counter moved.
    ///
    /// Also updates the job's stored figures, so a client that polls sees the
    /// same numbers as one that subscribed.
    pub fn progress(&self, counters: &[(&str, u64)]) {
        if let Ok(mut jobs) = self.jobs.inner.jobs.lock()
            && let Some(job) = jobs.get_mut(&self.id)
        {
            for (name, value) in counters {
                match job
                    .progress
                    .iter_mut()
                    .find(|(existing, _)| existing == name)
                {
                    Some(slot) => slot.1 = *value,
                    None => job.progress.push(((*name).to_string(), *value)),
                }
            }
        }

        self.jobs.broadcast(
            self.id,
            JobEvent::Progress(
                counters
                    .iter()
                    .map(|(name, value)| ((*name).to_string(), *value))
                    .collect(),
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{JobEvent, JobId, Jobs, State};
    use tokio::sync::broadcast::error::TryRecvError;

    #[tokio::test]
    async fn a_subscriber_does_not_receive_another_jobs_events() {
        // **One stream per job, not one per process.** A subscriber to the first
        // job was sent the second job's counters, because both jobs broadcast on
        // one channel and [`JobEvent`] carries no job id, so the consumer had
        // nothing to filter on.
        let jobs = Jobs::new();
        let watched = jobs.spawn("pack", |_| async {
            std::future::pending::<()>().await;
            Ok(super::Json::Null)
        });
        let other = jobs.spawn("merge", |_| async {
            std::future::pending::<()>().await;
            Ok(super::Json::Null)
        });

        let (mut receiver, _history) = jobs.subscribe(watched).unwrap();

        // The watched job has already broadcast `running`. Taken off here, so
        // what the assertion below reports is the other job's events alone.
        while receiver.try_recv().is_ok() {}

        jobs.emit_progress(other, vec![("chunks".to_string(), 42)]);

        let first = receiver.try_recv();
        assert!(
            matches!(&first, Err(TryRecvError::Empty)),
            "a subscriber must not receive another job's events: {first:?}"
        );
    }

    #[tokio::test]
    async fn another_jobs_events_cannot_lag_this_subscribers_stream() {
        // **The shared capacity was a shared failure.** One channel held the
        // events of every job, so a busy job pushed an unrelated subscriber past
        // the bound. `handlers.rs` maps that lag to a closed stream, so the
        // subscriber lost a stream it never asked to lose.
        //
        // The count clears the capacity the whole process used to share, so this
        // fails before the fix as well as after it.
        const FOREIGN_EVENTS: u32 = 2_048;

        let jobs = Jobs::new();
        let watched = jobs.spawn("pack", |_| async {
            std::future::pending::<()>().await;
            Ok(super::Json::Null)
        });
        let other = jobs.spawn("merge", |_| async {
            std::future::pending::<()>().await;
            Ok(super::Json::Null)
        });

        let (mut receiver, _history) = jobs.subscribe(watched).unwrap();
        while receiver.try_recv().is_ok() {}

        for value in 0..FOREIGN_EVENTS {
            jobs.emit_progress(other, vec![("chunks".to_string(), u64::from(value))]);
        }

        let first = receiver.try_recv();
        assert!(
            matches!(&first, Err(TryRecvError::Empty)),
            "another job's events must neither arrive nor lag this stream: {first:?}"
        );
    }

    #[tokio::test]
    async fn a_job_starts_queued_and_ends_succeeded() {
        let jobs = Jobs::new();
        let id = jobs.spawn("pack", |_| async { Ok(super::Json::string("done")) });

        // Wait for it, without polling in a loop the test could hang on.
        let mut seen = None;
        for _ in 0..200 {
            if let Some(job) = jobs.get(id)
                && job.state.is_terminal()
            {
                seen = Some(job);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let job = seen.expect("the job should have finished");
        assert_eq!(job.state, State::Succeeded);
        assert_eq!(job.kind, "pack");
        assert_eq!(job.result, Some(super::Json::string("done")));
        assert!(job.finished.is_some());
    }

    #[tokio::test]
    async fn progress_reaches_a_poller() {
        // The counters land on the job itself, not only on the event stream, so a
        // client that polls sees the same numbers as one that subscribes.
        let jobs = Jobs::new();
        let id = jobs.spawn("merge", |handle| async move {
            handle.progress(&[("chunks", 12), ("bytes", 4096)]);
            Ok(super::Json::Null)
        });

        let mut job = None;
        for _ in 0..200 {
            if let Some(current) = jobs.get(id)
                && current.state.is_terminal()
            {
                job = Some(current);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let job = job.expect("the job should have finished");
        assert_eq!(
            job.progress,
            vec![("chunks".to_string(), 12), ("bytes".to_string(), 4096)]
        );
    }

    #[tokio::test]
    async fn a_job_json_carries_every_field_a_client_reads() {
        let jobs = Jobs::new();
        let id = jobs.spawn("pack", |_| async { Ok(super::Json::string("ok")) });

        for _ in 0..200 {
            if jobs.get(id).is_some_and(|job| job.state.is_terminal()) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let document = jobs.get(id).unwrap().to_json().to_string();

        for field in [
            "\"id\"",
            "\"kind\"",
            "\"state\"",
            "\"started\"",
            "\"finished\"",
            "\"error\"",
            "\"result\"",
            "\"progress\"",
        ] {
            assert!(
                document.contains(field),
                "{field} is missing from {document}"
            );
        }
    }

    #[tokio::test]
    async fn cancelling_a_finished_job_says_so() {
        // A `409` rather than a `202`. A client that lost the race deserves a
        // definite answer, and "it already succeeded" is more use than a `202`
        // for something that will not happen.
        let jobs = Jobs::new();
        let id = jobs.spawn("pack", |_| async { Ok(super::Json::Null) });

        for _ in 0..200 {
            if jobs.get(id).is_some_and(|job| job.state.is_terminal()) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let error = jobs.cancel(id).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
    }

    #[tokio::test]
    async fn cancelling_a_running_job_stops_it() {
        // The property that makes cancelling safe at all: the work runs inside a
        // tokio task, so an abort drops the future and runs whatever destructors
        // it held. Nothing here needs a rollback path.
        let jobs = Jobs::new();
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let watched = std::sync::Arc::clone(&flag);
        let id = jobs.spawn("pack", move |_| async move {
            struct Guard(std::sync::Arc<std::sync::atomic::AtomicBool>);
            impl Drop for Guard {
                fn drop(&mut self) {
                    self.0.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }

            let _guard = Guard(watched);
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        jobs.cancel(id).unwrap();

        for _ in 0..200 {
            if flag.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        assert!(
            flag.load(std::sync::atomic::Ordering::SeqCst),
            "aborting the task must have dropped its future"
        );
        assert_eq!(jobs.get(id).unwrap().state, State::Cancelled);
    }

    #[test]
    fn a_job_id_round_trips_through_hex() {
        // A client holds the id as a string and sends it back, so this is the
        // shape the whole job resource depends on.
        let id = JobId::new();

        assert_eq!(JobId::parse(&id.hex()).unwrap(), id);
        assert_eq!(id.hex().len(), 64);
    }

    #[test]
    fn two_job_ids_are_different() {
        // Counter and clock together. A counter alone would repeat two requests
        // in the same nanosecond, and a repeated id would name two jobs at once.
        let first = JobId::new();
        let second = JobId::new();

        assert_ne!(first, second);
    }

    #[test]
    fn a_bad_job_id_says_what_one_looks_like() {
        let error = JobId::parse("not-a-job").unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidFilename);
        assert!(error.to_string().contains("64 hexadecimal"));
    }

    #[tokio::test]
    async fn a_late_subscriber_is_given_what_it_missed() {
        // Otherwise a client that connects after a job has been running is told
        // a job that has made no progress.
        let jobs = Jobs::new();
        let id = jobs.spawn("pack", |_| async { Ok(super::Json::Null) });

        for _ in 0..200 {
            if jobs.get(id).is_some_and(|job| job.state.is_terminal()) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let (_receiver, replay) = jobs.subscribe(id).unwrap();
        assert!(
            replay
                .iter()
                .any(|event| matches!(event, JobEvent::State(State::Succeeded))),
            "a late subscriber must be told how the job ended"
        );
    }

    #[tokio::test]
    async fn subscribing_to_a_job_that_is_not_there_is_not_found() {
        let jobs = Jobs::new();

        assert_eq!(
            jobs.subscribe(JobId::new()).unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
    }

    #[tokio::test]
    async fn a_job_that_fails_reports_failed_and_not_succeeded() {
        // **The lie this test exists for.** `spawn` used to set `Succeeded`
        // unconditionally and carry the failure as an `{"error": ...}` object in
        // the result, so a failed job read `{state: "succeeded", error: "..."}` —
        // two fields disagreeing, with the one a client checks first being the
        // false one.
        let jobs = Jobs::new();
        let id = jobs.spawn("pack", |_| async {
            Err("no usable version in /srv/docs".to_string())
        });

        let mut job = None;
        for _ in 0..200 {
            if let Some(current) = jobs.get(id)
                && current.state.is_terminal()
            {
                job = Some(current);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let job = job.expect("the job should have finished");
        assert_eq!(job.state, State::Failed);
        assert_eq!(job.error.as_deref(), Some("no usable version in /srv/docs"));
        assert_eq!(
            job.result, None,
            "a failed job must not also carry a result"
        );
    }

    #[tokio::test]
    async fn a_failed_job_document_never_says_succeeded() {
        // The same property as the bytes a client reads, because the fields are
        // the contract and a disagreement between them is the failure mode.
        let jobs = Jobs::new();
        let id = jobs.spawn("merge", |_| async {
            Err("refusing: already locked".to_string())
        });

        for _ in 0..200 {
            if jobs.get(id).is_some_and(|job| job.state.is_terminal()) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let document = jobs.get(id).unwrap().to_json().to_string();

        assert!(document.contains(r#""state":"failed""#), "{document}");
        assert!(
            !document.contains(r#""state":"succeeded""#),
            "a failed job must not read as succeeded: {document}"
        );
        assert!(!document.contains(r#""result":{ "#), "{document}");
    }

    #[tokio::test]
    async fn cancelling_does_not_relabel_a_job_that_already_finished() {
        // **The same lie, one race along.** `cancel` checks the state, then
        // aborts, then writes `Cancelled` — three steps, and a job that finished
        // between the check and the write was being marked `cancelled` after it
        // had succeeded. The write is now conditional, so a job already in a
        // terminal state keeps the state it earned.
        let jobs = Jobs::new();
        let id = jobs.spawn("pack", |_| async { Ok(super::Json::Null) });

        for _ in 0..200 {
            if jobs.get(id).is_some_and(|job| job.state.is_terminal()) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        // Already finished, so the handle is gone and the state must survive.
        let _ = jobs.cancel(id);

        assert_eq!(jobs.get(id).unwrap().state, State::Succeeded);
    }

    #[tokio::test]
    async fn a_watcher_is_told_why_a_job_failed() {
        // **The gap this test is for.** `Result` was broadcast only on `Ok`, so a
        // client on the event stream was sent `state: failed` and then silence,
        // and had to poll `GET /v1/jobs/{id}` to learn the reason — which is the
        // endpoint it falls back to precisely when the stream has not told it
        // anything.
        let jobs = Jobs::new();
        let id = jobs.spawn("pack", |_| async {
            Err("no usable version in /srv/docs".to_string())
        });

        let (mut receiver, history) = jobs.subscribe(id).unwrap();

        // Wait for the failure, draining the stream until it arrives.
        let mut seen = history;
        for _ in 0..200 {
            if seen
                .iter()
                .any(|event| matches!(event, JobEvent::Error(reason) if reason.contains("no usable version")))
            {
                break;
            }
            match receiver.recv().await {
                Ok(event) => seen.push(event),
                Err(_) => break,
            }
        }

        assert!(
            seen.iter().any(|event| matches!(
                event,
                JobEvent::Error(reason) if reason == "no usable version in /srv/docs"
            )),
            "a watcher must be told the reason, got {seen:?}"
        );

        // And it is the same sentence the poll reports, so the two cannot differ.
        let job = jobs.get(id).unwrap();
        assert_eq!(job.error.as_deref(), Some("no usable version in /srv/docs"));
    }

    #[tokio::test]
    async fn a_job_is_written_running_not_only_announced_running() {
        // **The lie this test is for.** `spawn` broadcast `running` and never
        // stored it, so the only writes to `job.state` were terminal ones and a
        // poller read `"state": "queued"` for the whole length of the work.
        let jobs = Jobs::new();

        // A job that is still going, so its state can be read while it is not
        // terminal — reading it after the fact only proves it finished.
        let (finish, finished) = tokio::sync::oneshot::channel::<()>();
        let id = jobs.spawn("merge", move |_| async move {
            let _ = finished.await;
            Ok(super::Json::Null)
        });

        let mut seen = None;
        for _ in 0..200 {
            if let Some(job) = jobs.get(id)
                && job.state == State::Running
            {
                seen = Some(job);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let _ = finish.send(());

        let job = seen.expect("a job that has started must be stored as running");
        assert_eq!(job.state, State::Running);
        assert!(!job.state.is_terminal());
    }

    #[test]
    fn subscribe_neither_loses_an_event_nor_repeats_one() {
        // **The guarantee `subscribe` makes, against real threads.** The two
        // calls it has to make consistent contain no `await` between them, so a
        // current-thread runtime cannot put a publisher and a subscriber inside
        // that window at once — an async version of this test passes against the
        // broken implementation, which is the reason it is written with
        // `std::thread` instead.
        //
        // Taking the receiver first and snapshotting the history second allowed
        // both ways to be wrong, and each shows up as a different tally:
        //
        // * **twice** — the event was sent before the receiver existed and
        //   recorded before the snapshot, so it is in the replay *and* delivered
        //   live.
        // * **never** — the event was sent before the receiver existed and
        //   recorded after the snapshot, so it is in neither.
        const PUBLISHED: u64 = 20_000;
        const WATCHERS: usize = 4;
        const SUBSCRIBES: usize = 2_000;

        /// What a subscriber has held, as `(lowest, highest)`.
        type Held = Option<(u64, u64)>;

        let jobs = std::sync::Arc::new(Jobs::new());

        // A job to subscribe to. The task never has to run: `subscribe` only
        // needs the entry to be there, and a runtime is needed only because
        // `spawn` hands the work to one.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread runtime");
        let id = runtime.block_on(async {
            jobs.spawn("pack", |_| async {
                std::future::pending::<()>().await;
                Ok(super::Json::Null)
            })
        });
        drop(runtime);

        let publisher = {
            let jobs = std::sync::Arc::clone(&jobs);
            std::thread::spawn(move || {
                for value in 1..=PUBLISHED {
                    jobs.emit_progress(id, vec![("chunks".to_string(), value)]);
                }
            })
        };

        let watchers: Vec<_> = (0..WATCHERS)
            .map(|_| {
                let jobs = std::sync::Arc::clone(&jobs);
                std::thread::spawn(move || {
                    let mut held: Held = None;

                    for _ in 0..SUBSCRIBES {
                        let (mut receiver, history) = jobs
                            .subscribe(id)
                            .expect("the job was spawned, so it is there");

                        // The `chunks` value out of a progress event, if it is one.
                        let chunks = |event: &JobEvent| match event {
                            JobEvent::Progress(counters) => counters
                                .iter()
                                .find(|(name, _)| name == "chunks")
                                .map(|(_, value)| *value),
                            _ => None,
                        };

                        let replayed: Vec<u64> = history.iter().filter_map(chunks).collect();
                        let highest_replayed = replayed.iter().copied().max().unwrap_or(0);
                        let mut seen = replayed.clone();

                        while let Ok(event) = receiver.try_recv() {
                            if let Some(value) = chunks(&event) {
                                assert!(
                                    replayed.is_empty() || value > highest_replayed,
                                    "event {value} reached a subscriber twice: it is in the \
                                     replay and it was delivered live"
                                );
                                seen.push(value);
                            }
                        }

                        seen.sort_unstable();
                        seen.dedup();
                        if let (Some(lowest), Some(highest)) =
                            (seen.first().copied(), seen.last().copied())
                        {
                            assert_eq!(
                                highest - lowest + 1,
                                seen.len() as u64,
                                "events {lowest} to {highest} arrived, which is a hole: a \
                                 subscriber lost one"
                            );

                            held = Some(match held {
                                None => (lowest, highest),
                                Some(previous) => (previous.0.min(lowest), previous.1.max(highest)),
                            });
                        }
                    }

                    held.expect("a watcher must have caught events, not none")
                })
            })
            .collect();

        publisher
            .join()
            .expect("the publisher thread should not panic");

        let mut caught = 0usize;
        for watcher in watchers {
            let (_, highest) = watcher.join().expect("a watcher should not panic");
            assert!(
                highest > 1,
                "a watcher must have seen more than the first event"
            );
            caught += usize::try_from(highest).unwrap_or(0);
        }
        assert!(caught > 1, "the watchers must actually have caught events");
    }

    #[test]
    fn a_terminal_state_is_one_a_job_will_not_leave() {
        // The property `DELETE` reports against, stated so a new variant cannot
        // be added without somebody deciding what cancelling it means.
        assert!(!State::Queued.is_terminal());
        assert!(!State::Running.is_terminal());
        assert!(State::Succeeded.is_terminal());
        assert!(State::Failed.is_terminal());
        assert!(State::Cancelled.is_terminal());
    }
}
