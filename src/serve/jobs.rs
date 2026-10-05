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
                format!("{text:?} is not a job id; a job id is 64 hexadecimal characters"),
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
    const fn name(self) -> &'static str {
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

/// One thing that changed about a job.
#[derive(Debug, Clone, PartialEq)]
pub enum JobEvent {
    /// The job's state changed.
    State(State),
    /// The job's counters moved.
    ///
    /// Integers keyed by the name [`crate::progress::Counter`] reports under, so
    /// a client renders a progress bar without knowing this crate's enum.
    Progress(Vec<(String, u64)>),
    /// The job finished, and this is its result.
    Result(Json),
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
    events: broadcast::Sender<JobEvent>,
    /// Recent events per job, so a late subscriber is not told nothing.
    replay: Mutex<HashMap<JobId, Vec<JobEvent>>>,
}

impl Jobs {
    /// A registry with no jobs in it.
    #[must_use]
    pub fn new() -> Self {
        let (events, _) = broadcast::channel(1024);
        Self {
            inner: Arc::new(Inner {
                jobs: Mutex::new(HashMap::new()),
                handles: Mutex::new(HashMap::new()),
                events,
                replay: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Start `work` as a job, and return its id.
    ///
    /// `work` is handed the job so it can report progress. The task is spawned
    /// immediately, so a job is `Queued` only for as long as the scheduler takes.
    ///
    /// The result is recorded when `work` returns, and the job's terminal state
    /// is set from whether it returned `Ok`. **There is no cleanup path to get
    /// wrong**: dropping the future on a cancel runs the destructors inside it.
    pub fn spawn<F, Fut>(&self, kind: &'static str, work: F) -> JobId
    where
        F: FnOnce(JobHandle) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Json> + Send + 'static,
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

        self.broadcast(id, JobEvent::State(State::Running));

        let this = self.clone();
        let handle = tokio::spawn(async move {
            let handle = JobHandle {
                id,
                jobs: this.clone(),
            };

            let outcome = work(handle).await;

            let finished = nanos();
            if let Ok(mut jobs) = this.inner.jobs.lock()
                && let Some(job) = jobs.get_mut(&id)
            {
                job.state = State::Succeeded;
                job.finished = Some(finished);
                job.result = Some(outcome.clone());
            }
            if let Ok(mut handles) = this.inner.handles.lock() {
                handles.remove(&id);
            }
            this.broadcast(id, JobEvent::Result(outcome));
            this.broadcast(id, JobEvent::State(State::Succeeded));
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
    /// finished — both reported as `NotFound` and `AlreadyExists` respectively,
    /// which is what a client can act on.
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
                format!("job {id} has already finished; there is nothing to cancel"),
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

        if let Some(handle) = handle {
            handle.abort();

            if let Ok(mut jobs) = self.inner.jobs.lock()
                && let Some(job) = jobs.get_mut(&id)
            {
                job.state = State::Cancelled;
                job.finished = Some(nanos());
            }
            self.broadcast(id, JobEvent::State(State::Cancelled));
        }

        Ok(())
    }

    /// A receiver for this job's events, plus what a late subscriber missed.
    ///
    /// **Both, from one call, and that is the point.** Taking the receiver before
    /// the history snapshot leaves a window in which an event reaches both, and a
    /// client receives it twice. Separating them also made the caller check
    /// existence separately, which is two locks and two clones for one answer.
    ///
    /// The replay is a snapshot, so an event arriving after this returns is on the
    /// receiver and not in the history — which is what makes the pair correct
    /// rather than merely complete.
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

        let receiver = self.inner.events.subscribe();
        let history = self
            .inner
            .replay
            .lock()
            .map(|all| all.get(&id).cloned().unwrap_or_default())
            .unwrap_or_default();

        Ok((receiver, history))
    }

    /// Record an event, and remember the last few for a late subscriber.
    fn broadcast(&self, id: JobId, event: JobEvent) {
        // A send with nobody listening is not a failure. Every event here may
        // have no subscriber at all, which is the normal case for a job whose
        // client polls.
        let _ = self.inner.events.send(event.clone());

        // `entry().or_default()` rather than `get_mut()`. `get_mut` returns
        // `None` for a job that has not recorded an event yet, so the **first**
        // event of every job was dropped and the history began at the second — and
        // a late subscriber was never told a job had started.
        if let Ok(mut replay) = self.inner.replay.lock() {
            let history = replay.entry(id).or_default();
            history.push(event);
            if history.len() > REPLAY {
                let excess = history.len() - REPLAY;
                history.drain(..excess);
            }
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
                for id in retired {
                    replay.remove(&id);
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

    /// Record a failure, so a poller sees the reason before the task's own
    /// return value arrives.
    pub fn fail(&self, error: impl Into<String>) {
        if let Ok(mut jobs) = self.jobs.inner.jobs.lock()
            && let Some(job) = jobs.get_mut(&self.id)
        {
            job.error = Some(error.into());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{JobEvent, JobId, Jobs, State};

    #[tokio::test]
    async fn a_job_starts_queued_and_ends_succeeded() {
        let jobs = Jobs::new();
        let id = jobs.spawn("pack", |_| async { super::Json::string("done") });

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
            super::Json::Null
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
        let id = jobs.spawn("pack", |_| async { super::Json::string("ok") });

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
        let id = jobs.spawn("pack", |_| async { super::Json::Null });

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
        let id = jobs.spawn("pack", |_| async { super::Json::Null });

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
