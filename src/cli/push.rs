//! Pushing a pack to a `blu serve` and reporting the server's merge.

use super::{KeepOrLevel, remote_for};

/// A `blu serve` this run is pushing a pack to, and the set to put it in.
///
/// Upload, ask the server to merge it into one set, wait, and report. The
/// reporting lives here too, so a pushed pack and a merged one cannot say
/// different things about the same event.
///
/// ## Why the set name is asked for rather than derived
///
/// **A server holds many sets and this run cannot know which one was meant.**
/// The set arrives as the last segment of the `http://` target, so a target
/// with no set is refused rather than guessed at. The name is checked with
/// [`blulib::client::check_set`] before the first byte moves, so a name that
/// is really a path is refused here rather than after a transfer.
pub(crate) struct Push {
    /// The client, which has already refused an address it cannot speak to.
    remote: blulib::client::Remote,
    /// The set on that server, checked to be one path component.
    set: String,
}

impl Push {
    /// Open a push to `address`, into `set`.
    ///
    /// The client forwards `token` as a Bearer header when one is set.
    /// The client never checks the token.
    /// A proxy before the server checks it.
    ///
    /// # Errors
    ///
    /// The call fails when `address` is not a server address.
    /// The call fails when `set` is not one path component.
    /// The call fails when `token` is empty or holds whitespace.
    /// Both address and set are checked here, before any work.
    /// A mistyped value fails before a transfer.
    pub(crate) fn open(address: &str, set: &str, token: Option<&str>) -> std::io::Result<Self> {
        let remote = remote_for(address, token)?;

        blulib::client::check_set(set)?;

        Ok(Self {
            remote,
            set: set.to_string(),
        })
    }

    /// The merge policy this push asks the server for.
    ///
    /// **The command's own `--compression`, translated rather than reinterpreted.**
    /// `keep` means the same thing on both sides — store what the pack carries —
    /// and a level is a level. The two policies are different types because they
    /// answer different questions (what is on the wire versus what a set is
    /// opened with), and this is the one place the answer crosses.
    fn compression(compression: KeepOrLevel) -> blulib::client::MergeCompression {
        match compression {
            KeepOrLevel::Keep => blulib::client::MergeCompression::Keep,
            KeepOrLevel::Level(level) => blulib::client::MergeCompression::Reencode(level),
        }
    }

    /// Upload `pack`, ask the server to merge it, and report what it published.
    ///
    /// **The merge is the server's, not a local one followed by an upload.**
    /// `merge_pack_file` is one request sequence: begin the upload, stream the
    /// file, commit, and start a merge into this set. The server opens the set,
    /// takes its own write lock, and commits a version in its own numbering. So
    /// the version this reports is the server's, and a pack pushed to a set that
    /// already holds it is answered with the version that holds it rather than
    /// an error.
    ///
    /// **The pack stays on disk, and this run never holds all of it.** The
    /// client streams the file to the server's scratch space, so a pack larger
    /// than memory pushes like any other. The file is opened before the first
    /// request, so a path this machine cannot read fails before the server hears
    /// about it.
    ///
    /// The job is awaited rather than handed back, so the exit code is about the
    /// merge: a job that failed or was cancelled is an error carrying the
    /// server's own sentence, and this prints nothing on stdout for it. There is
    /// no path here that reports a merge which did not happen.
    pub(crate) async fn pack(
        &self,
        pack: &std::path::Path,
        compression: KeepOrLevel,
    ) -> std::io::Result<()> {
        // The file is opened and measured before the first request: a path this
        // machine does not have, or one it cannot read, fails here rather than
        // after an upload was begun. The length is the open file's own, so the
        // line below says how many bytes the upload will send.
        let file = blulib::client::PackFile::open(pack)?;

        tracing::info!(
            server = %self.remote.address(),
            set = %self.set,
            pack = %pack.display(),
            bytes = file.len(),
            "pushing pack"
        );

        let job = self
            .remote
            .merge_pack_file(&self.set, file, Self::compression(compression))
            .await?;

        // Reported as it moves, on the same target and with the same counter
        // names a local merge uses. The merge is happening on the other machine,
        // so this is the only place a long push says anything at all.
        let finished = self
            .remote
            .await_job_with(&job, |counters| {
                let shown = counters
                    .iter()
                    .map(|(name, value)| format!("{name} {value}"))
                    .collect::<Vec<_>>()
                    .join(" ");
                tracing::info!("merging on {}: {shown}", self.remote.address());
            })
            .await?;

        self.report(&finished, pack)
    }

    /// Say what the server's merge published, in the words a local merge uses.
    ///
    /// **One set of sentences for both sides of a transfer.** The figures are the
    /// server's — it read them out of the plan it committed — and the wording is
    /// `report_merge`'s and `report_already_merged`'s, so a script that reads a
    /// merge report does not have to learn a second one because the merge
    /// happened over HTTP. Only the two names differ, and they have to: the set is
    /// a name on a server rather than a path here, and the server is named because
    /// a push is a thing done *to* somewhere and a report that did not say where
    /// would be about a set on no machine in particular.
    fn report(&self, job: &blulib::client::Job, pack: &std::path::Path) -> std::io::Result<()> {
        use blulib::client::{Merged as RemoteMerged, Outcome};

        let Some(Outcome::Merged(merged)) = &job.result else {
            return Err(std::io::Error::other(format!(
                "job {} finished without saying what it merged",
                job.id
            )));
        };

        match merged {
            RemoteMerged::Committed(committed) => {
                tracing::info!(
                    server = %self.remote.address(),
                    set = %committed.set,
                    version = committed.version,
                    snapshot = %&committed.snapshot,
                    base = committed.base,
                    chunks = committed.chunks,
                    fresh = committed.fresh,
                    held = committed.held,
                    bytes = committed.bytes,
                    "merge complete"
                );

                let since = match committed.base {
                    Some(base) => format!(" since {base}"),
                    None => String::new(),
                };

                say!(
                    "merged version {}{since}: {} chunks, {} new, {} already held, {} bytes \
                     stored from {}",
                    committed.version,
                    committed.chunks,
                    committed.fresh,
                    committed.held,
                    committed.bytes,
                    pack.display(),
                )?;
                say!(
                    "{}  {} on {}",
                    &committed.snapshot,
                    committed.set,
                    self.remote.address(),
                )
            }
            RemoteMerged::Already(already) => {
                tracing::info!(
                    server = %self.remote.address(),
                    set = %already.set,
                    version = already.version,
                    "merge: already merged, nothing to do"
                );

                say!(
                    "already merged: {} on {} holds this version as version {}. Nothing to do",
                    already.set,
                    self.remote.address(),
                    already.version,
                )
            }
        }
    }
}
