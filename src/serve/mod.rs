// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Serving a backup set over HTTP.
//!
//! `blu serve` runs this. It exposes the two directions a backup has over a
//! network, and it does it through the crate's own pack format rather than
//! through a new wire format for chunks.
//!
//! ## What the server is
//!
//! **A relay over a deduplicating store, and nothing more.** A pack is one
//! version's chunks laid end to end in a single file, so a version's bytes are
//! contiguous and are one request rather than thousands. The measurements
//! behind that are in the README. A server that spoke a chunk-at-a-time
//! protocol would throw away the only property that makes a version worth
//! moving as a unit.
//!
//! The four commands map onto the wire like this.
//!
//! **Push — remote backup.** The client runs [`blu backup`] to snapshot a
//! tree into a local set, and [`blu pack`] to write one version out as a
//! pack. It uploads the pack. The server [`blu merge`]s it into the set it
//! keeps. The chunks are verified on the way in, one at a time, against the
//! hash they are addressed by — the same check a local merge does.
//!
//! **Pull — remote restore.** The server [`blu pack`]s a version out of a
//! set it keeps. The client fetches the bytes it does not already hold and
//! [`blu merge`]s or [`blu apply`]s the result into a local set, then
//! restores from that.
//!
//! ## What the server is not
//!
//! **It never takes a path on its own disk.** There is no endpoint that says
//! "back up `/srv/data`", and none that says "restore into `/srv/out`". That is
//! a deliberate boundary and it is the one that keeps this small. A server that
//! snapshotted its own filesystem would need every rule the local commands have
//! about claims, about `..` in recorded paths, about refusing a destination that
//! holds the set being restored from — all of it, at the one place where it is
//! least wanted. None of it is needed here, because no tree is ever named. The
//! server moves packs; a client moves trees.
//!
//! ## Auth
//!
//! There is none. **Anyone who can reach the port can read every set this
//! server keeps and can append versions to them.** That is a decision, not an
//! oversight. The binding defaults to `127.0.0.1` so the common case is a
//! service on the same machine, and `--bind` says otherwise out loud, but the
//! server does not check and does not warn.
//!
//! Put it behind something that does, if it needs it. There is no token flag,
//! no TLS, and no middleware to configure.
//!
//! ## Where the state lives
//!
//! A job in [`Jobs`] and a handle in [`Sets`]. The rest is derived per request.
//! In particular a generated pack is **not** kept: a pack id encodes what it
//! names, and the bytes are computed from the set on each request. A pack can be
//! larger than the set it came from once deduplication is undone, and holding a
//! copy of every one of them would make the server's disk a function of how many
//! versions have been asked for.
//!
//! ## Wire format
//!
//! JSON, encoded by hand in [`Json`], and described by the checked-in
//! `openapi/blu.json`. Pack *bytes* are not JSON at all — they are a pack,
//! verbatim, which is what makes a client able to `Pack::open` what it fetched
//! and hand it straight to the local merge.
//!
//! [`blu backup`]: https://docs.rs/blu
//! [`blu merge`]: https://docs.rs/blu
//! [`blu pack`]: https://docs.rs/blu
//! [`blu apply`]: https://docs.rs/blu

use std::io;
use std::path::{Path, PathBuf};

mod error;
mod handlers;
mod history;
mod jobs;
mod json;
pub(crate) mod observe;
mod openapi;
mod pack;
mod sets;
mod span;
mod upload;
mod versions;

pub use error::ApiError;
pub use jobs::{Job, JobEvent, JobHandle, JobId, Jobs, State};
pub use observe::Handle as ProgressLayer;
pub use observe::filter as progress_filter;

/// The `tracing` layer that forwards a running job's progress counters into its
/// registry, for `main.rs` to install on the subscriber.
///
/// **The library does not install it.** A subscriber is process-global and this
/// crate is a library first: a caller embedding `blu` owns its own tracing
/// setup and would not find one of these under it. So this hands back a layer and
/// leaves installation to the caller, who is the only one who knows where the
/// subscriber lives.
///
/// **Install it with [`progress_filter`].** An unfiltered layer is offered every
/// event in the process and does a target comparison on each; the filter admits
/// this crate's callsites and refuses the rest. It is not optional in the sense
/// that matters: it is what makes the layer's input small, and
/// `progress_filter` deliberately ignores `RUST_LOG`, so a client's view of a
/// job does not move when the terminal is turned down.
///
/// # Panics
///
/// Never. A subscriber can only be installed once per process, so this builds a
/// layer rather than installing one — there is nothing here that can fail.
#[must_use]
pub fn progress_layer(jobs: Jobs) -> ProgressLayer {
    ProgressLayer::new(jobs)
}

/// The event names `GET /v1/jobs/{id}/events` can put on the wire.
///
/// Public for the same reason [`SCHEMAS`] is: the document's description of that
/// endpoint promises a closed set of names to switch on, and only `tests/openapi.rs`
/// comparing it against this list makes the promise one somebody checks.
pub use handlers::EVENT_NAMES;
pub use json::Json;
/// The `OpenAPI` document, served at `GET /openapi.json`.
pub use openapi::DOCUMENT;
/// The schema names `DOCUMENT` must define.
///
/// Public so `tests/openapi.rs` can check the checked-in document against the
/// server's own idea of what it serves, without reaching into a private module.
pub use openapi::SCHEMAS;
pub use pack::{ByteRange, Requested, Resolved};
pub use sets::Sets;
pub use upload::Uploads;

/// Where the server keeps sets, where it binds, and where it puts scratch files.
///
/// Three paths and an address, and no more. Each is resolved to an absolute
/// path before anything uses it, so that every later containment check compares
/// absolute against absolute.
#[derive(Debug, Clone)]
pub struct Config {
    /// The directory holding the backup sets this server serves.
    pub root: PathBuf,
    /// The address to bind. `127.0.0.1:8480` unless `--bind` says otherwise.
    pub bind: String,
    /// Where uploaded and materialised packs are written.
    pub scratch: PathBuf,
}

impl Config {
    /// A configuration serving `root`, binding `bind`, using `scratch`.
    ///
    /// The scratch directory is resolved under the root unless it was given in
    /// full, so a server with no `--scratch` puts its temporary files beside the
    /// sets rather than in a system temporary directory that may be on another
    /// filesystem — or, on a machine with `/tmp` mounted `noexec` or `tmpfs`
    /// sized for a different job, that cannot hold a pack at all.
    #[must_use]
    pub fn new(root: &Path, bind: impl Into<String>, scratch: Option<&Path>) -> Self {
        let root = absolute(root);
        let scratch = scratch.map_or_else(|| root.join(SCRATCH_NAME), absolute);

        Self {
            root,
            bind: bind.into(),
            scratch,
        }
    }
}

/// The directory a scratch path defaults to, under the root.
const SCRATCH_NAME: &str = ".blu-serve";

/// Every route this server answers, as `(method, path)`.
///
/// **The router is built from this, so the table and the routes cannot drift.**
/// `tests/openapi.rs` checks it against the checked-in `OpenAPI` document in both
/// directions: a route with no entry in `paths`, and an entry in `paths` with no
/// route, are both failures. A hand-written specification is only a liability if
/// nothing says whether it is true, so something says.
pub const ROUTES: &[(&str, &str)] = &[
    ("GET", "/openapi.json"),
    ("GET", "/v1/version"),
    ("GET", "/v1/sets"),
    ("GET", "/v1/sets/{set}/versions"),
    ("GET", "/v1/sets/{set}/versions/{version}"),
    ("GET", "/v1/sets/{set}/history"),
    ("POST", "/v1/sets/{set}/packs"),
    ("GET", "/v1/packs/{id}"),
    ("POST", "/v1/packs/{id}/plan"),
    ("POST", "/v1/uploads"),
    ("PUT", "/v1/uploads/{id}"),
    ("POST", "/v1/uploads/{id}/commit"),
    ("DELETE", "/v1/uploads/{id}"),
    ("POST", "/v1/sets/{set}/merges"),
    ("GET", "/v1/jobs"),
    ("GET", "/v1/jobs/{id}"),
    ("GET", "/v1/jobs/{id}/events"),
    ("DELETE", "/v1/jobs/{id}"),
];

/// A listening server, before it starts serving.
///
/// Separated from [`serve`] so that a caller can learn the address **before**
/// anything is accepted. `--bind 127.0.0.1:0` asks the operating system for a free
/// port, and the only way to learn which one is to bind first and then ask.
///
/// Dropping this stops the server and removes the scratch directory's contents.
///
/// `Debug` by hand rather than by derive, because actix's `Server` is not `Debug`
/// and this crate denies `missing_debug_implementations` on a public type. The
/// address is the whole of what is interesting before the server runs.
impl std::fmt::Debug for Bound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bound")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

/// The address and the server that serves it. Private fields, because both are
/// reached through [`Bound::addr`] and [`Bound::run`].
pub struct Bound {
    address: std::net::SocketAddr,
    server: actix_web::dev::Server,
    _scratch: Scratch,
}

impl Bound {
    /// The address actually bound.
    ///
    /// Resolved, so under `:0` this is the port the operating system chose rather
    /// than the zero that was asked for.
    #[must_use]
    pub fn addr(&self) -> std::net::SocketAddr {
        self.address
    }

    /// Serve until the process is asked to stop.
    ///
    /// # Errors
    ///
    /// Returns an error if the server fails while running. It does not return an
    /// error because a set is damaged: sets are opened per request, and a request
    /// against a damaged set fails with `422` rather than taking the server down.
    pub async fn run(self) -> io::Result<()> {
        self.server.await
    }
}

/// Bind, and return the server ready to run.
///
/// **Whether a request is traced is decided here, once, from whether a tracer
/// provider is installed.** [`crate::telemetry::is_installed`] is the answer
/// after startup — the provider a failed exporter build left behind is not
/// installed, so a server whose telemetry was asked for and could not be built
/// traces nothing, which is the honest outcome: there is nowhere for the spans
/// to go. A library caller that installs its own provider *before* binding gets
/// the middleware, and one that does not pays nothing for it.
///
/// # Errors
///
/// Returns an error if the scratch directory cannot be created, if the address
/// cannot be parsed, or if the listener cannot be bound.
pub fn bind(config: &Config, jobs: &Jobs) -> io::Result<Bound> {
    std::fs::create_dir_all(&config.scratch)?;

    let (server, address) = handlers::configure(config, jobs, crate::telemetry::is_installed())?;

    Ok(Bound {
        address,
        server,
        _scratch: Scratch::new(config.scratch.clone()),
    })
}

/// Serve until stopped, for a caller that does not need the address first.
///
/// # Errors
///
/// As [`bind`] and [`Bound::run`].
pub async fn serve(config: Config, jobs: &Jobs) -> io::Result<()> {
    bind(&config, jobs)?.run().await
}

/// Removes a scratch directory's contents when the server stops.
///
/// A `Drop` guard rather than a call at the end of `run`, because the end is not
/// the only way out: a `?` on the bind and a Ctrl-C both leave. A pack that was
/// uploaded and never merged is a file with no meaning, and one left by a killed
/// run is the same file — so the next run would otherwise start by sorting out
/// the last one's leftovers.
///
/// `pub` because it is named in [`Bound`], which is public.
#[derive(Debug)]
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(path: std::path::PathBuf) -> Self {
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        // Best effort, and deliberately not a report. The server is going away,
        // and there is nobody left to read an error about a temporary directory.
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Resolve a path to an absolute one, creating nothing.
///
/// A relative `--root` would otherwise be compared against an absolute segment
/// path, and the comparison would never fire. This is the same rule
/// `check_clean_destination` applies to a destination, for the same reason.
fn absolute(path: &Path) -> PathBuf {
    if let Ok(cwd) = std::env::current_dir()
        && path.is_relative()
    {
        return cwd.join(path);
    }

    path.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::{Config, SCRATCH_NAME};

    #[test]
    fn a_scratch_directory_defaults_beside_the_sets() {
        // Not in `/tmp`. A pack can be larger than a temporary filesystem is
        // sized for, and the sets are what the scratch files exist to move.
        let config = Config::new(std::path::Path::new("/srv/sets"), "127.0.0.1:8480", None);

        assert_eq!(
            config.scratch,
            std::path::Path::new("/srv/sets").join(SCRATCH_NAME)
        );
    }

    #[test]
    fn a_scratch_directory_that_was_given_is_kept() {
        let config = Config::new(
            std::path::Path::new("/srv/sets"),
            "127.0.0.1:8480",
            Some(std::path::Path::new("/var/tmp/packs")),
        );

        assert_eq!(config.scratch, std::path::Path::new("/var/tmp/packs"));
    }

    #[test]
    fn the_root_is_made_absolute() {
        // Relative, because that is what a shell hands over. Absolute, because
        // every containment check below compares against an absolute segment.
        let config = Config::new(std::path::Path::new("sets"), "127.0.0.1:8480", None);

        assert!(config.root.is_absolute());
    }

    #[test]
    fn every_route_is_a_verb_and_a_path_that_start_slash() {
        // The table is what `tests/openapi.rs` checks the spec against, so a
        // malformed entry here fails the test with a confusing message rather
        // than here where the message would be obvious.
        for (method, path) in super::ROUTES {
            assert!(
                ["GET", "PUT", "POST", "DELETE"].contains(method),
                "`{method}` is not a method this server uses"
            );
            assert!(path.starts_with('/'), "`{path}` is not an absolute path");
        }
    }

    #[test]
    fn no_route_is_listed_twice() {
        // A duplicate would let the spec agree with itself while the router
        // answered only the first of the two.
        let mut seen = std::collections::HashSet::new();
        for route in super::ROUTES {
            assert!(seen.insert(*route), "`{route:?}` is listed twice");
        }
    }
}
