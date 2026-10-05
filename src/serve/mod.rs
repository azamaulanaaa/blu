//! Serving a backup set over HTTP.
//!
//! `lepatch serve` runs this. It exposes the two directions a backup has over a
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
//! **Push — remote backup.** The client runs [`lepatch backup`] to snapshot a
//! tree into a local set, and [`lepatch pack`] to write one version out as a
//! pack. It uploads the pack. The server [`lepatch merge`]s it into the set it
//! keeps. The chunks are verified on the way in, one at a time, against the
//! hash they are addressed by — the same check a local merge does.
//!
//! **Pull — remote restore.** The server [`lepatch pack`]s a version out of a
//! set it keeps. The client fetches the bytes it does not already hold and
//! [`lepatch merge`]s or [`lepatch apply`]s the result into a local set, then
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
//! A job in [`jobs`] and a handle in [`sets`]. The rest is derived per request.
//! In particular a generated pack is **not** kept: [`pack`] ids encode what
//! they name, and the bytes are computed from the set on each request. A pack
//! can be larger than the set it came from once deduplication is undone, and
//! holding a copy of every one of them would make the server's disk a function
//! of how many versions have been asked for.
//!
//! ## Wire format
//!
//! JSON, encoded by hand in [`json`], and described by the checked-in
//! `openapi/lepatch.json`. Pack *bytes* are not JSON at all — they are a pack,
//! verbatim, which is what makes a client able to `Pack::open` what it fetched
//! and hand it straight to the local merge.
//!
//! [`lepatch backup`]: https://docs.rs/lepatch
//! [`lepatch merge`]: https://docs.rs/lepatch
//! [`lepatch pack`]: https://docs.rs/lepatch
//! [`lepatch apply`]: https://docs.rs/lepatch

use std::io;
use std::path::{Path, PathBuf};

mod error;
mod jobs;
mod json;
mod openapi;
mod pack;
mod sets;
mod upload;
mod versions;

pub use error::ApiError;
pub use jobs::{Job, JobId, Jobs, State};
pub use json::Json;
pub use sets::Sets;
pub use upload::Uploads;

pub(crate) use error::status_for;
pub(crate) use openapi::{DOCUMENT, SCHEMAS};
pub(crate) use pack::Packs;

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
        let scratch = scratch.map_or_else(
            || root.join(SCRATCH_NAME),
            |path| absolute(path),
        );

        Self {
            root,
            bind: bind.into(),
            scratch,
        }
    }
}

/// The directory a scratch path defaults to, under the root.
const SCRATCH_NAME: &str = ".lepatch-serve";

/// Every route this server answers, as `(method, path)`.
///
/// **The router is built from this, so the table and the routes cannot drift.**
/// `tests/openapi.rs` checks it against the checked-in OpenAPI document in both
/// directions: a route with no entry in `paths`, and an entry in `paths` with no
/// route, are both failures. A hand-written specification is only a liability if
/// nothing says whether it is true, so something says.
pub const ROUTES: &[(&str, &str)] = &[
    ("GET", "/openapi.json"),
    ("GET", "/v1/version"),
    ("GET", "/v1/sets"),
    ("GET", "/v1/sets/{set}/versions"),
    ("GET", "/v1/sets/{set}/versions/{version}"),
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

/// Run the server until it is stopped.
///
/// # Errors
///
/// Returns an error if the scratch directory cannot be created, if the address
/// cannot be parsed, or if the listener cannot be bound. It does **not** return
/// an error because a set is damaged: a set is opened per request, and a
/// request against a damaged set fails with `422` rather than taking the server
/// down with it.
pub async fn serve(config: Config, jobs: &Jobs) -> io::Result<()> {
    let config = config;

    std::fs::create_dir_all(&config.scratch)?;

    let sets = Sets::new(config.root.clone());
    let uploads = Uploads::new(config.scratch.clone());

    let app = actix_web::App::new()
        .app_data(actix_web::web::Data::new(sets))
        .app_data(actix_web::web::Data::new(uploads))
        .app_data(actix_web::web::Data::new(Packs::new()))
        .app_data(actix_web::web::Data::new(jobs.clone()));

    crate::serve::handlers::configure(app, &config.bind).await
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

        assert_eq!(config.scratch, std::path::Path::new("/srv/sets").join(SCRATCH_NAME));
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