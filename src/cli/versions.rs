//! The `versions` command: list versions and per-file history.

use super::{
    IndexSet, StoreTarget, Target, Timestamp, connect_store, remote_for, require_staged_set,
    storage,
};

/// Run `versions` against a local path, a keyed store, or a server.
///
/// Split out of `dispatch_inner` only to keep that match readable; the store
/// route pulls into staging first. With `files` this reports file history
/// rather than the version listing, on every target alike.
pub(crate) async fn dispatch_versions(
    target: Target,
    numbers_only: bool,
    files: Vec<std::path::PathBuf>,
) -> std::io::Result<()> {
    match target {
        Target::Local(name) => {
            if files.is_empty() {
                run_versions(&name, numbers_only).await
            } else {
                run_history(&name, &files, numbers_only).await
            }
        }
        Target::Serve {
            address,
            set,
            token,
        } => {
            if files.is_empty() {
                run_versions_remote(&set, numbers_only, &address, token.as_deref()).await
            } else {
                run_history_remote(&set, &files, numbers_only, &address, token.as_deref()).await
            }
        }
        Target::Store(target) => {
            if files.is_empty() {
                run_versions_stored(&target, numbers_only).await
            } else {
                run_history_stored(&target, &files, numbers_only).await
            }
        }
    }
}

/// List the versions a backup set holds, oldest first, each with its snapshot hash
/// and the local time it ran.
///
/// The count is a **lower bound**. A version whose index file was never
/// written still exists and still restores. So this may report fewer than the set
/// holds, and never more. A set with no index files lists nothing rather than
/// failing. The command `blu reindex` rebuilds the index of such a set. A real
/// set with no committed version lists empty. A name that is not a set is an
/// error.
async fn run_versions(name: &std::path::Path, numbers_only: bool) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // No segments at all is a mistyped name, not an empty set. Answering "no
    // versions" to a name that names nothing would be a failure that a script
    // acts on.
    if set.segments()?.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "no backup set at {} (looked in {})",
                set.base().display(),
                set.directory().display()
            ),
        ));
    }

    let versions = set.indexed_versions()?;

    // The hashes, which the file names cannot supply. Opened read-only, because
    // opening for writing truncates an unfinished run. A listing must not be the
    // thing that discards it. A set with every segment but no committed version
    // lists empty rather than erroring, and that falls out. The open succeeds.
    // The loop below has nothing to iterate, and nothing is printed.
    //
    // Which is why there is no arm for "no set here". This used to be
    // `Err(e) if e.kind() == NotFound => return Ok(())`, and the kind was doing
    // two jobs. The one it was meant for is unreachable. The check above already
    // established that the set has segments. That is the only condition which
    // makes the open report `NotFound`. So the arm never fired on the case it was
    // written for. The other job is the problem. `NotFound` is what the operating
    // system returns for a file that has gone. So a segment removed between that
    // check and the open would have been swallowed too. Then `blu versions`
    // would exit 0 having printed nothing about a set that was being deleted.
    let store = storage::BlobStore::builder(set.base())
        .open_reader()
        .await?;

    for version in versions {
        // Through the store, which checks the file against itself. An index that
        // is stale, foreign, or edited is a reason to fall back to a scan. It is
        // never a reason to name the version. One that does not resolve is
        // skipped rather than reported. A listing must not name a version that
        // the store cannot serve.
        //
        // Both modes go through `resolve` for that reason. `--numbers-only` used
        // to read the index file on its own. That path checksums one file and
        // never consults the store. So a leftover index in a set with fewer
        // versions would be named by `versions --numbers-only`. The same index
        // would be omitted by `versions`. Both commands ran from the same
        // directory at the same moment.
        let Ok(snapshot) = store.snapshot_via_index(version) else {
            continue;
        };
        let Ok(taken_at) = store.version_time(version).await else {
            continue;
        };

        print_version(version, &snapshot, taken_at, numbers_only)?;
    }

    Ok(())
}

/// One line of a version listing, on whichever side the set is.
///
/// **One function, so the two listings cannot drift.** A serve listing prints
/// this and prints nothing else, which is what makes a script that reads a local
/// set work unchanged against a remote one: same fields in the same order, the
/// same two-space separator, the same `say!` and so the same broken-pipe
/// behaviour. A second formatting path would be a difference nothing tests until
/// a script meets it.
///
/// The time is local time. The store keeps UTC. Conversion happens here, at the
/// point of display, and nowhere else.
fn print_version(
    version: u32,
    snapshot: &storage::BlobId,
    taken_at: Timestamp,
    numbers_only: bool,
) -> std::io::Result<()> {
    if numbers_only {
        say!("{version}")
    } else {
        say!("{version}  {snapshot}  {}", taken_at.to_local_string())
    }
}

/// List a set held by a `blu serve`, printing what a local listing prints.
///
/// The fetch is [`blulib::client::Remote`]'s.
/// Everything here prints what came back.
/// `resolve_target` decided what text is a set name.
/// The last segment of the serve target names the set.
/// Nothing here is a path on this machine.
/// The client forwards `token` when one is set.
async fn run_versions_remote(
    set: &str,
    numbers_only: bool,
    address: &str,
    token: Option<&str>,
) -> std::io::Result<()> {
    let remote = remote_for(address, token)?;

    for listed in remote.versions(set).await? {
        print_version(
            listed.version,
            &listed.snapshot,
            listed.taken_at,
            numbers_only,
        )?;
    }

    Ok(())
}

/// Report the versions where the requested paths changed, oldest first.
///
/// Reads the numbering off the store's own commit records rather than the
/// index files, so a version whose index was never written still reports. A
/// version where none of the paths changed is skipped.
async fn run_history(
    name: &std::path::Path,
    files: &[std::path::PathBuf],
    numbers_only: bool,
) -> std::io::Result<()> {
    let set = IndexSet::new(name);

    // A mistyped name, not an empty set. Answering no events about a name that
    // names nothing would be a failure a script acts on.
    if set.segments()?.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "no backup set at {} (looked in {})",
                set.base().display(),
                set.directory().display()
            ),
        ));
    }

    let store = storage::BlobStore::builder(set.base())
        .open_reader()
        .await?;
    let versions = store.versions()?;
    let mut named = Vec::with_capacity(versions.len());
    for version in versions {
        named.push((version, store.snapshot(version)?));
    }

    let events = blulib::command::HistoryBuilder::new(&store)
        .versions(&named)
        .files(files)
        .run()
        .await?;

    print_history(&events, numbers_only)
}

/// Report a keyed store's file history through staging.
///
/// Pulls the set into staging and reports with exactly the code a local run
/// uses, so the lines match a local report field for field. Reads never push
/// back.
async fn run_history_stored(
    target: &StoreTarget,
    files: &[std::path::PathBuf],
    numbers_only: bool,
) -> std::io::Result<()> {
    let backend = connect_store(&target.url)?;
    let staging = IndexSet::new(&target.staging);
    storage::pull_from_remote_async(&backend, &staging).await?;
    require_staged_set(&staging, target)?;

    run_history(&target.staging, files, numbers_only).await
}

/// Report a `blu serve` set's file history, printing what a local report of
/// the same set would.
///
/// The server diffs in one pass and answers the events. Everything here is
/// printing what came back; `resolve_target` decided what text is a set name.
async fn run_history_remote(
    set: &str,
    files: &[std::path::PathBuf],
    numbers_only: bool,
    address: &str,
    token: Option<&str>,
) -> std::io::Result<()> {
    let remote = remote_for(address, token)?;
    blulib::client::check_set(set)?;

    let mut named = Vec::with_capacity(files.len());
    for file in files {
        let Some(text) = file.to_str() else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "{} is not valid UTF-8. A serve request carries paths as text, so such a \
                     path cannot travel. Report it against a local path or a `file://` store",
                    file.display()
                ),
            ));
        };
        named.push(text);
    }

    let events = remote.history(set, &named).await?;

    print_history(&events, numbers_only)
}

/// One file-history report, on whichever side the set is.
///
/// **One function, so the listings cannot drift.** A remote report prints this
/// and prints nothing else, which is what makes a script that reads a local
/// set work unchanged against a remote one. Under `--numbers-only` each
/// touching version is named once.
fn print_history(events: &[blulib::command::Event], numbers_only: bool) -> std::io::Result<()> {
    if numbers_only {
        let mut shown: Option<u32> = None;
        for event in events {
            if shown != Some(event.version) {
                say!("{}", event.version)?;
                shown = Some(event.version);
            }
        }
        return Ok(());
    }

    for event in events {
        say!(
            "{}  {}  {}",
            event.version,
            event.status.word(),
            event.path.display()
        )?;
    }

    Ok(())
}

/// List a keyed store through staging.
///
/// Pulls the set into staging and lists it with exactly the code a local run
/// uses, so the lines match a local listing field for field.
async fn run_versions_stored(target: &StoreTarget, numbers_only: bool) -> std::io::Result<()> {
    let backend = connect_store(&target.url)?;
    let staging = IndexSet::new(&target.staging);
    storage::pull_from_remote_async(&backend, &staging).await?;
    require_staged_set(&staging, target)?;

    run_versions(&target.staging, numbers_only).await
}
