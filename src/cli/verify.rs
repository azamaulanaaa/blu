//! The `verify` command: check a version's chunks against their hashes.

use super::{
    IndexSet, StoreTarget, Target, VerifyBuilder, connect_store, require_staged_set,
    select_version, storage,
};

/// Run `verify` against a local path or a keyed store.
///
/// An `http://` or `https://` target fails: the server exposes versions and packs,
/// not chunk verification, so there is nothing here to check one against.
pub(crate) async fn dispatch_verify(target: Target, version: Option<u32>) -> std::io::Result<()> {
    match target {
        Target::Local(name) => run_verify(&name, version).await,
        Target::Store(target) => run_verify_stored(&target, version).await,
        Target::Serve { .. } => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "`blu verify` takes a local path or a `file://` target: the server exposes \
             versions and packs, not chunk verification",
        )),
    }
}

/// Check a version's chunks against their recorded hashes.
async fn run_verify(name: &std::path::Path, version: Option<u32>) -> std::io::Result<()> {
    let set = IndexSet::new(name);
    let store = storage::BlobStore::builder(set.base())
        .open_reader()
        .await?;
    let (version, snapshot) = select_version(&set, &store, version)?;

    tracing::info!(name = %set.base().display(), version, "verifying");

    let report = VerifyBuilder::new(snapshot, store).run().await?;

    for failure in &report.failures {
        eprintln!("error: {failure}");
    }

    say!(
        "checked {} chunks ({} bytes) in version {version}: {}",
        report.chunks,
        report.bytes,
        if report.is_ok() {
            "all chunks match their recorded hashes".to_string()
        } else {
            format!("{} chunks FAILED", report.failures.len())
        }
    )?;

    // After the summary, and not before it. The exit code is this line. A
    // summary that could terminate the process first would be able to answer
    // for it.
    report.into_result()
}

/// Check a keyed store through staging.
///
/// Pulls the set into staging and checks it with exactly the code a local run
/// uses. Reads never push back.
async fn run_verify_stored(target: &StoreTarget, version: Option<u32>) -> std::io::Result<()> {
    let backend = connect_store(&target.url)?;
    let staging = IndexSet::new(&target.staging);
    storage::pull_from_remote_async(&backend, &staging).await?;
    require_staged_set(&staging, target)?;

    run_verify(&target.staging, version).await
}
