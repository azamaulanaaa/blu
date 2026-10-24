//! Temporary-directory helpers.

/// Remove a temporary directory off the runtime thread.
///
/// The directory holds a whole pack on a remote restore.
/// A remote backup stages a whole tree in it.
/// Removing the directory removes every file inside it.
/// That work does not belong on a runtime thread.
///
/// A [`tempfile::TempDir`] value removes the directory when the value drops.
/// That stays the fallback for a run that returns early.
/// The drop tries again when the removal here fails.
///
/// A removal that fails changes nothing about the run.
/// The work is already done, and the run reports its own result.
pub(crate) async fn remove_work_dir(work: &mut tempfile::TempDir) {
    // The task that removes the directory owns the path.
    let path = work.path().to_path_buf();

    // The removal removes every file in the directory.
    // A runtime worker must not do that work.
    let removed = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&path)).await;

    // A removal that worked leaves the drop nothing to do.
    // A removal that failed leaves the drop to try again.
    match removed {
        Ok(Ok(())) => work.disable_cleanup(true),
        Ok(Err(error)) => tracing::warn!(
            path = %work.path().display(),
            %error,
            "the temporary directory is still there. The drop tries again."
        ),
        Err(join) => tracing::warn!(
            path = %work.path().display(),
            task = %join,
            "the temporary directory is still there. The drop tries again."
        ),
    }
}
