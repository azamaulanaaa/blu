//! The `apply` command: bring a directory up to the version a pack carries.

use super::{
    ApplyBuilder, ApplyOptions, Pack, lock_destination, report_unattributed, report_unowned,
};

/// Bring `destination` up to the version `pack` carries.
///
/// The destination is claimed for the length of the run, on the same terms that
/// a restore claims one. Two applies into one directory would interleave. The
/// claim is a file *beside* the directory, so it never appears in the result.
///
/// A dry run takes no claim and writes nothing. It opens no file, prunes no
/// entry, and records no manifest. It has already probed the destination against
/// the version, and read back every chunk that it was about to write.
pub(crate) async fn run_apply(
    pack: &std::path::Path,
    destination: &std::path::Path,
    prune: bool,
    recheck: bool,
    dry_run: bool,
    files: Vec<std::path::PathBuf>,
) -> std::io::Result<()> {
    let _claim = if dry_run {
        None
    } else {
        Some(lock_destination(destination)?)
    };

    let pack = Pack::open(pack)?;
    tracing::info!(
        pack = %pack.path().display(),
        destination = %destination.display(),
        chunks = pack.len(),
        dry_run,
        "applying pack"
    );

    let applied = ApplyBuilder::new(destination, &pack)
        .options(ApplyOptions {
            prune,
            recheck,
            dry_run,
            files,
        })
        .run()
        .await?;

    tracing::info!(
        held = applied.held,
        files = applied.files,
        written = applied.written,
        bytes = applied.bytes,
        removed = applied.removed,
        dry_run,
        "applied pack"
    );

    report_applied(&applied, &pack, destination, dry_run)
}

/// Say what an apply changed, or what it would have changed.
///
/// Two wordings rather than one with the verbs filled in. The real run's
/// line is what a script parses. Interpolating into that line would change bytes
/// that nobody asked to change.
pub(crate) fn report_applied(
    applied: &blulib::command::Applied,
    pack: &Pack,
    destination: &std::path::Path,
    dry_run: bool,
) -> std::io::Result<()> {
    // The bytes a fetch would have needed. That figure says whether this
    // was an update or a whole tree.
    if dry_run {
        say!(
            "dry run: would fetch {} of {} bytes in {} range(s). {} of {} chunks are \
             already held. {} chunks would be written into {} file(s)",
            applied.fetched,
            pack.payload_len(),
            applied.ranges,
            applied.held,
            applied.chunks,
            applied.written,
            applied.files,
        )?;
    } else {
        say!(
            "fetched {} of {} bytes in {} range(s). {} of {} chunks were already \
             held. This run wrote {} chunks into {} file(s)",
            applied.fetched,
            pack.payload_len(),
            applied.ranges,
            applied.held,
            applied.chunks,
            applied.written,
            applied.files,
        )?;
    }

    // Named on stderr, because stdout is data. Entries are left in place unless
    // `--prune`. The run says so either way, because silence here would read as
    // "nothing there".
    if applied.extra > 0 {
        let what = match (dry_run, applied.removed) {
            (true, 0) => "would be left alone. Pass --prune to remove them",
            (true, _) => "would be removed by --prune",
            (false, _) => "left alone. Pass --prune to remove them",
        };
        eprintln!(
            "note: {} unrecorded entr{} under {} {what}",
            applied.extra,
            if applied.extra == 1 { "y" } else { "ies" },
            destination.display(),
        );
    }

    if applied.removed > 0 {
        say!(
            "{} {} unrecorded entr{}",
            if dry_run { "would remove" } else { "removed" },
            applied.removed,
            if applied.removed == 1 { "y" } else { "ies" }
        )?;
    }

    if applied.unowned > 0 {
        if dry_run {
            // The figure is a bound, not a count. It is how many entries record an
            // owner at all. Only trying to set one says whether this process can.
            // Claiming either outcome would be a guess.
            eprintln!(
                "note: {} entr{} in this version record an owner. An apply may not be \
                 able to set an owner without privilege",
                applied.unowned,
                if applied.unowned == 1 { "y" } else { "ies" },
            );
        } else {
            report_unowned(applied.unowned, "apply");
        }
    }

    if applied.unattributed > 0 {
        if dry_run {
            // The figure is a bound for the same reason as `unowned` above.
            eprintln!(
                "note: {} entr{} in this version record extended attributes. An apply \
                 may not be able to set them on this filesystem",
                applied.unattributed,
                if applied.unattributed == 1 {
                    "y"
                } else {
                    "ies"
                },
            );
        } else {
            report_unattributed(applied.unattributed, "apply");
        }
    }

    Ok(())
}
