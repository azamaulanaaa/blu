//! stdout helpers and the notes that accompany a report.

/// Print one line of a command's output to stdout, reporting whether it went.
///
/// `println!` cannot be used. `SIGPIPE` is ignored at startup. So a write to a
/// pipe whose reader has gone returns `EPIPE`, and the print macros panic on a
/// write error. That case is a *success*. The reader leaving is what `head` does
/// on purpose. So the macro returns `Ok(())`, and the command carries on to its
/// own result. Any other write failure is an error.
///
/// This deliberately does not decide the exit code. The macro used to call
/// `process::exit` directly. That meant a command could print its summary before
/// computing its result, then exit 0 with a result that it had not looked at
/// yet. `blu verify set | true` reported success on a store that had failed
/// verification, because the write failed before `into_result` was ever called.
/// The macro also skipped every `Drop` in scope. A `Drop` is how a `Lock` or a
/// `DestinationLock` releases its claim file. So any command that both held a
/// claim and printed would have leaked it.
macro_rules! say {
    ($($arg:tt)*) => {{
        use std::io::Write as _;

        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        match writeln!(out, $($arg)*) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => Ok(()),
            Err(e) => Err(std::io::Error::other(format!("could not write output: {e}"))),
        }
    }};
}

/// Say that some entries could not be given the owner the version records.
///
/// On stderr, and unconditionally. The alternative is a tree that looks complete
/// and has the wrong owner on part of it. A restore of a system tree as an
/// unprivileged user always hits this. It is not an edge case to be phrased as
/// one. So the wording says what is true, and what would fix it. The files are
/// right, and the owners are not. The wording does not lead with a failure that
/// the run did not have.
pub(crate) fn report_unowned(count: u64, what: &str) {
    eprintln!(
        "note: {count} entr{} in this {what} recorded an owner this process could not set. \
         Setting an owner takes privilege. Their contents, permissions and times are correct. \
         Run as root to set them",
        if count == 1 { "y" } else { "ies" },
    );
}

/// Say that some entries missed the extended attributes the version records.
///
/// On stderr, and unconditionally, for the same reason as [`report_unowned`].
/// A filesystem without attribute support meets this on every entry that
/// records any. The wording says what is true. The files are right, and the
/// extended attributes are not. The run still succeeded.
pub(crate) fn report_unattributed(count: u64, what: &str) {
    eprintln!(
        "note: {count} entr{} in this {what} recorded extended attributes this filesystem \
         could not set. Their contents, permissions and times are correct",
        if count == 1 { "y" } else { "ies" },
    );
}
