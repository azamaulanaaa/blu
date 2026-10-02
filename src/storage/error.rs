//! One wording for every filesystem error, and one refusal for an impossible
//! allocation.
//!
//! Two functions because the crate makes around forty filesystem calls and each
//! of them was inventing its own message — the same operation reported two ways
//! is a bug report nobody can read — and a third because every place a length
//! read out of a file becomes a buffer wants the same `map_err`. What they share
//! is being here rather than at the call sites: the wording is worth having in
//! one place, and so is a mistake worth preventing in one place, which for
//! [`zeroed`] is a zero standing in for a length that does not fit.

use std::{io, path::Path};

/// A filesystem call's error, naming the path and what was being done to it.
///
/// A bare `?` on `fs::File::open(path)` produces `No such file or directory (os
/// error 2)`, which is a true sentence about a file the user has to find. The
/// crate already has `not_a_store` and `not_a_pack` for the errors that mean
/// "this is not a store" and "this is not a pack", and it has around forty of
/// these — the calls where the operation failed for an ordinary reason and the
/// message is whatever the operating system said.
///
/// The kind is carried through, which is the whole reason this is a function and
/// not a `format!`. A bare `io::Error::new(Other, ...)` would be a lie about a
/// permission problem, and a crate that hands `io::Error` to callers has already
/// promised that the kind they match on is the one the operating system reported:
///
/// ```no_run
/// # fn main() -> std::io::Result<()> {
/// # let path = std::path::Path::new("photos.0000.bin");
/// let file = lepatch::storage::at(path, "open", std::fs::File::open(path))?;
/// # let _ = file;
/// # Ok(())
/// # }
/// ```
///
/// One construct rather than a `map_err` at each site, because a `map_err` needs
/// the error and the path in the right order to be readable, and gets them the
/// wrong way round about as often as not.
///
/// # Errors
///
/// Returns `Ok` for `Ok`, and otherwise the same error with `doing` and the path
/// put in front of the operating system's own message. The [`io::ErrorKind`] is
/// the one the error came in with, unchanged — that is the reason this is a
/// function and not a `format!`.
pub fn at<T>(path: &Path, doing: &str, result: io::Result<T>) -> io::Result<T> {
    result.map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("could not {doing} {}: {e}", path.display()),
        )
    })
}

/// [`at`], for a call that returns a future.
///
/// A second function rather than one overloaded on both, because a future is not
/// an `io::Result` and a wrapper taking either would need a second type parameter
/// to say which. Two names is the smaller cost.
///
/// ```no_run
/// # async fn example() -> std::io::Result<()> {
/// # let path = std::path::Path::new("photos.0000.bin");
/// let file = lepatch::storage::at_async(
///     path,
///     "open",
///     tokio::fs::File::open(path),
/// ).await?;
/// # let _ = file;
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// The same as [`at`]: `Ok` for `Ok`, and otherwise the error with `doing` and
/// the path in front of it, keeping its [`io::ErrorKind`].
pub async fn at_async<T, F>(path: &Path, doing: &str, result: F) -> io::Result<T>
where
    F: std::future::Future<Output = io::Result<T>>,
{
    at(path, doing, result.await)
}

/// A zeroed buffer of exactly `bytes`, or an error saying what would not fit.
///
/// Used wherever a length read out of a file becomes an allocation. The reason
/// this is a function rather than three `map_err`s is that the tempting shortcut
/// in every one of those cases is a zero, and **a zero-length buffer is not a
/// small buffer**: `read_exact` into one succeeds, because there is nothing to
/// read. So the failure is not caught later. It is passed on — an index that
/// decodes as empty, a chunk that decompresses from nothing, a pack that is short
/// by every chunk while its own index claims otherwise.
///
/// There is no fallback to choose between here. A length this platform cannot
/// address is not a small length, and the alternative to an error is not a worse
/// answer but a wrong one. `what` is the caller's description of the thing being
/// sized, so the message says which one rather than "a length".
pub(crate) fn zeroed(bytes: u64, what: &str) -> io::Result<Vec<u8>> {
    usize::try_from(bytes).map(|n| vec![0u8; n]).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{what} is {bytes} bytes, which this platform cannot address"),
        )
    })
}
