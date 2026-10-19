//! One wording for every filesystem error, and one refusal for an impossible
//! allocation.
//!
//! The crate makes around forty filesystem calls. Each call invented its own
//! message. The same operation reported two ways is a bug report nobody can read.
//! So the crate has two functions for those calls.
//!
//! A third function serves every place where a length read from a file becomes a
//! buffer. Each of those places wants the same `map_err`.
//!
//! All three functions live here instead of at the call sites. The wording is
//! worth having in one place. So is a mistake worth preventing in one place. For
//! [`zeroed`], that mistake is a zero standing in for a length that does not fit.

use std::{io, path::Path};

/// A filesystem call's error, naming the path and what was being done to it.
///
/// A bare `?` on `fs::File::open(path)` produces `No such file or directory (os
/// error 2)`. The message is true. The user still has to find the file.
///
/// The crate already has `not_a_store` and `not_a_pack`. They cover the errors
/// that mean "this is not a store" and "this is not a pack". The crate also has
/// around forty of the errors handled here. Those calls failed for an ordinary
/// reason. Their message is whatever the operating system said.
///
/// The kind is carried through, which is the whole reason this is a function and
/// not a `format!`. A bare `io::Error::new(Other, ...)` would be a lie about a
/// permission problem. A crate that hands `io::Error` to callers has already
/// promised that callers match on the kind the operating system reported.
///
/// ```no_run
/// # fn main() -> std::io::Result<()> {
/// # let path = std::path::Path::new("photos.0000.bin");
/// let file = blulib::storage::at(path, "open", std::fs::File::open(path))?;
/// # let _ = file;
/// # Ok(())
/// # }
/// ```
///
/// This is one construct instead of a `map_err` at each site. A `map_err` needs
/// the error and the path in the right order to be readable. It gets them the
/// wrong way round about as often as not.
///
/// # Errors
///
/// Returns `Ok` for `Ok`. Otherwise, it returns the same error with `doing` and
/// the path in front of the operating system's own message. The [`io::ErrorKind`]
/// is the one the error came in with. The value does not change. That is the
/// reason this is a function and not a `format!`.
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
/// This is a second function instead of one overloaded on both. A future is not
/// an `io::Result`. A wrapper taking either would need a second type parameter to
/// say which one. Two names cost less.
///
/// ```no_run
/// # async fn example() -> std::io::Result<()> {
/// # let path = std::path::Path::new("photos.0000.bin");
/// let file = blulib::storage::at_async(
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
/// The same as [`at`]. Returns `Ok` for `Ok`. Otherwise, it returns the error
/// with `doing` and the path in front of it, keeping its [`io::ErrorKind`].
pub async fn at_async<T, F>(path: &Path, doing: &str, result: F) -> io::Result<T>
where
    F: std::future::Future<Output = io::Result<T>>,
{
    at(path, doing, result.await)
}

/// A zeroed buffer of exactly `bytes`, or an error saying what would not fit.
///
/// Used wherever a length read from a file becomes an allocation. This is a
/// function instead of three `map_err`s. The tempting shortcut in every one of
/// those cases is a zero. **A zero-length buffer is not a small buffer**.
/// `read_exact` into one succeeds, because there is nothing to read.
///
/// So the failure is not caught later. It is passed on. An index decodes as
/// empty. A chunk decompresses from nothing. A pack is short by every chunk while
/// its own index claims otherwise.
///
/// There is no fallback to choose between here. A length this platform cannot
/// address is not a small length. The alternative to an error is a wrong answer,
/// not a worse one. `what` is the caller's description of the thing being sized,
/// so the message says which one rather than "a length".
pub(crate) fn zeroed(bytes: u64, what: &str) -> io::Result<Vec<u8>> {
    usize::try_from(bytes).map(|n| vec![0u8; n]).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{what} is {bytes} bytes, which this platform cannot address"),
        )
    })
}
