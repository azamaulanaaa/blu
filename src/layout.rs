//! On-disk layout of a versioned backup set.
//!
//! Named for what it is. This was `src/index.rs`, and it contains no index: the
//! per-version index files are read by [`crate::storage::VersionIndex`] and
//! checked by [`crate::storage::resolve`]. What is here is the *set* — the
//! segments, the index file names, the lock, and the numbering that ties them
//! together — which is the layout, and which the module's own first line has said
//! all along. A module called `index` holding `IndexSet`, next to a `storage`
//! module holding the actual `VersionIndex`, is the kind of thing a reader
//! navigates around instead of through.
//!
//! A backup set is a sequence of numbered segments plus a lock, all named after a
//! common base path:
//!
//! ```text
//! photos.0000.bin     segment 0: the first N bytes of records
//! photos.0001.bin     segment 1, sealed once full and never written again
//! ...
//! photos.v0001.idx    where version 1's snapshot is, so a restore need not scan
//! photos.lock         held for the duration of a write
//! ```
//!
//! ## Segments
//!
//! Each file is under a fixed limit, so nothing here meets a maximum file size,
//! a tool that cannot read the set, or a transfer that has to succeed in one
//! piece, and a segment can be moved, verified or replaced on its own.
//!
//! A full segment is **sealed** and never written again, so a torn write can only
//! be in the last one: a failure inside any other is permanent and detectable
//! rather than something the next open papers over by truncating it.
//!
//! Every segment's header carries its own number, and a file whose header does
//! not match the number in its name is refused rather than read as though it
//! belonged — a segment copied in from elsewhere is caught that way. Segments
//! are numbered from zero with no gaps; a gap is refused too, because a set
//! missing a segment in the middle cannot be read as though the missing chunk
//! data were simply not there.
//!
//! ## Index files
//!
//! One small file per version, saying which segment that version's snapshot is
//! in — so answering "where is version 7's snapshot" costs a file name rather
//! than the headers of every record in the set.
//!
//! **They are a cache, not a second source of truth.** The store's commit records
//! are the record of what versions exist; an index file is derived from them and
//! can be lost, stale, or wrong without the store disagreeing about anything. If
//! the two conflict, the store wins and the index is rebuilt. The commit record
//! is the commit, a single append to a single file, and the index is written
//! after it, best-effort, so losing one costs a scan rather than a backup.
//! `lepatch reindex` rebuilds them from the store.
//!
//! The `v` in `photos.v0001.idx` separates the two kinds of file: without it,
//! `photos.0001.idx` and `photos.0001.bin` would name different things in the
//! same directory, and a glob or a person clearing out old index files would hit
//! both.
//!
//! The base path is treated as an opaque prefix rather than a stem, so a base
//! of `photos.tar` yields `photos.tar.0000.bin` instead of silently losing the
//! extension the way `Path::with_extension` would.

use std::{
    ffi::OsStr,
    fs, io,
    path::{Path, PathBuf},
};

/// Extension of a store segment.
///
/// Named for the segment rather than for a blob, because that is what it is: a
/// segment is a run of records, and "blob" already means a chunk's content
/// everywhere else in this crate. A `blob_path` returning `photos.0000.bin` was
/// naming a file that holds thousands of blobs, or none.
pub const SEGMENT_EXTENSION: &str = "bin";
/// Extension of a per-version index file.
pub const INDEX_EXTENSION: &str = "idx";
/// Extension of the write lock.
pub const LOCK_EXTENSION: &str = "lock";

/// Digits in a segment's number.
///
/// The width is what makes lexical order over a directory listing the same as
/// numeric order, so a listing is already in order. It also means the naming
/// scheme can only express so many segments: [`MAX_SEGMENT`] is the last one
/// whose number fits, and a run that needs another is refused rather than
/// writing a file that [`IndexSet::segments`] would not find.
pub const SEGMENT_DIGITS: usize = 4;

/// The highest segment number the naming scheme can express.
///
/// `10^SEGMENT_DIGITS - 1`. A segment's number is zero-padded to
/// [`SEGMENT_DIGITS`], which pads but never truncates, so the number just past
/// this one is *written* as a five-digit name and then *rejected* by
/// the reader that numbers the set's files, which the file would exist, the set would not see it,
/// and any commit record inside it would be invisible to every later run.
pub const MAX_SEGMENT: u32 = 9_999;

/// A backup set rooted at a base path.
///
/// # Example
///
/// The names are the whole contract, so they are worth pinning in something that
/// runs. The base path is treated as an opaque *prefix* rather than a stem, so a
/// base of `photos.tar` yields `photos.tar.0000.bin` — losing the extension the
/// way [`Path::with_extension`] would — and the `v` in `photos.v0001.idx` keeps
/// the two kinds of file from ever naming the same thing in one directory.
///
/// This example is not `no_run`: it touches no disk outside a path that does not
/// exist, so it runs and its assertions are checked.
///
/// ```
/// # fn main() -> std::io::Result<()> {
/// use lepatch::layout::IndexSet;
///
/// let set = IndexSet::new("/data/photos.tar");
///
/// assert_eq!(set.segment_path(0),    std::path::Path::new("/data/photos.tar.0000.bin"));
/// assert_eq!(set.segment_path(1),    std::path::Path::new("/data/photos.tar.0001.bin"));
/// assert_eq!(set.index_path(1),      std::path::Path::new("/data/photos.tar.v0001.idx"));
/// assert_eq!(set.lock_path(),        std::path::Path::new("/data/photos.tar.lock"));
///
/// // Nothing exists yet, and that is an empty set rather than an error.
/// assert!(set.segments()?.is_empty());
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSet {
    base: PathBuf,
}

/// The set's own name inside `full`, and the digits between the tail's tag and
/// its extension — if the rest of it is a tail at all.
///
/// `<base>.<tag>NNNN.<ext>`, where `tag` is empty for a segment and `v` for an
/// index file. **The whole grammar of a set's naming, in one place**, because
/// there are two readers of it and they used to spell it between them: the scan
/// that numbers the files a directory holds, and the name a caller gives to open
/// one. The shapes are the same shape, so a change to the scheme is one edit
/// rather than two in two different languages.
///
/// As **bytes**, because one reader must handle a name that is not UTF-8 — the
/// set's own name is not required to be, and [`IndexSet::numbered_file`] went to
/// lengths precisely because it had to. The caller that has a `&str` converts
/// what comes back, which refuses a base that fell inside a character rather than
/// handing back half of one.
///
/// Returns `None` for anything else, which every caller treats as "not a name of
/// ours": most of what is in a set's directory is not part of the set.
fn tail_of<'a>(full: &'a [u8], tag: &str, ext: &str) -> Option<(&'a [u8], &'a [u8])> {
    // The name is the base, then a fixed-shape tail. Checking the whole length up
    // front means the slices below are known to be in bounds, and it rejects a
    // name carrying a longer base than this set's.
    let tail = 1 + tag.len() + SEGMENT_DIGITS + 1 + ext.len();
    let (base, rest) = full.split_at_checked(full.len().checked_sub(tail)?)?;

    // `.`, then the tag — empty for a segment, `v` for an index file.
    if rest.first() != Some(&b'.') || &rest[1..=tag.len()] != tag.as_bytes() {
        return None;
    }

    let after_tag = 1 + tag.len();
    let digits = rest.get(after_tag..after_tag + SEGMENT_DIGITS)?;
    if !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }

    // `.`, then the extension, which is whatever is left.
    let suffix = rest.get(after_tag + SEGMENT_DIGITS..)?;
    if suffix.first() != Some(&b'.') || &suffix[1..] != ext.as_bytes() {
        return None;
    }

    Some((base, digits))
}

/// The set a segment file belongs to, or `path` itself when it is not one.
///
/// Unwraps the same fixed-shape tail that a segment listing matches, and nothing
/// else: a name is a set's name until it is shaped exactly like one of the files
/// that set is made of.
///
/// A name that is not valid UTF-8 is left exactly as it was given. Building an
/// `OsStr` from a truncated byte slice needs `from_encoded_bytes_unchecked`,
/// which is `unsafe`, and this crate forbids unsafe code — so a set whose base
/// name is not valid UTF-8 keeps the spelling it always had rather than being
/// addressed by a lossy one.
fn strip_segment_tail(path: PathBuf) -> PathBuf {
    let Some(name) = path.file_name().and_then(OsStr::to_str) else {
        return path;
    };
    // `None` where the offset the tail's arithmetic works out is not a character
    // boundary, which a name can put there by having a multi-byte character
    // under it: `café` is three characters and four bytes, and the fourth is in
    // the middle of the last one. Splitting there would panic, so the name is
    // left as it was given — which is what every name this function does not
    // recognise gets, and the same answer a name that is not UTF-8 got above.
    let Some((base, _)) = tail_of(name.as_bytes(), "", SEGMENT_EXTENSION) else {
        return path;
    };
    let Some(base) = std::str::from_utf8(base)
        .ok()
        .filter(|base| !base.is_empty())
    else {
        return path;
    };

    // The directory is the caller's and is kept exactly as it was given; only
    // the file name is read as naming something inside the set.
    match path.parent() {
        Some(parent) => parent.join(base),
        // Unreachable while the `to_str` above answered: a path with no parent is
        // a root, and a root has no file name for the tail to be cut from. Kept
        // rather than folded into the other arm, because it returns the right
        // thing — the same path with the tail off — and a future caller reaching
        // it should get that rather than the name it was given.
        None => PathBuf::from(base),
    }
}

impl IndexSet {
    /// A backup set whose files are named after `base`.
    ///
    /// `base` may name the set or one of its segments. `photos` and
    /// `photos.0000.bin` are one set, and the second is the name a directory
    /// listing shows and the one a person copies, so both should work wherever a
    /// set is named rather than only the one this tool invented.
    ///
    /// The tail is stripped on **shape**, not on what happens to be on disk, so a
    /// set that does not exist yet behaves exactly like one that does, and a
    /// mistyped segment path opens the set the user meant rather than a second
    /// one under a name they did not say. The cost is that a set genuinely named
    /// `report.2024.bin` cannot be addressed — its name reads as a segment of
    /// `report`. That is the same trade every command makes about a path shaped
    /// like one of its own files, and the alternative, probing the disk, makes
    /// the answer depend on what exists, which is worse to explain.
    ///
    /// Only a segment's tail is unwrapped. An index file or a lock names the set
    /// just as well, and taking those too would be three shapes to get right and
    /// three ways to be wrong; they are refused instead.
    pub fn new(base: impl Into<PathBuf>) -> Self {
        Self {
            base: strip_segment_tail(base.into()),
        }
    }

    /// The base path this set is named after.
    #[must_use]
    pub fn base(&self) -> &Path {
        &self.base
    }

    /// Path of segment `segment` of the store.
    ///
    /// Zero-padded to [`SEGMENT_DIGITS`], so lexical order over the files in a
    /// directory is numeric order over the segments. Padding is not truncation:
    /// a number wider than the width is named with its own digits, which no
    /// longer sorts, and which the set's own file-numbering reader will not read back.
    /// Callers should hold `segment` to [`MAX_SEGMENT`]; the store refuses to
    /// roll over past it.
    ///
    /// This was `blob_path`, which named a segment file as though it held one
    /// blob. It does not: it holds whatever records the run wrote into it, and a
    /// segment boundary is a write policy rather than anything the content
    /// implies, so there is no "the blob" for such a path to belong to.
    ///
    /// **A `Segment` newtype is the safer signature and is not used here.** This
    /// and [`IndexSet::index_path`] both take a `u32` and mean different things, so
    /// `set.index_path(index.segment)` would name a real file in the wrong series
    /// and nothing would object. A newtype would make that a compile error.
    ///
    /// It is declined on the arithmetic, which was counted: 78 call sites across
    /// the crate and its tests, 164 once the `segment:` fields in
    /// [`Placement`](crate::storage::Placement), `VersionIndex` and the test
    /// fixtures are included. The mistake does not exist today — no caller passes a
    /// version here — and the audit that proposed the newtype called its own item
    /// "real, but low value-per-churn".
    ///
    /// The same reasoning retired `AsyncReadSeek`'s proposed `pub(crate)` and
    /// `BlobStore<true>`'s proposed `Read`/`Write` in reverse: that one was 15
    /// sites after `open_for_writing` removed the rest, and it was done. What is
    /// left here is what it was before.
    ///
    /// So the argument is recorded on the two functions whose signatures are the
    /// whole of it, in the expectation that the next person to want this will read
    /// the count rather than re-derive it. 78 sites is a day's mechanical churn
    /// with the compiler checking every one, and it is worth doing the day a caller
    /// is caught mixing them.
    #[must_use]
    pub fn segment_path(&self, segment: u32) -> PathBuf {
        self.suffixed(&format!("{segment:0SEGMENT_DIGITS$}.{SEGMENT_EXTENSION}"))
    }

    /// Every segment of the set that is on disk, in order.
    ///
    /// Reads the directory rather than probing for `0`, `1`, `2` until one is
    /// missing, and takes only files named exactly `base.NNNN.bin` — this reads a
    /// directory a person can put things in. A base path whose directory does not
    /// exist yet is an empty set rather than an error.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory exists but cannot be read, or if the set
    /// is missing a segment in the middle.
    pub fn segments(&self) -> io::Result<Vec<PathBuf>> {
        let base = self.base_name();
        let directory = self.directory();

        let entries = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let mut found: Vec<(u32, PathBuf)> = Vec::new();
        for entry in entries {
            let path = entry?.path();
            if path.parent() != Some(directory) {
                continue;
            }

            let Some(number) = Self::numbered_file(&path, base, "", SEGMENT_EXTENSION) else {
                continue;
            };

            found.push((number, path));
        }

        found.sort_by_key(|(number, _)| *number);

        // A gap is refused rather than tolerated: the segments are the only place
        // the data is, so a missing one would read as though those chunks had
        // never been stored.
        for (index, (number, _)) in found.iter().enumerate() {
            let expected = u32::try_from(index).unwrap_or(u32::MAX);
            if *number != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{} is missing segment {expected}: the segments here stop at {} and then \
                         continue at {number}, so whatever is in between is not here",
                        self.base.display(),
                        expected.saturating_sub(1),
                    ),
                ));
            }
        }

        Ok(found.into_iter().map(|(_, path)| path).collect())
    }

    /// Path of the index file for `version`.
    ///
    /// Takes the same `u32` [`IndexSet::segment_path`] does and means something
    /// else, so it is the second half of the pair whose signatures a `Segment` and
    /// a `Version` newtype would fix. The count and the reasoning are on that one;
    /// this is here so neither signature can be changed without the other being
    /// read.
    #[must_use]
    pub fn index_path(&self, version: u32) -> PathBuf {
        self.suffixed(&format!("v{version:0SEGMENT_DIGITS$}.{INDEX_EXTENSION}"))
    }

    /// Every version that has an index file, in order.
    ///
    /// The names, not the contents: which of them are *current* is the store's
    /// business, not this function's.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory exists but cannot be read.
    pub fn indexed_versions(&self) -> io::Result<Vec<u32>> {
        let base = self.base_name();
        let directory = self.directory();

        let entries = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let mut found: Vec<u32> = Vec::new();
        for entry in entries {
            let path = entry?.path();
            if path.parent() != Some(directory) {
                continue;
            }

            if let Some(version) = Self::numbered_file(&path, base, "v", INDEX_EXTENSION) {
                found.push(version);
            }
        }

        found.sort_unstable();
        Ok(found)
    }

    /// Path of the write lock.
    #[must_use]
    pub fn lock_path(&self) -> PathBuf {
        self.suffixed(LOCK_EXTENSION)
    }

    /// The base path's file name, which every file of this set is named after.
    ///
    /// An [`OsStr`], not a `String`. This is the user's path and it is not
    /// necessarily UTF-8: a set at `/data/\xFFphotos` has segments named
    /// `\xFFphotos.0000.bin`, and every one of them is a legal file name. Reading
    /// it as a `String` needs a choice — reject it, or replace what is not UTF-8
    /// — and the code was making that choice twice, inconsistently, and getting
    /// it wrong both ways.
    fn base_name(&self) -> &OsStr {
        self.base.file_name().unwrap_or(OsStr::new(""))
    }

    /// Directory the set's files live in.
    ///
    /// The working directory for a base path with no parent, which is what a bare
    /// name like `photos` gives.
    #[must_use]
    pub fn directory(&self) -> &Path {
        self.base
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
    }

    /// Create the directory the set lives in, if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created, for instance
    /// because a parent is a regular file or the caller lacks permission.
    pub fn ensure_directory(&self) -> io::Result<()> {
        fs::create_dir_all(self.directory())
    }

    /// The number in `path`'s file name, if it is one of this set's.
    ///
    /// Every file of a set is named `<base>.<tag>NNNN.<ext>`, where `tag` is
    /// empty for a segment and `v` for an index file. Both have the same shape
    /// and differed only in those two literals, so they are one function.
    ///
    /// Returns `None` for anything else, which is skipped rather than reported:
    /// this reads a directory a person can put things in, and most of what is in
    /// it is not part of the set.
    ///
    /// The name is compared as **bytes**, not decoded. That is the whole point:
    /// `to_str` answers `None` for a name that is not UTF-8, and the set's own
    /// name is not required to be. A set at a non-UTF-8 base path used to be
    /// written by [`IndexSet::suffixed`], which keeps the bytes, and then
    /// invisible to the scan, which decoded them — so `lepatch` wrote a store it
    /// could not read back, and a second run failed with `File exists` against
    /// the segment it had already made. The two ends disagreed about a name they
    /// had both just agreed on.
    ///
    /// [`OsStr::as_encoded_bytes`] is the right tool despite the encoding being
    /// formally unspecified: both sides go through it, so the comparison is
    /// between two views produced the same way, which is what it is defined to
    /// support. It is not a claim about what the bytes mean.
    fn numbered_file(path: &Path, base: &OsStr, tag: &str, ext: &str) -> Option<u32> {
        let full = path.file_name()?.as_encoded_bytes();
        let (name, digits) = tail_of(full, tag, ext)?;

        if name != base.as_encoded_bytes() {
            return None;
        }

        std::str::from_utf8(digits).ok()?.parse().ok()
    }

    /// Append `suffix` to the base path's file name, leaving the base itself
    /// untouched.
    fn suffixed(&self, suffix: &str) -> PathBuf {
        let mut name = self
            .base
            .file_name()
            .map_or_else(std::ffi::OsString::new, std::ffi::OsStr::to_os_string);
        name.push(".");
        name.push(suffix);

        self.base.with_file_name(name)
    }

    /// Take the write lock, excluding other processes.
    ///
    /// Advisory, and released on drop only for a clean exit: a killed process
    /// leaves the file behind, and the error says how to recover.
    ///
    /// # Errors
    ///
    /// Returns an error if another process holds the lock, and an error if the
    /// lock file cannot be created.
    pub fn lock(&self) -> io::Result<Lock> {
        self.ensure_directory()?;

        let path = self.lock_path();

        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => Ok(Lock {
                path,
                file: Some(file),
            }),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Err(io::Error::new(
                e.kind(),
                format!(
                    "{} is locked by another lepatch process; if none is running, remove it",
                    path.display()
                ),
            )),
            Err(e) => Err(e),
        }
    }
}

/// The write lock, released on drop.
#[derive(Debug)]
pub struct Lock {
    path: PathBuf,
    file: Option<fs::File>,
}

impl Drop for Lock {
    fn drop(&mut self) {
        self.file.take();
        let _ = fs::remove_file(&self.path);
    }
}
