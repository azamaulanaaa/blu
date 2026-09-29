//! `lepatch` — content-defined-chunking backup with deduplicated versions.
//!
//! A backup is a *snapshot*: a description of a directory tree, the distinct
//! chunks its files are made of, and how those chunks map back onto the files.
//! Chunks are stored by the hash of their contents, so an unchanged file costs
//! nothing after the first time it is seen — and neither does one last seen in
//! any earlier version, because deduplication is the store's business and it
//! holds every version it has ever been given.
//!
//! # Layout
//!
//! A backup set is a sequence of numbered segment files, a per-version index file
//! for each version, and a lock; see [`index::IndexSet`]. A segment is *sealed*
//! once it is full and is never written to again, which is what makes a torn write
//! possible only in the last one and corruption in the rest permanent and
//! detectable.
//!
//! The index files are a cache: they say which segment a version's snapshot is in,
//! so listing a set costs a file name per version rather than a scan of every
//! record in it. They are derived from the store's commit records, and deleting
//! every one of them loses nothing — `lepatch reindex` rebuilds them.
//!
//! The store records its own versions, so publishing one is a single append to a
//! single file and there is no second file whose contents have to be kept in step
//! with it.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> std::io::Result<()> {
//! use lepatch::{
//!     command::{Options, backup, lock_destination, restore},
//!     storage::{BlobStore, Compression, SegmentLimit},
//! };
//!
//! // The claim excludes other runs from this destination. A backup takes no lock
//! // of its own, so taking it is the caller's job — and this example was not
//! // doing it, which left a reader following it with two backups that would
//! // interleave if they happened to overlap.
//! let _claim = lock_destination(std::path::Path::new("~/Pictures-restored"))?;
//!
//! // First backup. Four gibibytes per segment and zstd level 1, both the defaults.
//! let store =
//!     BlobStore::<true>::open("photos", SegmentLimit::default(), Compression::default()).await?;
//! let first = backup("~/Pictures", Options::default(), store).await?;
//!
//! // Later backup. Nothing new to store means nothing is written.
//! let store =
//!     BlobStore::<true>::open("photos", SegmentLimit::default(), Compression::default()).await?;
//! backup("~/Pictures", Options::default(), store).await?;
//!
//! // A read-only store takes no segment limit, because a read never rolls over.
//! let store = BlobStore::<false>::open("photos").await?;
//! restore("~/Pictures-restored", first.snapshot, store).await?;
//! # Ok(())
//! # }
//! ```

pub mod command;
pub mod fsattr;
pub mod index;
pub mod metadata;
pub mod pack;
pub mod progress;
pub mod reader;
pub mod slice;
pub mod storage;
pub mod varint;

/// The README's code blocks, compiled as doctests.
///
/// A README is not attached to anything, so nothing was checking it. Its library
/// examples had drifted a long way: one called `BlobStore::open` with two
/// arguments when it takes three, and the other called a `read_snapshot` that
/// does not exist, passed a `BTreeSet` where the API now takes a `HashSet`, and
/// referred to four names it never defined. A reader copying either got nothing
/// that compiled.
///
/// `include_str!` makes the Markdown documentation, so every ```rust block in it
/// becomes a doctest. The blocks marked `console` or `text` are transcripts, not
/// code, and are left alone. Lines beginning `# ` set up what a snippet needs
/// without appearing in the rendered README, which is what lets a fragment read
/// like a fragment and still compile.
///
/// `cfg(doctest)` so the item exists only while doctests are being collected —
/// it documents nothing at runtime and costs one unused type otherwise.
///
/// This runs under `cargo test --doc`, which `--all-targets` does *not* imply,
/// so the `doctests` task in `mise.toml` and the matching CI step are what make
/// it worth having.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeExamples;
