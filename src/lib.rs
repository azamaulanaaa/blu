//! `lepatch`: content-defined-chunking backup with deduplicated versions.
//!
//! A backup is a *snapshot*. A snapshot describes a directory tree, the distinct
//! chunks its files are made of, and how those chunks map back onto the files.
//! The store holds chunks by the hash of their contents. An unchanged file
//! costs nothing after the first time the store sees it. A file that the store
//! saw in any earlier version costs nothing either. Deduplication is the
//! store's business, and the store holds every version it has ever been given.
//!
//! # Layout
//!
//! A backup set is a sequence of numbered segment files. A backup set also holds
//! a per-version index file for each version, and a lock. See
//! [`layout::IndexSet`]. A segment is *sealed* once it is full. The store never
//! writes to a sealed segment again. Sealing is what makes a torn write
//! possible only in the last segment, and corruption in the other segments
//! permanent and detectable.
//!
//! The index files are a cache. They say which segment holds a version's
//! snapshot. Listing a set then costs one file name per version. Listing a set
//! does not scan every record in the set. The store derives the index files from
//! its commit records. Deleting every one of them loses nothing. The
//! `lepatch reindex` command rebuilds them.
//!
//! The store records its own versions. Publishing one version is a single append
//! to a single file. No second file has contents that must stay in step with
//! that record.
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
//!     BlobStore::open_for_writing("photos", SegmentLimit::default(), Compression::default()).await?;
//! let first = backup("~/Pictures", Options::default(), store).await?;
//!
//! // Later backup. Nothing new to store means nothing is written.
//! let store =
//!     BlobStore::open_for_writing("photos", SegmentLimit::default(), Compression::default()).await?;
//! backup("~/Pictures", Options::default(), store).await?;
//!
//! // A read-only store takes no segment limit, because a read never rolls over.
//! let store = BlobStore::open_for_reading("photos").await?;
//! restore("~/Pictures-restored", first.snapshot, store).await?;
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod command;
pub mod fsattr;
pub mod layout;
pub mod metadata;
pub mod pack;
pub mod progress;
pub mod reader;
pub mod serve;
pub mod slice;
pub mod storage;
pub mod varint;

/// The README's code blocks, compiled as doctests.
///
/// A README is not attached to anything, so nothing was checking it. Its library
/// examples had drifted a long way. One example called
/// `BlobStore::open_for_writing` with two arguments. That function takes three
/// arguments. The other example called a `read_snapshot` that does not exist.
/// It passed a `BTreeSet` where the API now takes a `HashSet`. It referred to
/// four names it never defined. A reader who copied either example got nothing
/// that compiled.
///
/// `include_str!` makes the Markdown documentation. Every ```rust block in the
/// documentation becomes a doctest. The blocks marked `console` or `text` are
/// transcripts, not code, and the build leaves them alone. Lines beginning `# `
/// prepare what a snippet needs. These lines do not appear in the rendered
/// README. This lets a fragment read like a fragment and still compile.
///
/// `cfg(doctest)` limits the item to the time when doctests are collected. The
/// item documents nothing at runtime. It costs one unused type otherwise.
///
/// This runs under `cargo test --doc`. The `--all-targets` flag does *not*
/// imply it. The `doctests` task in `mise.toml` and the matching CI step make
/// it worth having.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeExamples;
