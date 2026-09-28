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
//! A backup set is a store file and a lock; see [`index::IndexSet`]. The store
//! records its own versions, so publishing one is a single append to a single
//! file and there is no second file whose contents have to be kept in step with
//! it.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> std::io::Result<()> {
//! use lepatch::{command::{backup, restore, Options}, storage::BlobStore};
//!
//! // First backup.
//! let store = BlobStore::<true>::open("photos.bin").await?;
//! let first = backup("~/Pictures", Options::default(), store).await?;
//!
//! // Later backup. Nothing new to store means nothing is written.
//! let store = BlobStore::<true>::open("photos.bin").await?;
//! backup("~/Pictures", Options::default(), store).await?;
//!
//! // Restore.
//! let store = BlobStore::<false>::open("photos.bin").await?;
//! restore("~/Pictures-restored", first.snapshot, store).await?;
//! # Ok(())
//! # }
//! ```

pub mod command;
pub mod fsattr;
pub mod index;
pub mod metadata;
pub mod progress;
pub mod reader;
pub mod slice;
pub mod storage;
