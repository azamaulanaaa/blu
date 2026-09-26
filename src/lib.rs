//! `lepatch` — content-defined-chunking backup with deduplicated versions.
//!
//! A backup is a *snapshot*: a description of a directory tree, the distinct
//! chunks its files are made of, and how those chunks map back onto the files.
//! Snapshots are stored as blobs in an append-only store, and consecutive
//! backups of the same location deduplicate against each other, so an unchanged
//! file costs nothing after the first time it is seen.
//!
//! # Layout
//!
//! A backup set is a base path plus three kinds of file; see [`index::IndexSet`].
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> std::io::Result<()> {
//! use lepatch::{command::{backup, restore, Options}, storage::BlobFileStorage};
//!
//! // First backup.
//! let store = BlobFileStorage::<true>::new("photos.bin").await?;
//! let key = backup("~/Pictures", Options::default(), store).await?;
//!
//! // Later backup, reusing the chunks already stored.
//! let store = BlobFileStorage::<true>::new("photos.bin").await?;
//! let key = backup(
//!     "~/Pictures",
//!     Options { base_key: Some(key), ..Options::default() },
//!     store,
//! ).await?;
//!
//! // Restore.
//! let store = BlobFileStorage::<false>::new("photos.bin").await?;
//! restore("~/Pictures-restored", key, store).await?;
//! # Ok(())
//! # }
//! ```

pub mod command;
pub mod fsattr;
pub mod index;
pub mod metadata;
pub mod reader;
pub mod storage;
pub mod writer;
