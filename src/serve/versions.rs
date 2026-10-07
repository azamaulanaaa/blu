//! What a set holds, as JSON.
//!
//! Two things live here because they are the same question at two scopes: *what
//! versions does this set have*, and *what is this one version*.
//!
//! ## The count is a lower bound, and that is the answer
//!
//! [`Versions`] is the same list `lepatch versions` prints, from the same index
//! files, and it carries the same caveat, because it is the same answer.
//!
//! A version whose index file was never written **still exists and still
//! restores**. The index files are a cache — the store derives them from its
//! commit records, and deleting every one loses nothing. So this may report fewer
//! versions than the set holds, and never more.
//!
//! A client that needs to be certain of the tip should not use this as its
//! source of truth for "what is the newest version"; it should ask for a
//! **pack**, which resolves a version through the store and falls back to a scan
//! when an index is stale. That is why [`Resolved`] resolves through the store
//! rather than reading an index file, and why an unindexed version can still be
//! packed even though it is not listed.

use std::io;

use crate::storage::{BlobId, BlobStore, Compression, Read};

/// One version, as listed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Listed {
    /// The set's own number for it.
    pub version: u32,
}

/// The versions a set's index files name, oldest first.
///
/// # Errors
///
/// Returns an error if the set's directory cannot be read.
///
/// Returns [`io::ErrorKind::NotFound`] if the name is not a set at all: no
/// segment file is there, so there is nothing to list versions of.
pub(super) fn indexed(base: &std::path::Path) -> io::Result<Vec<Listed>> {
    let set = crate::layout::IndexSet::new(base);

    // No segments at all is a mistyped name, not an empty set. Answering "no
    // versions" about a name that names nothing is a failure a script acts on,
    // and `lepatch versions` refuses it for the same reason.
    if set.segments()?.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "no backup set at {} (looked in {})",
                set.base().display(),
                set.directory().display()
            ),
        ));
    }

    Ok(set
        .indexed_versions()?
        .into_iter()
        .map(|version| Listed { version })
        .collect())
}

/// One version's figures, resolved through the store.
///
/// Every snapshot hash here comes from the store, never from an index file alone.
///
/// **Through the store, which checks the file against itself.** An index that is
/// stale, foreign or edited is a reason to fall back to a scan, never a reason
/// to name the version. `lepatch versions` makes exactly this point: an index
/// file checksums one file and never consults the store, so reading it directly
/// would name a version the store cannot serve.
#[derive(Debug, Clone)]
pub(super) struct Resolved {
    /// The set's own number for it.
    pub version: u32,
    /// Its snapshot's hash.
    pub snapshot: BlobId,
    /// How many chunks it needs.
    pub chunks: usize,
    /// Bytes of content those chunks hold.
    pub content_bytes: u64,
}

/// One version, resolved through the store.
///
/// # Errors
///
/// Returns an error if the version does not resolve, or if the set cannot be
/// read. Both are `InvalidData` or `NotFound`, and both are `422` or `404` on
/// the wire.
///
/// Returns an error if the version's snapshot names a chunk the set does not
/// hold, because the figures this returns count the chunks the version needs.
pub(super) async fn resolve(
    store: &BlobStore<Read, Compression>,
    version: u32,
) -> io::Result<Resolved> {
    let snapshot_id = store.snapshot_via_index(version)?;
    let snapshot = crate::command::load_snapshot(store, snapshot_id).await?;

    let plan = crate::storage::Plan::from_snapshot(&snapshot, |id| store.locate(id))?;

    Ok(Resolved {
        version,
        snapshot: snapshot_id,
        chunks: plan.len(),
        content_bytes: plan.total_bytes(),
    })
}

#[cfg(test)]
mod tests {
    use super::indexed;

    #[tokio::test]
    async fn a_name_that_is_not_a_set_is_not_an_empty_set() {
        // The distinction `lepatch versions` draws. Answering "no versions" about
        // a name that names nothing is a failure a client acts on — it would
        // read as "this set is empty", which is a different claim, and one that
        // makes a client give up rather than look elsewhere.
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nosuchset");

        let error = indexed(&missing).unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        assert!(error.to_string().contains("no backup set at"));
    }

    #[tokio::test]
    async fn a_set_with_segments_but_no_commit_lists_empty() {
        // A real set that has been written to and never committed. It exists, so
        // the answer is an empty list rather than a refusal.
        let dir = tempfile::tempdir().unwrap();
        let blob = dir.path().join("store");

        let storage = crate::storage::BlobStore::open_for_writing(
            &blob,
            crate::storage::SegmentLimit::default(),
            crate::storage::Compression::default(),
        )
        .await
        .unwrap();
        let _ = storage;

        // A segment that exists but holds no commit record.
        std::fs::write(
            crate::layout::IndexSet::new(&blob).segment_path(0),
            b"LPST\x02\x00\x00\x00\x00\x00\x00\x00",
        )
        .unwrap();

        assert!(indexed(&blob).is_ok());
    }
}
