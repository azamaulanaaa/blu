//! Writing one version's chunks to a single distributable file.
//!
//! A pack is derived output: it is regenerated from a set and its version alone,
//! and losing one loses nothing. That is why it is the one artifact here with a
//! dry run that has something to say about cost — a pack over a large version is
//! gigabytes, and whether one is wanted is often not known until somebody has
//! looked at what it would contain.
//!
//! The plan is the pack's own. The chunks are resolved here, through
//! [`Plan::from_snapshot`], rather than by the caller passing a list in: the
//! figure a dry run reports and the bytes a real run writes are then the same
//! computation, and a caller cannot report a plan it did not build.

use std::{io, path::Path};

use crate::{
    metadata::Snapshot,
    pack::Pack,
    storage::{self, BlobId, Placement, StorageGet},
};

/// Knobs for a single [`pack`] run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Options {
    /// Work out what a pack would contain, and write none of it.
    ///
    /// Off by default, and the default is the decision. A pack overwrites whatever
    /// is at its output path — it is regenerated output, not a record — so a run
    /// that is only reporting what it would produce has to be asked for rather than
    /// inferred from the absence of a destination.
    pub dry_run: bool,
}

/// The version a pack is a delta against.
///
/// A delta is the same file carrying the same version with fewer chunks in it, and
/// the base it assumes is part of what has to be worked out before the pack's
/// contents are: it is what decides which chunks survive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Base {
    /// Version number the client is assumed to already hold.
    pub version: u32,
    /// Hash of that version's snapshot, which goes in the pack's header.
    pub id: BlobId,
    /// That version itself, which the delta is computed against.
    pub snapshot: Snapshot,
}

/// What a pack holds, or would hold.
///
/// On a dry run the two figures that can only come from the written file are
/// `None`, and the reason is the file's own format: the size on disk includes a
/// header and a chunk index whose length depends on varint-encoded offsets, and the
/// content hash is the hash of those bytes together. Both are properties of a file
/// that does not exist yet. Everything else — which chunks, how many, how much
/// payload, how big the file table — is known before a byte is fetched, and is what
/// a dry run reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Packed {
    /// Chunks in the pack.
    pub chunks: u64,
    /// Chunks the whole version has, which a delta omits some of.
    pub of: u64,
    /// Bytes of chunk payload in the pack.
    pub payload: u64,
    /// Bytes of file table in the pack.
    ///
    /// The version's snapshot, which travels with the pack so a client needs
    /// nothing else to read it.
    pub table: u64,
    /// The version this pack is a delta against, if it is one.
    pub base: Option<u32>,
    /// Bytes the pack occupies on disk, once it has been written.
    pub bytes: Option<u64>,
    /// The pack's content hash, once it has been written.
    pub content: Option<BlobId>,
}

/// Resolve a version into the chunks a pack would carry, and write it.
///
/// `locate` answers where a chunk is in the store, and is given as a callback
/// rather than a store so that this works for any backend — the same reason
/// [`storage::Plan::from_snapshot`] takes one.
///
/// A `base` makes the pack a delta: the same file, carrying the same version, with
/// only the chunks a holder of that version does not already have. Nothing about
/// the format changes, and the header records the assumption so an apply can refuse
/// rather than apply what its reader happens to hold.
///
/// # Errors
///
/// Returns an error if the store cannot resolve a chunk the version names, if a
/// snapshot cannot be recorded on this platform, or — unless `options.dry_run` — if
/// the file cannot be written.
pub async fn pack<S, F>(
    store: &S,
    snapshot: &Snapshot,
    mut locate: F,
    base: Option<Base>,
    out: &Path,
    options: Options,
) -> io::Result<Packed>
where
    S: StorageGet + ?Sized,
    F: FnMut(&BlobId) -> io::Result<Placement>,
{
    // A function of the tree, not of how the set laid its chunks out, so the pack
    // is reproducible. The decoded snapshot is handed to the writer as well as
    // resolved into a plan, because a pack carries its own file table and so is
    // the version rather than a reference to it.
    let whole = storage::Plan::from_snapshot(snapshot, &mut locate)?;
    let of = u64::try_from(whole.len()).unwrap_or(0);

    // A delta is the same pack with fewer chunks in it: no format change and no new
    // code path downstream, because the snapshot still travels with it and the
    // chunks that are not here are exactly the ones a holder of `base` has.
    let plan = match base.as_ref() {
        None => whole,
        Some(base) => whole.since(snapshot, &base.snapshot),
    };

    let table = u64::try_from(
        snapshot
            .to_bytes()
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("this version cannot be packed on this platform: {e}"),
                )
            })?
            .len(),
    )
    .unwrap_or(u64::MAX);

    let mut report = Packed {
        chunks: u64::try_from(plan.len()).unwrap_or(0),
        of,
        payload: plan.total_bytes(),
        table,
        base: base.as_ref().map(|base| base.version),
        bytes: None,
        content: None,
    };

    if options.dry_run {
        return Ok(report);
    }

    // `write_delta`, not `write` over a filtered plan, because the header has to
    // say what the delta assumes — that is what lets an apply refuse rather than
    // apply what it happens to hold.
    let written = match base.as_ref() {
        Some(base) => Pack::write_delta(store, &plan, snapshot, base.id, out).await?,
        None => Pack::write(store, &plan, snapshot, out).await?,
    };

    // Read back off the written pack rather than computed here, so the figures a
    // pack reports are the ones its own index holds. A report assembled from the
    // plan could disagree with the file by the header and the index, and those are
    // precisely the bytes a client fetching it is counting.
    report.bytes = Some(written.file_len()?);
    report.content = Some(crate::pack::hash_file(out).await?);

    Ok(report)
}
