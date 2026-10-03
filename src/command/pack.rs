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
    pack::{Pack, StorageStored},
    storage::{self, BlobId, Placement},
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

/// The version being packed, and what the set it came from calls it.
///
/// The pair because neither half is enough. The snapshot is what the pack
/// *holds* — the file table, and through it every chunk — and the number is what
/// the header *says about where it came from*, which is not derivable from the
/// snapshot: two sets number their versions independently, so the same snapshot
/// is version 2 of one and version 9 of another, and a pack that recorded only
/// the snapshot could not tell a receiver which.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    /// What the set this is packed from calls the version.
    ///
    /// Written into the header, where it is fixed-length and so readable without
    /// walking the variable part. It buys `merge` the ability to notice that a
    /// pack arrived from behind; it is not a coordinate in the receiving set and
    /// nothing maps it onto one.
    pub number: u32,
    /// The version itself, which the pack carries as its own file table.
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
///
/// `payload` is the *stored* size rather than the content size, and it is knowable
/// before the pack is written because the plan carries each chunk's placement:
/// a record is a header plus whatever the set already had for that chunk, so a
/// compressed set's pack is smaller than its content and the figure is the bytes a
/// fetch would actually move.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Packed {
    /// Chunks in the pack.
    pub chunks: u64,
    /// Chunks the whole version has, which a delta omits some of.
    pub of: u64,
    /// Bytes of chunk records in the pack, headers included.
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
/// `version.number` is recorded in the header: a receiver reading the pack back
/// needs to be able to see that this arrived from a version behind the one it is
/// already on, which a snapshot hash cannot tell it.
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
    version: &Version,
    mut locate: F,
    base: Option<Base>,
    out: &Path,
    options: Options,
) -> io::Result<Packed>
where
    S: StorageStored + ?Sized,
    F: FnMut(&BlobId) -> io::Result<Placement>,
{
    let Version {
        number,
        snapshot: version_snapshot,
    } = version;

    // A function of the tree, not of how the set laid its chunks out, so the pack
    // is reproducible. The decoded snapshot is handed to the writer as well as
    // resolved into a plan, because a pack carries its own file table and so is
    // the version rather than a reference to it.
    let whole = storage::Plan::from_snapshot(version_snapshot, &mut locate)?;
    let of = u64::try_from(whole.len()).unwrap_or(0);

    // A delta is the same pack with fewer chunks in it: no format change and no new
    // code path downstream, because the snapshot still travels with it and the
    // chunks that are not here are exactly the ones a holder of `base` has.
    let plan = match base.as_ref() {
        None => whole,
        Some(base) => whole.since(version_snapshot, &base.snapshot),
    };

    let table = u64::try_from(
        version_snapshot
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
        // The records the file would carry, each a header over the bytes the set
        // already holds for that chunk. Worked out from the plan's placements
        // rather than from `Plan::total_bytes`, which is content: the two differ
        // by every chunk's header and by every frame the set did not have to make.
        payload: plan.chunks().iter().fold(0u64, |total, chunk| {
            total
                .saturating_add(storage::header_len(
                    chunk.placement.stored_len,
                    chunk.placement.raw_len,
                ))
                .saturating_add(chunk.placement.stored_len)
        }),
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
        Some(base) => {
            Pack::write_delta(store, &plan, version_snapshot, base.id, *number, out).await?
        }
        None => Pack::write(store, &plan, version_snapshot, *number, out).await?,
    };

    // Read back off the written pack rather than computed here, so the figures a
    // pack reports are the ones its own index holds. A report assembled from the
    // plan could disagree with the file by the header and the index, and those are
    // precisely the bytes a client fetching it is counting.
    report.bytes = Some(written.file_len()?);
    report.content = Some(crate::pack::hash_file(out).await?);

    Ok(report)
}
