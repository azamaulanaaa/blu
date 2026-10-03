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
//!
//! ## What form a chunk is written in
//!
//! A record is written in one of three ways: the form the source set already holds
//! the chunk in, the content as it is, or a zstd frame at a level of the caller's
//! choosing. The first is the default and the reason the pack format is cheap; the
//! others exist because a pack is also a *delivery* artifact, and what is worth
//! delivering depends on who is receiving it.
//!
//! **Re-encoding changes the bytes in a record and nothing else.** A chunk's name
//! is the BLAKE3 of its *content*, and every form decodes to the same content, so
//! the hash in the record header and in the pack's index is the same number under
//! all three routes — which is what makes the option safe at all. What a receiver
//! checks, and what a restore reconstructs, is content; only the bytes in flight
//! differ, and only the ones that are deliberately being re-derived.
//!
//! The cost of the two non-default routes is a decode per chunk, and under a level an
//! encode too — twice over, once to learn how long each record will be and once to
//! write it (see [`reencode_forms`]). That is the price of a pack built for delivery
//! rather than for a receiver who will keep the sender's frames, and it is paid only
//! when asked for.

use std::{collections::HashMap, io, path::Path};

use tokio::io::AsyncReadExt as _;

use crate::{
    metadata::Snapshot,
    pack::{Pack, StorageStored, Stored},
    storage::{self, BlobId, Codec, Compressor as _, Placement, Plan, StorageGet},
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
    /// The write policy each chunk's record is encoded under, or `None` to keep
    /// the form the set already holds it in.
    ///
    /// `None` is the default, and is the decision for the reason the pack format
    /// was built this way: copying a compressed set's frames is what makes packing
    /// one cheap, and re-deriving every one of them is the work the format exists
    /// to avoid. `Some(Compression::Off)` writes the content raw and
    /// `Some(Compression::Zstd { level })` writes a frame at that level, which is
    /// the same vocabulary `merge --compression` takes and the same one a user's
    /// `0..9` becomes through [`storage::Compression::from_level`].
    ///
    /// **A [`storage::Compression`] rather than a dedicated enum, which is a
    /// constraint of this crate's exports and not a preference.** The two commands'
    /// options read as one vocabulary and are parsed by one parser in the CLI, so
    /// an enum of this module's own would want a name beside
    /// [`MergeCompression`](crate::command::MergeCompression) and a second place to
    /// keep it exported; naming the store's own policy says what it is — a write
    /// policy, the same thing `backup --compression-level` is — and lets the
    /// compressor's fallback be used directly rather than reimplemented here.
    ///
    /// [`MergeCompression`]: crate::command::MergeCompression
    pub compression: Option<storage::Compression>,
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
///
/// **It follows the [`Options::compression`] the run was given, and that is the
/// resolution of what the figure means under a non-default one.** It is not a
/// property of the version and never was: it counts the records *this run writes*,
/// so a pack built with `--compression 9` reports more payload than the same pack
/// built with `keep`, and both report exactly what is in their own file. Holding it
/// at the source set's figures instead would mean reporting a number about a file
/// that was not written, which is the one thing a figure in this struct must never
/// do. `chunks`, `of` and `table` are properties of the version and do not move
/// with the option; `bytes` stays the size of the file on disk and is `None` on a
/// dry run for the reason above, which no option changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Packed {
    /// Chunks in the pack.
    pub chunks: u64,
    /// Chunks the whole version has, which a delta omits some of.
    pub of: u64,
    /// Bytes of chunk records in the pack, headers included.
    ///
    /// The records as **this** run writes them: a run that re-encoded reports the
    /// re-encoded sizes. See the note on the struct about why.
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
/// `options.compression` decides the form each record is written in, and it is
/// routed in one place, so the flag and what actually happens cannot be two things
/// that have drifted apart.
///
/// **The store is asked for both of the things the two routes need.** `keep` reads
/// stored bytes, which is [`StorageStored`]; a re-encode reads *content*, which is
/// [`StorageGet`], because a chunk has to be decoded before it can be encoded again.
/// Both bounds are on this one function rather than split across two, because the
/// route is chosen here and a caller cannot choose a route by picking a function:
/// [`BlobStore`](storage::BlobStore) implements both, and a backend that wanted the
/// option would implement both rather than have the default be chosen for it.
///
/// # Errors
///
/// Returns an error if the store cannot resolve a chunk the version names, if a
/// snapshot cannot be recorded on this platform, if a chunk cannot be decoded or
/// re-encoded into the form [`Options::compression`] asks for, or — unless
/// `options.dry_run` — if the file cannot be written.
pub async fn pack<S, F>(
    store: &S,
    version: &Version,
    mut locate: F,
    base: Option<Base>,
    out: &Path,
    options: Options,
) -> io::Result<Packed>
where
    S: StorageStored + StorageGet + ?Sized,
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

    // The one place the option routes. The plan is re-resolved under the policy
    // rather than filtered afterwards, because a record's *length* is part of the
    // plan's own geometry — the header's payload total is the sum of the records
    // the plan describes, and it is written before any of them — so a plan still
    // carrying the source set's placements would write a header claiming sizes the
    // records do not have. `Source` then answers the writer from the same policy,
    // and the writer's own check that each record matches its placement is what
    // holds the two to the same answer.
    let (plan, source) = match options.compression {
        None => (plan, Source::Stored(store)),
        Some(policy) => {
            let forms = reencode_forms(store, &plan, policy).await?;
            let whole = storage::Plan::from_snapshot(version_snapshot, |id| match forms.get(id) {
                // Measured: this is what the record will be.
                Some(placement) => Ok(*placement),
                // Not measured, because a delta does not carry it. The plan above
                // is already the delta's, and this one is the whole version before
                // `since` drops the chunks a holder of the base already has — so
                // every chunk the version reads is here and not every chunk here
                // is in the plan. The `since` below drops the ones that were never
                // measured, and their placement is never written and never
                // counted, so the store's own answer is only ever a placeholder.
                //
                // Read as dead code once: every `--compression` test used a full
                // pack, and every `--from` test used the default, so nothing ran
                // through the pair. `a_delta_packed_with_a_compression_level_
                // applies_and_merges` is the fixture that does.
                None => locate(id),
            })?;

            let plan = match base.as_ref() {
                None => whole,
                Some(base) => whole.since(version_snapshot, &base.snapshot),
            };

            (plan, Source::Reencoded { store, policy })
        }
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
        // The writer works this out too, and it is that figure rather than this
        // one that ends up in the file — see [`crate::pack::planned_payload_len`], which
        // is one function rather than the two this was.
        payload: crate::pack::planned_payload_len(&plan),
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
            Pack::write_delta(&source, &plan, version_snapshot, base.id, *number, out).await?
        }
        None => Pack::write(&source, &plan, version_snapshot, *number, out).await?,
    };

    // Read back off the written pack rather than computed here, so the figures a
    // pack reports are the ones its own index holds. A report assembled from the
    // plan could disagree with the file by the header and the index, and those are
    // precisely the bytes a client fetching it is counting.
    report.bytes = Some(written.file_len()?);
    report.content = Some(crate::pack::hash_file(out).await?);

    Ok(report)
}

/// Where a pack's records come from, decided by [`Options::compression`].
///
/// A type rather than a branch at the call site because there are two callers of
/// the writer in this function — a full pack and a delta — and a route chosen
/// inside each of them is two places for the flag and the behaviour to disagree.
/// Here there is one, and the writer cannot tell which it has.
///
/// It implements [`StorageStored`] rather than standing in for a store: the writer
/// asks a store for "the bytes this chunk is stored as", and the two routes answer
/// that question with two different reads of the same chunk.
enum Source<'a, S: ?Sized> {
    /// The set's own bytes, copied as they stand.
    Stored(&'a S),
    /// The chunk's content, read and encoded afresh.
    Reencoded {
        /// Where the content comes from.
        store: &'a S,
        /// The policy it is encoded under.
        policy: storage::Compression,
    },
}

impl<S> StorageStored for Source<'_, S>
where
    S: StorageStored + StorageGet + ?Sized,
{
    /// The stored form of `id` under whichever route this is.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot yield the chunk, or — on the
    /// re-encoding route — if its content cannot be read or encoded.
    async fn stored(&self, id: &BlobId) -> io::Result<Stored> {
        match self {
            Self::Stored(store) => store.stored(id).await,
            Self::Reencoded { store, policy } => {
                let content = content_of(*store, id).await?;
                let raw_len = u64::try_from(content.len()).unwrap_or(u64::MAX);

                // The store's own compressor, so the fallback — a frame only where
                // it is strictly smaller — is the store's rule rather than a second
                // one here that could disagree with it about which payloads are
                // worth a frame. It hands the content back untouched in three cases
                // out of four, which is why `Owned` is only paid for the fourth.
                let (codec, bytes) = policy.compress(&content);

                Ok(Stored {
                    bytes: bytes.into_owned(),
                    codec,
                    raw_len,
                })
            }
        }
    }
}

/// A chunk's content, whole.
///
/// One chunk at a time and no more: the writer buffers one record at a time
/// precisely so a pack of a large version does not hold one, and a re-encoding
/// route that read the whole set into memory to encode it would undo that.
///
/// **The content is not hashed here.** A pack's records name their chunks, and a
/// reader checks those names — `merge` decodes and hashes every record on the way
/// in whatever route stored it — so a set that has rotted underneath a pack is
/// caught on the way in rather than here. That is the same bargain the verbatim
/// route makes, and it is deliberate: the source here is the user's own set, not a
/// file somebody downloaded, which is the reason `merge` bothers to check.
///
/// # Errors
///
/// Returns an error if the store cannot yield the chunk or its bytes cannot be
/// read.
async fn content_of<S>(store: &S, id: &BlobId) -> io::Result<Vec<u8>>
where
    S: StorageGet + ?Sized,
{
    let mut reader = store.get(id).await?;
    let mut content = Vec::new();
    reader.read_to_end(&mut content).await?;

    Ok(content)
}

/// The placement each record in `plan` will have under `policy`, by measuring.
///
/// **Measured rather than predicted, which means encoding each chunk twice.** The
/// writer's header carries the payload's total length and is written before any
/// record, so the length of every record has to be known before the first byte of
/// one is written; and a zstd frame's length is a property of the bytes zstd
/// produces, which nothing here can work out without producing them.
///
/// The alternatives were considered and are both worse:
///
/// * *Predict it from the content length.* There is no such function. Compression
///   is what it is because its output size depends on the input's structure, and a
///   header built from a guess would make every record after the first one land at
///   the wrong offset.
/// * *Keep the encoded chunks.* A whole version's frames in memory to encode once,
///   which is precisely the bound the writer holds itself to by buffering one
///   record at a time, and for a pack over a large version it is the difference
///   between a pack that can be written and one that cannot.
///
/// So the encode runs twice under a level and the second run's output is checked
/// against the first by the writer's own per-record agreement check: if a frame
/// came out a different length the run fails rather than writing a pack whose
/// header contradicts its own payload.
///
/// With compression **off** nothing is encoded at all, and nothing is read: a raw
/// record's stored length *is* its content length, so the plan already carries the
/// answer and `--compression 0` costs what `keep` costs.
///
/// # Errors
///
/// Returns an error if a chunk the plan names cannot be read, or — under a level —
/// if the store's compressor cannot produce a frame for it.
async fn reencode_forms<S>(
    store: &S,
    plan: &Plan,
    policy: storage::Compression,
) -> io::Result<HashMap<BlobId, Placement>>
where
    S: StorageGet + ?Sized,
{
    let mut forms = HashMap::with_capacity(plan.len());

    for chunk in plan.chunks() {
        let placement = if policy.level().is_some() {
            let content = content_of(store, &chunk.id).await?;
            let (codec, bytes) = policy.compress(&content);

            Placement {
                codec,
                stored_len: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                raw_len: u64::try_from(content.len()).unwrap_or(u64::MAX),
                ..chunk.placement
            }
        } else {
            // Off: the record holds the content, so the length is the one the plan
            // already resolved and neither a read nor an encode is needed to know it.
            Placement {
                codec: Codec::Raw,
                stored_len: chunk.placement.raw_len,
                ..chunk.placement
            }
        };

        forms.insert(chunk.id, placement);
    }

    Ok(forms)
}
