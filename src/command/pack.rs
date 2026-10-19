//! Writing one version's chunks to a single distributable file.
//!
//! A pack is derived output: it is regenerated from a set and its version alone,
//! and losing one loses nothing.
//! That is why it is the one artifact here with a dry run that has something to
//! say about cost.
//! A pack over a large version is gigabytes. Whether one is wanted is often
//! not known until somebody has looked at what it would contain.
//!
//! The plan is the pack's own.
//! The chunks are resolved here, through [`Plan::from_snapshot`], rather than by
//! the caller passing a list in.
//! The figure a dry run reports and the bytes a real run writes are then the same
//! computation.
//! A caller cannot report a plan it did not build.
//!
//! ## What form a chunk is written in
//!
//! A record is written in one of three ways.
//! One way is the form the source set already holds the chunk in.
//! Another way is the content as it is.
//! The third way is a zstd frame at a level of the caller's choosing.
//! The first is the default and the reason the pack format is cheap.
//! The others exist because a pack is also a *delivery* artifact.
//! What is worth delivering depends on who is receiving it.
//!
//! **Re-encoding changes the bytes in a record and nothing else.**
//! A chunk's name is the BLAKE3 of its *content*.
//! Every form decodes to the same content.
//! The hash in the record header and in the pack's index is therefore the same
//! number under all three routes.
//! That is what makes the option safe at all.
//! What a receiver checks, and what a restore reconstructs, is content.
//! Only the bytes in flight differ, and only the ones that are deliberately being
//! re-derived.
//!
//! The two non-default routes pay a decode per chunk, and a level pays an
//! encode too.
//! A real run encodes each chunk **once**.
//! The writer reserves the width of the payload total in the header, writes the
//! records, and patches the total in afterwards.
//! A dry run does the opposite.
//! It measures each frame to report the figures of a file it does not write,
//! and it discards that encode.
//! See [`reencode_forms`].
//! That is the price of a pack built for delivery rather than for a receiver who
//! will keep the sender's frames.
//! The price is paid only when asked for.

use std::{
    collections::HashMap,
    io,
    path::{Path, PathBuf},
};

use tokio::io::AsyncReadExt as _;
use tracing::instrument;

use crate::{
    command::load_snapshot,
    metadata::Snapshot,
    pack::{Pack, Route, StorageStored, Stored},
    storage::{
        self, BlobId, BlobStore, Codec, Compressor as _, Placement, Plan, StorageGet, WriteMode,
    },
};

/// Write one version's chunks to a single file.
///
/// Start with [`PackBuilder::new`]. Pin a version with `version`.
/// Pack a delta with `from`. Re-encode with `compression`.
/// Preview with `dry_run`. Call `run`.
///
/// A pack carries its own file table. It needs nothing else to travel.
/// A full pack suits any set. A delta suits a holder of its base.
/// The base decides which chunks survive. Nothing about the format changes.
///
/// Keeping the stored form is the default. Copying a compressed set's frames
/// is what makes packing one cheap. Re-encoding decodes every chunk first.
/// That work pays only when delivery wants it.
#[derive(Debug)]
pub struct PackBuilder<'a, W: WriteMode, C: storage::Compressor> {
    store: &'a BlobStore<W, C>,
    out: PathBuf,
    version: Option<u32>,
    base: Option<u32>,
    compression: Option<storage::Compression>,
    dry_run: bool,
}

impl<'a, W: WriteMode, C: storage::Compressor> PackBuilder<'a, W, C> {
    /// Start a pack from `store` into `out`.
    ///
    /// The builder packs the latest version as a full pack. Narrow it below.
    /// `out` is overwritten when the run writes.
    #[must_use]
    pub fn new(store: &'a BlobStore<W, C>, out: impl Into<PathBuf>) -> Self {
        Self {
            store,
            out: out.into(),
            version: None,
            base: None,
            compression: None,
            dry_run: false,
        }
    }

    /// Pack this version number.
    ///
    /// Without it the run packs the latest version. The set must hold one.
    #[must_use]
    pub fn version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }

    /// Pack only what this version's holder lacks.
    ///
    /// The file carries the same version with fewer chunks. The header records
    /// the assumption. An apply refuses a reader that holds another base.
    #[must_use]
    pub fn from(mut self, base: u32) -> Self {
        self.base = Some(base);
        self
    }

    /// Encode each record under `policy`.
    ///
    /// Without it the run keeps the form the set holds. `Off` writes content
    /// raw. A level writes a fresh zstd frame at that level.
    #[must_use]
    pub fn compression(mut self, policy: storage::Compression) -> Self {
        self.compression = Some(policy);
        self
    }

    /// Work out what the pack would hold, and write none of it.
    ///
    /// The run resolves every chunk and locates it. It writes no file.
    /// The size on disk and the content hash stay empty. Both describe a file
    /// that does not exist yet.
    #[must_use]
    pub fn dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }

    /// Write the pack, or work out what it would hold.
    ///
    /// # Errors
    ///
    /// Returns an error when the set holds no such version.
    /// Returns an error when the store cannot resolve a chunk the version names.
    /// Returns an error when a chunk cannot be decoded or re-encoded.
    /// Returns an error when the file cannot be written, outside a dry run.
    #[instrument(level = "info", skip(self), err)]
    pub async fn run(self) -> io::Result<Packed> {
        let store = self.store;

        let (number, snapshot_id) = match self.version {
            Some(number) => (number, store.snapshot(number)?),
            None => store.latest().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "the set holds no versions, so there is nothing to pack",
                )
            })?,
        };
        let snapshot = load_snapshot(store, snapshot_id).await?;

        let base = match self.base {
            None => None,
            Some(number) => {
                let id = store.snapshot(number)?;
                Some(Base {
                    version: number,
                    id,
                    snapshot: load_snapshot(store, id).await?,
                })
            }
        };

        let version = Version { number, snapshot };

        pack(
            store,
            &version,
            |id| store.locate(id),
            base,
            &self.out,
            Options {
                dry_run: self.dry_run,
                compression: self.compression,
            },
        )
        .await
    }
}

/// Knobs for a single [`pack`] run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct Options {
    /// Work out what a pack would contain, and write none of it.
    ///
    /// Off by default, and the default is the decision.
    /// A pack overwrites whatever is at its output path, and it is regenerated
    /// output rather than a record.
    /// A run that is only reporting what it would produce has to be asked for.
    /// Such a run cannot be inferred from the absence of a destination.
    pub dry_run: bool,
    /// The write policy each chunk's record is encoded under, or `None` to keep
    /// the form the set already holds it in.
    ///
    /// `None` is the default, and is the decision for the reason the pack format
    /// was built this way.
    /// Copying a compressed set's frames is what makes packing one cheap.
    /// Re-deriving every one of them is the work the format exists to avoid.
    /// `Some(Compression::Off)` writes the content raw.
    /// `Some(Compression::Zstd { level })` writes a frame at that level.
    /// That is the same vocabulary `merge --compression` takes.
    /// It is also the vocabulary a user's `0..9` becomes through
    /// [`storage::Compression::from_level`].
    ///
    /// **A [`storage::Compression`] rather than a dedicated enum. This is a
    /// constraint of this crate's exports, not a preference.**
    /// The two commands' options read as one vocabulary.
    /// The CLI parses them with one parser.
    /// An enum of this module's own would want a name beside
    /// [`MergeCompression`](crate::command::MergeCompression) and a second place to
    /// keep it exported.
    /// Naming the store's own policy says what it is.
    /// That policy is a write policy, the same thing `backup --compression-level`
    /// is.
    /// It also lets the compressor's fallback be used directly rather than
    /// reimplemented here.
    ///
    /// [`MergeCompression`]: crate::command::MergeCompression
    pub compression: Option<storage::Compression>,
}

/// The version a pack is a delta against.
///
/// A delta is the same file carrying the same version with fewer chunks in it.
/// The base it assumes is part of what has to be worked out before the pack's
/// contents are.
/// It is the base that decides which chunks survive.
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
/// The pair exists because neither half is enough.
/// The snapshot is what the pack *holds*: the file table, and through it every
/// chunk.
/// The number is what the header *says about where it came from*.
/// The number is not derivable from the snapshot.
/// Two sets number their versions independently, so the same snapshot is
/// version 2 of one and version 9 of another.
/// A pack that recorded only the snapshot could not tell a receiver which.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version {
    /// What the set this is packed from calls the version.
    ///
    /// Written into the header, where it is fixed-length and so readable without
    /// walking the variable part.
    /// It buys `merge` the ability to notice that a pack arrived from behind.
    /// It is not a coordinate in the receiving set, and nothing maps it onto one.
    pub number: u32,
    /// The version itself, which the pack carries as its own file table.
    pub snapshot: Snapshot,
}

/// What a pack holds, or would hold.
///
/// On a dry run the two figures that can only come from the written file are
/// `None`.
/// The reason is the file's own format.
/// The size on disk includes a header and a chunk index.
/// The index length depends on varint-encoded offsets.
/// The content hash is the hash of those bytes together.
/// Both are properties of a file that does not exist yet.
/// Everything else is known before a byte is fetched.
/// That includes which chunks, how many, how much payload, and how big the file
/// table is.
/// A dry run reports all of it.
///
/// `payload` is the *stored* size rather than the content size.
/// It is knowable before the pack is written because the plan carries each
/// chunk's placement.
/// A record is a header plus whatever the set already had for that chunk.
/// A compressed set's pack is therefore smaller than its content.
/// The figure is the bytes a fetch would actually move.
///
/// **It follows the [`PackBuilder::compression`] the run was given. That is the
/// resolution of what the figure means under a non-default one.**
/// It is not a property of the version and never was.
/// It counts the records *this run writes*.
/// A pack built with `--compression 9` reports more payload than the same pack
/// built with `keep`.
/// Both report exactly what is in their own file.
/// Holding it at the source set's figures instead would mean reporting a
/// number about a file that was not written.
/// That is the one thing a figure in this struct must never do.
/// `chunks`, `of` and `table` are properties of the version and do not move
/// with the option.
/// `bytes` stays the size of the file on disk and is `None` on a dry run for
/// the reason above, which no option changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Packed {
    /// Chunks in the pack.
    pub chunks: u64,
    /// Chunks the whole version has, which a delta omits some of.
    pub of: u64,
    /// Bytes of chunk records in the pack, headers included.
    ///
    /// The records as **this** run writes them.
    /// A run that re-encoded reports the re-encoded sizes.
    /// See the note on the struct about why.
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
/// `version.number` is recorded in the header.
/// A receiver reading the pack back needs to see that this arrived from a
/// version behind the one it is already on.
/// A snapshot hash cannot tell the receiver that.
///
/// `locate` answers where a chunk is in the store. It is given as a callback
/// rather than a store, so this works for any backend.
/// [`storage::Plan::from_snapshot`] takes one for the same reason.
///
/// A `base` makes the pack a delta. The file is the same and carries the same
/// version. It holds only the chunks a holder of that version does not already
/// have.
/// Nothing about the format changes.
/// The header records the assumption so an apply can refuse rather than apply what
/// its reader happens to hold.
///
/// `options.compression` decides the form each record is written in, and it is
/// routed in one place.
/// The flag and what actually happens therefore cannot be two things that have
/// drifted apart.
///
/// **The store is asked for both of the things the two routes need.**
/// `keep` reads stored bytes, which is [`StorageStored`].
/// A re-encode reads *content*, which is [`StorageGet`], because a chunk has to
/// be decoded before it can be encoded again.
/// Both bounds are on this one function rather than split across two.
/// The route is chosen here, and a caller cannot choose a route by picking a
/// function.
/// [`BlobStore`](storage::BlobStore) implements both.
/// A backend that wanted the option would implement both rather than have the
/// default be chosen for it.
///
/// # Errors
///
/// Returns an error if the store cannot resolve a chunk the version names.
/// Returns an error if a snapshot cannot be recorded on this platform.
/// Returns an error if a chunk cannot be decoded or re-encoded into the form
/// [`PackBuilder::compression`] asks for.
/// Returns an error if the file cannot be written, unless `options.dry_run` is
/// set.
async fn pack<S, F>(
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
    // is reproducible.
    // The decoded snapshot is handed to the writer as well as resolved into a
    // plan.
    // A pack carries its own file table, so it is the version rather than a
    // reference to it.
    let whole = storage::Plan::from_snapshot(version_snapshot, &mut locate)?;
    let of = u64::try_from(whole.len()).unwrap_or(0);

    // A delta is the same pack with fewer chunks in it.
    // No format changes and no new code path appears downstream.
    // The snapshot still travels with it, and the chunks that are not here are
    // exactly the ones a holder of `base` has.
    let plan = match base.as_ref() {
        None => whole,
        Some(base) => whole.since(version_snapshot, &base.snapshot),
    };

    // The one place the option routes.
    // The plan is re-resolved under the policy rather than filtered afterwards.
    // A record's *length* is part of the plan's own geometry.
    // The header's payload total is the sum of the records the plan describes,
    // and the header is written before any of them.
    // A plan still carrying the source set's placements would therefore write a
    // header claiming sizes the records do not have.
    // `Source` then answers the writer from the same policy.
    // The writer's own check that each record matches its placement holds the two
    // to the same answer.
    //
    // **A dry run measures whether the policy is `Off` or a level.** Its figures
    // have to describe the records the run *would* write.
    // `Off` encodes nothing to measure: a raw record's stored length *is* its
    // content length, so the plan already carries it, and `--compression 0`
    // costs what `keep` costs.
    // A real run under a level measures nothing either.
    // Nothing can state a frame's width before zstd makes the frame, so that run
    // encodes each record once as it writes it and patches the header's total in
    // afterwards.
    let (plan, source, route) = match options.compression {
        None => (plan, Source::Stored(store), Route::Verbatim),
        Some(policy) if policy.level().is_none() || options.dry_run => {
            let forms = reencode_forms(store, &plan, policy).await?;
            let whole = storage::Plan::from_snapshot(version_snapshot, |id| match forms.get(id) {
                // Measured: this is what the record will be.
                Some(placement) => Ok(*placement),
                // Not measured, because a delta does not carry it.
                // The plan above is already the delta's.
                // This one is the whole version before `since` drops the chunks a
                // holder of the base already has.
                // Every chunk the version reads is therefore here, and not every
                // chunk here is in the plan.
                // The `since` below drops the ones that were never measured.
                // Their placement is never written and never counted, so the
                // store's own answer is only ever a placeholder.
                //
                // Read as dead code once: every `--compression` test used a full
                // pack, and every `--from` test used the default.
                // Nothing ran through the pair.
                // `a_delta_packed_with_a_compression_level_applies_and_merges` is
                // the fixture that does.
                None => locate(id),
            })?;

            let plan = match base.as_ref() {
                None => whole,
                Some(base) => whole.since(version_snapshot, &base.snapshot),
            };

            (plan, Source::Reencoded { store, policy }, Route::Verbatim)
        }
        // A real run under a level. The plan stays the source set's, because
        // the record widths are not in it: the writer encodes each record once
        // and checks its content length against this plan as it goes.
        Some(policy) => (plan, Source::Reencoded { store, policy }, Route::Reencoded),
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
        // already holds for that chunk.
        // This is worked out from the plan's placements rather than from
        // `Plan::total_bytes`, which is content.
        // The two differ by every chunk's header and by every frame the set did not
        // have to make.
        // Under `keep` and `0` this is the figure the file carries, and the
        // written pack below reports the same one.
        // A real run under a level replaces it, because its records are made
        // while they are written.
        // See [`crate::pack::planned_payload_len`], which is one function rather
        // than the two this was.
        payload: crate::pack::planned_payload_len(&plan),
        table,
        base: base.as_ref().map(|base| base.version),
        bytes: None,
        content: None,
    };

    if options.dry_run {
        return Ok(report);
    }

    // A base goes to the writer rather than into a filtered plan, because the
    // header has to say what the delta assumes.
    // That is what lets an apply refuse rather than apply what it happens to hold.
    //
    // One call either way. The route decided above is the writer's, so the two
    // are chosen in one place rather than once per kind of pack.
    let written = Pack::write_from(
        &source,
        &plan,
        version_snapshot,
        *number,
        base.as_ref().map(|base| base.id),
        route,
        out,
    )
    .await?;

    // The figures a pack reports come off the written pack rather than being
    // computed here a second time.
    // A report assembled from the plan could disagree with the file by the header
    // and the index.
    // Those are precisely the bytes a client fetching it is counting.
    // The payload is the writer's own total, which is the number its header
    // carries. Under a level that total is a property of the frames the run
    // made, so the plan could not have stated it.
    report.payload = written.payload_len();
    report.bytes = Some(written.file_len()?);
    report.content = Some(crate::pack::hash_file(out).await?);

    Ok(report)
}

/// Where a pack's records come from, decided by [`PackBuilder::compression`].
///
/// A type rather than a branch at the call site, because this function has two
/// callers of the writer: a full pack and a delta.
/// A route chosen inside each of them is two places for the flag and the
/// behaviour to disagree.
/// Here there is one, and the writer cannot tell which it has.
///
/// It implements [`StorageStored`] rather than standing in for a store.
/// The writer asks a store for "the bytes this chunk is stored as".
/// The two routes answer that question with two different reads of the same
/// chunk.
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
    /// Returns an error if the store cannot yield the chunk.
    /// Returns an error if the content cannot be read or encoded, on the
    /// re-encoding route.
    async fn stored(&self, id: &BlobId) -> io::Result<Stored> {
        match self {
            Self::Stored(store) => store.stored(id).await,
            Self::Reencoded { store, policy } => {
                let content = content_of(*store, id).await?;
                let raw_len = u64::try_from(content.len()).unwrap_or(u64::MAX);

                // The store's own compressor, so the fallback is the store's rule
                // rather than a second one here.
                // The fallback keeps a frame only where the frame is strictly
                // smaller.
                // A second rule could disagree with the store about which payloads
                // are worth a frame.
                // The store hands the content back untouched in three cases out of
                // four, which is why `Owned` is only paid for the fourth.
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
/// One chunk at a time and no more.
/// The writer buffers one record at a time precisely so a pack of a large
/// version does not hold one.
/// A re-encoding route that read the whole set into memory to encode it would undo
/// that.
///
/// **The content is not hashed here.**
/// A pack's records name their chunks, and a reader checks those names.
/// `merge` decodes and hashes every record on the way in, whichever route stored
/// it.
/// A set that has rotted underneath a pack is therefore caught on the way in
/// rather than here.
/// That is the same bargain the verbatim route makes, and it is deliberate.
/// The source here is the user's own set, not a file somebody downloaded.
/// That is the reason `merge` bothers to check.
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
/// **Measured rather than predicted.** A zstd frame's length is a property of the
/// bytes zstd produces.
/// Nothing here can work out that length without producing the bytes.
/// Predicting it from the content length is not a cheaper form of the work.
/// There is no such function.
///
/// **A dry run at any policy, and a real run that writes the content raw.**
/// A dry run reports figures for a file it does not write. Those figures have to
/// describe the records the run *would* write, so the run measures a level.
/// `Off` encodes nothing, because a raw record's stored length *is* its content
/// length.
/// The plan already carries that answer, so `--compression 0` costs what `keep`
/// costs.
///
/// **A real run under a level does not come here at all.** That run encodes each
/// record once as it writes it, and the writer patches the header's payload
/// total in afterwards.
/// The encoded chunks are still discarded here rather than kept.
/// Keeping them would hold a whole version's frames in memory instead of
/// buffering one record at a time.
/// For a pack over a large version that is the difference between a pack that
/// can be written and one that cannot.
///
/// # Errors
///
/// Returns an error if a chunk the plan names cannot be read.
/// Returns an error if the store's compressor cannot produce a frame for it,
/// under a level.
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
