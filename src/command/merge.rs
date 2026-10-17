// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Applying a pack *into* a backup set, as a new version of it.
//!
//! [`super::apply`] is the other half and works the other way round.
//! A pack brings a directory up to a version, and this brings a *set* up to a
//! version.
//! This is the receiving side of a future push protocol.
//! It is built first and tested entirely with the packs `blu pack` already
//! writes, so the protocol has something real to be designed around rather than
//! a guess about it.
//!
//! ## Where the chunks come from
//!
//! A pack is already a [`StorageGet`], so it is already a store as far as this
//! command is concerned.
//! Nothing here knows a pack is a file.
//! The chunks reach `put` through the ordinary path a backup uses.
//! The plan is resolved against *two* sources.
//! One source is the set, which may already hold some of them.
//! The other source is the pack, which carries the rest.
//!
//! That resolution is not a convenience, it is the integrity check.
//! A snapshot names every chunk it reads, so resolving it against "the set or the
//! pack" fails for any chunk neither holds.
//! A pack that is short of a chunk its own snapshot needs is refused before
//! anything is written.
//! For a full pack that is the whole completeness check.
//! The pack claims to be self-contained, and this is where the claim is cashed.
//! For a delta it is the check that the chunks the pack left out really are in
//! the set.
//! The base containment check below only implies that.
//!
//! ## A delta is refused unless the set holds its base
//!
//! A delta pack omits the chunks its base already has.
//! It cannot check that its reader has them.
//! The bytes that would prove it are exactly the bytes it left out.
//! The reader therefore has to establish the base itself.
//!
//! **The test here is containment of the base snapshot among the set's committed
//! versions. It says nothing about the set's current state.**
//! That is the one place where [`super::apply`]'s logic would be wrong here.
//! The two holders are not alike, so it is worth spelling out.
//! Apply establishes its base against a *directory*, and a directory is not a
//! store.
//! A directory may hold extra files, or be a half-applied tree from a killed run.
//! What apply has to establish is that the tree is clean and is exactly the base
//! version.
//! A backup set is append-only.
//! No command in this crate removes a chunk or a version, and a commit record is
//! never rewritten.
//! A set at a *later* version therefore still holds every chunk of every earlier
//! one.
//! A set that holds the base at all is a set that can merge the delta.
//! A set that is ahead is no different from one exactly on the base.
//! So is a set full of unrelated versions, or one sitting on a torn tail from a
//! killed run.
//! Importing apply's check would refuse merges that are perfectly safe.
//! Worse, it would read as though the set had to be pristine.
//! Purity is a property of the holder and not of the store.
//!
//! So the check is: some committed version of the set names the base's snapshot
//! hash.
//! `versions()` and `snapshot(v)` are the store's own commit list, which is the
//! record of what exists.
//! The index files are a cache of it and are not consulted, for the same reason
//! `versions` is not a count of them.
//!
//! ## A full pack may create the set
//!
//! Merging a full pack into a name with no set behind it **creates the set**.
//! A full pack carries its snapshot and every chunk that snapshot reads.
//! There is then nothing to check it against and nothing that could be missing.
//! Refusing would mean merge could only ever add to a set `blu backup` had
//! made.
//! This command exists to receive a version into a set that need not have one.
//! Such a set is a new machine, a new location, or a set rebuilt from packs
//! after the original was lost.
//! The create is the same one any backup does: the open makes segment 0 and the
//! commit record publishes the version.
//!
//! ## No rollback, and none is needed
//!
//! Every chunk is stored, then `sync_data`, then `commit`.
//! The commit record is the only thing that makes a version exist.
//! A run that fails before it leaves records in the last segment that no version
//! references.
//! That costs wasted space, and no corruption.
//! Nothing names those records, and a store that holds a record nobody reads is
//! still a store whose every named version restores.
//! The space is not reclaimed, and deliberately not.
//! Recovery truncates only what is *past* the last valid record.
//! A record that was written whole and never referenced is a perfectly valid
//! record that outlives every later run.
//! One cut short by the crash is the tail recovery does discard.
//! The cost of a failed merge is therefore bounded by what it wrote and
//! permanent.
//! That is a fair trade for never having to know which records a later run can
//! safely drop.
//! This is the store's existing guarantee, the same one a backup has.
//! This command does not invent a second mechanism on top of it.
//! There is nothing to undo, because nothing was published.
//! A version either exists or it does not, and the record that says so is written
//! once.
//!
//! ## Merging the same pack twice
//!
//! **A no-op, reported and successful.**
//! A pack whose snapshot the set already holds changes nothing.
//! A pack whose tree matches the tip changes nothing either.
//! No version is published and no chunk is stored.
//! The run says which version the set is already holding it as.
//!
//! The alternatives were considered. Both are worse.
//!
//! * *Refuse it.* A transfer may be retried.
//!   A client that merged a pack and lost the reply has to be able to ask again.
//!   The client that has to decide whether to send cannot tell a refusal from
//!   "never merged".
//!   The retry protocol would then need an out-of-band answer to a question the
//!   set can answer itself in one scan of its commit list.
//! * *Make a second version.* It was what this did, and it is what a log does by
//!   default.
//!   The second arrival records again, deduplicates every chunk, and publishes a
//!   version number nobody asked for.
//!   That number then competes with real ones.
//!   A script that pushes and then verifies against the latest version reads a
//!   duplicate as the most recent thing that happened.
//!   Such a number is a *fact about the receiving set* being manufactured by a
//!   transfer that happened to be repeated.
//!   Nothing was learned the second time.
//!
//! What it costs is one scan of the commit list, which the run does anyway to
//! find a delta's base.
//! It is the reason [`version_holding`] is a public-ish seam rather than a detail
//! of planning.
//! The check has to happen before any planning or writing at all, and identically
//! under a dry run.
//! A dry run that reported a merge of something already merged would tell the
//! user their set is about to grow a version.
//! Their set would not grow one.
//!
//! ## An older pack is warned about, not refused
//!
//! The pack records the version it was built from ([`Pack::version`]).
//! A merge compares it against the set's tip.
//! A pack from below the tip means the sender's view is behind.
//! The version it carries was built from an older one, so whatever they changed
//! since has not left.
//! That is worth saying out loud, and it is not worth refusing.
//! The two sets number independently, and there is nothing a refusal could
//! assert.
//! The pack is internally consistent, and its chunks are the chunks its snapshot
//! names.
//! Merging it produces a version that restores exactly what the pack holds.
//!
//! It goes to **stderr** rather than stdout because stdout is the report channel.
//! The warning is about what the merge is about to do, not a figure the run
//! produced.
//! A script reading the report must not have to skip a line to find the one it
//! wants.
//!
//! **Equal is not lower.**
//! A pack from the tip's own version is the ordinary case of a sender who is
//! exactly up to date.
//! Warning about it would train people to ignore the warning.
//!
//! ## What form a chunk arrives in
//!
//! A pack carries each chunk as the record its source set already held.
//! That record is a zstd frame, or the content itself.
//! A merge therefore has a choice about what it does with that form, and it is
//! [`Compression`]'s to make.
//!
//! **The default is to keep it**, and the reason is that the alternative is work
//! done twice.
//! Decoding every frame of a compressed pack only to compress the result again
//! is the expensive half of a transfer.
//! The receiving set's own policy governs that second compression.
//! Such a transfer is performed for nothing.
//! Keeping the form is what the pack format was built for.
//! It is also what a set that already compresses wants.
//! Such a set holds frames, and a frame is a frame whichever set wrote it.
//!
//! **The frame is still decoded, and this is the part that looks like waste.**
//! A chunk is named by the hash of its content.
//! What a pack says about a chunk is a claim.
//! The only thing that could check it is the content itself.
//! See [`StoragePut::put_stored`], which says why at the length it deserves.
//! What the default saves is the encode.
//! What it keeps is the guarantee every other path has: a damaged or hostile
//! pack is refused on this path exactly as on any other.
//! A merge that refuses it publishes no version.
//!
//! **A frame is kept only if it earned its place**, and the store decides that
//! rather than this command.
//! A frame no smaller than its content is stored raw instead.
//! So is a frame naming a codec this build cannot read.
//! Both lengths are in the record, so the check is two integer comparisons.
//! Without it a pack could grow the set it is merged into.
//!
//! A level re-encodes, for the set that has a compression policy of its own and
//! does not want the sender's frames.
//! It changes what is stored and nothing else.
//! The same chunks are read, the same hashes checked, and the same figures
//! reported.
//! It cannot rewrite what the set already holds, because every record names the
//! codec it was written with.

use std::io::{self, Cursor};

use tracing::instrument;

use crate::{
    metadata::{Snapshot, Timestamp},
    pack::{Pack, PackReader},
    progress::{Counter, Phase, Progress},
    storage::{self, BlobId, Codec, Placement, StorageGet, StoragePut},
};

/// What a [`MergeBuilder`] run did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Merged {
    /// The run published a new version.
    Committed {
        /// Version the commit record published.
        ///
        /// The store's own number, allocated at commit time. Nothing here plans a
        /// version: a merge that guessed one and was wrong would publish a version
        /// under a name that already meant something else.
        version: u32,
        /// The figures the merge stored.
        plan: MergePlan,
    },
    /// The set already held the pack's snapshot, so nothing was written.
    ///
    /// Success, and deliberately so: see this module's docs on merging the same
    /// pack twice. `version` is the set's own number for the snapshot the pack
    /// carries, which is the whole of the answer a caller has to report.
    Already {
        /// Version of the set already holding this snapshot.
        version: u32,
    },
}

/// What a [`MergePlanBuilder`] run found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Planned {
    /// A real merge would have committed this.
    Merge(MergePlan),
    /// A real merge would have done nothing.
    Already {
        /// Version of the set already holding the pack's snapshot.
        version: u32,
    },
}

/// What a merge stored, or would store.
///
/// [`MergePlan::version`] is what the run *would* commit, given the set as it
/// stands, on the same terms as [`super::BackupPlan::version`]: the set's commit
/// list plus one. A real run ignores it and reports [`Merged::Committed`]'s
/// `version`, which is the number the store allocated rather than one derived a
/// moment earlier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergePlan {
    /// Version this run would commit.
    pub version: u32,
    /// Snapshot the pack carries, and the name the new version is known by.
    pub snapshot: BlobId,
    /// Version of the set holding this pack's base, if the pack is a delta.
    pub base: Option<u32>,
    /// Distinct chunks the merged version reads.
    pub chunks: u64,
    /// Of those, how many the set already holds and would not be written.
    pub held: u64,
    /// Of those, how many have to be written.
    pub fresh: u64,
    /// Bytes of chunk content that would be stored.
    ///
    /// Content, before compression.
    /// A pack hands the chunk's bytes as they were hashed, so this is what goes
    /// in.
    /// How large the records come out depends on the set's compression policy.
    /// The store chooses that policy when it is opened.
    ///
    /// **The same figure under every [`Compression`], and that is deliberate.**
    /// It counts what the version *is* rather than what a particular policy makes
    /// of it.
    /// A report therefore does not change its meaning with a flag.
    /// A script comparing two merges is comparing the two runs rather than the
    /// two encodings.
    /// A run that kept a pack's frames stored fewer bytes on disk than this says.
    /// The segment file's size on disk is the thing to measure.
    pub bytes: u64,
}

/// What a merge does with each chunk's stored form.
///
/// The two are the whole of the choice, and the second one is the set's own write
/// policy rather than anything this command decides.
/// A re-encoded chunk goes to [`StoragePut::put`], which encodes it under the
/// policy the set was opened with.
/// So a caller asking for a level opens the set with it and passes
/// [`Compression::Reencode`].
/// The two are not separate arguments because they cannot be allowed to
/// disagree.
/// The store's compressor is a type parameter this command does not get to
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// Store each chunk's stored form as the pack carries it.
    ///
    /// A chunk that reached the source set as a zstd frame is written as that same
    /// frame.
    /// A merge of a compressed pack therefore copies frames rather than inflating
    /// every chunk and compressing it again.
    /// **The frame is still decoded** to check the chunk's hash.
    /// See [`StoragePut::put_stored`].
    /// A pack's claim about what a chunk is is not evidence of it.
    /// This is the one path where a chunk's bytes are not hashed on the way in.
    ///
    /// Read through [`crate::pack::StorageStored`], which is [`StorageGet`] without
    /// the decode: the same record, opened and held to the index by the same code.
    /// This path used to open the record itself.
    /// A copy of that is a second answer to "which bytes is this chunk".
    /// That is the one question a damaged pack makes expensive to get wrong.
    #[default]
    Keep,
    /// Decode each chunk and store it under the set's own compression policy.
    Reencode,
}

/// Apply a pack into a set as a new version.
///
/// Start with [`MergeBuilder::new`]. Set the write path with `mode`.
/// Call `run`.
///
/// The set keeps the version the pack carries. Nothing else is consulted.
/// The pack carries its own file table. There is no second file to keep
/// in step with the first.
///
/// Keeping the stored form is the default. A chunk that reached the source
/// set as a frame lands as that frame. The store still decodes and hashes
/// every chunk on the way in. A damaged pack is refused before it commits.
/// Re-encoding decodes each chunk and stores it under the set's own policy.
///
/// Merging the same pack twice writes nothing. The run names the version
/// that already holds the snapshot. A retried transfer stays answerable.
#[derive(Debug)]
pub struct MergeBuilder<'a, C: storage::Compressor> {
    store: &'a storage::BlobStore<storage::Write, C>,
    pack: &'a Pack,
    mode: Compression,
}

impl<'a, C: storage::Compressor> MergeBuilder<'a, C> {
    /// Start a merge of `pack` into `store`.
    ///
    /// The builder keeps stored forms. Change the write path with `mode`.
    #[must_use]
    pub fn new(store: &'a storage::BlobStore<storage::Write, C>, pack: &'a Pack) -> Self {
        Self {
            store,
            pack,
            mode: Compression::Keep,
        }
    }

    /// Store each chunk through `mode`.
    ///
    /// `Keep` writes the form the pack carries. `Reencode` decodes each chunk
    /// and stores it under the set's own policy. The figures match either way.
    #[must_use]
    pub fn mode(mut self, mode: Compression) -> Self {
        self.mode = mode;
        self
    }

    /// Merge the pack and publish the version.
    ///
    /// A failure before the commit record publishes nothing. There is no
    /// rollback because there is nothing to undo. Unreferenced records cost
    /// space and no correctness.
    ///
    /// # Errors
    ///
    /// Returns an error when the pack's snapshot cannot be read or decoded.
    /// Returns an error when the pack is a delta whose base the set lacks.
    /// Returns an error when a chunk the version reads is in neither source.
    /// Returns an error when a chunk does not hash to the hash it names.
    /// Returns an error when the set cannot be written or committed.
    #[instrument(level = "info", skip_all, err)]
    pub async fn run(self) -> io::Result<Merged> {
        merge(self.store, self.pack, self.mode).await
    }
}

/// Work out what a merge would store, and store none of it.
///
/// Start with [`MergePlanBuilder::new`]. Attach the set with `store`.
/// Call `run`.
///
/// The plan resolves the snapshot against the set and the pack together.
/// A chunk neither holds refuses the plan before anything is written.
/// Every chunk about to be stored is read out of the pack and hashed.
/// A plan refuses what a merge refuses and costs what a merge costs.
///
/// Nothing is created, not even the set directory. No lock is taken.
/// A delta is refused for a set that does not hold its base.
/// The figures count content, not stored bytes, so they match under
/// every write path.
#[derive(Debug)]
pub struct MergePlanBuilder<'a, C: storage::Compressor> {
    pack: &'a Pack,
    store: Option<&'a storage::BlobStore<storage::Read, C>>,
}

impl<'a, C: storage::Compressor> MergePlanBuilder<'a, C> {
    /// Start a plan for `pack`.
    ///
    /// Without a store every chunk reads as new. That is the first version
    /// arriving somewhere. Attach the set with `store` to compare against it.
    #[must_use]
    pub fn new(pack: &'a Pack) -> Self {
        Self { pack, store: None }
    }

    /// Compare against `store`.
    ///
    /// Pass `None` for a name with nothing behind it. A dry run that created
    /// the set to learn that would break its own promise.
    #[must_use]
    pub fn store(mut self, store: Option<&'a storage::BlobStore<storage::Read, C>>) -> Self {
        self.store = store;
        self
    }

    /// Work out the figures and store nothing.
    ///
    /// # Errors
    ///
    /// Returns an error when the pack's snapshot cannot be read or decoded.
    /// Returns an error when the pack is a delta whose base the set lacks.
    /// Returns an error when a chunk the version reads is in neither source.
    /// Returns an error when a chunk about to be stored does not hash to it.
    #[instrument(level = "info", skip_all, err)]
    pub async fn run(self) -> io::Result<Planned> {
        merge_plan(self.pack, self.store).await
    }
}

/// Merge the pack into the store, keeping or re-encoding each chunk.
async fn merge<C: storage::Compressor>(
    store: &storage::BlobStore<storage::Write, C>,
    pack: &Pack,
    compression: Compression,
) -> io::Result<Merged> {
    // Both before a byte is written, and in that order.
    // The snapshot says what the version is.
    // The base check decides whether this version can be put here at all.
    let (snapshot, table) = checked_snapshot(pack)?;
    let snapshot_id = pack.snapshot_id();

    // Before the base check and before the plan, because a pack the set already
    // holds has nothing to decide.
    // It would publish a version number nobody asked for, over a version the set
    // can name already.
    // Doing it first also means a delta that is itself already merged is reported
    // as such.
    // Otherwise such a delta is refused for a base it will never need.
    if let Some(version) = version_holding(store, &snapshot_id)? {
        return Ok(Merged::Already { version });
    }

    // A pack rebuilt from an unchanged tree carries a new stamp.
    // Its hash never matches, but its tree does.
    // That merge writes nothing and reports the tip.
    if let Some(version) = tip_with_same_tree(store, &snapshot).await {
        return Ok(Merged::Already { version });
    }

    let base = match pack.base() {
        Some(base) => Some(establish_base(Some(store), base, pack.path())?),
        None => None,
    };

    // Ordered against the tip before anything is written. A version that
    // predates the tip never reaches the commit record.
    let latest = latest_taken_at(store).await?;
    crate::metadata::refuse_older_taken_at(latest, snapshot.taken_at)?;

    // The plan is what makes a merge refuse a pack that is short of a chunk its
    // own snapshot reads.
    // It is also where the counts come from.
    let plan = plan_against(Some(store), pack, &snapshot)?;

    let mut merged = MergePlan {
        version: u32::try_from(store.versions()?.len())
            .unwrap_or(u32::MAX)
            .saturating_add(1),
        snapshot: snapshot_id,
        base,
        chunks: 0,
        held: 0,
        fresh: 0,
        bytes: 0,
    };

    // **Reported the way `verify` reports, and for the same reason.** The plan is
    // resolved by now, so the chunk count is known before a byte is written and
    // can be stated outright rather than counted up to a total nobody knows.
    //
    // The two counters are the two halves of [`MergePlan`], and the pairing is
    // deliberate:
    //
    // * `chunks` is every distinct chunk the merged version reads, which is
    //   [`MergePlan::chunks`].
    // * `distinct` is the ones this run actually stored, which is
    //   [`MergePlan::fresh`]. The set already holding the rest is exactly
    //   `chunks - distinct`, so nothing is lost by not giving [`MergePlan::held`]
    //   a counter of its own: a client can do the subtraction.
    //
    // `Written` is deliberately **not** reported. Its doc says "bytes written to
    // the destination", and the only byte total a merge has is
    // [`MergePlan::bytes`], which this module's docs are explicit is *content*
    // before compression rather than what lands in a segment. The rate on that
    // counter would then be a content rate wearing the name of a disk rate, which
    // is the exact kind of quiet lie the counter split exists to prevent.
    let mut progress = Progress::new(Phase::Merge);
    progress.set(
        Counter::Chunks,
        u64::try_from(plan.as_ref().len()).unwrap_or(0),
    );
    progress.report();

    let mut reader = PackReader::open(pack).await?;
    for chunk in &plan {
        merged.chunks = merged.chunks.saturating_add(1);

        if store.locate(&chunk.id).is_ok() {
            merged.held = merged.held.saturating_add(1);
            continue;
        }

        // Present in the pack, because the plan resolved it from there and the
        // plan has not been resolved twice. Asked for anyway rather than
        // unwrapped, so the invariant is checked rather than asserted.
        let located = pack.locate(&chunk.id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("{} does not hold chunk {}", pack.path().display(), chunk.id),
            )
        })?;

        // The *whole* chunk, not the length this version reads of it.
        // A pack stores a chunk whole, and both write paths check the hash of
        // everything they are given.
        // Both check that hash against the name the chunk is addressed by.
        // A prefix would therefore be refused for the very reason the check exists.
        //
        // The error is wrapped because the store's own is a statement about a
        // record.
        // A person reading it has no way to know it came from merging a pack.
        // Such a reader also cannot tell that the bytes are the pack's rather than
        // the set's.
        // A damaged pack is the likeliest cause by a long way.
        // Saying so tells them to go and look at the file they downloaded.
        let refused = |e: io::Error| {
            io::Error::new(
                e.kind(),
                format!("refusing to merge: {} could not be stored: {e}", chunk.id),
            )
        };

        match compression {
            // The chunk's stored form goes in as it stands.
            // The frame a source set deduplicated it into is therefore copied
            // rather than decoded and built again.
            // The store still decodes it. See [`StoragePut::put_stored`].
            // This saves the encode and nothing else.
            Compression::Keep => {
                let stored = reader.read_stored(&chunk.id).await?;
                let stored_len = u64::try_from(stored.bytes.len()).unwrap_or(u64::MAX);

                store
                    .put_stored(
                        &chunk.id,
                        Box::new(Cursor::new(stored.bytes)),
                        stored_len,
                        stored.raw_len,
                        stored.codec,
                    )
                    .await
                    .map_err(refused)?;
            }
            // Today's path: the chunk's *content* is handed over, and the length
            // handed with it is the content length.
            // That is not the length of the pack's record.
            // The record's length covers the header and, for a chunk the set held
            // compressed, a frame.
            // `get` decodes, so what comes back is content and content is what
            // `put` hashes.
            Compression::Reencode => {
                let content = reader.read_content(&chunk.id).await?;
                store
                    .put(
                        &chunk.id,
                        Box::new(Cursor::new(content)),
                        located.content_len,
                    )
                    .await
                    .map_err(refused)?;
            }
        }

        merged.fresh = merged.fresh.saturating_add(1);
        merged.bytes = merged.bytes.saturating_add(located.content_len);

        // **Inside the loop, not after it.** A merge of a large pack is the one
        // run whose interesting part is the writing, so the counter has to move
        // while it is happening rather than land in one line at the end. The
        // rate limiting is [`Progress::report`]'s own; nothing here throttles.
        //
        // The `held` arm above does not report, and that is honest rather than
        // an oversight: a chunk the set already holds is a hash lookup, so a run
        // spending its time on those is a run with nothing to say.
        progress.add(Counter::Distinct, 1);
        progress.report();
    }

    // The loop is the last thing that moved a counter, so this is where the run
    // is reported. A merge that fails inside the loop reports nothing, which is
    // the right answer: there was no version.
    //
    // `Distinct` here means the chunks **this run stored**, which is not what
    // the same counter means in a backup. See `blulib::telemetry::metrics`.
    crate::telemetry::metrics::record_run(&progress);

    // The snapshot is a blob like any other, addressed by the hash of its own
    // bytes. The commit record names that hash.
    // The snapshot is therefore stored before the commit rather than carried by
    // it.
    let table_len = u64::try_from(table.len())
        .map_err(|_| io::Error::other("the snapshot is too large to store"))?;
    store
        .put(&snapshot_id, Box::new(Cursor::new(table)), table_len)
        .await?;

    // The run's one data sync, then the commit that publishes it.
    // The order is the store's contract, not this command's.
    // A version that exists before the data it names is durable is a version
    // nothing can be restored from.
    store.sync_data().await?;
    let version = store
        .commit_with_time(&snapshot_id, snapshot.taken_at)
        .await?;

    Ok(Merged::Committed {
        version,
        plan: merged,
    })
}

/// Say what merging `pack` into `set` would store, and store none of it.
async fn merge_plan<C: storage::Compressor>(
    pack: &Pack,
    set: Option<&storage::BlobStore<storage::Read, C>>,
) -> io::Result<Planned> {
    let (snapshot, _table) = checked_snapshot(pack)?;
    let snapshot_id = pack.snapshot_id();

    // The same check, in the same place relative to the base and the plan, as
    // `merge`'s, and for the same reason.
    // A plan that reported a merge of something already merged would tell the
    // user their set is about to grow a version it is not.
    // That is the one lie a dry run cannot tell.
    if let Some(version) = match set {
        Some(store) => version_holding(store, &snapshot_id)?,
        None => None,
    } {
        return Ok(Planned::Already { version });
    }

    // A pack rebuilt from an unchanged tree carries a new stamp.
    // Its hash never matches, but its tree does.
    // That plan reports nothing to do.
    if let Some(store) = set
        && let Some(version) = tip_with_same_tree(store, &snapshot).await
    {
        return Ok(Planned::Already { version });
    }

    let base = match pack.base() {
        Some(base) => Some(establish_base(set, base, pack.path())?),
        None => None,
    };

    // Ordered against the tip like a real merge. A plan that reported a merge
    // of an older version would tell the user their set is about to grow a
    // version it must refuse.
    if let Some(store) = set {
        let latest = latest_taken_at(store).await?;
        crate::metadata::refuse_older_taken_at(latest, snapshot.taken_at)?;
    }

    let plan = plan_against(set, pack, &snapshot)?;

    let mut merged = MergePlan {
        version: match set {
            Some(store) => u32::try_from(store.versions()?.len())
                .unwrap_or(u32::MAX)
                .saturating_add(1),
            None => 1,
        },
        snapshot: snapshot_id,
        base,
        chunks: 0,
        held: 0,
        fresh: 0,
        bytes: 0,
    };

    let mut reader = PackReader::open(pack).await?;
    for chunk in &plan {
        merged.chunks = merged.chunks.saturating_add(1);

        if set.is_some_and(|store| store.locate(&chunk.id).is_ok()) {
            merged.held = merged.held.saturating_add(1);
            continue;
        }

        let located = pack.locate(&chunk.id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("{} does not hold chunk {}", pack.path().display(), chunk.id),
            )
        })?;

        // The check `put` would make, made here instead: this run is not writing,
        // so this is the only thing that can refuse a damaged pack.
        check_chunk_with(&mut reader, pack, &chunk.id, located.content_len).await?;

        merged.fresh = merged.fresh.saturating_add(1);
        merged.bytes = merged.bytes.saturating_add(located.content_len);
    }

    Ok(Planned::Merge(merged))
}

/// The pack's file table, and the bytes to store it as.
///
/// Read once and hashed here rather than calling [`Pack::snapshot`] and then
/// [`Pack::snapshot_bytes`].
/// That pair would read the same section twice and let the two reads disagree on
/// a file that is being written underneath.
/// What is stored has to be the bytes that were verified, and this is the only
/// way to arrange that.
///
/// The hash check is `Pack::snapshot`'s, restated because the value it returns is
/// not what is wanted here.
/// A decoded snapshot cannot be stored, since re-encoding one is a promise that
/// it encodes to the same bytes.
///
/// # Errors
///
/// Returns an error if the section cannot be read.
/// Returns an error if its bytes do not hash to the snapshot the header
/// names, or if they do not decode.
fn checked_snapshot(pack: &Pack) -> io::Result<(Snapshot, Vec<u8>)> {
    let bytes = pack.snapshot_bytes()?;

    if blake3::hash(&bytes) != pack.snapshot_id() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "refusing to merge: the file table {} carries does not hash to the one its \
                 header names",
                pack.path().display()
            ),
        ));
    }

    let snapshot = Snapshot::try_from(bytes.as_slice())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok((snapshot, bytes))
}

/// The version of the set holding `base`, or a refusal naming why not.
///
/// `set` is `None` for a name with no set behind it.
/// That is a refusal rather than an empty answer.
/// There is no committed version, so no version holds the base.
///
/// See this module's docs for why this is containment among committed versions
/// and not a check that the set is exactly the base.
/// This is public because a caller needs it *before* opening the set for
/// writing, which creates a set that does not exist.
/// A delta refused against a name with nothing behind it must leave that name
/// alone.
/// The store handle that would answer the question is the thing being
/// created.
///
/// # Errors
///
/// Returns an error if the set cannot list its versions or read a version's
/// snapshot, or if no committed version holds `base`.
/// The error names the hash and what to do instead.
pub fn establish_base<W: storage::WriteMode, C: storage::Compressor>(
    store: Option<&storage::BlobStore<W, C>>,
    base: BlobId,
    path: &std::path::Path,
) -> io::Result<u32> {
    if let Some(store) = store
        && let Some(version) = version_holding(store, &base)?
    {
        return Ok(version);
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "refusing to merge: {} is a delta against the version whose snapshot hashes to \
             {base}, and no committed version of the set holds that snapshot. A delta omits \
             the chunks that version has, so nothing here could tell you were missing them — \
             merge the full pack instead, or merge the base version first",
            path.display()
        ),
    ))
}

/// The committed version of `store` whose snapshot is `id`, if there is one.
///
/// The commit list is the record of what exists, so this is a scan of a few
/// hashes rather than a lookup.
/// The store names a version's snapshot by number and nothing names a snapshot
/// by the version that has it.
/// The index files are not consulted either.
/// They are a cache of the same answer.
/// `blu versions` already treats a stale or missing one as a scan rather
/// than as a fact.
///
/// Public because a caller has to answer this *before* deciding what the run is.
/// This answer decides whether a merge is going to write anything.
/// It therefore decides whether the warning about a pack from behind is about
/// anything.
/// Nothing else decides either question.
/// Doing it here and in [`MergeBuilder`] is deliberate.
/// The run under the lock is the one that counts, and this is the one that lets
/// the CLI report without taking one.
///
/// # Errors
///
/// Returns an error if the set cannot list its versions or read a version's
/// snapshot.
pub fn version_holding<W: storage::WriteMode, C: storage::Compressor>(
    store: &storage::BlobStore<W, C>,
    id: &BlobId,
) -> io::Result<Option<u32>> {
    for version in store.versions()? {
        if store.snapshot(version)? == *id {
            return Ok(Some(version));
        }
    }

    Ok(None)
}

/// The tip version whose tree matches `snapshot`, if the tip matches.
///
/// Compare trees without stamps.
/// A rebuilt pack carries a new stamp.
/// Its hash never matches, but its tree can.
/// Only the tip counts here.
/// An older match means the tip moved on, so the merge still commits.
///
/// Public so the CLI reports a same-tree pack as already merged before it
/// warns about the pack version.
/// The run under the lock asks again, and that answer counts.
pub async fn tip_with_same_tree<W: storage::WriteMode, C: storage::Compressor>(
    store: &storage::BlobStore<W, C>,
    snapshot: &Snapshot,
) -> Option<u32> {
    use tokio::io::AsyncReadExt as _;

    let (tip, tip_id) = store.latest()?;
    let mut reader = store.get(&tip_id).await.ok()?;
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.ok()?;
    let tip_tree = Snapshot::try_from(buffer.as_slice()).ok()?;
    snapshot.matches_tree(&tip_tree).then_some(tip)
}

/// The stamp of a set's newest version, when the set holds one.
///
/// Reads the tip through the commit list. An empty set holds nothing.
///
/// # Errors
///
/// Returns an error when the tip's stamp cannot be read. The stamp lives in
/// the tip's snapshot blob, or in its index file where one exists.
async fn latest_taken_at<W: storage::WriteMode, C: storage::Compressor>(
    store: &storage::BlobStore<W, C>,
) -> io::Result<Option<Timestamp>> {
    match store.latest() {
        None => Ok(None),
        Some((version, _)) => store.version_time(version).await.map(Some),
    }
}

/// Resolve the snapshot against the set and the pack together, as one store.
///
/// The plan is what a backup would build if its chunks arrived from both places.
/// It is also the whole of the merge's completeness check.
/// A chunk the version reads that neither source holds fails here, naming
/// itself, before anything is written.
///
/// # Errors
///
/// Returns an error if a chunk the version reads cannot be resolved.
/// Returns an error if the source holding it has too little of it.
/// That source is a store or a pack that has a chunk shorter than the version
/// reads.
fn plan_against<W: storage::WriteMode, C: storage::Compressor>(
    set: Option<&storage::BlobStore<W, C>>,
    pack: &Pack,
    snapshot: &Snapshot,
) -> io::Result<storage::Plan> {
    storage::Plan::from_snapshot(snapshot, |id| {
        if let Some(store) = set {
            match store.locate(id) {
                Ok(placement) => return Ok(placement),
                // `NotFound` is the whole of what this is for.
                // Anything else is a set that cannot be asked.
                // Such a set is reported rather than treated as holding nothing.
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }

        in_pack(pack, id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "refusing to merge: the version reads chunk {id}, which neither {} nor \
                     the set holds",
                    pack.path().display()
                ),
            )
        })
    })
}

/// Where a chunk sits in a pack, as a [`Placement`].
///
/// A small fiction, and the only place this command bends the store's types.
/// A [`Placement`] names a segment and an offset because a *store* is several
/// files and a pack is one.
/// Every placement in a plan built against a pack therefore names segment 0
/// and a file offset.
/// The `offset` is where the chunk's *record* starts rather than where its
/// payload does, and `stored_len` is zero.
/// A pack's index no longer carries the record's length.
/// The record's own header carries that length, and the record is read there
/// rather than looked up here.
/// Nothing reads that field from a plan built over a pack.
/// A plan reads `raw_len`, which is the one number that has to be right, and
/// the caller goes back to [`Pack::get`] for the bytes.
///
/// The codec is `Raw`, and it is the one field here that is not a fact about the
/// bytes.
/// A plan never decodes a placement, and the bytes themselves arrive through
/// [`Pack::get`].
/// `Pack::get` reads the codec out of the record itself rather than from here.
/// Naming a codec in an index that is not read to decode anything would be a
/// second claim.
/// That claim would have to stay in step with the record's own.
fn in_pack(pack: &Pack, id: &BlobId) -> Option<Placement> {
    let located = pack.locate(id)?;

    Some(Placement {
        segment: 0,
        offset: pack.payload_start().saturating_add(located.offset),
        stored_len: 0,
        raw_len: located.content_len,
        codec: Codec::Raw,
    })
}

/// Hash the chunk `id` out of the pack.
///
/// Refuse a mismatch. Run the check that `put` runs on a real merge. Read the content with the reused reader. Hash the bytes. Refuse a hash that differs from `id`.
///
/// # Errors
///
/// Returns an error if the pack misses the chunk. Returns an error if the content ends before `len` bytes. Returns an error if the bytes do not hash to `id`.
async fn check_chunk_with(
    reader: &mut PackReader<'_>,
    pack: &Pack,
    id: &BlobId,
    len: u64,
) -> io::Result<()> {
    let bytes = reader.read_content(id).await?;
    let have = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if have < len {
        let left = len.saturating_sub(have);
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            format!(
                "refusing to merge: {} ends {left} bytes early in chunk {id}",
                pack.path().display()
            ),
        ));
    }
    if blake3::hash(&bytes) != *id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "refusing to merge: the bytes {} holds for chunk {id} do not hash to it",
                pack.path().display()
            ),
        ));
    }
    Ok(())
}
