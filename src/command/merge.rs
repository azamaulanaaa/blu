//! Applying a pack *into* a backup set, as a new version of it.
//!
//! [`super::apply`] is the other half and works the other way round: a pack
//! brings a directory up to a version, and this brings a *set* up to a version.
//! The receiving side of a future push protocol, built first and tested entirely
//! with the packs `lepatch pack` already writes, so the protocol has something
//! real to be designed around rather than a guess about it.
//!
//! ## Where the chunks come from
//!
//! A pack is already a [`StorageGet`], so it is already a store as far as this
//! command is concerned, and nothing here knows a pack is a file. The chunks
//! reach `put` through the ordinary path a backup uses, with one difference: the
//! plan is resolved against *two* sources — the set, which may already hold some
//! of them, and the pack, which carries the rest.
//!
//! That resolution is not a convenience, it is the integrity check. A snapshot
//! names every chunk it reads, so resolving it against "the set or the pack"
//! fails for any chunk neither holds, and a pack that is short of a chunk its own
//! snapshot needs is refused before anything is written. For a full pack that is
//! the whole completeness check — the pack claims to be self-contained, and this
//! is where the claim is cashed. For a delta it is the check that the chunks the
//! pack left out really are in the set, which the base containment check below
//! only implies.
//!
//! ## A delta is refused unless the set holds its base
//!
//! A delta pack omits the chunks its base already has, and it cannot check that
//! its reader has them: the bytes that would prove it are exactly the bytes it
//! left out. So the reader has to establish the base itself.
//!
//! **The test here is containment of the base snapshot among the set's committed
//! versions, and nothing about the set's current state.** That is the one place
//! where [`super::apply`]'s logic would be wrong here, and it is worth spelling
//! out because the two holders are not alike. Apply establishes its base against
//! a *directory*, and a directory is not a store: it may hold extra files, or be
//! a half-applied tree from a killed run, so what apply has to establish is that
//! the tree is clean and exactly the base version. A backup set is append-only —
//! there is no command in this crate that removes a chunk or a version, and a
//! commit record is never rewritten — so a set at a *later* version still holds
//! every chunk of every earlier one. A set that holds the base at all is a set
//! that can merge the delta, and a set that is ahead, full of unrelated versions,
//! or sitting on a torn tail from a killed run is no different from one that is
//! exactly on the base. Importing apply's check would refuse merges that are
//! perfectly safe and, worse, would read as though the set had to be pristine,
//! which is a property of the holder and not of the store.
//!
//! So the check is: some committed version of the set names the base's snapshot
//! hash. `versions()` and `snapshot(v)` are the store's own commit list, which is
//! the record of what exists; the index files are a cache of it and are not
//! consulted, for the same reason `versions` is not a count of them.
//!
//! ## A full pack may create the set
//!
//! Merging a full pack into a name with no set behind it **creates the set**.
//! A full pack carries its snapshot and every chunk that snapshot reads, so there
//! is nothing to check it against and nothing that could be missing; refusing
//! would mean merge could only ever add to a set `lepatch backup` had made, and
//! this command's whole reason to exist is receiving a version into a set that
//! need not have one — a new machine, a new location, a set rebuilt from packs
//! after the original was lost. The create is the same one any backup does: the
//! open makes segment 0 and the commit record publishes the version.
//!
//! ## No rollback, and none is needed
//!
//! Every chunk is stored, then `sync_data`, then `commit`. The commit record is
//! the only thing that makes a version exist, so a run that fails before it
//! leaves records in the last segment that no version references: wasted space,
//! and no corruption, because nothing names those records and a store that
//! holds a record nobody reads is still a store whose every named version
//! restores. The space is not reclaimed, and deliberately not: recovery
//! truncates only what is *past* the last valid record, so a record that was
//! written whole and never referenced is a perfectly valid record that outlives
//! every later run, while one cut short by the crash is the tail recovery does
//! discard. So the cost of a failed merge is bounded by what it wrote and
//! permanent, and that is a fair trade for never having to know which records a
//! later run can safely drop. That is the store's existing guarantee — the same
//! one a backup has — and this command does not
//! invent a second mechanism on top of it. There is nothing to undo, because
//! nothing was published: a version either exists or it does not, and the record
//! that says so is written once.
//!
//! ## Merging the same pack twice
//!
//! **A no-op, reported and successful.** A pack whose snapshot the set already
//! holds changes nothing: no version is published and no chunk is stored, and the
//! run says which version the set is already holding it as.
//!
//! The alternatives were considered and both are worse:
//!
//! * *Refuse it.* A transfer may be retried, and a client that merged a pack and
//!   lost the reply has to be able to ask again. A refusal cannot be told from
//!   "never merged" by the client that has to decide whether to send, so the
//!   retry protocol would need an out-of-band answer to a question the set can
//!   answer itself in one scan of its commit list.
//! * *Make a second version.* It was what this did, and it is what a log does by
//!   default: the second arrival records again, deduplicates every chunk, and
//!   publishes a version number nobody asked for. That number then competes with
//!   real ones — a script that pushes and then verifies against the latest
//!   version reads a duplicate as the most recent thing that happened — and it is
//!   a *fact about the receiving set* being manufactured by a transfer that
//!   happened to be repeated. Nothing was learned the second time.
//!
//! What it costs is one scan of the commit list, which the run does anyway to
//! find a delta's base, and it is the reason [`version_holding`] is a public-ish
//! seam rather than a detail of planning: the check has to happen before any
//! planning or writing at all, and identically under a dry run. A dry run that
//! reported a merge of something already merged would be telling the user their
//! set is about to grow a version it is not.
//!
//! ## An older pack is warned about, not refused
//!
//! The pack records the version it was built from ([`Pack::version`]), and a
//! merge compares it against the set's tip. A pack from below the tip means the
//! sender's view is behind: the version it carries was built from an older one, so
//! whatever they changed since has not left. That is worth saying out loud and it
//! is not worth refusing, because the two sets number independently and there is
//! nothing a refusal could assert — the pack is internally consistent, its chunks
//! are the chunks its snapshot names, and merging it produces a version that
//! restores exactly what the pack holds.
//!
//! It goes to **stderr** rather than stdout because stdout is the report channel:
//! the warning is about what the merge is about to do, not a figure the run
//! produced, and a script reading the report must not have to skip a line to find
//! the one it wants.
//!
//! **Equal is not lower.** A pack from the tip's own version is the ordinary case
//! of a sender who is exactly up to date, and warning about it would train people
//! to ignore the warning.
//!
//! ## What form a chunk arrives in
//!
//! A pack carries each chunk as the record its source set already held — a zstd
//! frame, or the content itself — so a merge has a choice about what it does with
//! that form, and it is [`Compression`]'s to make.
//!
//! **The default is to keep it**, and the reason is that the alternative is work
//! done twice: decoding every frame of a compressed pack only to compress the
//! result again under the receiving set's own policy, which is the expensive half
//! of a transfer performed for nothing. Keeping the form is what the pack format
//! was built for, and it is what a set that already compresses wants — it holds
//! frames, and a frame is a frame whichever set wrote it.
//!
//! **The frame is still decoded, and this is the part that looks like waste.** A
//! chunk is named by the hash of its content, so what a pack says about a chunk is
//! a claim, and the only thing that could check it is the content itself; see
//! [`StoragePut::put_stored`], which says why at the length it deserves. What the
//! default saves is the encode, and what it keeps is the guarantee every other
//! path has: a damaged or hostile pack is refused on this path exactly as on any
//! other, and a merge that refuses it publishes no version.
//!
//! **A frame is kept only if it earned its place**, which the store decides rather
//! than this command: a frame no smaller than its content, or one naming a codec
//! this build cannot read, is stored raw instead. Both lengths are in the record,
//! so the check is two integer comparisons, and without it a pack could grow the
//! set it is merged into.
//!
//! A level re-encodes, for the set that has a compression policy of its own and
//! does not want the sender's frames. It changes what is stored and nothing else —
//! the same chunks are read, the same hashes checked, the same figures reported —
//! and it cannot rewrite what the set already holds, because every record names
//! the codec it was written with.

use std::io::{self, Cursor, SeekFrom};

use tokio::{
    fs as tfs,
    io::{AsyncReadExt, AsyncSeekExt},
};
use tracing::instrument;

use crate::{
    metadata::Snapshot,
    pack::{Pack, Stored},
    storage::{
        self, BlobId, Codec, MAX_RECORD_HEADER, Placement, RecordKind, StorageGet, StoragePut,
    },
};

/// Scratch size for hashing a chunk out of a pack, so a plan never holds a whole
/// chunk and an unexpectedly large blob costs reads rather than allocation.
const SCRATCH: usize = 64 * 1024;

/// What a [`merge`] run did.
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

/// What a [`merge_plan`] run found.
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
    /// Content, before compression: a pack hands the chunk's bytes as they were
    /// hashed, so this is what goes in, and how large the records come out
    /// depends on the set's compression policy, which is chosen when the store is
    /// opened.
    ///
    /// **The same figure under every [`Compression`], and that is deliberate.** It
    /// counts what the version *is* rather than what a particular policy makes of
    /// it, so a report does not change its meaning with a flag and a script
    /// comparing two merges is comparing the two runs rather than the two
    /// encodings. A run that kept a pack's frames stored fewer bytes on disk than
    /// this says, and the on-disk size is the segment file's to be measured from.
    pub bytes: u64,
}

/// What a merge does with each chunk's stored form.
///
/// The two are the whole of the choice, and the second one is the set's own write
/// policy rather than anything this command decides: a re-encoded chunk goes to
/// [`StoragePut::put`], which encodes it under the policy the set was opened
/// with. So a caller asking for a level opens the set with it and passes
/// [`Compression::Reencode`] — the two are not separate arguments because they
/// cannot be allowed to disagree, and the store's compressor is a type parameter
/// this command does not get to change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// Store each chunk's stored form as the pack carries it.
    ///
    /// A chunk that reached the source set as a zstd frame is written as that same
    /// frame, so a merge of a compressed pack copies frames rather than inflating
    /// every chunk and compressing it again. **The frame is still decoded** to
    /// check the chunk's hash — see [`StoragePut::put_stored`] — because a pack's
    /// claim about what a chunk is is not evidence of it, and this is the one path
    /// where a chunk's bytes are not hashed on the way in.
    #[default]
    Keep,
    /// Decode each chunk and store it under the set's own compression policy.
    Reencode,
}

/// Apply `pack` into the set at `store` as a new version.
///
/// The store is a concrete [`BlobStore`](storage::BlobStore) rather than a bare
/// [`StoragePut`](storage::StoragePut), and that is not a narrowing for taste.
/// A merge has to answer "which versions does this set hold", to check a delta's
/// base against and to report what the merge would have committed, and that
/// question is about a set's commit list — which the storage traits do not carry
/// and could not, since a `StoragePut` may be any backend at all. It is also
/// what lets the counts below come from the store's own index rather than from
/// what `put` happened to decide.
///
/// `compression` chooses between [`StoragePut::put_stored`] and
/// [`StoragePut::put`] per chunk, and changes nothing else: the same chunks are
/// read, the same hashes are checked and the same figures are reported, because
/// [`MergePlan::bytes`] counts content rather than stored bytes.
///
/// # Errors
///
/// Returns an error if the pack's snapshot cannot be read, does not hash to the
/// one its header names, or does not decode; if the pack is a delta and no
/// committed version of the set names its base; if a chunk the version reads is
/// in neither the pack nor the set; if a chunk the pack does hold does not hash
/// to the hash it is addressed by, which both [`StoragePut::put`] and
/// [`StoragePut::put_stored`] check per record; or if the set cannot be written
/// or committed.
///
/// A failure before the commit record leaves the set with no new version. See
/// this module's docs on why there is no rollback.
#[instrument(level = "info", skip_all, err)]
pub async fn merge<C: storage::Compressor>(
    store: &storage::BlobStore<storage::Write, C>,
    pack: &Pack,
    compression: Compression,
) -> io::Result<Merged> {
    // Both before a byte is written, and in that order: the snapshot says what
    // the version is, and the base check decides whether this version can be put
    // here at all.
    let (snapshot, table) = checked_snapshot(pack)?;
    let snapshot_id = pack.snapshot_id();

    // Before the base check and before the plan, because a pack the set already
    // holds has nothing to decide: it would publish a version number nobody asked
    // for, over a version the set can name already. Doing it first also means a
    // delta that is itself already merged is reported as such rather than refused
    // for a base it will never need.
    if let Some(version) = version_holding(store, &snapshot_id)? {
        return Ok(Merged::Already { version });
    }

    let base = match pack.base() {
        Some(base) => Some(establish_base(Some(store), base, pack.path())?),
        None => None,
    };

    // The plan is what makes a merge refuse a pack that is short of a chunk its
    // own snapshot reads, and it is where the counts come from.
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

        // The *whole* chunk, not the length this version reads of it: a pack
        // stores a chunk whole, and both write paths check the hash of everything
        // they are given against the name that chunk is addressed by, so a prefix
        // would be refused for the very reason the check exists.
        //
        // The error is wrapped because the store's own is a statement about a
        // record and a person reading it has no way to know it came from merging a
        // pack — or that the bytes are the pack's rather than the set's. A damaged
        // pack is the likeliest cause by a long way, and saying so is what tells
        // them to go and look at the file they downloaded.
        let refused = |e: io::Error| {
            io::Error::new(
                e.kind(),
                format!("refusing to merge: {} could not be stored: {e}", chunk.id),
            )
        };

        match compression {
            // The chunk's stored form goes in as it stands, so the frame a source
            // set deduplicated it into is copied rather than decoded and built
            // again. The store still decodes it — see [`StoragePut::put_stored`] —
            // so this saves the encode and nothing else.
            Compression::Keep => {
                let stored = stored_chunk(pack, &chunk.id, located).await?;
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
            // handed with it is the content length, which is not the length of the
            // pack's record — that one covers the header and, for a chunk the set
            // held compressed, a frame. `get` decodes, so what comes back is
            // content and content is what `put` hashes.
            Compression::Reencode => store
                .put(&chunk.id, pack.get(&chunk.id).await?, located.content_len)
                .await
                .map_err(refused)?,
        }

        merged.fresh = merged.fresh.saturating_add(1);
        merged.bytes = merged.bytes.saturating_add(located.content_len);
    }

    // The snapshot is a blob like any other — addressed by the hash of its own
    // bytes, which is what the commit record names — so it is stored before the
    // commit rather than carried by it.
    let table_len = u64::try_from(table.len())
        .map_err(|_| io::Error::other("the snapshot is too large to store"))?;
    store
        .put(&snapshot_id, Box::new(Cursor::new(table)), table_len)
        .await?;

    // The run's one data sync, then the commit that publishes it. The order is
    // the store's contract, not this command's: a version that exists before the
    // data it names is durable is a version nothing can be restored from.
    store.sync_data().await?;
    let version = store.commit(&snapshot_id).await?;

    Ok(Merged::Committed {
        version,
        plan: merged,
    })
}

/// Say what merging `pack` into `set` would store, and store none of it.
///
/// `set` is `None` for a name with no set behind it, which is the first version
/// arriving somewhere and the case where this has to answer "everything is new"
/// rather than fail the way opening a store for reading would. Opening for
/// writing to find out is not an option: that *creates* the set, and a dry run
/// that created one would be the one thing it promised not to do.
///
/// The plan is the one a real merge builds — the same resolution against the
/// same two sources — and every chunk that would be written is read out of the
/// pack and hashed, because `put` is what performs that check on the real path
/// and a plan that skipped it would report success for a pack a real merge
/// refuses. So this costs what the merge costs and mutates nothing.
///
/// **It takes no [`Compression`], and reports the same figures under every one.**
/// A chunk is hashed as content whichever path stores it, so the check a plan
/// makes is the check both paths make, and [`MergePlan::bytes`] counts content
/// rather than stored bytes — there is nothing about the encoding for a plan to
/// decide differently.
///
/// # Errors
///
/// As [`merge`], and for the same reasons at the same points: a delta whose base
/// the set does not hold is refused by a plan too, since the plan is what would
/// decide that.
pub async fn merge_plan<C: storage::Compressor>(
    pack: &Pack,
    set: Option<&storage::BlobStore<storage::Read, C>>,
) -> io::Result<Planned> {
    let (snapshot, _table) = checked_snapshot(pack)?;
    let snapshot_id = pack.snapshot_id();

    // The same check, in the same place relative to the base and the plan, as
    // `merge`'s — and for the same reason. A plan that reported a merge of
    // something already merged would be telling the user their set is about to
    // grow a version it is not, which is the one lie a dry run cannot tell.
    if let Some(version) = match set {
        Some(store) => version_holding(store, &snapshot_id)?,
        None => None,
    } {
        return Ok(Planned::Already { version });
    }

    let base = match pack.base() {
        Some(base) => Some(establish_base(set, base, pack.path())?),
        None => None,
    };

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
        check_chunk(pack, &chunk.id, located.content_len).await?;

        merged.fresh = merged.fresh.saturating_add(1);
        merged.bytes = merged.bytes.saturating_add(located.content_len);
    }

    Ok(Planned::Merge(merged))
}

/// One chunk's stored form as the pack carries it: its record's payload, and
/// what that record says about the payload.
///
/// Read out of the pack's payload region rather than through [`StorageGet`],
/// which decodes: this is the one path a merge takes where the bytes handed to
/// the store are the pack's own rather than the chunk's content.
///
/// **The record's own header is what says how the payload is stored**, and it is
/// read through [`crate::storage`]'s decoder so a header whose check does not
/// verify stops the chunk here rather than being taken as a length. The header is
/// then held to the index the same way [`Pack::get`] holds it — same hash, same
/// content length, and a record that fits inside the payload — so the two paths
/// into a chunk cannot disagree about which bytes it is.
///
/// Bounding the read by the end of the *payload* rather than by the next record's
/// offset is weaker than [`Pack::get`]'s bound, and deliberately: what a
/// misjudged span could cost here is reading a few bytes of a neighbouring record,
/// which the hash check in [`StoragePut::put_stored`] then refuses, rather than
/// serving them as a chunk. Reading the index to find the true span would mean a
/// sort per chunk to guard against an outcome the identity check already catches.
///
/// # Errors
///
/// Returns an error if the record's header does not decode or does not agree with
/// the index, if the record does not fit inside the payload, or if the payload
/// cannot be read.
async fn stored_chunk(
    pack: &Pack,
    id: &BlobId,
    located: crate::pack::Located,
) -> io::Result<Stored> {
    let path = pack.path();
    let mut file = crate::storage::at_async(path, "open", tfs::File::open(path)).await?;

    let start = pack.payload_start().saturating_add(located.offset);
    file.seek(SeekFrom::Start(start)).await?;

    // Variable width, so read rather than stepped over, and bounded by what is
    // left of the payload: a header that did not fit there is not this record's
    // header, and reading on would pull in the snapshot's bytes.
    let left = pack.payload_len().saturating_sub(located.offset);
    let head_len = left.min(u64::try_from(MAX_RECORD_HEADER).unwrap_or(u64::MAX));
    let mut head = crate::storage::zeroed(
        head_len,
        &format!("the record header of chunk {id} in {}", path.display()),
    )?;
    file.read_exact(&mut head).await?;

    let Some(header) = crate::storage::decode_header(&head, 0) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "refusing to merge: the record {} holds for chunk {id} does not begin with a \
                 record header: its magic, kind, codec or its own check do not verify",
                path.display()
            ),
        ));
    };

    let total = header.total_len.saturating_add(header.stored_len);
    // The claim is bounded before it is believed, here as it is on the reading
    // path, and for the same reason: a pack is a file that was downloaded, and
    // a frame declaring more content than the chunker can ever produce would
    // have the decode reserve that much before finding out. `stored_len` is
    // bounded already, by what is left of the payload; `raw_len` is only
    // bounded by the index, which is as untrusted as the header it agrees with.
    if header.raw_len > crate::pack::MAX_CHUNK {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "chunk {id} claims {} bytes of content, which is more than the largest chunk \
                 this crate's chunker produces ({})",
                header.raw_len,
                crate::pack::MAX_CHUNK
            ),
        ));
    }

    if header.kind != RecordKind::Blob
        || BlobId::from_bytes(header.hash) != *id
        || header.raw_len != located.content_len
        || total > left
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "refusing to merge: the record {} holds for chunk {id} is not the one its index \
                 names: it claims {total} bytes of a {left}-byte payload, and {} bytes of content \
                 where the index says {}, stored as {:?}",
                path.display(),
                header.raw_len,
                located.content_len,
                header.codec,
            ),
        ));
    }

    file.seek(SeekFrom::Start(start.saturating_add(header.total_len)))
        .await?;
    let mut bytes = crate::storage::zeroed(
        header.stored_len,
        &format!("chunk {id} as {} stores it", path.display()),
    )?;
    file.read_exact(&mut bytes).await?;

    Ok(Stored {
        bytes,
        codec: header.codec,
        raw_len: header.raw_len,
    })
}

/// The pack's file table, and the bytes to store it as.
///
/// Read once and hashed here rather than calling [`Pack::snapshot`] and then
/// [`Pack::snapshot_bytes`], which would read the same section twice and let the
/// two reads disagree on a file that is being written underneath. What is stored
/// has to be the bytes that were verified, and this is the only way to arrange
/// that.
///
/// The hash check is `Pack::snapshot`'s, restated because the value it returns is
/// not what is wanted here: a decoded snapshot cannot be stored, since
/// re-encoding one is a promise that it encodes to the same bytes.
///
/// # Errors
///
/// Returns an error if the section cannot be read, if its bytes do not hash to
/// the snapshot the header names, or if they do not decode.
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
/// `set` is `None` for a name with no set behind it, which is a refusal rather
/// than an empty answer: there is no committed version, so no version holds the
/// base.
///
/// See this module's docs for why this is containment among committed versions
/// and not a check that the set is exactly the base. Public because a caller
/// needs it *before* opening the set for writing, which creates a set that does
/// not exist: a delta refused against a name with nothing behind it must leave
/// that name alone, and the store handle that would answer the question is the
/// thing being created.
///
/// # Errors
///
/// Returns an error if the set cannot list its versions or read a version's
/// snapshot, or if no committed version holds `base` — naming the hash and what
/// to do instead.
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
/// hashes rather than a lookup: the store names a version's snapshot by number
/// and nothing names a snapshot by the version that has it. The index files are
/// not consulted either — they are a cache of the same answer, and `lepatch
/// versions` already treats a stale or missing one as a scan rather than as a
/// fact.
///
/// Public because a caller has to answer this *before* deciding what the run is:
/// whether a merge is going to write anything, and therefore whether the warning
/// about a pack from behind is about anything, is decided by this and nothing
/// else. Doing it here and in [`merge`] is deliberate — the run under the lock is
/// the one that counts, and this is the one that lets the CLI report without
/// taking one.
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

/// Resolve the snapshot against the set and the pack together, as one store.
///
/// The plan is what a backup would build if its chunks arrived from both places,
/// and it is the whole of the merge's completeness check: a chunk the version
/// reads that neither source holds fails here, naming itself, before anything is
/// written.
///
/// # Errors
///
/// Returns an error if a chunk the version reads cannot be resolved, or if the
/// source holding it has too little of it — a store or a pack that has a chunk
/// shorter than the version reads.
fn plan_against<W: storage::WriteMode, C: storage::Compressor>(
    set: Option<&storage::BlobStore<W, C>>,
    pack: &Pack,
    snapshot: &Snapshot,
) -> io::Result<storage::Plan> {
    storage::Plan::from_snapshot(snapshot, |id| {
        if let Some(store) = set {
            match store.locate(id) {
                Ok(placement) => return Ok(placement),
                // `NotFound` is the whole of what this is for; anything else is
                // a set that cannot be asked and is reported rather than treated
                // as holding nothing.
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
/// A small fiction, and the only place this command bends the store's types: a
/// [`Placement`] names a segment and an offset because a *store* is several
/// files, and a pack is one, so every placement in a plan built against a pack
/// names segment 0 and a file offset. The `offset` is where the chunk's *record*
/// starts rather than where its payload does, and `stored_len` is zero: a pack's
/// index no longer carries the record's length — the record's own header does,
/// and it is read there rather than looked up here — and nothing reads that field
/// from a plan built over a pack. A plan reads `raw_len`, which is the one number
/// that has to be right, and the caller goes back to [`Pack::get`] for the bytes.
///
/// The codec is `Raw`, and it is the one field here that is not a fact about the
/// bytes: a plan never decodes a placement, and the bytes themselves arrive
/// through [`Pack::get`], which reads the codec out of the record itself rather
/// than from here. Naming a codec in an index that is not read to decode anything
/// would be a second claim to keep in step with the record's own.
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

/// Hash `len` bytes of the chunk `id` out of the pack and refuse a mismatch.
///
/// What [`StoragePut::put`] does with every record on a real merge, done here
/// for a run that stores nothing. Streamed rather than buffered, so a plan over
/// a large pack does not hold a chunk.
///
/// # Errors
///
/// Returns an error if the chunk cannot be read, is short of `len` bytes, or
/// does not hash to `id`.
async fn check_chunk(pack: &Pack, id: &BlobId, len: u64) -> io::Result<()> {
    use tokio::io::AsyncReadExt as _;

    let mut reader = pack.get(id).await?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; SCRATCH];

    let mut left = len;
    while left > 0 {
        let want = usize::try_from(left.min(u64::try_from(SCRATCH).unwrap_or(u64::MAX)))
            .unwrap_or(SCRATCH);
        let read = reader.read(&mut buffer[..want]).await?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "refusing to merge: {} ends {left} bytes early in chunk {id}",
                    pack.path().display()
                ),
            ));
        }

        hasher.update(&buffer[..read]);
        left -= u64::try_from(read).unwrap_or(u64::MAX);
    }

    if hasher.finalize() != *id {
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
