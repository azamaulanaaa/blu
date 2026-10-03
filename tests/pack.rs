//! A pack: one version's chunks in one file, and the byte ranges a client that
//! already holds some of them needs.
//!
//! The tests are about the properties that make a pack usable for a delta
//! download and safe to cache:
//!
//! * it is a function of the tree, so it is byte-identical every time and can be
//!   named by its own hash;
//! * the index is at the end, so a client learns every offset before reading a
//!   byte of payload;
//! * `needed` is in terms of what a client does *not* have, and adjacent ranges
//!   merge, which is the entire point of packing;
//! * a truncated or corrupt pack is refused rather than read as a shorter one.

#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

mod common;

use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};

use lepatch::{
    metadata::{Attributes, File, FileChunk, Snapshot},
    pack::{Pack, Range, StorageStored, hash_file},
    storage::{BlobStore, Codec, Plan, StorageGet},
};
use std::io::{Read, Seek, SeekFrom};
use tokio::io::AsyncReadExt;

/// A set's latest snapshot, read from the store and decoded.
async fn snapshot_of(blob: &Path) -> Snapshot {
    let store = BlobStore::open_for_reading(blob).await.unwrap();
    let id = store.latest().unwrap().1;
    let mut reader = store.get(&id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();

    Snapshot::try_from(buffer.as_slice()).unwrap()
}

/// The BLAKE3 of a set's latest snapshot — what a pack of that version names.
async fn snapshot_id(blob: &Path) -> lepatch::storage::BlobId {
    let store = BlobStore::open_for_reading(blob).await.unwrap();

    let id = store.latest().unwrap().1;
    let mut reader = store.get(&id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();

    blake3::hash(&buffer)
}

/// The plan for a set's latest version, and the chunk hashes it names.
async fn plan_of(blob: &Path) -> Plan {
    let store = BlobStore::open_for_reading(blob).await.unwrap();

    let snapshot = snapshot_of(blob).await;

    Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap()
}

/// The chunk hashes a set's latest version names.
async fn hashes_of(blob: &Path) -> HashSet<lepatch::storage::BlobId> {
    plan_of(blob).await.iter().map(|c| c.id).collect()
}

/// A tree, backed up once, and the plan for it.
struct Built {
    dir: tempfile::TempDir,
    blob: std::path::PathBuf,
    plan: Plan,
    /// What the set calls this version, which goes in the pack's header.
    version: u32,
}

impl Built {
    /// Write a pack of this version to `at`, and return it.
    async fn pack(&self, at: &Path) -> Pack {
        let store = BlobStore::open_for_reading(&self.blob).await.unwrap();
        let snapshot = snapshot_of(&self.blob).await;

        Pack::write(&store, &self.plan, &snapshot, self.version, at)
            .await
            .unwrap()
    }
}

/// Pack a set's latest version to `at`, recorded as version 1.
///
/// The number is the set's own, and these fixtures hold one version, so 1 is what
/// `versions` would print beside it. Stated at every call rather than worked out
/// here because a helper that invented the number would be exactly the thing the
/// header exists to make impossible to get wrong quietly.
async fn pack(blob: &Path, at: &Path) -> Pack {
    let store = BlobStore::open_for_reading(blob).await.unwrap();
    let plan = plan_of(blob).await;
    let snapshot = snapshot_of(blob).await;

    Pack::write(&store, &plan, &snapshot, 1, at).await.unwrap()
}

async fn build(files: usize, size: usize) -> Built {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    for i in 0..files {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::noisy(size + i * 1_000),
        )
        .unwrap();
    }

    let blob = dir.path().join("store");
    common::run_backup(&source, &blob).await;
    let plan = plan_of(&blob).await;

    Built {
        dir,
        blob,
        plan,
        version: 1,
    }
}

/// A pack holds every chunk its version needs, each as a store record.
///
/// The payload is the run of records and nothing else: `offset` is where a
/// chunk's record starts, and the records are written end to end — which is what
/// makes a full download one range, and what lets a range covering a chunk cover
/// the whole of it now that the index says where each record begins rather than
/// how long it is.
///
/// What each record holds is the chunk, in the form the store already had it, and
/// `get` hands that back as content: a chunk whose record is a frame comes back
/// decompressed.
#[tokio::test]
async fn a_pack_holds_its_versions_chunks() {
    let built = build(5, 200_000).await;
    let path = built.dir.path().join("v1.pack");
    let pack = built.pack(&path).await;
    let bytes = fs::read(&path).unwrap();

    assert_eq!(pack.len(), built.plan.len());

    let store = BlobStore::open_for_reading(&built.blob).await.unwrap();
    let mut file = fs::File::open(&path).unwrap();

    let mut at = 0u64;
    for (i, chunk) in built.plan.iter().enumerate() {
        let located = pack.locate(&chunk.id).unwrap();

        // In plan order, and with nothing between them.
        assert_eq!(
            located.offset, at,
            "chunk {i} is not where the one before it ended"
        );
        at += record_len(&bytes, pack.payload_start(), located.offset);

        // And the record at that offset is one: the store's magic, its kind, its
        // codec byte, and the chunk's own hash in it. Read through the header the
        // file itself carries, so the length added below is the record's own claim
        // rather than a second one about it.
        let mut header = [0u8; 48];
        file.seek(SeekFrom::Start(pack.payload_start() + located.offset))
            .unwrap();
        file.read_exact(&mut header).unwrap();
        assert_eq!(&header[..4], b"LPB1", "chunk {i} does not begin a record");
        assert_eq!(header[4], 0, "chunk {i}'s record is not a blob record");
        assert!(
            header[5] <= 1,
            "chunk {i}'s record names a codec this crate does not have"
        );
        // The hash sits after the two lengths, whose widths depend on the chunk,
        // so where it is is worked out from the varints rather than assumed.
        let stored_at = 6;
        let raw_at = stored_at + varint_len(&header, stored_at);
        let hash_at = raw_at + varint_len(&header, raw_at);
        assert_eq!(
            &header[hash_at..hash_at + 32],
            chunk.id.as_bytes(),
            "chunk {i}'s record does not name the chunk the index does"
        );

        // And what comes back through `get` is the content, whole.
        let mut actual = Vec::new();
        pack.get(&chunk.id)
            .await
            .unwrap()
            .read_to_end(&mut actual)
            .await
            .unwrap();

        let mut expected = Vec::new();
        store
            .get(&chunk.id)
            .await
            .unwrap()
            .read_to_end(&mut expected)
            .await
            .unwrap();

        assert_eq!(
            actual, expected,
            "chunk {i} is not its own bytes in the pack"
        );
        assert_eq!(
            actual.len() as u64,
            located.content_len,
            "and the index says how many bytes of content that is"
        );
    }

    assert_eq!(
        at,
        pack.payload_len(),
        "the records do not fill the payload"
    );
    assert!(
        pack.payload_len() > built.plan.total_bytes(),
        "a record is its chunk plus a header, so the payload is larger than the content"
    );
}

/// The index is at the end, so its cost does not grow with the payload.
///
/// This is the property that lets a client decide whether it wants the pack before
/// it has read any of it, and it is the reason the format is laid out this way.
#[tokio::test]
async fn the_index_is_at_the_end() {
    let built = build(5, 200_000).await;
    let path = built.dir.path().join("v1.pack");
    let pack = built.pack(&path).await;

    let file_len = pack.file_len().unwrap();
    let sections = sections(&fs::read(&path).unwrap());
    let index_len = u64::try_from(sections.index_len).unwrap();

    assert!(sections.index_len > 0, "the index should not be empty");
    assert!(
        index_len < file_len / 2,
        "the index is {index_len} of {file_len} bytes, so a client reading only \
         the tail reads a small fraction of the file"
    );

    // The file table travels with the payload and is still small next to it: this
    // is what makes a pack worth handing over as one file.
    assert_eq!(sections.snapshot_len as u64, pack.snapshot_len());
    assert!(
        u64::try_from(sections.snapshot_len).unwrap() < pack.payload_len() / 8,
        "the file table is {} bytes against {} of payload",
        sections.snapshot_len,
        pack.payload_len()
    );

    // The index length alone locates the index, so a client that only wants
    // offsets never reads the snapshot — which is why it sits before the index
    // rather than after it.
    assert_eq!(
        sections.snapshot_start as u64,
        pack.snapshot_start(),
        "the snapshot should begin where the index length says it does"
    );
    assert_eq!(u64::try_from(sections.file_len).unwrap(), file_len);

    // **In payload order, on disk as well as in memory.** A client reading this
    // file rather than this crate walks the index itself, and the order it finds
    // is the one the records were written in — which is what makes a run of
    // adjacent records one range for it too. The reader here sorts for itself,
    // so nothing in this crate would notice if the writer sorted the other way.
    let bytes = fs::read(&path).unwrap();
    let index = &bytes[sections.index_start..sections.index_start + sections.index_len];
    let offsets: Vec<u64> = index_entries(index)
        .iter()
        .map(|at| decode_varint(index, at + 32))
        .collect();

    assert_eq!(
        offsets.first().copied(),
        Some(0),
        "the first record is at 0"
    );
    assert!(
        offsets.windows(2).all(|pair| pair[0] < pair[1]),
        "the index on disk should be in payload order, and it is {offsets:?}"
    );

    // Opening costs the index and nothing else, and gives the same answer as the
    // pack that was just written.
    let reopened = Pack::open(&path).unwrap();
    assert_eq!(reopened.len(), pack.len());
    assert_eq!(reopened.snapshot_id(), pack.snapshot_id());
    assert_eq!(reopened.payload_len(), pack.payload_len());
}

/// A pack's index is held in hash order, and every chunk still resolves from it.
///
/// Holding the index in the order chunks are addressed by is what turns a lookup
/// from a scan of the whole index into a binary search. It is also what forces
/// the byte-range questions to sort for themselves, because hash order is not the
/// order the records sit in — so what has to survive the change is both halves: the
/// lookup still finds every chunk, and a full download is still one range.
///
/// The fixture is wide enough that the two orders are visibly different, and that
/// is asserted rather than assumed: a pack of two or three chunks could come out of
/// a sort in the same order it went in, and a test that could not tell would be a
/// test that stops testing anything the moment the fixture changes.
///
/// What would falsify it: a `locate` that misses, a `get` that hands back the
/// wrong bytes, a writer and a reader disagreeing about the order they hold the
/// index in, or `needed` answering one range per chunk.
#[tokio::test]
async fn a_pack_resolves_every_chunk_from_a_hash_ordered_index() {
    let built = build(12, 180_000).await;
    let path = built.dir.path().join("v1.pack");
    let pack = built.pack(&path).await;

    assert!(
        pack.len() >= 20,
        "the fixture should hold enough chunks for the two orders to differ, got {}",
        pack.len()
    );

    let by_hash = pack
        .chunks()
        .windows(2)
        .all(|pair| pair[0].id.as_bytes() <= pair[1].id.as_bytes());
    assert!(by_hash, "the index should be held in hash order");

    // And that order is *not* the payload's, which is the reason `needed` cannot
    // walk the index and merge ranges as it goes.
    let by_payload = pack
        .chunks()
        .windows(2)
        .all(|pair| pair[0].offset < pair[1].offset);
    assert!(
        !by_payload,
        "a hash-ordered index that also happened to be in payload order would leave the \
         sort in `needed` untested"
    );

    // Every chunk resolves to its own entry, and to its own bytes.
    let store = BlobStore::open_for_reading(&built.blob).await.unwrap();
    for planned in built.plan.chunks() {
        let located = pack
            .locate(&planned.id)
            .unwrap_or_else(|| panic!("chunk {} is not in the index", planned.id));
        assert_eq!(located.offset, pack.locate(&planned.id).unwrap().offset);

        let mut actual = Vec::new();
        pack.get(&planned.id)
            .await
            .unwrap()
            .read_to_end(&mut actual)
            .await
            .unwrap();

        let mut expected = Vec::new();
        store
            .get(&planned.id)
            .await
            .unwrap()
            .read_to_end(&mut expected)
            .await
            .unwrap();

        assert_eq!(
            actual, expected,
            "chunk {} differs from the store",
            planned.id
        );
        assert_eq!(actual.len() as u64, located.content_len);
    }

    // A pack this build wrote and the same pack read back hold the index in the
    // same order, so a caller may compare them.
    assert_eq!(Pack::open(&path).unwrap().chunks(), pack.chunks());

    // And the property a pack exists for is untouched by the sort: the whole
    // payload, as one range, and a client that records it holds every chunk.
    let needed = pack.needed(&HashSet::new());
    assert_eq!(
        needed.len(),
        1,
        "a full download should still be one range, got {}",
        needed.len()
    );
    assert_eq!(pack.needed_bytes(&HashSet::new()), pack.payload_len());
    assert_eq!(pack.chunks_covering(&needed).len(), pack.len());
}

/// A client holding nothing needs the whole payload, as one range.
///
/// One request, not one per chunk. That is the whole reason a pack exists.
#[tokio::test]
async fn a_client_holding_nothing_needs_one_range() {
    let built = build(5, 200_000).await;
    let pack = built.pack(&built.dir.path().join("v1.pack")).await;

    let needed = pack.needed(&HashSet::new());

    assert_eq!(
        needed.len(),
        1,
        "a full download should be one range, not {}",
        needed.len()
    );
    assert_eq!(pack.needed_bytes(&HashSet::new()), pack.payload_len());
}

/// A client holding a version's chunks needs only the rest, in one range.
///
/// This is the measured case that justifies packing, and it is asserted on a tree
/// built from `common::untagged_stream` rather than `common::noisy`.
///
/// That matters more than it looks. `noisy` is a function of position only, so
/// `noisy(n)` is a prefix of `noisy(m)`: a tree of them is one long byte stream cut
/// at various points, deduplicates far more than real data would, and "rewriting"
/// a file by asking for a shorter `noisy` leaves the original stream truncated —
/// the best possible case for content-defined chunking. A test on that fixture
/// shows the property is *achievable*, not that it *holds*.
///
/// On this fixture the delta is a single range and about 6% of the pack, against
/// 1.9% on the degenerate one. So the robust claim is the request count, and the
/// fraction is fixture-dependent.
#[tokio::test]
async fn a_patch_is_one_range_on_a_tree_that_is_not_a_special_case() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    let blob = dir.path().join("store");

    for (i, size) in [
        500_000usize,
        540_000,
        580_000,
        620_000,
        660_000,
        700_000,
        740_000,
        780_000,
    ]
    .into_iter()
    .enumerate()
    {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::untagged_stream(u8::try_from(i).unwrap(), size),
        )
        .unwrap();
    }
    common::run_backup(&source, &blob).await;

    let v1 = hashes_of(&blob).await;

    // One binary genuinely rewritten, with new content rather than a truncation of
    // the old.
    fs::write(source.join("f3.bin"), common::untagged_stream(200, 640_000)).unwrap();
    common::run_backup(&source, &blob).await;

    let v2 = plan_of(&blob).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let snapshot = snapshot_of(&blob).await;
    let pack = Pack::write(&store, &v2, &snapshot, 2, &dir.path().join("v2.pack"))
        .await
        .unwrap();

    let needed = pack.needed(&v1);

    assert_eq!(
        needed.len(),
        1,
        "a one-file patch on ordinary content should be one range, got {}",
        needed.len()
    );
    assert!(
        pack.needed_bytes(&v1) > 0,
        "the patch should need something"
    );
    assert!(
        pack.needed_bytes(&v1) < pack.payload_len(),
        "the patch should need less than the whole pack"
    );
}

/// A client that already holds everything needs nothing.
#[tokio::test]
async fn a_client_holding_everything_needs_nothing() {
    let built = build(4, 150_000).await;
    let pack = built.pack(&built.dir.path().join("v1.pack")).await;

    let have: HashSet<_> = built.plan.iter().map(|c| c.id).collect();

    assert_eq!(pack.needed(&have), []);
    assert_eq!(pack.needed_bytes(&have), 0);
}

/// A client that records what it fetched can ask again and be told nothing.
///
/// The loop a delta protocol is: fetch, record, re-ask. If the recording is wrong
/// in either direction the protocol breaks — over-reporting and the client stalls,
/// under-reporting and it fetches twice — so both are asserted.
#[tokio::test]
async fn recording_a_fetch_makes_the_next_ask_empty() {
    let built = build(6, 200_000).await;
    let pack = built.pack(&built.dir.path().join("v1.pack")).await;

    let first = pack.needed(&HashSet::new());
    assert_eq!(first.len(), 1);

    let have = pack.chunks_covering(&first);
    assert_eq!(
        have.len(),
        pack.len(),
        "a full fetch should record every chunk"
    );

    assert!(
        pack.needed(&have).is_empty(),
        "a client holding everything it fetched should need nothing"
    );
}

/// A pack is byte-identical for the same tree.
///
/// The property that makes a pack nameable by its own hash, and therefore
/// immutable and cacheable forever. It follows from the plan being ordered by the
/// snapshot rather than by write order, and it is worth asserting through the
/// whole stack rather than at the plan.
#[tokio::test]
async fn the_same_tree_packs_to_the_same_bytes() {
    // One tree, two independent stores. The stores lay their chunks out
    // differently — a second store has no reason to put a chunk where the first
    // did — so identical packs prove the output depends on the tree and not on
    // how the store happened to be written.
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    for i in 0..5 {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::noisy(180_000 + i * 1_000),
        )
        .unwrap();
    }

    let first = dir.path().join("store-a");
    let second = dir.path().join("store-b");
    common::run_backup(&source, &first).await;
    common::run_backup(&source, &second).await;

    let a = pack(&first, &dir.path().join("a.pack")).await;
    let b = pack(&second, &dir.path().join("b.pack")).await;

    assert_eq!(
        fs::read(a.path()).unwrap(),
        fs::read(b.path()).unwrap(),
        "two backups of one tree produced different packs"
    );
    assert_eq!(
        hash_file(a.path()).await.unwrap(),
        hash_file(b.path()).await.unwrap()
    );
}

/// Writing the same version twice produces the same bytes.
///
/// The weaker form of the property above, and the one that catches a writer that
/// stamps a timestamp or a run number into the file. Cheap enough to be worth
/// having both.
#[tokio::test]
async fn writing_a_version_twice_produces_the_same_bytes() {
    let built = build(5, 180_000).await;
    let a = built.pack(&built.dir.path().join("a.pack")).await;
    let b = built.pack(&built.dir.path().join("b.pack")).await;

    assert_eq!(fs::read(a.path()).unwrap(), fs::read(b.path()).unwrap());
}

/// A store that hands a pack one byte more than the plan resolved.
///
/// The set the pack was planned against moving underneath the run, which is the
/// only thing that can make the store disagree with the plan: a set is appended
/// to while a pack of it is written, and a record's header carries a count worked
/// out before the run began.
struct GrowsByOne<S>(S);

impl<S: lepatch::pack::StorageStored> lepatch::pack::StorageStored for GrowsByOne<S> {
    async fn stored(
        &self,
        id: &lepatch::storage::BlobId,
    ) -> std::io::Result<lepatch::pack::Stored> {
        let mut stored = self.0.stored(id).await?;
        stored.bytes.push(0);
        Ok(stored)
    }
}

/// A pack is renamed into place, so a failed run leaves the previous one alone.
///
/// `lepatch pack` overwrites, so the path is nearly always one a previous run
/// wrote and somebody wants back. Writing in place destroyed it before the run
/// knew it could finish — the header goes first, and the header's payload length
/// is a count of records that have not been written yet.
#[tokio::test]
async fn a_pack_that_fails_to_write_leaves_the_one_that_was_there() {
    let built = build(4, 60_000).await;
    let out = built.dir.path().join("out");
    fs::create_dir_all(&out).unwrap();
    let path = out.join("photos.lpk");

    let written = built.pack(&path).await;
    let before = fs::read(&path).unwrap();

    // Overwriting is the ordinary case, and it must not leave the temporary
    // behind either: the rename consumed it.
    built.pack(&path).await;

    let store = BlobStore::open_for_reading(&built.blob).await.unwrap();
    let snapshot = snapshot_of(&built.blob).await;
    let refused = Pack::write(
        &GrowsByOne(store),
        &built.plan,
        &snapshot,
        built.version,
        &path,
    )
    .await
    .unwrap_err();

    assert!(
        refused
            .to_string()
            .contains("the set changed while this pack"),
        "a store that disagrees with the plan must be refused by name, and not: {refused}"
    );
    assert_eq!(
        fs::read(&path).unwrap(),
        before,
        "a run that failed took the pack an earlier run wrote"
    );
    assert_eq!(
        Pack::open(&path).unwrap(),
        written,
        "the pack left behind is not the one the last good run wrote"
    );

    let leftovers: Vec<_> = fs::read_dir(&out)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name != "photos.lpk")
        .collect();

    assert!(
        leftovers.is_empty(),
        "writing a pack left {leftovers:?} beside it"
    );
}

/// Truncating a pack is detected rather than read as a shorter one.
///
/// The trailing magic is what does it: a truncated file's last bytes are payload,
/// which is indistinguishable from an index length, so without the magic a short
/// download would be read as a valid pack missing its tail.
#[tokio::test]
async fn a_truncated_pack_is_refused() {
    let built = build(4, 150_000).await;
    let path = built.dir.path().join("v1.pack");
    built.pack(&path).await;

    let bytes = fs::read(&path).unwrap();
    for cut in [1usize, 8, 12, 40, 100] {
        if cut >= bytes.len() {
            continue;
        }

        let short = built.dir.path().join(format!("short{cut}.pack"));
        fs::write(&short, &bytes[..bytes.len() - cut]).unwrap();

        assert!(
            Pack::open(&short).is_err(),
            "a pack missing its last {cut} bytes was opened as one"
        );
    }
}

/// A pack whose header disagrees with its own index is refused.
///
/// The two are redundant, and redundancy is the point: the index is what locates
/// chunks, so if the header's counts and the index's entries disagree then one of
/// them is wrong, and picking the wrong one produces a pack that reads as valid and
/// hands out ranges covering the wrong bytes. Neither the index nor the header is
/// trustworthy on its own, so they are checked against each other.
#[tokio::test]
async fn a_header_that_contradicts_its_index_is_refused() {
    let built = build(4, 150_000).await;
    let path = built.dir.path().join("v1.pack");
    built.pack(&path).await;

    let good = fs::read(&path).unwrap();

    // Find the two counts by decoding the varints where the header puts them,
    // rather than assuming byte offsets — a test that hardcodes a layout breaks
    // when the layout changes, and breaks by being unreadable rather than by
    // failing usefully. The flag byte sits between the snapshot hash and the
    // counts, and is clear for a full pack, so this is where they start.
    // The `+ 4` is the version field the header now carries, which is why this
    // offset is written out rather than imported from the module.
    let count_at = 4 + 4 + 4 + 32 + 1;
    assert_eq!(
        good[count_at - 1],
        0,
        "sanity: a full pack's flag byte is clear"
    );
    let count_len = varint_len(&good, count_at);
    let len_at = count_at + count_len;
    let len_len = varint_len(&good, len_at);

    assert_eq!(
        decode_varint(&good, count_at),
        built.plan.len() as u64,
        "sanity: the header's own count"
    );

    // Claim one more chunk than the index holds, by incrementing the varint's
    // final byte.
    let mut wrong_count = good.clone();
    wrong_count[count_at + count_len - 1] += 1;
    let path = built.dir.path().join("wrong-count.pack");
    fs::write(&path, &wrong_count).unwrap();
    let err = Pack::open(&path).unwrap_err();
    assert!(
        err.to_string().contains("index lists"),
        "a wrong chunk count was not caught: {err}"
    );

    // Claim a payload length that does not reach the index.
    let mut wrong_len = good.clone();
    wrong_len[len_at + len_len - 1] += 1;
    let path = built.dir.path().join("wrong-len.pack");
    fs::write(&path, &wrong_len).unwrap();
    let err = Pack::open(&path).unwrap_err();
    assert!(
        err.to_string().contains("payload ends at"),
        "a wrong payload length was not caught: {err}"
    );
}

/// Where a written pack's sections are, read from its trailer.
///
/// Every test that damages a pack needs to know where a section begins, and the
/// answer is arithmetic over the trailer rather than a constant — a test that
/// hard-coded the trailer's width would be asserting about the format it was
/// written against, which is exactly what stops being true when the format moves.
struct Sections {
    file_len: usize,
    snapshot_start: usize,
    snapshot_len: usize,
    index_start: usize,
    index_len: usize,
}

/// Read a pack's trailer. Panics if the bytes are not a pack this crate wrote.
fn sections(bytes: &[u8]) -> Sections {
    const TRAILER: usize = 20;

    let file_len = bytes.len();
    assert!(file_len > TRAILER, "a pack is longer than its trailer");

    let trailer = &bytes[file_len - TRAILER..];
    assert_eq!(&trailer[16..], b"LPK1", "a pack must end with its magic");

    let index_len = usize::try_from(u64::from_le_bytes(trailer[..8].try_into().unwrap())).unwrap();
    let snapshot_len =
        usize::try_from(u64::from_le_bytes(trailer[8..16].try_into().unwrap())).unwrap();
    let index_start = file_len - TRAILER - index_len;

    Sections {
        file_len,
        snapshot_start: index_start - snapshot_len,
        snapshot_len,
        index_start,
        index_len,
    }
}

/// The offsets, within a pack's index, where each entry's hash begins.
///
/// Walked rather than computed, because an entry is 32 bytes plus two varints
/// and the varints' widths depend on the offsets the fixture happened to
/// produce. A test that assumed a width would be asserting about its own fixture
/// rather than about the pack.
fn index_entries(index: &[u8]) -> Vec<usize> {
    let mut at = 0usize;
    let mut out = Vec::new();
    while at < index.len() {
        out.push(at);
        at += 32;
        at += varint_len(index, at);
        at += varint_len(index, at);
    }
    out
}

fn varint_len(bytes: &[u8], at: usize) -> usize {
    lepatch::varint::read(bytes, at)
        .map(|(_, used)| used)
        .expect("a varint running off the end of the buffer")
}

/// Bytes one record occupies, read out of the record's own header.
///
/// The header and the index used to both carry this number, and a test that read
/// it from the index could not tell a pack whose records were laid out as claimed
/// from one whose index claimed something else. So the record's own header is the
/// only place a test reads it from too, which is what makes a disagreement between
/// the two visible at all.
fn record_len(bytes: &[u8], payload_start: u64, offset: u64) -> u64 {
    let at = usize::try_from(payload_start + offset).unwrap();
    let stored_at = at + 6;
    let stored_len = decode_varint(bytes, stored_at);
    let raw_at = stored_at + varint_len(bytes, stored_at);

    // The header is the two lengths, the hash and the check; the record is that
    // with the stored payload on the end.
    let header_len = 6 + varint_len(bytes, stored_at) + varint_len(bytes, raw_at) + 32 + 8;

    u64::try_from(header_len)
        .unwrap()
        .saturating_add(stored_len)
}

/// A record header carrying `stored_len` and `raw_len` for `id`, check and all.
///
/// Written out rather than a length field patched in place, and that is the whole
/// point of it: patching a length leaves the header's own eight-byte check stale,
/// so the header stops decoding and the pack is refused for being unreadable —
/// which says nothing about whether the index and the record disagree. This is
/// what a header claiming those lengths *would* be, so the only thing wrong with
/// the pack built around it is the claim.
fn record_header(
    codec: u8,
    id: &lepatch::storage::BlobId,
    stored_len: u64,
    raw_len: u64,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"LPB1");
    out.push(0);
    out.push(codec);
    lepatch::varint::write(&mut out, stored_len);
    lepatch::varint::write(&mut out, raw_len);
    out.extend_from_slice(id.as_bytes());

    let check: [u8; 8] = blake3::hash(&out).as_bytes()[..8].try_into().unwrap();
    out.extend_from_slice(&check);

    out
}

/// Where a chunk's record header starts, and what it says.
///
/// `(offset in the file, byte width of the header, stored length, content length,
/// codec)` — everything a test needs to stand a different header in its place.
fn record_claim(
    bytes: &[u8],
    payload_start: u64,
    chunk: &lepatch::pack::Located,
) -> (usize, usize, u64, u64, u8) {
    let at = usize::try_from(payload_start + chunk.offset).unwrap();
    let stored_at = at + 6;
    let raw_at = stored_at + varint_len(bytes, stored_at);
    let stored_len = decode_varint(bytes, stored_at);
    let raw_len = decode_varint(bytes, raw_at);
    let header_len = 6 + varint_len(bytes, stored_at) + varint_len(bytes, raw_at) + 32 + 8;

    (at, header_len, stored_len, raw_len, bytes[at + 5])
}

/// The first chunk of `codec` whose two lengths can both be nudged by one without
/// either varint changing width.
///
/// Same width means the replacement header is the same size as the one it stands
/// in for, so every byte after it stays where it is and the only thing wrong with
/// the file is the claim inside that header. `codec` picks the record's kind,
/// which is not incidental: see [`an_index_and_a_record_header_cannot_disagree`].
fn nudgeable<'a>(
    bytes: &[u8],
    payload_start: u64,
    pack: &'a Pack,
    codec: u8,
) -> &'a lepatch::pack::Located {
    pack.chunks()
        .iter()
        .find(|chunk| {
            let (_, _, stored_len, raw_len, held) = record_claim(bytes, payload_start, chunk);
            held == codec
                && lepatch::varint::len(stored_len + 1) == lepatch::varint::len(stored_len)
                && lepatch::varint::len(raw_len + 1) == lepatch::varint::len(raw_len)
        })
        .unwrap_or_else(|| panic!("the fixture should hold a record stored as codec {codec}"))
}

/// An index and a record's own header cannot disagree about the same bytes.
///
/// The index no longer carries a record's length, so there is no second number to
/// catch a header that lies about one. What is left is the header checked against
/// what the index does say: the room the index gives the record, and the content
/// length it names. Both are refusals, at the chunk, before any bytes are served —
/// the alternative is a chunk handed out with somebody else's bytes at the end of
/// it.
///
/// **The records damaged here are compressed ones, and that is the whole
/// subtlety.** A raw record's stored length and its content length are the same
/// number, so a raw record damaged in either one is caught by both checks at once
/// and neither can be seen to be doing any work: a test on raw records alone would
/// pass with either check deleted. A frame's two lengths are different numbers, so
/// each can be wrong on its own. The raw case is asserted too, as the last one,
/// because a record whose two lengths move together is refused by whichever check
/// fires first and that is worth pinning as well.
///
/// Fixture: [`common::untagged_stream`], whose second half is a repeated block and
/// so compresses. `noisy` is incompressible by construction, so every record of a
/// pack built from it is raw and this test would have nothing to work with.
///
/// What would falsify it: a `get` that answered at all, or one that answered with
/// the wrong bytes — a record served a byte too long reads the next record's
/// header as content, and the caller is handed a chunk that does not hash to the
/// name it asked for.
#[tokio::test]
async fn an_index_and_a_record_header_cannot_disagree() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());
    let blob = dir.path().join("store");

    for i in 0..3 {
        fs::write(
            source.join(format!("f{i}.bin")),
            common::untagged_stream(u8::try_from(i).unwrap(), 300_000 + i * 40_000),
        )
        .unwrap();
    }
    common::run_backup(&source, &blob).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let plan = plan_of(&blob).await;
    let path = dir.path().join("v1.pack");
    let pack = Pack::write(&store, &plan, &snapshot_of(&blob).await, 1, &path)
        .await
        .unwrap();
    let good = fs::read(&path).unwrap();

    for codec in [1u8, 0u8] {
        let victim = nudgeable(&good, pack.payload_start(), &pack, codec);
        let (at, header_len, stored_len, raw_len, held) =
            record_claim(&good, pack.payload_start(), victim);

        // Sanity: the record as it stands reads back as its own chunk.
        let mut actual = Vec::new();
        pack.get(&victim.id)
            .await
            .unwrap()
            .read_to_end(&mut actual)
            .await
            .unwrap();
        assert_eq!(blake3::hash(&actual), victim.id, "codec {held}");

        // A frame one byte longer than the index leaves room for: the check the
        // index's own length used to make, and the one that stops a record being
        // read into its neighbour's bytes. A raw record cannot be damaged this way
        // without its two lengths disagreeing, which is a different refusal.
        if held == 1 {
            let mut too_long = good.clone();
            too_long[at..at + header_len].copy_from_slice(&record_header(
                held,
                &victim.id,
                stored_len + 1,
                raw_len,
            ));
            let at_path = dir.path().join(format!("too-long-{held}.pack"));
            fs::write(&at_path, &too_long).unwrap();

            let error = Pack::open(&at_path)
                .unwrap()
                .get(&victim.id)
                .await
                .err()
                .expect("a record claiming more bytes than the index gives it must be refused");
            assert!(
                error.to_string().contains("where the index gives it"),
                "a record reaching past the span the index gives it was not caught: {error}"
            );
        }

        // A content length the index does not name. On a raw record this moves the
        // stored length with it, so the two refusals are not separable and either
        // message is a correct answer.
        let mut wrong_content = good.clone();
        wrong_content[at..at + header_len].copy_from_slice(&record_header(
            held,
            &victim.id,
            stored_len + u64::from(held == 0),
            raw_len + 1,
        ));
        let at_path = dir.path().join(format!("wrong-content-{held}.pack"));
        fs::write(&at_path, &wrong_content).unwrap();

        let error = Pack::open(&at_path)
            .unwrap()
            .get(&victim.id)
            .await
            .err()
            .expect("a record claiming a content length its index does not name must be refused");
        assert!(
            error
                .to_string()
                .contains("bytes of content where the index says"),
            "a header disagreeing with the index about a chunk's content length was not caught: \
             {error}"
        );
    }

    // And the pack each of those was cut from is an ordinary one, which is what
    // shows the refusals were about the claims rather than about the files.
    assert_eq!(Pack::open(&path).unwrap().len(), pack.len());
}
/// Decode a little-endian base-128 varint, for the test's own assertions.
///
/// This was a hand-written copy of the crate's decoder, kept because the crate's
/// was private. It had drifted: it had no ten-byte cap and no overflow check, so
/// it accepted overlong encodings that all three production decoders refuse —
/// which is to say the test was checking the pack's bytes with a reader more
/// lenient than any reader that will ever see them. It now uses the real one.
fn decode_varint(bytes: &[u8], at: usize) -> u64 {
    lepatch::varint::read(bytes, at)
        .map(|(value, _)| value)
        .expect("a varint running off the end of the buffer")
}

/// A pack in a format this build does not know is named as such, not misread.
///
/// A format version that moves forward is a normal event. The wrong response is to
/// guess — a reader that accepts a future version will locate chunks by an index
/// it is interpreting wrongly, and the ranges it produces look entirely plausible.
#[tokio::test]
async fn a_future_pack_format_is_named_rather_than_misread() {
    let built = build(3, 120_000).await;
    let path = built.dir.path().join("v1.pack");
    built.pack(&path).await;

    let mut bytes = fs::read(&path).unwrap();
    bytes[4..8].copy_from_slice(&9999u32.to_le_bytes());
    let path = built.dir.path().join("future.pack");
    fs::write(&path, &bytes).unwrap();

    let err = Pack::open(&path).unwrap_err();
    assert!(
        err.to_string().contains("format 9999"),
        "a future format version was not named: {err}"
    );
}

/// A pack records the version it was packed from, and hands it back.
///
/// The number is the set's, not the pack's, so it can only come from the writer —
/// nothing about a snapshot implies it, and two sets number independently. If it
/// were defaulted rather than passed, every pack written by a caller that forgot
/// would carry a `0` that reads back as a real version number.
///
/// What would falsify it: a header that does not carry the field, a reader that
/// reads it at the wrong offset (which would show up as the snapshot hash being
/// four bytes short of a shift), or a delta — the header is the same for both, so
/// a delta that lost the number while a full pack kept it would pass a full-pack
/// test and break the case that matters most.
#[tokio::test]
async fn a_pack_records_the_version_it_was_packed_from() {
    let built = build(3, 100_000).await;
    let store = BlobStore::open_for_reading(&built.blob).await.unwrap();
    let snapshot = snapshot_of(&built.blob).await;
    let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap();
    let base = common::id_of(&[7u8; 32]);

    for number in [1u32, 2, 9, u32::MAX] {
        let at = built.dir.path().join(format!("v{number}.pack"));
        let written = Pack::write(&store, &plan, &snapshot, number, &at)
            .await
            .unwrap();
        assert_eq!(
            written.version(),
            number,
            "the pack this crate just wrote should report the version it was given"
        );

        // Through the file, not through the struct that was returned: the point
        // is what a *reader* learns, and a field the writer kept to itself would
        // satisfy the line above and nothing.
        assert_eq!(
            Pack::open(&at).unwrap().version(),
            number,
            "the version should survive the file: {number} did not round-trip"
        );

        // And it is fixed-length in the header, so it costs nothing in the
        // variable section: the same pack with a four-byte-larger number is the
        // same size.
        let delta_at = built.dir.path().join(format!("d{number}.pack"));
        let delta = Pack::write_delta(&store, &plan, &snapshot, base, number, &delta_at)
            .await
            .unwrap();
        assert_eq!(delta.version(), number, "a delta carries it too");
    }

    let length_of = |name: &str| {
        fs::metadata(built.dir.path().join(name))
            .unwrap_or_else(|e| panic!("{name} should have been written: {e}"))
            .len()
    };

    assert_eq!(
        length_of("v1.pack"),
        length_of(&format!("v{}.pack", u32::MAX)),
        "a longer version number changed the size, so it is not in the fixed part"
    );
}

/// A pack in format `5` is refused by name, before a byte of it is believed.
///
/// `5` is the previous format: the same run of records for a payload, but with
/// each record's length repeated in the index beside the length its own header
/// already carries. The two differ in the index alone, and an index whose entries
/// are wider than this build reads them is not something a reader can notice by
/// looking — the offsets it would decode are numbers of the right shape, pointing
/// at the wrong bytes.
///
/// What this pins is *where* the refusal happens. The trailer and the index are
/// read before the header is, because that is what makes opening a pack cheap;
/// the format check then has to come before any field of the header is trusted —
/// the recorded version and the snapshot hash are read after it — or a `5` would
/// be reported with this build's numbers and a `6` field would be read out of a
/// file that has none.
#[tokio::test]
async fn a_pack_in_the_previous_format_is_refused_by_name() {
    let built = build(3, 100_000).await;
    let path = built.dir.path().join("v1.pack");
    built.pack(&path).await;

    // Patched rather than built: a real `5` pack is a file this build cannot
    // produce, and patching the four format bytes is the only honest way to hold
    // one. The rest of the file is a valid `6` pack's, which is the point — the
    // only thing wrong with it is the format it claims.
    let mut bytes = fs::read(&path).unwrap();
    bytes[4..8].copy_from_slice(&5u32.to_le_bytes());
    let at = built.dir.path().join("v5.pack");
    fs::write(&at, &bytes).unwrap();

    let err = Pack::open(&at).unwrap_err();
    let said = err.to_string();
    assert!(
        said.contains("pack format 5") && said.contains("reads 6"),
        "a format 5 pack should be refused naming both formats, got: {said}"
    );
    assert!(
        said.contains("not a pack this build can read"),
        "and refused as unreadable rather than as damaged: {said}"
    );

    // The same file with the format byte put back is a perfectly good pack again,
    // which is what shows the refusal was about the format and nothing else.
    bytes[4..8].copy_from_slice(&6u32.to_le_bytes());
    let at = built.dir.path().join("restored.pack");
    fs::write(&at, &bytes).unwrap();
    assert_eq!(
        Pack::open(&at).unwrap().version(),
        built.version,
        "restoring the format byte should restore a pack that reads back"
    );
}

/// A pack with a damaged index is refused, whichever way the damage shows up.
///
/// The index is the only thing that locates chunks, so a reader that decoded a
/// corrupt one would hand out byte ranges that are internally consistent and point
/// at the wrong bytes. Every way the numbers can be wrong is refused: truncated
/// mid-number, overlong past what a `u64` holds, and running off the end of the
/// index entirely.
#[tokio::test]
async fn a_damaged_index_is_refused() {
    let built = build(4, 150_000).await;
    let path = built.dir.path().join("v1.pack");
    built.pack(&path).await;

    let good = fs::read(&path).unwrap();
    let sections = sections(&good);

    let index_at = sections.index_start;
    let index_len = sections.index_len;
    assert!(index_len > 32, "the index should have room to damage");

    // The first entry's hash is 32 bytes, and its two numbers follow.
    let numbers_at = index_at + 32;
    let numbers_end = index_at + index_len;

    // Nothing but continuation bits, across the index's numbers and no further.
    // Every varint read then runs past what a u64 can hold, which must be refused
    // rather than read as a very large offset.
    //
    // Bounded to the index, because the snapshot and the trailer sit after it:
    // running on to the end of the file would damage those instead and the pack
    // would be refused for a reason that has nothing to do with the index.
    let mut endless = good.clone();
    for byte in endless.iter_mut().take(numbers_end).skip(numbers_at) {
        *byte = 0x80;
    }
    let path = built.dir.path().join("endless.pack");
    fs::write(&path, &endless).unwrap();
    assert!(
        Pack::open(&path).is_err(),
        "an index number that never terminates was accepted"
    );

    // A continuation bit on the last byte of the tenth, where only the final bit of
    // a u64 is left to contribute: a value one past the type.
    let mut overlong = good.clone();
    for byte in overlong.iter_mut().take(numbers_at + 9).skip(numbers_at) {
        *byte = 0xff;
    }
    overlong[numbers_at + 9] = 0x02;
    let path = built.dir.path().join("overlong.pack");
    fs::write(&path, &overlong).unwrap();
    assert!(
        Pack::open(&path).is_err(),
        "an index number too large for a u64 was accepted"
    );

    // An index region that stops partway into its last number. Shortening the
    // length in the trailer is what does this: the file keeps its magic, so the
    // index is read, and it runs out in the middle of a value. Shortening the file
    // instead would be caught by the magic, which is the other test.
    let mut cut_number = good.clone();
    let at = sections.file_len - 20;
    cut_number[at..at + 8].copy_from_slice(&u64::try_from(index_len - 1).unwrap().to_le_bytes());
    let path = built.dir.path().join("cut-number.pack");
    fs::write(&path, &cut_number).unwrap();
    assert!(
        Pack::open(&path).is_err(),
        "an index ending mid-number was accepted"
    );

    // An index that stops partway into an entry's hash.
    let mut cut = good.clone();
    cut.truncate(index_at + 40);
    let path = built.dir.path().join("cut.pack");
    fs::write(&path, &cut).unwrap();
    assert!(
        Pack::open(&path).is_err(),
        "an index that ends mid-entry was accepted"
    );
}

/// A chunk the index places outside the payload is refused when the pack opens.
///
/// The header check bounds the *payload*; nothing bounded where each chunk sits
/// within it. An offset that reaches past the end is a number a client is told to
/// add to the payload's start — `needed` and `chunks_covering` both do exactly
/// that — so accepting it means the arithmetic that consumes a pack can hand a
/// client a range outside the file rather than failing.
///
/// This is the "starts inside the payload" half of what the index claims. The
/// other half — that no record starts before the one before it has ended — is
/// `records_that_overlap_are_refused`, which builds the index to say so; the two
/// fixtures here set the offset past the end or past what the type can add, which
/// is a different claim again.
#[tokio::test]
async fn a_chunk_outside_the_payload_is_refused() {
    let built = build(4, 150_000).await;
    let path = built.dir.path().join("v1.pack");
    built.pack(&path).await;

    let good = fs::read(&path).unwrap();
    let sections = sections(&good);
    let index_at = sections.index_start;
    let index_len = sections.index_len;
    let index = &good[index_at..index_at + index_len];
    let entries = index_entries(index);
    assert!(entries.len() >= 2, "the fixture should have a second entry");

    // The first entry's two numbers, and the width they occupy together. Filling
    // exactly those bytes with continuation-then-terminator gives an offset far
    // past the end of the payload, without moving the entry after it.
    let first = entries[0];
    let numbers_at = first + 32;
    let numbers_len = entries[1] - numbers_at;
    assert!(
        numbers_len >= 2,
        "the fixture's first entry has two varints"
    );

    let mut far = good.clone();
    for byte in far
        .iter_mut()
        .skip(index_at + numbers_at)
        .take(numbers_len - 1)
    {
        *byte = 0xff;
    }
    far[index_at + numbers_at + numbers_len - 1] = 0x01;
    let path = built.dir.path().join("far-outside.pack");
    fs::write(&path, &far).unwrap();
    let err = Pack::open(&path).unwrap_err();
    assert!(
        err.to_string().contains("record header occupies"),
        "a chunk placed past the payload was accepted: {err}"
    );

    // The same bytes filled to the width of both numbers, so the offset is a
    // number near the top of the type: adding the payload's start to it is
    // arithmetic on a claim, and this is the case that has to be refused before
    // anything does that addition.
    let mut wrapping = good.clone();
    for byte in wrapping
        .iter_mut()
        .skip(index_at + numbers_at)
        .take(numbers_len)
    {
        *byte = 0xff;
    }
    // The last byte keeps only the single bit a ten-byte varint has left, which
    // is what stops the value overflowing the type and keeps it in range.
    wrapping[index_at + numbers_at + numbers_len - 1] = 0x01;
    let path = built.dir.path().join("wrapping.pack");
    fs::write(&path, &wrapping).unwrap();
    assert!(
        Pack::open(&path).is_err(),
        "a chunk whose offset cannot be added to the payload's start was accepted"
    );
}

/// Records that do not tile the payload are refused when the pack opens.
///
/// The index carries no record length, so a record's extent is "where the next
/// one begins" — which is a range only if the records do not overlap. Everything
/// that answers with bytes reads it that way: `needed` turns a chunk into a byte
/// range, `chunks_covering` asks the same question, and `get` bounds a record's
/// header by its own span. So an index naming one record inside the one before it
/// does not produce a refused read; it produces ranges that overlap, a header
/// read out of a neighbour, and a client with no way to tell.
///
/// What is built here is an index that says a record starts one byte into the one
/// before it. The offset is rewritten **in place**, which needs the two entries
/// to agree on the width of that one number — a wider rewrite would move every
/// entry after it, and the pack would then be refused for a truncation rather
/// than for the overlap. Nothing else in the file changes, which is what makes
/// the refusal the one under test.
#[tokio::test]
async fn records_that_overlap_are_refused() {
    let built = build(4, 150_000).await;
    let path = built.dir.path().join("v1.pack");
    built.pack(&path).await;

    let good = fs::read(&path).unwrap();
    let sections = sections(&good);
    let index = &good[sections.index_start..sections.index_start + sections.index_len];

    // The entry whose offset is rewritten, and the one before it: a pair whose
    // offsets are written the same width, and where this one's plus one is still
    // that width.
    let pairs: Vec<(usize, usize)> = index_entries(index)
        .windows(2)
        .map(|pair| (pair[0], pair[1]))
        .collect();

    let chosen = pairs.iter().find(|(before, after)| {
        decode_varint(index, after + 32) > decode_varint(index, before + 32)
            && varint_len(index, after + 32) == varint_len(index, before + 32)
            && lepatch::varint::len(decode_varint(index, before + 32).saturating_add(1))
                == varint_len(index, before + 32)
    });

    let Some((before, after)) = chosen.copied() else {
        panic!("the fixture should hold two adjacent entries whose offsets are the same width")
    };

    let at = sections.index_start + after + 32;
    let width = varint_len(index, after + 32);
    let overlapping = decode_varint(index, before + 32).saturating_add(1);

    let mut bytes = good.clone();
    let mut written = Vec::new();
    lepatch::varint::write(&mut written, overlapping);
    assert_eq!(
        written.len(),
        width,
        "sanity: the rewritten offset is the width the entry already used"
    );
    bytes[at..at + width].copy_from_slice(&written);

    let path = built.dir.path().join("overlapping.pack");
    fs::write(&path, &bytes).unwrap();
    let error = Pack::open(&path).unwrap_err();

    assert!(
        error.to_string().contains("do not tile"),
        "an index naming a record inside the one before it was accepted: {error}"
    );
}

/// An index naming the same chunk twice is refused.
///
/// `locate` answers with the first match, so a repeated hash means a client that
/// already holds the chunk is told to fetch a different one at a different
/// offset. `decode_index` has always claimed to catch this.
#[tokio::test]
async fn a_chunk_listed_twice_is_refused() {
    let built = build(4, 150_000).await;
    let path = built.dir.path().join("v1.pack");
    built.pack(&path).await;

    let good = fs::read(&path).unwrap();
    let sections = sections(&good);
    let index_at = sections.index_start;
    let index_len = sections.index_len;
    let entries = index_entries(&good[index_at..index_at + index_len]);
    assert!(entries.len() >= 2, "the fixture should have a second entry");

    // Copy the first entry's hash over the second's, leaving both entries'
    // numbers alone — so the index still decodes and still describes a payload
    // of the right size, and the only thing wrong with it is the repetition.
    let mut doubled = good.clone();
    let first_hash: [u8; 32] = good[index_at..index_at + 32].try_into().unwrap();
    let second = index_at + entries[1];
    doubled[second..second + 32].copy_from_slice(&first_hash);

    let path = built.dir.path().join("doubled.pack");
    fs::write(&path, &doubled).unwrap();
    let err = Pack::open(&path).unwrap_err();
    assert!(
        err.to_string().contains("twice"),
        "an index naming one chunk twice was accepted: {err}"
    );
}

/// A `Range` a client built with its ends the wrong way round covers nothing.
///
/// Both fields are public, and the type is part of the client protocol, so the
/// inverted value is one a caller can produce. It used to panic in `len` under a
/// debug assertion and wrap to a near-`u64::MAX` total in release.
#[test]
fn an_inverted_range_covers_nothing() {
    let inverted = Range { start: 10, end: 0 };
    assert_eq!(inverted.len(), 0, "an inverted range covers no bytes");
    assert!(
        !inverted.is_empty(),
        "an inverted range is not a well-formed range, and saying so is what the \
         is_empty pair is for"
    );
}

/// A file with a valid trailer but a damaged header is refused.
///
/// The trailer is read first, so a file whose *header* has been damaged is the case
/// that gets past the cheap check and has to be caught by the second one. Without
/// it, a pack whose payload was overwritten by something else would open cleanly.
#[tokio::test]
async fn a_damaged_header_is_refused() {
    let built = build(3, 120_000).await;
    let path = built.dir.path().join("v1.pack");
    built.pack(&path).await;

    let good = fs::read(&path).unwrap();

    let mut bytes = good.clone();
    bytes[..4].copy_from_slice(b"XXXX");
    let path = built.dir.path().join("bad-magic.pack");
    fs::write(&path, &bytes).unwrap();

    let err = Pack::open(&path).unwrap_err();
    assert!(
        err.to_string().contains("does not begin with"),
        "a pack with a damaged header magic was accepted: {err}"
    );
}

/// A file that is not a pack is refused.
#[test]
fn a_file_that_is_not_a_pack_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("not.pack");

    fs::write(&path, b"just some bytes, honestly").unwrap();
    assert!(Pack::open(&path).is_err());

    // And a pack-sized file with the wrong magic.
    let mut fake = vec![0u8; 4096];
    fake[..4].copy_from_slice(b"NOPE");
    let end = fake.len() - 4;
    fake[end..].copy_from_slice(b"NOPE");
    fs::write(&path, &fake).unwrap();
    assert!(Pack::open(&path).is_err());

    // And an empty file.
    fs::write(&path, b"").unwrap();
    assert!(Pack::open(&path).is_err());
}

/// A version with no file content still has a pack, and it still opens.
///
/// The counts are two one-byte varints and the payload is nothing, so only two
/// bytes lie between the header's fixed part and the trailer. A reader that asks
/// for a fixed 20 there gets a read error on a pack this crate just wrote.
#[tokio::test]
async fn a_pack_of_an_empty_version_opens() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    let blob = dir.path().join("store");
    common::run_backup(&source, &blob).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let plan = plan_of(&blob).await;
    assert!(plan.is_empty(), "an empty tree should plan to no chunks");

    let path = dir.path().join("v1.pack");
    Pack::write(&store, &plan, &snapshot_of(&blob).await, 1, &path)
        .await
        .unwrap();

    let pack = Pack::open(&path).expect("a pack this crate wrote should open");
    assert!(pack.is_empty());
    assert_eq!(pack.payload_len(), 0);
    assert_eq!(pack.len(), 0);
}

/// An index that names nothing over a payload that is not empty is refused.
///
/// The one case the geometry check has that is not about an offset: a pack whose
/// count and index agree — both zero — and whose payload length says there are
/// records in it anyway. Nothing walks that payload, because a reader locates
/// chunks through the index and the index says there are none, so the file would
/// open as an empty version while carrying bytes this crate wrote and cannot
/// account for. An empty *plan* is fine and is written by this crate; this is
/// that file with its count and index taken away.
#[tokio::test]
async fn an_empty_index_over_a_payload_is_refused() {
    /// The header's chunk count: the first of the two varints after the magic, the
    /// format, the version, the snapshot hash and the delta flag.
    const COUNT_AT: usize = 4 + 4 + 4 + 32 + 1;

    let built = build(3, 100_000).await;
    let path = built.dir.path().join("v1.pack");
    built.pack(&path).await;

    let mut bytes = fs::read(&path).unwrap();
    let sections = sections(&bytes);

    // The header's chunk count, and the index the trailer points at.
    let count_len = varint_len(&bytes, COUNT_AT);
    assert!(
        count_len == 1,
        "the fixture's chunk count is written in one byte"
    );

    bytes[COUNT_AT] = 0;

    // The index goes out of the file as well as out of the trailer, and from the
    // middle rather than the end: the trailer's two lengths are how `open` finds
    // the index and the snapshot, so a trailer claiming no index while the bytes
    // were still there would be refused first for a payload that does not reach
    // its own snapshot — a different question, and one asked elsewhere.
    let start = sections.index_start;
    bytes.drain(start..start + sections.index_len);
    let trailer = bytes.len() - 20;
    bytes[trailer..trailer + 8].copy_from_slice(&0u64.to_le_bytes());

    let path = built.dir.path().join("no-chunks.pack");
    fs::write(&path, &bytes).unwrap();
    let error = Pack::open(&path).unwrap_err();

    assert!(
        error.to_string().contains("names no chunks"),
        "a payload described by an index that names nothing was accepted: {error}"
    );
}

/// A pack carries the version it was built from, and names it correctly.
///
/// This is the property that makes a pack self-contained. Without the snapshot in
/// the file, a client can locate every chunk in it and cannot put one of them
/// anywhere — the file table is what says which slice of which chunk lands at
/// which offset in which file, and it is not derivable from the payload.
#[tokio::test]
async fn a_pack_carries_the_version_it_was_built_from() {
    let built = build(3, 100_000).await;
    let expected = snapshot_of(&built.blob).await;
    let pack = built.pack(&built.dir.path().join("v1.pack")).await;

    assert_eq!(pack.snapshot().unwrap(), expected);

    // The header's hash is over exactly the bytes that follow the payloads, so a
    // client with its own copy of the snapshot can confirm the two go together
    // without either having to be decoded.
    assert_eq!(pack.snapshot_id(), snapshot_id(&built.blob).await);

    let reopened = Pack::open(pack.path()).unwrap();
    assert_eq!(reopened.snapshot().unwrap(), expected);
}

/// The file table is checked against the header's hash before it is parsed.
///
/// A pack is a file that was downloaded, and a damaged snapshot section would
/// otherwise decode into a file table describing a different tree — one that
/// writes real bytes to the wrong paths. Refused instead.
#[tokio::test]
async fn a_pack_refuses_a_snapshot_that_is_not_the_one_it_names() {
    let built = build(3, 100_000).await;
    let path = built.dir.path().join("v1.pack");
    let pack = built.pack(&path).await;

    // One byte inside the snapshot section. The index is left alone, so this
    // isolates the hash check from everything `open` already refuses.
    let mut damaged = fs::read(&path).unwrap();
    let at = usize::try_from(pack.snapshot_start()).unwrap() + 16;
    damaged[at] ^= 0x01;
    fs::write(&path, &damaged).unwrap();

    // The geometry is still sound, so opening it succeeds — and that is the point:
    // the damage is invisible to a reader that only wants offsets.
    let reopened = Pack::open(&path).unwrap();
    assert_eq!(reopened.len(), pack.len());

    let error = reopened.snapshot().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not hash to the one its header names"),
        "a snapshot section that is not the named one should be refused, got: {error}"
    );
}

/// A pack yields the same chunk bytes a store does, so it can stand in for one.
///
/// This is the property that makes a pack usable as a delivered artifact rather
/// than only as an inspection format: `restore` takes a snapshot and a
/// `StorageGet`, and a pack now satisfies the second. Without it a client would
/// have to reassemble a tree out of ranges itself.
#[tokio::test]
async fn a_pack_yields_the_bytes_a_store_would() {
    let built = build(4, 150_000).await;
    let path = built.dir.path().join("v1.pack");
    let pack = built.pack(&path).await;

    let store = BlobStore::open_for_reading(&built.blob).await.unwrap();

    for planned in built.plan.chunks() {
        let mut from_pack = pack.get(&planned.id).await.unwrap();
        let mut from_store = store.get(&planned.id).await.unwrap();

        let mut a = Vec::new();
        let mut b = Vec::new();
        from_pack.read_to_end(&mut a).await.unwrap();
        from_store.read_to_end(&mut b).await.unwrap();

        assert_eq!(a, b, "chunk {} differs between pack and store", planned.id);
        assert_eq!(
            a.len() as u64,
            pack.locate(&planned.id).unwrap().content_len,
            "and the index says how many bytes of content that chunk is"
        );
    }
}

/// A chunk a pack does not hold is refused, not answered with the wrong bytes.
///
/// The stream is bounded to one chunk, so a caller that read past the end would
/// otherwise be handed the next chunk's data — which is a plausible-looking
/// stream, and the sort of thing that produces a tree nobody can account for.
#[tokio::test]
async fn a_pack_will_not_hand_out_a_chunk_it_does_not_hold() {
    let built = build(3, 100_000).await;
    let pack = built.pack(&built.dir.path().join("v1.pack")).await;

    let absent = lepatch::storage::BlobId::from_bytes([0xABu8; 32]);
    // Matched rather than `unwrap_err`, which would need the stream type to be
    // `Debug` — a boxed trait object with no useful representation either way.
    let Err(error) = pack.get(&absent).await else {
        panic!("a pack handed out a chunk it does not hold");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);

    // And one it does hold reads back as exactly its own bytes: nothing past the
    // end leaks into it.
    let held = built.plan.chunks()[0].id;
    let mut reader = pack.get(&held).await.unwrap();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(
        bytes.len() as u64,
        pack.locate(&held).unwrap().content_len,
        "the stream is bounded to the chunk's content, not to its record"
    );
    assert_eq!(blake3::hash(&bytes), held);
}

/// `chunks_covering` reports what a range actually contains, not what it is near.
///
/// A range that covers part of a chunk is not that chunk, and recording it anyway
/// would leave a client believing it holds data it does not.
#[tokio::test]
async fn a_partial_range_records_nothing() {
    let built = build(4, 100_000).await;
    let pack = built.pack(&built.dir.path().join("v1.pack")).await;

    let first = pack.chunks()[0];
    let start = pack.payload_start() + first.offset;
    let bytes = fs::read(pack.path()).unwrap();
    let whole_len = record_len(&bytes, pack.payload_start(), first.offset);
    let half = lepatch::pack::Range {
        start,
        end: start + whole_len / 2,
    };

    assert!(
        pack.chunks_covering(&[half]).is_empty(),
        "half a chunk should not be recorded as the chunk"
    );

    let whole = lepatch::pack::Range {
        start,
        end: start + whole_len,
    };
    assert_eq!(pack.chunks_covering(&[whole]).len(), 1);
}

/// A placement for a chunk a plan does not have to locate.
///
/// These two tests are about set arithmetic on the chunk table, so where the chunks
/// live is noise. Every placement is raw and at offset zero, which is not a real
/// arrangement and never read.
fn placed() -> lepatch::storage::Placement {
    lepatch::storage::Placement {
        segment: 0,
        offset: 0,
        stored_len: 10,
        raw_len: 10,
        codec: lepatch::storage::Codec::Raw,
    }
}

/// A snapshot of one file holding `chunks` back to back, ten bytes each.
///
/// Built with real slices because `Plan::from_snapshot` drops any chunk no slice
/// reaches — a chunk table alone plans to nothing, which is correct (a version
/// listing a chunk it never reads has nothing to read it for) and would have made
/// both tests above vacuous.
fn one_file(chunks: &[lepatch::storage::BlobId]) -> Snapshot {
    let mut snapshot = Snapshot {
        chunks: chunks.to_vec(),
        ..Snapshot::new()
    };

    snapshot.files.push(lepatch::metadata::File {
        path: "one.bin".into(),
        len: chunks.len() as u64 * 10,
        attrs: lepatch::metadata::Attributes::default(),
    });

    for (index, _) in chunks.iter().enumerate() {
        snapshot.file_chunks.push(lepatch::metadata::FileChunk {
            chunk_index: u32::try_from(index).unwrap(),
            file_index: 0,
            chunk_offset: 0,
            file_offset: index as u64 * 10,
            length: 10,
        });
    }

    snapshot
}

/// A snapshot of one ten-byte file per entry, each holding that entry's chunk.
///
/// Distinct chunks are listed once, in the order they are first seen — the same
/// rule a real snapshot's chunk table follows, and the one `Plan::from_snapshot`
/// assumes when it flattens slices to one entry per chunk.
fn tree(files: &[(&str, lepatch::storage::BlobId)]) -> Snapshot {
    let mut snapshot = Snapshot::new();
    let mut chunks: Vec<lepatch::storage::BlobId> = Vec::new();

    for (index, (path, id)) in files.iter().enumerate() {
        snapshot.files.push(File {
            path: (*path).into(),
            len: 10,
            attrs: Attributes::default(),
        });

        let chunk_index = if let Some(seen) = chunks.iter().position(|held| held == id) {
            seen
        } else {
            chunks.push(*id);
            chunks.len() - 1
        };

        snapshot.file_chunks.push(FileChunk {
            chunk_index: u32::try_from(chunk_index).unwrap(),
            file_index: u32::try_from(index).unwrap(),
            chunk_offset: 0,
            file_offset: 0,
            length: 10,
        });
    }

    snapshot.chunks = chunks;
    snapshot
}

/// A delta plan is what the target places that the base does not already place.
///
/// The whole of `Plan::since`, apart from any file. Determinism is the part worth
/// pinning: a delta that reordered itself between runs would not be the same bytes
/// twice, and a pack's content hash is that property.
#[test]
fn a_delta_plan_is_what_the_base_does_not_already_place() {
    let a = blake3::hash(b"a");
    let b = blake3::hash(b"b");
    let c = blake3::hash(b"c");

    let base = tree(&[("x.bin", a), ("y.bin", b)]);
    let target = tree(&[("x.bin", a), ("y.bin", b), ("z.bin", c)]);

    let store = |_: &lepatch::storage::BlobId| Ok::<_, std::io::Error>(placed());
    let full = Plan::from_snapshot(&target, store).unwrap();
    assert_eq!(full.len(), 3, "the whole version needs all three");

    let delta = full.since(&target, &base);

    assert_eq!(delta.len(), 1, "only c is new to a holder of a and b");
    assert_eq!(delta.chunks()[0].id, c);

    // From nothing, everything.
    assert_eq!(full.since(&target, &Snapshot::new()).len(), 3);
}

/// A chunk the base holds in one file is still sent for another.
///
/// The bug a chunk-set subtraction has and a placement comparison does not. The same
/// bytes appear in two files; the base has one of them, so the delta must carry the
/// chunk — or the second file is never written and the apply reports success on a
/// tree missing a file.
///
/// Not a contrived shape: a tree of identical files, or a new file whose content
/// happens to repeat something already there, both produce it. It is how this was
/// found.
#[test]
fn a_chunk_the_base_holds_in_one_place_is_still_sent_for_another() {
    let shared = blake3::hash(b"same bytes in two files");

    let base = tree(&[("old.bin", shared)]);
    let target = tree(&[("old.bin", shared), ("new.bin", shared)]);

    let store = |_: &lepatch::storage::BlobId| Ok::<_, std::io::Error>(placed());
    let full = Plan::from_snapshot(&target, store).unwrap();
    assert_eq!(
        full.len(),
        1,
        "both files hold the one chunk, so the whole version needs one chunk"
    );

    let delta = full.since(&target, &base);

    assert_eq!(
        delta.len(),
        1,
        "the base places it in old.bin but not new.bin, so it has to be sent"
    );
}

/// A chunk the base places identically in every place it is needed is omitted.
///
/// The other half, and the case the omission is sound for: if the base already has
/// these exact bytes at these exact offsets, the directory is right there.
#[test]
fn a_chunk_the_base_places_identically_is_left_out() {
    let shared = blake3::hash(b"same bytes in two files");
    let snapshot = tree(&[("a.bin", shared), ("b.bin", shared)]);

    let store = |_: &lepatch::storage::BlobId| Ok::<_, std::io::Error>(placed());
    let full = Plan::from_snapshot(&snapshot, store).unwrap();

    assert!(
        full.since(&snapshot, &snapshot).is_empty(),
        "a version against itself needs nothing sent"
    );
}

/// A delta of a tree with nothing in common is the whole thing.
///
/// Not an optimisation to detect: a delta against an unrelated base *is* the whole
/// version, and saying so is more useful than refusing.
#[test]
fn a_delta_from_an_unrelated_base_is_the_whole_version() {
    let x = blake3::hash(b"x");
    let y = blake3::hash(b"y");
    let z = blake3::hash(b"z");

    let store = |_: &lepatch::storage::BlobId| Ok::<_, std::io::Error>(placed());
    let target = one_file(&[x, y]);
    let full = Plan::from_snapshot(&target, store).unwrap();
    let unrelated = Snapshot {
        chunks: vec![z],
        ..Snapshot::new()
    };

    assert_eq!(full.since(&target, &unrelated).len(), 2);
    assert!(!full.since(&target, &unrelated).is_empty());
}

/// A delta pack is a pack of fewer chunks, and applies to its holder.
///
/// The end-to-end claim: `--from 7` writes something smaller that a client on
/// version 7 can actually use, and the result is the same tree either way. The
/// bytes come from two different packs, and nothing downstream should be able to
/// tell.
#[tokio::test]
async fn a_delta_pack_carries_a_client_up_from_the_version_it_names() {
    let built = build(6, 200_000).await;
    let source = built.dir.path().join("source");
    let client = built.dir.path().join("client");

    // The base pack is written *before* the update, because `snapshot_of` reads the
    // newest version and after a later backup that would be the other one.
    let first = BlobStore::open_for_reading(&built.blob).await.unwrap();
    let v1 = snapshot_of(&built.blob).await;
    let v1_full = Plan::from_snapshot(&v1, |id| first.locate(id)).unwrap();
    let v1_pack = Pack::write(&first, &v1_full, &v1, 1, &built.dir.path().join("v1.pack"))
        .await
        .unwrap();

    // The client takes version 1, which is what leaves the record the delta will
    // be checked against.
    lepatch::command::apply(&client, &v1_pack, lepatch::command::ApplyOptions::default())
        .await
        .unwrap();

    // Change one file and take version 2.
    fs::write(source.join("f2.bin"), common::untagged_stream(99, 260_000)).unwrap();
    common::run_backup(&source, &built.blob).await;

    let store = BlobStore::open_for_reading(&built.blob).await.unwrap();
    let v2 = snapshot_of(&built.blob).await;
    let full = Plan::from_snapshot(&v2, |id| store.locate(id)).unwrap();
    let delta = full.since(&v2, &v1);

    let at = built.dir.path().join("delta.pack");
    let packed = Pack::write_delta(&store, &delta, &v2, v1_pack.snapshot_id(), 2, &at)
        .await
        .unwrap();

    assert_eq!(
        packed.len(),
        delta.len(),
        "the pack should hold exactly the delta's chunks"
    );
    assert!(
        packed.payload_len() < full.total_bytes(),
        "and be smaller than the whole version: {} of {}",
        packed.payload_len(),
        full.total_bytes()
    );
    assert!(
        delta.len() < full.len(),
        "one rewritten file out of six should not need every chunk"
    );

    // It names the version it assumes, which is the only thing that makes it safe to
    // omit the rest.
    assert_eq!(
        packed.base(),
        Some(v1_pack.snapshot_id()),
        "a delta must record the version whose chunks it left out"
    );

    // It still carries the whole version's file table, which is what makes it
    // self-contained: a client can lay out every file from this one file.
    assert_eq!(
        packed.snapshot().unwrap().chunks,
        v2.chunks,
        "the file table must be the whole version, not just the delta"
    );

    let applied =
        lepatch::command::apply(&client, &packed, lepatch::command::ApplyOptions::default())
            .await
            .unwrap();

    assert!(
        applied.written > 0,
        "the delta should have supplied the changed file's chunks"
    );
    common::assert_same_tree(&source, &client);
}

/// A delta pack will not apply to a client that is not on the version it names.
///
/// The failure the whole feature rests on, stated as a test. A delta omits the
/// chunks its assumed holder has, and nothing in it can tell whether they are there
/// — the bytes that would prove it are the bytes it left out. So a client on a
/// different version must be **refused**, before anything is written, rather than
/// having the delta applied to whatever it happens to hold.
#[tokio::test]
async fn a_delta_pack_refuses_a_client_that_is_not_on_its_base() {
    let built = build(5, 200_000).await;
    let source = built.dir.path().join("source");
    let client = built.dir.path().join("client");

    let first = BlobStore::open_for_reading(&built.blob).await.unwrap();
    let v1 = snapshot_of(&built.blob).await;
    let v1_pack = Pack::write(
        &first,
        &Plan::from_snapshot(&v1, |id| first.locate(id)).unwrap(),
        &v1,
        1,
        &built.dir.path().join("v1.pack"),
    )
    .await
    .unwrap();

    // Move the client to version 2 — one update past the delta's base.
    fs::write(source.join("f0.bin"), common::untagged_stream(11, 240_000)).unwrap();
    common::run_backup(&source, &built.blob).await;
    let second = BlobStore::open_for_reading(&built.blob).await.unwrap();
    let v2 = snapshot_of(&built.blob).await;
    let v2_pack = Pack::write(
        &second,
        &Plan::from_snapshot(&v2, |id| second.locate(id)).unwrap(),
        &v2,
        2,
        &built.dir.path().join("v2.pack"),
    )
    .await
    .unwrap();
    lepatch::command::apply(&client, &v2_pack, lepatch::command::ApplyOptions::default())
        .await
        .unwrap();

    // What version 2's version of the file version 3 changes looks like, so the
    // refusal can be checked to have left it alone.
    let before = fs::read(client.join("f1.bin")).unwrap();

    // Version 3, and a delta for a client on version 1.
    fs::write(source.join("f1.bin"), common::untagged_stream(12, 250_000)).unwrap();
    common::run_backup(&source, &built.blob).await;
    let third = BlobStore::open_for_reading(&built.blob).await.unwrap();
    let v3 = snapshot_of(&built.blob).await;
    let full = Plan::from_snapshot(&v3, |id| third.locate(id)).unwrap();
    let delta = full.since(&v2, &v1);
    assert!(delta.len() < full.len(), "the delta should be smaller");

    let packed = Pack::write_delta(
        &third,
        &delta,
        &v3,
        v1_pack.snapshot_id(),
        3,
        &built.dir.path().join("v3-from-v1.pack"),
    )
    .await
    .unwrap();

    let result =
        lepatch::command::apply(&client, &packed, lepatch::command::ApplyOptions::default()).await;

    let error = match result {
        Err(e) => e,
        Ok(applied) => panic!(
            "a delta pack applied to a client one version past its base, writing \
             {} chunks and reporting success — the tree is now neither version",
            applied.written
        ),
    };

    assert!(
        error.to_string().contains("is a delta"),
        "the refusal should say what kind of pack this is, got: {error}"
    );
    assert!(
        error.to_string().contains("apply the full pack instead"),
        "and what to do instead, got: {error}"
    );

    // And the tree is untouched: the refusal came before the first write, so the
    // client is still exactly version 2 and nothing partial was left behind.
    assert_eq!(
        fs::read(client.join("f1.bin")).unwrap(),
        before,
        "the file version 3 changes must not have been touched at all"
    );
    assert_eq!(
        fs::read(client.join("f0.bin")).unwrap(),
        fs::read(source.join("f0.bin")).unwrap(),
        "and the rest of the tree should still be version 2"
    );
}

/// A delta needs the base, and a full pack does not.
///
/// The two are told apart by the header rather than by payload length, because a
/// delta's payload is short for the ordinary reason that it holds fewer chunks — a
/// small full pack is not a delta, and a delta is not a small full pack.
#[tokio::test]
async fn a_full_pack_names_no_base_and_a_delta_names_one() {
    let built = build(4, 150_000).await;
    let store = BlobStore::open_for_reading(&built.blob).await.unwrap();
    let snapshot = snapshot_of(&built.blob).await;
    let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap();

    let full = Pack::write(
        &store,
        &plan,
        &snapshot,
        1,
        &built.dir.path().join("full.pack"),
    )
    .await
    .unwrap();
    assert_eq!(full.base(), None, "a full pack assumes nothing");

    let some = common::id_of(&[42u8; 32]);
    let delta = Pack::write_delta(
        &store,
        &plan,
        &snapshot,
        some,
        1,
        &built.dir.path().join("delta.pack"),
    )
    .await
    .unwrap();
    assert_eq!(
        delta.base(),
        Some(some),
        "and a delta names what it assumes"
    );
}

/// A delta of a delta from the same base is the same delta.
#[tokio::test]
async fn a_delta_is_a_function_of_the_two_versions() {
    let built = build(4, 150_000).await;
    let source = built.dir.path().join("source");
    let v1 = snapshot_of(&built.blob).await;

    fs::write(source.join("f1.bin"), common::untagged_stream(31, 190_000)).unwrap();
    common::run_backup(&source, &built.blob).await;

    let store = BlobStore::open_for_reading(&built.blob).await.unwrap();
    let v2 = snapshot_of(&built.blob).await;
    let full = Plan::from_snapshot(&v2, |id| store.locate(id)).unwrap();

    let first = built.dir.path().join("a.pack");
    let second = built.dir.path().join("b.pack");

    for at in [&first, &second] {
        let delta = full.since(&v2, &v1);
        Pack::write_delta(&store, &delta, &v2, v2.chunks[0], 2, at)
            .await
            .unwrap();
    }

    assert_eq!(
        hash_file(&first).await.unwrap(),
        hash_file(&second).await.unwrap(),
        "the same two versions must delta to the same bytes, or a client cannot \
         verify what it was sent"
    );
}

/// The layout, written out by hand rather than read back through the pack.
///
/// Every other test here asks a `Pack` where things are, so a change to the
/// format would move the writer and the reader together and nothing would fail.
/// The offsets below are therefore arithmetic on the header's field widths,
/// stated here rather than imported from the module that writes them — and the
/// first record's magic is read at the byte that arithmetic puts it at, which is
/// the one thing in a pack that cannot be arranged by accident.
///
/// What would falsify it: a header field moved, a count that stopped being a
/// varint, the payload no longer starting where the two counts end, or a payload
/// that is not records — the last of which would show up here as a first record
/// without a record's magic in it.
#[tokio::test]
async fn the_layout_is_the_one_this_test_writes_out() {
    let built = build(3, 120_000).await;
    let path = built.dir.path().join("v1.pack");
    let pack = built.pack(&path).await;
    let bytes = fs::read(&path).unwrap();

    // magic (4) | format version (4) | version (4) | snapshot hash (32) |
    // base flag (1), then the two counts as varints.
    assert_eq!(&bytes[..4], b"LPK1", "a pack begins with its magic");
    assert_eq!(
        &bytes[4..8],
        &6u32.to_le_bytes(),
        "and names the format this build writes"
    );
    assert_eq!(
        &bytes[8..12],
        &1u32.to_le_bytes(),
        "and the version of the set it was packed from"
    );
    assert_eq!(
        &bytes[12..44],
        pack.snapshot_id().as_bytes(),
        "and the snapshot it carries, at offset twelve"
    );
    assert_eq!(bytes[44], 0, "and a clear base flag for a full pack");

    let count_at = 45;
    let count_len = varint_len(&bytes, count_at);
    let len_at = count_at + count_len;
    let len_len = varint_len(&bytes, len_at);

    assert_eq!(
        decode_varint(&bytes, count_at),
        pack.len() as u64,
        "the first count is the chunk count"
    );
    assert_eq!(
        decode_varint(&bytes, len_at),
        pack.payload_len(),
        "and the second is the payload, which is now records and not content"
    );

    let payload_start = count_at + count_len + len_len;
    assert_eq!(
        payload_start as u64,
        pack.payload_start(),
        "the payload starts where the two counts end"
    );
    assert_eq!(
        &bytes[payload_start..payload_start + 4],
        b"LPB1",
        "and what is at that offset is a store record, not a file's bytes"
    );

    // The trailer: index length, snapshot length, magic — twenty bytes at the end,
    // and the sections are located by subtracting from it rather than by trusting
    // an offset.
    let end = bytes.len();
    let trailer = &bytes[end - 20..];
    assert_eq!(&trailer[16..], b"LPK1", "a pack ends with its magic too");
    let sections = sections(&bytes);
    assert_eq!(sections.file_len, end, "the trailer is twenty bytes");
    assert_eq!(
        sections.index_start + sections.index_len + 20,
        end,
        "and the index runs up to it"
    );
    assert_eq!(
        sections.snapshot_start as u64,
        payload_start as u64 + pack.payload_len(),
        "the snapshot begins where the records end"
    );
}

/// A record whose header does not verify is refused, not read as content.
///
/// The eight-byte check at the end of a store record header is what makes a
/// record self-synchronising, and it has to be load-bearing for a pack too: a pack
/// is a file that was downloaded, and the payload region is now records rather
/// than a file's own bytes, so a header that is wrong would otherwise be read as
/// somebody's content. The codec byte is the byte to flip — everything ahead of it
/// that matters is a length or a hash, and this one is what decides how the rest is
/// interpreted.
///
/// Opening still succeeds, which is the point: the damage is invisible to a reader
/// that only wants offsets, so the refusal has to be where the bytes are read.
///
/// What would falsify it: a `get` that returned the chunk anyway, a refusal that
/// came from somewhere else — the index and the header still agree with each other
/// here — or a message that named a different fault.
#[tokio::test]
async fn a_record_whose_header_does_not_verify_is_refused() {
    let built = build(3, 100_000).await;
    let path = built.dir.path().join("v1.pack");
    let pack = built.pack(&path).await;
    let first = pack.chunks()[0];

    let mut damaged = fs::read(&path).unwrap();
    let at = usize::try_from(pack.payload_start() + first.offset).unwrap();
    damaged[at + 5] ^= 0x01;
    let at_damaged = built.dir.path().join("damaged-header.pack");
    fs::write(&at_damaged, &damaged).unwrap();

    let reopened = Pack::open(&at_damaged).expect(
        "the geometry is undamaged: the header and the index still agree with each other, \
         which is exactly why this has to be caught where the chunk is read",
    );

    let Err(error) = reopened.get(&first.id).await else {
        panic!("a chunk whose record header does not verify was read anyway");
    };
    assert!(
        error
            .to_string()
            .contains("does not begin with a record header"),
        "the refusal should name the record framing, got: {error}"
    );

    // The same pack with the byte put back reads the chunk again, so the refusal
    // was about that byte and nothing else.
    damaged[at + 5] ^= 0x01;
    fs::write(&at_damaged, &damaged).unwrap();
    let restored = Pack::open(&at_damaged).unwrap();
    let mut bytes = Vec::new();
    restored
        .get(&first.id)
        .await
        .unwrap()
        .read_to_end(&mut bytes)
        .await
        .unwrap();
    assert_eq!(blake3::hash(&bytes), first.id);
}

/// A record whose payload byte is flipped is refused, and publishes nothing.
///
/// The header's own check covers the header; a flipped byte in the payload is
/// caught by the chunk's hash instead, and this is where that is enforced for a
/// pack: `put` hashes what it is given against the name the chunk is addressed by,
/// so a merge of a damaged pack fails rather than storing bytes under a hash they
/// are not.
///
/// A *raw* record is the one to damage for this, deliberately: flipping a byte of
/// a frame is caught by the codec instead, which would leave this test passing for
/// the wrong reason — a frame that happens not to decode says nothing about the
/// hash check.
///
/// What would falsify it: a merge that succeeded, or one that failed after
/// publishing a version — so the set is checked for having published nothing.
#[tokio::test]
async fn a_record_whose_payload_byte_is_flipped_is_refused() {
    let built = build(3, 100_000).await;
    let path = built.dir.path().join("v1.pack");
    let pack = built.pack(&path).await;

    let bytes = fs::read(&path).unwrap();
    let raw = pack
        .chunks()
        .iter()
        .find(|chunk| {
            let at = usize::try_from(pack.payload_start() + chunk.offset).unwrap();
            bytes[at + 5] == 0
        })
        .copied();
    let raw = raw.expect("the fixture should hold at least one uncompressed record");

    // The last byte of a record is in its payload: a record is a header and a
    // payload, and the header is at most 66 of the record's bytes.
    let at = usize::try_from(
        pack.payload_start() + raw.offset + record_len(&bytes, pack.payload_start(), raw.offset)
            - 1,
    )
    .unwrap();
    let mut damaged = bytes.clone();
    damaged[at] ^= 0x01;
    let at_damaged = built.dir.path().join("damaged-payload.pack");
    fs::write(&at_damaged, &damaged).unwrap();

    let reopened = Pack::open(&at_damaged).unwrap();
    let received = built.dir.path().join("received");
    let store = BlobStore::open_for_writing(
        &received,
        lepatch::storage::SegmentLimit::default(),
        lepatch::storage::Compression::default(),
    )
    .await
    .unwrap();

    let error = lepatch::command::merge(
        &store,
        &reopened,
        lepatch::command::MergeCompression::Reencode,
    )
    .await
    .expect_err("a merge of a pack with a flipped payload byte should be refused");
    assert!(
        error.to_string().contains("does not hash to"),
        "a flipped payload byte must be caught by the chunk hash, not by the codec, got: {error}"
    );
    assert!(
        store.latest().is_none(),
        "and a refused merge must not have published a version"
    );
}

/// The verbatim write path refuses a damaged record too, and publishes nothing.
///
/// The test above pins this for the re-encoding path, where the chunk reaches the
/// store as content. `keep` is the path where it is *not* obvious: the bytes go in
/// as the pack carries them, so the store is handed a frame and a pair of lengths
/// rather than a hash of the thing it can see — and the tempting implementation is
/// to take the pack's word for it, which is exactly what would let a damaged or
/// hostile pack store bytes under a name they are not.
///
/// So the decode that looks like waste is the property. This test is what says so:
/// a byte flipped in a *raw* record's payload leaves the codec alone and the
/// lengths alone, so nothing but hashing the decoded content can catch it.
///
/// What would falsify it: a `put_stored` that skips the decode, or that trusts the
/// pack — the merge would succeed, and the set would hold a record under `id`
/// whose bytes are not what `id` names. Publishing a version as well as storing
/// the record is the second half, so the set is checked for having published
/// nothing.
#[tokio::test]
async fn a_flipped_payload_byte_is_refused_on_the_verbatim_path_too() {
    let built = build(3, 100_000).await;
    let path = built.dir.path().join("v1.pack");
    let pack = built.pack(&path).await;

    let bytes = fs::read(&path).unwrap();
    let raw = pack
        .chunks()
        .iter()
        .find(|chunk| {
            let at = usize::try_from(pack.payload_start() + chunk.offset).unwrap();
            bytes[at + 5] == 0
        })
        .copied();
    let raw = raw.expect("the fixture should hold at least one uncompressed record");

    let at = usize::try_from(
        pack.payload_start() + raw.offset + record_len(&bytes, pack.payload_start(), raw.offset)
            - 1,
    )
    .unwrap();
    let mut damaged = bytes.clone();
    damaged[at] ^= 0x01;
    let at_damaged = built.dir.path().join("damaged-keep.pack");
    fs::write(&at_damaged, &damaged).unwrap();

    let reopened = Pack::open(&at_damaged).unwrap();
    let received = built.dir.path().join("received-keep");
    let store = BlobStore::open_for_writing(
        &received,
        lepatch::storage::SegmentLimit::default(),
        lepatch::storage::Compression::default(),
    )
    .await
    .unwrap();

    let error =
        lepatch::command::merge(&store, &reopened, lepatch::command::MergeCompression::Keep)
            .await
            .expect_err("`keep` must not be a promise to believe the pack");
    assert!(
        error.to_string().contains("does not hash to"),
        "the chunk hash, not the codec, must be what catches a flipped byte: {error}"
    );
    assert!(
        store.latest().is_none(),
        "and a refused merge must not have published a version"
    );
}

/// A frame no smaller than its content is stored raw, on the verbatim path.
///
/// The size invariant is the store's, not the pack's: a frame is kept only when it
/// is strictly smaller than the content it stands for, so a pack carrying a frame
/// that did not win cannot make the receiving set bigger than the version it
/// merged. Both lengths are in the record, so the check is two integer comparisons
/// — and skipping it would be a way for a merge to *grow* a store, which is the one
/// thing the verbatim path exists not to do.
///
/// The fixture is a record rewritten to name `Zstd` about a payload that is a frame
/// of its content which came out no smaller, which is the only shape that reaches
/// the rule: a record the store wrote would never carry one.
///
/// Two constraints keep the fixture to one splice rather than a rebuilt pack. The
/// victim is the **last** record in the payload, so nothing after it moves; and its
/// replacement is the same width of varint, so the payload length can be patched
/// where it is rather than re-encoded. Both are asserted, so a fixture that stopped
/// satisfying either says so instead of quietly testing something else.
///
/// What would falsify it: a merge that stored the frame as it stood. The tree would
/// still restore — the frame is a genuine one — so the assertion is on the record
/// the store wrote. A test that only restored would pass with this rule deleted.
#[tokio::test]
async fn a_frame_no_smaller_than_its_content_is_stored_raw() {
    let built = build(3, 100_000).await;
    let path = built.dir.path().join("v1.pack");
    let pack = built.pack(&path).await;

    let bytes = fs::read(&path).unwrap();

    // The last record in payload order, and the one the fixture rewrites.
    let victim = pack
        .chunks()
        .iter()
        .max_by_key(|chunk| chunk.offset)
        .copied()
        .expect("the fixture should hold records");
    let (at, header_len, stored_len, raw_len, held) =
        record_claim(&bytes, pack.payload_start(), &victim);
    assert_eq!(
        (held, stored_len, raw_len),
        (0, raw_len, raw_len),
        "sanity: the victim is an uncompressed record, so its payload is the content \
         and can be re-encoded into a frame that loses"
    );

    let mut content = Vec::new();
    pack.get(&victim.id)
        .await
        .unwrap()
        .read_to_end(&mut content)
        .await
        .unwrap();
    assert_eq!(blake3::hash(&content), victim.id, "sanity: the content");

    // A real frame of that content, asserted not to be smaller. Left to chance it
    // would compress — the fixture's data is compressible, which is the point: a
    // frame that won is the ordinary case and would not reach the rule at all.
    let frame = zstd::bulk::compress(&content, 1).unwrap();
    let frame_len = u64::try_from(frame.len()).unwrap();
    assert!(
        frame_len >= raw_len,
        "sanity: this fixture's frame must not be smaller than its content, and is \
         {frame_len} against {raw_len}"
    );
    assert_eq!(
        lepatch::varint::len(frame_len),
        lepatch::varint::len(stored_len),
        "sanity: the two stored lengths must be the same width of varint, or the \
         record header changes width and the splice below moves every byte after it"
    );

    let mut record = record_header(1, &victim.id, frame_len, raw_len);
    record.extend_from_slice(&frame);

    // The header's payload length, grown by the difference between the two records
    // and patched where it already is.
    let count_at = 4 + 4 + 4 + 32 + 1;
    let len_at = count_at + varint_len(&bytes, count_at);
    let old_record_len = u64::try_from(header_len).unwrap() + stored_len;
    let header = grown_header(
        &bytes,
        len_at,
        decode_varint(&bytes, len_at)
            .saturating_add(u64::try_from(record.len()).unwrap())
            .saturating_sub(old_record_len),
    );

    let mut grown = Vec::with_capacity(bytes.len() + record.len());
    grown.extend_from_slice(&header);
    grown.extend_from_slice(&bytes[header.len()..at]);
    grown.extend_from_slice(&record);
    grown.extend_from_slice(&bytes[at + usize::try_from(old_record_len).unwrap()..]);

    let at_grown = built.dir.path().join("grown-frame.pack");
    fs::write(&at_grown, &grown).unwrap();

    // The pack opens and the record still reads back as the chunk it names, so the
    // only thing wrong with it is that its frame did not win.
    let reopened = Pack::open(&at_grown).unwrap();
    let mut read_back = Vec::new();
    reopened
        .get(&victim.id)
        .await
        .unwrap()
        .read_to_end(&mut read_back)
        .await
        .unwrap();
    assert_eq!(
        read_back, content,
        "sanity: a re-encoded record is still the chunk it names"
    );

    let received = built.dir.path().join("received-grown");
    let store = BlobStore::open_for_writing(
        &received,
        lepatch::storage::SegmentLimit::default(),
        lepatch::storage::Compression::default(),
    )
    .await
    .unwrap();

    lepatch::command::merge(&store, &reopened, lepatch::command::MergeCompression::Keep)
        .await
        .expect("a merge carrying a frame that lost is not a failure");

    let placement = store.locate(&victim.id).unwrap();
    assert_eq!(
        placement.codec,
        Codec::Raw,
        "a frame no smaller than its content must be stored raw, and this one was \
         {frame_len} bytes of frame for {raw_len} bytes of content"
    );
    assert_eq!(
        (placement.stored_len, placement.raw_len),
        (placement.raw_len, placement.raw_len),
        "a raw record's two lengths are the same number"
    );
}

/// `keep` carries the pack's own records across; anything else re-encodes them.
///
/// The routing is what the flag is, and the two halves are opposites: `keep` must
/// reproduce the codecs the pack arrived with, and a re-encoding merge must produce
/// whatever the *receiving* set's policy says. Both are checked against the store's
/// own index rather than against a size, because a size can come out the same either
/// way and a codec cannot.
///
/// The receiving set is opened with compression **off** on purpose. That is what
/// makes the two answers opposites rather than two views of the same thing: a
/// re-encoding merge into an uncompressed set stores every chunk raw whatever the
/// pack carried, while `keep` still brings the frames across. A test whose store
/// compressed would see `keep` and `Reencode` agree on every chunk that compresses,
/// and would pass with the routing broken.
///
/// What would falsify it: `keep` storing content (every record raw here), or
/// `Reencode` storing frames (every record compressed here). Either way the flag
/// would do the same thing twice, and the restores would still be correct.
#[tokio::test]
async fn keep_carries_the_packs_records_across_and_a_level_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let source = common::source(dir.path());

    // Half the tree compresses and half cannot, so the pack carries both kinds of
    // record and neither answer can be right "by accident" on all of them.
    for i in 0..3 {
        fs::write(
            source.join(format!("c{i}.bin")),
            common::untagged_stream(u8::try_from(i).unwrap(), 300_000 + i * 40_000),
        )
        .unwrap();
        fs::write(
            source.join(format!("n{i}.bin")),
            common::noisy(120_000 + i * 10_000),
        )
        .unwrap();
    }

    let origin = dir.path().join("origin");
    common::run_backup(&source, &origin).await;
    let pack = Pack::write(
        &BlobStore::open_for_reading(&origin).await.unwrap(),
        &plan_of(&origin).await,
        &snapshot_of(&origin).await,
        1,
        &dir.path().join("v1.pack"),
    )
    .await
    .unwrap();

    // What the pack itself carries, chunk by chunk: the answer `keep` must produce.
    let mut carried = HashMap::new();
    for chunk in &pack {
        let mut bytes = Vec::new();
        pack.get(&chunk.id)
            .await
            .unwrap()
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        let stored = frame_codec(&dir.path().join("v1.pack"), pack.payload_start(), chunk);
        assert_eq!(
            bytes.len() as u64,
            chunk.content_len,
            "sanity: the fixture's chunk is {} bytes of content",
            chunk.content_len
        );
        carried.insert(chunk.id, stored);
    }
    assert!(
        carried.values().any(|codec| *codec == Codec::Zstd),
        "sanity: the pack should carry at least one frame, or nothing is being kept"
    );
    assert!(
        carried.values().any(|codec| *codec == Codec::Raw),
        "sanity: and at least one raw record, or nothing is being re-encoded"
    );

    for (compression, expected_kept) in [
        (lepatch::command::MergeCompression::Keep, true),
        (lepatch::command::MergeCompression::Reencode, false),
    ] {
        let name = format!("received-{expected_kept}");
        let store = BlobStore::open_for_writing(
            &dir.path().join(&name),
            lepatch::storage::SegmentLimit::default(),
            // Off, so a re-encode has somewhere to go: every chunk raw.
            lepatch::storage::Compression::Off,
        )
        .await
        .unwrap();

        lepatch::command::merge(&store, &pack, compression)
            .await
            .unwrap();

        let reader = BlobStore::open_for_reading(&dir.path().join(&name))
            .await
            .unwrap();
        for (id, stored) in &carried {
            let placement = reader.locate(id).unwrap();
            assert_eq!(
                placement.codec,
                if expected_kept { *stored } else { Codec::Raw },
                "with {compression:?}, chunk {id} arrived as {:?} and the pack carried \
                 {stored:?}",
                placement.codec,
            );
        }
    }
}

/// A pack's header, up to and including a payload length of `payload_len`.
///
/// A patch rather than a rebuild, and it can only be a patch because the number it
/// writes has the same width of varint as the one already there: a header is at the
/// front of the file, so a number that changed width would move every byte behind
/// it and the index with it. Asserted rather than assumed, so a fixture that grew
/// past a varint boundary says why instead of producing a pack that will not open.
fn grown_header(bytes: &[u8], len_at: usize, payload_len: u64) -> Vec<u8> {
    let mut encoded = Vec::new();
    lepatch::varint::write(&mut encoded, payload_len);

    assert_eq!(
        encoded.len(),
        varint_len(bytes, len_at),
        "sanity: the payload length must keep its varint width, or this splice would \
         move every offset in the index and the test would be about rebuilding a pack"
    );

    let mut header = bytes[..len_at].to_vec();
    header.extend_from_slice(&encoded);
    header
}

/// The codec byte in the record at `offset`, read out of the pack's own bytes.
///
/// The pack's index says where a chunk is and how much content it has, and neither
/// says how the record stores it — the record's own header does, and it is the same
/// header a reader decodes. Read here rather than through a public accessor because
/// what this test needs is a fact about the *file*, which is what the receiving set
/// has to reproduce.
fn frame_codec(path: &Path, payload_start: u64, chunk: &lepatch::pack::Located) -> Codec {
    let bytes = fs::read(path).unwrap();
    let at = usize::try_from(payload_start + chunk.offset).unwrap();

    match bytes[at + 5] {
        0 => Codec::Raw,
        1 => Codec::Zstd,
        other => panic!("the record at {at} claims codec {other}, which is not one"),
    }
}

/// The three forms a pack can be built in, as a command line would name them.
///
/// The same three answers `merge --compression` gives, at the other end of the
/// transfer, and deliberately the same list: the matrix below is a product of two
/// choices from one vocabulary, and a fixture that listed a fourth case here and a
/// different third case there would be testing two features rather than one.
fn pack_forms() -> [(&'static str, Option<lepatch::storage::Compression>); 3] {
    [
        ("keep", None),
        ("zero", Some(lepatch::storage::Compression::Off)),
        ("level", Some(lepatch::storage::Compression::zstd(9))),
    ]
}

/// The same three answers, as what a merge takes.
///
/// A level is not in [`lepatch::command::MergeCompression`] and cannot be: which
/// frames a merge writes is the *receiving store's* policy, not an argument, since
/// the store's compressor is a type parameter the command does not get to change.
/// So "under a level" here is `Reencode` into a store opened at one, and the two
/// are the same run as far as anything observable is concerned.
fn merge_forms() -> Vec<(
    &'static str,
    lepatch::command::MergeCompression,
    lepatch::storage::Compression,
)> {
    vec![
        (
            "keep",
            lepatch::command::MergeCompression::Keep,
            lepatch::storage::Compression::Off,
        ),
        (
            "zero",
            lepatch::command::MergeCompression::Reencode,
            lepatch::storage::Compression::Off,
        ),
        (
            "level",
            lepatch::command::MergeCompression::Reencode,
            lepatch::storage::Compression::zstd(9),
        ),
    ]
}

/// A source set holding both kinds of record, packed and merged through the matrix.
///
/// The fixture is half compressible and half not, and that is load-bearing rather
/// than decoration: a frame is stored only where it is strictly smaller than its
/// content, so a tree of one kind gives an answer that every codec agrees with by
/// accident and a test over it cannot tell `keep` from `0`. With both, the codecs
/// recorded in the matrix's sets are genuinely different and a routing mistake
/// shows up as a difference rather than as nothing.
fn mixed_source(dir: &Path) -> std::path::PathBuf {
    let source = common::source(dir);

    for i in 0..3 {
        fs::write(
            source.join(format!("c{i}.bin")),
            common::untagged_stream(u8::try_from(i).unwrap(), 200_000 + i * 20_000),
        )
        .unwrap();
        fs::write(
            source.join(format!("n{i}.bin")),
            common::noisy(120_000 + i * 10_000),
        )
        .unwrap();
    }

    source
}

/// Pack a set's latest version under one of the three forms.
async fn packed_with(
    blob: &Path,
    at: &Path,
    compression: Option<lepatch::storage::Compression>,
) -> Pack {
    let store = BlobStore::open_for_reading(blob).await.unwrap();
    let snapshot = snapshot_of(blob).await;
    // sanity: the version resolves at all, so a later failure is about the option.
    assert!(
        !Plan::from_snapshot(&snapshot, |id| store.locate(id))
            .unwrap()
            .is_empty()
    );

    lepatch::command::pack(
        &store,
        &lepatch::command::Version {
            number: 1,
            snapshot,
        },
        |id| store.locate(id),
        None,
        at,
        lepatch::command::PackOptions {
            dry_run: false,
            compression,
        },
    )
    .await
    .unwrap();

    Pack::open(at).unwrap()
}

/// Every pack form restores through every merge form, byte for byte.
///
/// The option chooses an *encoding*, and the only thing it is allowed to change is
/// the bytes in the records. So each of the nine combinations has to restore to
/// exactly what a restore from the source set gives — and each is checked against
/// that same answer rather than against each other, because two sets that were
/// wrong in the same way would otherwise agree.
///
/// Every setting is checked against the *source set's own restore*, taken once
/// before the matrix runs. This is the test that would catch an option honoured in
/// one direction and ignored in the other, which is the failure a flag like this
/// invites: `keep` writing content while claiming to keep frames would still
/// restore, and so would a level that re-read a pack's stored bytes as if they
/// were content.
///
/// **What this cannot see, and what the test below it is for.** Restoring is
/// invariant under the encoding: every form decodes to the same content, so all
/// nine rows here would pass with the routing wired up wrongly — a routing bug that
/// passed a restore-only test, once already, in this same feature. The matrix
/// proves the option is safe. It does not prove the option does anything.
///
/// What would falsify it: any cell restoring differently by a byte, a form that
/// refused to build or merge, or a merge that published no version.
#[tokio::test]
async fn every_pack_form_restores_through_every_merge_form() {
    let dir = tempfile::tempdir().unwrap();
    let source = mixed_source(dir.path());
    let origin = dir.path().join("origin");
    common::run_backup(&source, &origin).await;

    // The answer every cell is measured against: what the set itself restores.
    let expected = dir.path().join("expected");
    common::run_restore_version(&origin, &expected, 1).await;
    assert!(
        !common::describe(&expected).is_empty(),
        "the fixture should have restored something to compare against"
    );

    for (pack_form, pack_compression) in pack_forms() {
        let pack_path = dir.path().join(format!("v-{pack_form}.pack"));
        let pack = packed_with(&origin, &pack_path, pack_compression).await;

        for (merge_form, merge_route, store_policy) in merge_forms() {
            let name = dir.path().join(format!("m-{pack_form}-{merge_form}"));
            let store = BlobStore::open_for_writing(
                &name,
                lepatch::storage::SegmentLimit::default(),
                store_policy,
            )
            .await
            .unwrap();

            lepatch::command::merge(&store, &pack, merge_route)
                .await
                .unwrap_or_else(|e| {
                    panic!("a pack built as {pack_form} must merge as {merge_form}: {e}")
                });

            // Every cell also re-reads every chunk through the merged set and checks
            // it against the hash it is addressed by: the store's own statement that
            // what it holds is what it says it holds, which is the property the
            // verbatim paths keep while skipping the encode.
            let reopened = BlobStore::open_for_reading(&name).await.unwrap();
            let report = lepatch::command::verify(reopened.snapshot(1).unwrap(), reopened)
                .await
                .unwrap_or_else(|e| panic!("{pack_form} into {merge_form} should verify: {e}"));
            assert!(
                report.chunks > 0,
                "{pack_form} into {merge_form} verified nothing, so it checked nothing: \
                 {report:?}"
            );

            let restored = dir.path().join(format!("out-{pack_form}-{merge_form}"));
            common::run_restore_version(&name, &restored, 1).await;
            common::assert_same_tree(&expected, &restored);
        }
    }
}

/// `keep` is not the same as `0`, and the codecs recorded say which is which.
///
/// The matrix above cannot tell the two apart, and that is not a gap in it: every
/// form decodes to the same content, so a restore is invariant under the encoding
/// and a routing bug passes a restore-only test. This is the test that closes that
/// door, and it asserts on **the codecs actually recorded in the merged set** rather
/// than on the restore or on a size.
///
/// Sizes would be nearly as weak: a frame at level 9 of content that compresses is
/// smaller than the same content raw, but "nearly smaller" is a range, and a
/// fixture at one end of it would let a level silently do nothing. A codec is a
/// single bit that cannot come out the same way twice by accident.
///
/// The receiving store is opened with compression **off** on purpose, so the codecs
/// in the merged set are the ones the *pack* chose rather than ones the receiving
/// policy overwrote: with `keep` the pack's own frames arrive, and with `0` every
/// record is raw however well it would compress. A test whose receiving store
/// compressed would see the two agree on every chunk that compresses.
///
/// What would falsify it: `keep` writing content (every record raw), or `0`
/// writing frames (every record compressed). Either way the flag would be doing the
/// same thing twice, and every restore in the matrix above would still be correct.
#[tokio::test]
async fn keep_carries_the_frames_across_and_zero_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let source = mixed_source(dir.path());
    let origin = dir.path().join("origin");
    common::run_backup(&source, &origin).await;

    // What the source set holds, chunk by chunk: the answer `keep` must reproduce.
    let held: HashMap<_, _> = plan_of(&origin)
        .await
        .iter()
        .map(|chunk| (chunk.id, chunk.placement.codec))
        .collect();
    assert!(
        held.values().any(|codec| *codec == Codec::Zstd),
        "sanity: the set should hold at least one frame, or nothing is being kept"
    );
    assert!(
        held.values().any(|codec| *codec == Codec::Raw),
        "sanity: and at least one raw record, or nothing is being re-encoded"
    );

    let mut sizes = Vec::new();
    for (form, compression, expected_frames) in [
        ("keep", None, true),
        ("zero", Some(lepatch::storage::Compression::Off), false),
        ("level", Some(lepatch::storage::Compression::zstd(9)), true),
    ] {
        let pack_path = dir.path().join(format!("v-{form}.pack"));
        let pack = packed_with(&origin, &pack_path, compression).await;
        let name = dir.path().join(format!("received-{form}"));

        // Off, so what is recorded is the pack's choice and not the store's.
        let store = BlobStore::open_for_writing(
            &name,
            lepatch::storage::SegmentLimit::default(),
            lepatch::storage::Compression::Off,
        )
        .await
        .unwrap();
        lepatch::command::merge(&store, &pack, lepatch::command::MergeCompression::Keep)
            .await
            .unwrap();

        let reader = BlobStore::open_for_reading(&name).await.unwrap();
        for (id, held_codec) in &held {
            let placement = reader.locate(id).unwrap();
            if expected_frames && *held_codec == Codec::Zstd {
                assert_eq!(
                    placement.codec,
                    Codec::Zstd,
                    "a pack built as {form} lost the frame the set held for {id}"
                );
            } else {
                assert_eq!(
                    placement.codec,
                    Codec::Raw,
                    "a pack built as {form} framed {id} raw, which {form} must not do"
                );
            }
        }

        // And the size moves with the choice, which is the same claim about the
        // encodings that the codecs make about the records: a set built from a pack
        // of raw content is larger than one built from a pack of the frames the set
        // already held.
        sizes.push((form, common::set_size(&name)));
    }

    let size_of = |form: &str| sizes.iter().find(|(f, _)| *f == form).unwrap().1;
    assert!(
        size_of("zero") > size_of("keep"),
        "a set merged from a pack built as zero should be larger than one from keep: \
         {} against {}",
        size_of("zero"),
        size_of("keep")
    );
    assert!(
        size_of("level") < size_of("zero"),
        "and one from a pack built at a level should be smaller than raw: {} against {}",
        size_of("level"),
        size_of("zero")
    );
}

/// A dry pack under a level reports the figures the real run produces, and writes
/// nothing.
///
/// Two claims, and the second is the one that is easy to lose by adding a parameter
/// to a path that used to take none. The figures: under a level the plan is
/// resolved through the same encoder the writer uses, so `payload` is the size of
/// the records that *would* be written rather than the size of the ones the set
/// holds — which is what makes the plan worth reading. The absence: no file at
/// `--out`, created or overwritten.
///
/// The plan and the run are compared figure for figure, on every number the report
/// carries. A plan that reported the source set's sizes would be reporting about a
/// file that was not written, and the only way to see that is to compare.
///
/// What would falsify it: a plan whose figures differ from the run's, a file at the
/// output path afterwards, or a plan whose payload did not move with the flag.
#[tokio::test]
async fn a_dry_pack_under_a_level_reports_what_the_run_would_write() {
    let dir = tempfile::tempdir().unwrap();
    let source = mixed_source(dir.path());
    let origin = dir.path().join("origin");
    common::run_backup(&source, &origin).await;

    let mut payloads = Vec::new();
    for (form, compression) in pack_forms() {
        let store = BlobStore::open_for_reading(&origin).await.unwrap();
        let snapshot = snapshot_of(&origin).await;
        let version = lepatch::command::Version {
            number: 1,
            snapshot,
        };
        let at = dir.path().join(format!("v-{form}.pack"));

        let plan = lepatch::command::pack(
            &store,
            &version,
            |id| store.locate(id),
            None,
            &at,
            lepatch::command::PackOptions {
                dry_run: true,
                compression,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            plan.bytes, None,
            "a plan is not a file, and should not claim a size on disk"
        );
        assert_eq!(
            plan.content, None,
            "a plan is not a file, and should not claim a content hash"
        );
        assert!(
            !at.exists(),
            "a dry pack as {form} wrote the file it was asked about"
        );

        let run = lepatch::command::pack(
            &store,
            &version,
            |id| store.locate(id),
            None,
            &at,
            lepatch::command::PackOptions {
                dry_run: false,
                compression,
            },
        )
        .await
        .unwrap();

        assert!(
            at.exists(),
            "the real run as {form} should have written its pack"
        );
        assert_eq!(
            plan.chunks, run.chunks,
            "the chunk count must not depend on the encoding"
        );
        assert_eq!(plan.of, run.of, "nor must the version's own count");
        assert_eq!(
            plan.table, run.table,
            "nor the file table, which the option does not touch"
        );
        assert_eq!(plan.base, run.base, "nor the delta's base");
        assert_eq!(
            plan.payload, run.payload,
            "a plan as {form} must report the payload the run writes, not one it \
             did not: the plan said {} and the run wrote {}",
            plan.payload, run.payload,
        );
        assert_eq!(
            run.bytes,
            Some(at.metadata().unwrap().len()),
            "and the run's own figure is the file's"
        );
        assert_eq!(
            run.content,
            Some(lepatch::pack::hash_file(&at).await.unwrap()),
            "and its content hash is the pack's own"
        );

        payloads.push((form, plan.payload));
    }

    // The figure is *meant* to move with the option — that is the resolution this
    // crate made of what `payload` means under a non-default one — so a plan that
    // reported one number for all three would be the bug, not the fix.
    let by = |form: &str| payloads.iter().find(|(f, _)| *f == form).unwrap().1;
    assert!(
        by("zero") > by("keep"),
        "raw records should be larger than the frames the set holds: {} against {}",
        by("zero"),
        by("keep")
    );
}

/// A record claiming more content than the chunker can ever produce is refused
/// before anything is decoded.
///
/// The ceiling is `fastcdc`'s maximum, which is 2^30, and a varint that can hold
/// it is five bytes wide — which is why this builds its pack rather than
/// patching one. Widening a real record's length would shift everything after it
/// by a byte, and each thing that shift touches (the index entry's own length, the
/// trailer's index length, the header's payload total) is separately checked, so a
/// splice that got one of them wrong would be refused for a reason that has
/// nothing to do with the ceiling. This way every width is chosen, and the only
/// thing wrong with the file is the claim.
///
/// What it pins is the **order**, not merely that a merge fails. The frame here
/// is a stand-in rather than a real one, so with the bound in place the refusal
/// names the ceiling, and with it removed the decode is reached and the refusal
/// names the frame instead. A real frame declaring that size would reserve a
/// gigabyte before finding out, and a failed reserve aborts rather than returning
/// an error — which is the reason the bound is here at all.
///
/// It also pins that a refused merge publishes nothing, which the earlier refusal
/// tests already established and this one re-checks on the route that bypasses
/// the reading path — and, now that the reading path reads the same record
/// through the same code, that *it* refuses the same fixture for the same reason.
/// The bound used to be on one route and not the other, and the commit that added
/// it to the second said neither had a test, which is how one of the two routes
/// came to be the only thing this fixture ever exercised.
#[tokio::test]
async fn a_record_claiming_more_content_than_a_chunk_can_hold_is_refused_first() {
    const CEILING: u64 = (1 << 30) + 1; // one past it: the bound is "more than"
    const FRAME: &[u8] = &[0x5au8; 40];

    let id = lepatch::storage::BlobId::from_bytes([0xABu8; 32]);
    let table = one_file(&[id]).to_bytes().unwrap();

    // The record: the store's own framing, with a claim no chunker makes.
    let mut record = Vec::new();
    record.extend_from_slice(b"LPB1");
    record.push(0); // a blob record
    record.push(1); // stored as a zstd frame
    push_varint(&mut record, u64::try_from(FRAME.len()).unwrap());
    push_varint(&mut record, CEILING); // ...of more than a gigabyte of content
    record.extend_from_slice(id.as_bytes());
    let check = blake3::hash(&record).as_bytes()[..8].to_vec();
    record.extend_from_slice(&check);
    record.extend_from_slice(FRAME);
    let payload_len = u64::try_from(record.len()).unwrap();

    let mut index = Vec::new();
    index.extend_from_slice(id.as_bytes());
    push_varint(&mut index, 0); // at the start of the payload
    push_varint(&mut index, CEILING); // and the index agrees with the header

    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"LPK1");
    bytes.extend_from_slice(&6u32.to_le_bytes()); // the pack format this build reads
    bytes.extend_from_slice(&1u32.to_le_bytes()); // version 1 of the set it came from
    bytes.extend_from_slice(blake3::hash(&table).as_bytes());
    bytes.push(0); // not a delta
    push_varint(&mut bytes, 1); // one chunk
    push_varint(&mut bytes, payload_len);
    bytes.extend_from_slice(&record);
    bytes.extend_from_slice(&table);
    bytes.extend_from_slice(&index);
    bytes.extend_from_slice(&u64::try_from(index.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(&u64::try_from(table.len()).unwrap().to_le_bytes());
    bytes.extend_from_slice(b"LPK1");

    let built = build(1, 1024).await;
    let path = built.dir.path().join("overclaimed.pack");
    fs::write(&path, &bytes).unwrap();

    let pack = Pack::open(&path).expect("the fixture is a pack this build can open");
    let received = built.dir.path().join("received");
    let store = BlobStore::open_for_writing(
        &received,
        lepatch::storage::SegmentLimit::default(),
        lepatch::storage::Compression::default(),
    )
    .await
    .unwrap();

    let error = lepatch::command::merge(&store, &pack, lepatch::command::MergeCompression::Keep)
        .await
        .expect_err("a chunk claiming more content than the chunker produces must be refused");
    assert!(
        error.to_string().contains("more than the largest chunk"),
        "the refusal should name the ceiling, got: {error}"
    );
    assert!(
        store.latest().is_none(),
        "and a refused merge must not have published a version"
    );

    // The same fixture through the two routes a reader takes. `get` is what a
    // restore and an apply call; `stored` is what a merge that keeps the frame
    // calls, and it is the route that was already refusing it above. Both bound
    // the claim before anything reserves it, and both must say so by name.
    for (route, error) in [
        ("get", pack.get(&id).await.err()),
        ("stored", pack.stored(&id).await.err()),
    ] {
        let error = error.unwrap_or_else(|| {
            panic!("{route} must refuse a record claiming more content than the chunker produces")
        });
        assert!(
            error.to_string().contains("more than the largest chunk"),
            "{route} should name the ceiling, got: {error}"
        );
    }
}

/// The varint the format's own lengths are written in: seven bits per byte, least
/// significant first, with the top bit set on every byte but the last.
fn push_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = u8::try_from(value & 0x7f).unwrap();
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}
