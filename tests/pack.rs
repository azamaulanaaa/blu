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

use std::{collections::HashSet, fs, path::Path};

use lepatch::{
    metadata::{Attributes, File, FileChunk, Snapshot},
    pack::{Pack, Range, hash_file},
    storage::{BlobStore, Plan, StorageGet},
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

/// A pack holds every chunk its version needs, and the bytes are the chunks.
#[tokio::test]
async fn a_pack_holds_its_versions_chunks() {
    let built = build(5, 200_000).await;
    let path = built.dir.path().join("v1.pack");
    let pack = built.pack(&path).await;

    assert_eq!(pack.len(), built.plan.len());
    assert_eq!(
        pack.payload_len(),
        built.plan.total_bytes(),
        "a pack's payload should be exactly the plan's bytes"
    );

    // And the bytes are the chunks, in order, byte for byte.
    let store = BlobStore::open_for_reading(&built.blob).await.unwrap();
    let mut file = fs::File::open(&path).unwrap();

    for (i, chunk) in built.plan.iter().enumerate() {
        let located = pack.locate(&chunk.id).unwrap();

        let mut from_store = store.get(&chunk.id).await.unwrap();
        let mut expected = vec![0u8; usize::try_from(chunk.length).unwrap()];
        tokio::io::AsyncReadExt::read_exact(from_store.as_mut(), &mut expected)
            .await
            .unwrap();

        let mut actual = vec![0u8; usize::try_from(chunk.length).unwrap()];
        file.seek(SeekFrom::Start(pack.payload_start() + located.offset))
            .unwrap();
        file.read_exact(&mut actual).unwrap();

        assert_eq!(
            actual, expected,
            "chunk {i} is not its own bytes in the pack"
        );
    }
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

    // Opening costs the index and nothing else, and gives the same answer as the
    // pack that was just written.
    let reopened = Pack::open(&path).unwrap();
    assert_eq!(reopened.len(), pack.len());
    assert_eq!(reopened.snapshot_id(), pack.snapshot_id());
    assert_eq!(reopened.payload_len(), pack.payload_len());
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
/// produce. A test that assumed a width would be asserting about its own
/// fixture rather than about the pack.
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

/// A pack in format `3` is refused by name, before its bytes are read as a `4`.
///
/// `3` has no version field, so the four bytes this build reads as one are the
/// first four of its snapshot hash. A reader that carried on past the format
/// check would take every chunk at an offset four bytes wrong and hand out byte
/// ranges that look entirely plausible — which is why the refusal has to happen
/// where it does, and has to name both formats so the person holding the file
/// knows which `lepatch` produced it.
#[tokio::test]
async fn a_pack_in_the_previous_format_is_refused_by_name() {
    let built = build(3, 100_000).await;
    let path = built.dir.path().join("v1.pack");
    built.pack(&path).await;

    // Patched rather than built: a real `3` pack is a file this build cannot
    // produce, and patching the four format bytes is the only honest way to hold
    // one. The rest of the file is a valid `4` pack's, which is the point — the
    // only thing wrong with it is the format it claims.
    let mut bytes = fs::read(&path).unwrap();
    bytes[4..8].copy_from_slice(&3u32.to_le_bytes());
    let at = built.dir.path().join("v3.pack");
    fs::write(&at, &bytes).unwrap();

    let err = Pack::open(&at).unwrap_err();
    let said = err.to_string();
    assert!(
        said.contains("pack format 3") && said.contains("reads 4"),
        "a format 3 pack should be refused naming both formats, got: {said}"
    );
    assert!(
        said.contains("not a pack this build can read"),
        "and refused as unreadable rather than as damaged: {said}"
    );

    // The same file with the format byte put back is a perfectly good pack again,
    // which is what shows the refusal was about the format and nothing else: a
    // reader that had carried on past the check would have been reading the top
    // of a snapshot hash as a version and believing it.
    bytes[4..8].copy_from_slice(&4u32.to_le_bytes());
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
/// within it. An offset and a length that reach past the end are numbers a client
/// is told to add up — `needed` and `chunks_covering` both do
/// `payload_start + offset + len` — so accepting them means the arithmetic that
/// consumes a pack can overflow rather than fail.
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
        err.to_string().contains("past the"),
        "a chunk placed past the payload was accepted: {err}"
    );

    // The same bytes as a number one past the type, so the offset and the length
    // together cannot be represented at all. This is the case that panicked in
    // `needed` rather than misbehaving quietly.
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
        "a chunk whose offset and length cannot be added was accepted"
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
            planned.length,
            "a pack holds the used prefix of a chunk, not the whole of it"
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
    assert_eq!(bytes.len() as u64, pack.locate(&held).unwrap().len);
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
    let half = lepatch::pack::Range {
        start,
        end: start + first.len / 2,
    };

    assert!(
        pack.chunks_covering(&[half]).is_empty(),
        "half a chunk should not be recorded as the chunk"
    );

    let whole = lepatch::pack::Range {
        start,
        end: start + first.len,
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
