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
    metadata::Snapshot,
    pack::{Pack, Range, chunks_covering, hash_file},
    storage::{BlobStore, Plan, StorageGet},
};
use std::io::{Read, Seek, SeekFrom};
use tokio::io::AsyncReadExt;

/// The BLAKE3 of a set's latest snapshot — what a pack of that version names.
async fn snapshot_id(blob: &Path) -> lepatch::storage::BlobId {
    let store = BlobStore::<false>::open(blob).await.unwrap();

    let id = store.latest().unwrap().1;
    let mut reader = store.get(&id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();

    blake3::hash(&buffer)
}

/// The plan for a set's latest version, and the chunk hashes it names.
async fn plan_of(blob: &Path) -> Plan {
    let store = BlobStore::<false>::open(blob).await.unwrap();

    let id = store.latest().unwrap().1;
    let mut reader = store.get(&id).await.unwrap();
    let mut buffer = Vec::new();
    reader.read_to_end(&mut buffer).await.unwrap();
    let snapshot = Snapshot::from_bytes(&buffer).unwrap();

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
}

impl Built {
    /// Write a pack of this version to `at`, and return it.
    async fn pack(&self, at: &Path) -> Pack {
        let store = BlobStore::<false>::open(&self.blob).await.unwrap();
        let snapshot = snapshot_id(&self.blob).await;

        Pack::write(&store, &self.plan, &snapshot, at)
            .await
            .unwrap()
    }
}

/// Pack a set's latest version to `at`.
async fn pack(blob: &Path, at: &Path) -> Pack {
    let store = BlobStore::<false>::open(blob).await.unwrap();
    let plan = plan_of(blob).await;
    let snapshot = snapshot_id(blob).await;

    Pack::write(&store, &plan, &snapshot, at).await.unwrap()
}

async fn build(files: usize, size: usize) -> Built {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

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

    Built { dir, blob, plan }
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
    let store = BlobStore::<false>::open(&built.blob).await.unwrap();
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

    // Twelve bytes of trailer: an 8-byte index length and the magic.
    let mut trailer = [0u8; 12];
    {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = fs::File::open(&path).unwrap();
        file.seek(SeekFrom::Start(file_len - 12)).unwrap();
        file.read_exact(&mut trailer).unwrap();
    }

    assert_eq!(&trailer[8..], b"LPK1", "a pack must end with its magic");

    let index_len = u64::from_le_bytes(trailer[..8].try_into().unwrap());
    assert!(index_len > 0, "the index should not be empty");
    assert!(
        index_len < file_len / 2,
        "the index is {index_len} of {file_len} bytes, so a client reading only \
         the tail reads a small fraction of the file"
    );

    // Opening costs the index and nothing else, and gives the same answer as the
    // pack that was just written.
    let reopened = Pack::open(&path).unwrap();
    assert_eq!(reopened.len(), pack.len());
    assert_eq!(reopened.snapshot(), pack.snapshot());
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
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
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

    let store = BlobStore::<false>::open(&blob).await.unwrap();
    let snapshot = snapshot_id(&blob).await;
    let pack = Pack::write(&store, &v2, &snapshot, &dir.path().join("v2.pack"))
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

    assert!(pack.needed(&have).is_empty());
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

    let have = chunks_covering(&pack, &first);
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
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();
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
    // failing usefully.
    let count_at = 4 + 4 + 32;
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

/// How many bytes the varint at `at` occupies.
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

    // Where the index starts: the file, less the trailer, less the index.
    let mut length = [0u8; 8];
    length.copy_from_slice(&good[good.len() - 12..good.len() - 4]);
    let index_len = usize::try_from(u64::from_le_bytes(length)).unwrap();
    let index_at = good.len() - 12 - index_len;
    assert!(index_len > 32, "the index should have room to damage");

    // The first entry's hash is 32 bytes, and its two numbers follow.
    let numbers_at = index_at + 32;

    // Nothing but continuation bits, from the first number to the end of the index.
    // Every varint read then runs past what a u64 can hold, which must be refused
    // rather than read as a very large offset.
    let mut endless = good.clone();
    for byte in endless.iter_mut().skip(numbers_at) {
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
    for byte in overlong.iter_mut().skip(numbers_at).take(9) {
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
    let at = cut_number.len() - 12;
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
    let mut length = [0u8; 8];
    length.copy_from_slice(&good[good.len() - 12..good.len() - 4]);
    let index_len = usize::try_from(u64::from_le_bytes(length)).unwrap();
    let index_at = good.len() - 12 - index_len;
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
    let mut length = [0u8; 8];
    length.copy_from_slice(&good[good.len() - 12..good.len() - 4]);
    let index_len = usize::try_from(u64::from_le_bytes(length)).unwrap();
    let index_at = good.len() - 12 - index_len;
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
    let source = dir.path().join("source");
    fs::create_dir_all(&source).unwrap();

    let blob = dir.path().join("store");
    common::run_backup(&source, &blob).await;

    let store = BlobStore::<false>::open(&blob).await.unwrap();
    let plan = plan_of(&blob).await;
    assert!(plan.is_empty(), "an empty tree should plan to no chunks");

    let path = dir.path().join("v1.pack");
    Pack::write(&store, &plan, &snapshot_id(&blob).await, &path)
        .await
        .unwrap();

    let pack = Pack::open(&path).expect("a pack this crate wrote should open");
    assert!(pack.is_empty());
    assert_eq!(pack.payload_len(), 0);
    assert_eq!(pack.len(), 0);
}

/// The snapshot a pack names is the one it was built from.
#[tokio::test]
async fn a_pack_names_its_snapshot() {
    let built = build(3, 100_000).await;
    let expected = snapshot_id(&built.blob).await;
    let pack = built.pack(&built.dir.path().join("v1.pack")).await;

    assert_eq!(pack.snapshot(), expected);
    assert_eq!(Pack::open(pack.path()).unwrap().snapshot(), expected);
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
        chunks_covering(&pack, &[half]).is_empty(),
        "half a chunk should not be recorded as the chunk"
    );

    let whole = lepatch::pack::Range {
        start,
        end: start + first.len,
    };
    assert_eq!(chunks_covering(&pack, &[whole]).len(), 1);
}
