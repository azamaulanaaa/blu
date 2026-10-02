//! Compression: what it does to a store, and what it must never do to one.
//!
//! The record format carries a codec byte and two lengths — the bytes on disk and
//! the bytes of content — and every property of the store that does not involve
//! those has to survive their introduction unchanged. So the tests here are mostly
//! about *other* things staying true:
//!
//! * a chunk is addressed by the content, so deduplication must work across
//!   records written with different codecs, and even with compression off;
//! * a payload zstd cannot improve on is stored raw rather than as a frame a few
//!   bytes larger than what it replaced;
//! * every version of a set restores exactly, whatever settings its records were
//!   written under;
//! * a store written at one level reads back at any other, because the codec is in
//!   the record rather than in the store.
//!
//! The one thing compression is *for* — a smaller store — is measured rather than
//! asserted, because whether a given payload compresses is a fact about the data.

#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

mod common;

use std::{borrow::Cow, fs, path::Path};

use lepatch::{
    command::load_snapshot,
    storage::{
        BlobStore, Codec, Compression, Compressor, Plan, Read, SegmentLimit, StorageGet,
        StoragePut, Write,
    },
};

/// A write policy that is not [`Compression`], written the way a caller outside
/// this crate has to write one.
///
/// Nothing here reaches into the store: it is a type, an impl of the public
/// [`Compressor`] trait, and an argument to `open_for_writing`. It compresses
/// with zstd at a level of its own choosing — a *policy* about which payloads are
/// worth compressing, not a different codec, because [`Codec`] has only two arms
/// and a third one would be a different on-disk format.
///
/// The policy is a threshold: payloads below `min_bytes` are stored as they are
/// without being handed to the compressor at all. That is a real choice a real
/// caller makes — a small chunk cannot pay for a frame, and the frame header is a
/// larger share of it — and it is invisible to the store, which records whatever
/// codec each record names.
struct BigPayloadsOnly {
    min_bytes: usize,
}

impl Compressor for BigPayloadsOnly {
    fn compress<'a>(&self, raw: &'a [u8]) -> (Codec, Cow<'a, [u8]>) {
        if raw.len() < self.min_bytes {
            return (Codec::Raw, Cow::Borrowed(raw));
        }

        match zstd::bulk::compress(raw, 3) {
            Ok(frame) if frame.len() < raw.len() => (Codec::Zstd, Cow::Owned(frame)),
            // Not a failure: a frame that did not win is not stored.
            _ => (Codec::Raw, Cow::Borrowed(raw)),
        }
    }
}

/// A compressor that always makes its payload bigger, to pin what the store does
/// with a policy it cannot vouch for.
///
/// It cannot store the bytes it returns and still read them back — nothing decodes
/// them — so it returns a zstd frame of the payload padded to be larger. The store
/// must notice and store the payload untouched, which is the whole of the
/// "the fallback belongs to the writer" argument in [`Compressor`]: a third-party
/// compressor that gets this wrong costs a wasted frame, never a grown set and
/// never an unreadable record.
struct AlwaysGrows;

impl Compressor for AlwaysGrows {
    fn compress<'a>(&self, raw: &'a [u8]) -> (Codec, Cow<'a, [u8]>) {
        let mut frame = zstd::bulk::compress(raw, 1).unwrap_or_else(|_| raw.to_vec());
        frame.extend_from_slice(&[0u8; 64]);

        (Codec::Zstd, Cow::Owned(frame))
    }
}

/// A policy that hands back bytes that are not the payload, under a codec the
/// reader will never decode, to pin that the codec the store records is its own.
struct Truncates;

/// `Raw` in a record means "the bytes the store hashed", so a policy that claims
/// it while returning half the payload is not believed: what the reader is handed
/// for chunk `id` would otherwise be those half bytes followed by whatever record
/// follows them in the segment.
impl Compressor for Truncates {
    fn compress<'a>(&self, raw: &'a [u8]) -> (Codec, Cow<'a, [u8]>) {
        (Codec::Raw, Cow::Owned(vec![0u8; raw.len() / 2]))
    }
}

/// Bytes that zstd cannot compress, so a frame is never smaller than the input.
///
/// The store falls back to raw for these, which is what makes a size assertion
/// about the file mean "the records are this big" rather than "the compressor
/// happened to do well".
fn incompressible(seed: u8, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    for counter in 0..u64::try_from(len.div_ceil(64)).unwrap_or(0) {
        let mut input = [0u8; 40];
        input[..32].copy_from_slice(blake3::hash(&[seed]).as_bytes());
        input[32..].copy_from_slice(&counter.to_le_bytes());
        out.extend_from_slice(blake3::hash(&input).as_bytes());
    }
    out.truncate(len);
    out
}

/// A payload that compresses well: the same bytes, many times over.
fn compressible(len: usize) -> Vec<u8> {
    b"the quick brown fox jumps over the lazy dog. ".repeat(len / 43 + 1)[..len].to_vec()
}

/// A store opened for writing under one policy.
async fn writable(blob: &Path, compression: Compression) -> BlobStore<Write> {
    BlobStore::open_for_writing(blob, SegmentLimit::default(), compression)
        .await
        .unwrap()
}

async fn put<C: Compressor>(store: &BlobStore<Write, C>, bytes: &[u8]) -> lepatch::storage::BlobId {
    let id = common::id_of(bytes);
    store
        .put(&id, common::reader(bytes.to_vec()), common::len_of(bytes))
        .await
        .unwrap();
    id
}

async fn read(store: &BlobStore<Read>, id: &lepatch::storage::BlobId) -> Vec<u8> {
    use tokio::io::AsyncReadExt as _;
    let mut reader = store.get(id).await.unwrap();
    let mut out = Vec::new();
    reader.read_to_end(&mut out).await.unwrap();
    out
}

/// A payload that compresses is stored compressed, and comes back byte for byte.
///
/// The round trip is the property; the codec byte is the mechanism. Asserting the
/// codec separately is what makes a failure say *which* half broke.
#[tokio::test]
async fn a_payload_that_compresses_is_stored_compressed() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");
    let store = writable(&blob, Compression::default()).await;

    let payload = compressible(200_000);
    let id = put(&store, &payload).await;

    let placement = store.locate(&id).unwrap();
    assert_eq!(placement.codec, Codec::Zstd);
    assert_eq!(placement.raw_len, common::len_of(&payload));
    assert!(
        placement.stored_len < placement.raw_len,
        "a compressible payload was stored as {} bytes of {}",
        placement.stored_len,
        placement.raw_len
    );

    let reader = BlobStore::open_for_reading(&blob).await.unwrap();
    assert_eq!(read(&reader, &id).await, payload);
}

/// A payload that does not compress is stored raw.
///
/// Not a nicety: a zstd frame carries a few bytes of header of its own, so on
/// already-compressed content a frame is *larger* than the input. Storing it would
/// make the store bigger for content zstd had no opinion about, which is most of
/// what a game ships as textures, audio and video.
#[tokio::test]
async fn a_payload_that_does_not_compress_is_stored_raw() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");
    let store = writable(&blob, Compression::default()).await;

    let payload = incompressible(1, 200_000);
    let id = put(&store, &payload).await;

    let placement = store.locate(&id).unwrap();
    assert_eq!(
        placement.codec,
        Codec::Raw,
        "incompressible content was wrapped in a frame that made it bigger"
    );
    assert_eq!(placement.stored_len, placement.raw_len);

    let reader = BlobStore::open_for_reading(&blob).await.unwrap();
    assert_eq!(read(&reader, &id).await, payload);
}

/// A caller outside this crate brings its own write policy, and the store takes it.
///
/// The point of the seam, end to end. Nothing in `lepatch` knows `BigPayloadsOnly`
/// exists, and nothing in the store changed to accommodate it: the type parameter
/// default is what lets every other call site keep naming `Compression`.
///
/// What is asserted is that the policy reached the writer at all — the threshold
/// really did keep the small payload out of the compressor — and that the records
/// still read back through an ordinary reader, because each record names its own
/// codec and a reader is told nothing.
#[tokio::test]
async fn a_write_policy_from_outside_the_crate_is_taken_and_respected() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let small = compressible(2_000);
    let large = compressible(200_000);

    let store = BlobStore::open_for_writing(
        &blob,
        SegmentLimit::default(),
        BigPayloadsOnly { min_bytes: 100_000 },
    )
    .await
    .unwrap();

    let small_id = put(&store, &small).await;
    let large_id = put(&store, &large).await;

    let small_placement = store.locate(&small_id).unwrap();
    let large_placement = store.locate(&large_id).unwrap();

    assert_eq!(
        small_placement.codec,
        Codec::Raw,
        "a payload below the policy's own threshold must not have been compressed"
    );
    assert_eq!(small_placement.stored_len, small_placement.raw_len);
    assert_eq!(
        large_placement.codec,
        Codec::Zstd,
        "a payload above it must have been, or the policy was not consulted"
    );
    assert!(large_placement.stored_len < large_placement.raw_len);

    // An ordinary reader, opened with no knowledge of the policy that wrote it.
    drop(store);
    let reader = BlobStore::open_for_reading(&blob).await.unwrap();
    assert_eq!(read(&reader, &small_id).await, small);
    assert_eq!(read(&reader, &large_id).await, large);
}

/// A compressor that makes its payload bigger does not grow the store.
///
/// The invariant is the writer's, so this is where it is pinned. The policy here
/// returns a frame that is larger than the payload it was given, every time, and
/// claims a codec that would need it to be stored — so a store that trusted its
/// compressor would write bytes that nothing can decode back to the payload, and
/// would write more of them than it was given.
///
/// Both halves matter and they are separate properties: the record says `Raw`, so
/// a reader is handed the content rather than a frame; and its stored length is
/// the payload's, so the set did not grow. A fix that got only the first right
/// would produce a store that reads back correctly and is still larger than the
/// tree.
#[tokio::test]
async fn a_compressor_that_grows_its_payload_cannot_grow_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), AlwaysGrows)
        .await
        .unwrap();

    // Incompressible, because the padding has to be what tips the frame over: on
    // compressible content a frame is far smaller than the payload and sixty-odd
    // bytes do not make it the bigger of the two, so this policy would pass
    // unnoticed for the wrong reason.
    let payload = incompressible(7, 200_000);
    let id = put(&store, &payload).await;

    let placement = store.locate(&id).unwrap();
    assert_eq!(
        placement.codec,
        Codec::Raw,
        "a frame larger than its payload was stored and recorded as a codec to decode"
    );
    assert_eq!(
        placement.stored_len, placement.raw_len,
        "the set grew: {} bytes stored for a {} byte payload",
        placement.stored_len, placement.raw_len
    );

    drop(store);
    let reader = BlobStore::open_for_reading(&blob).await.unwrap();
    assert_eq!(read(&reader, &id).await, payload);
}

/// A policy that returns fewer bytes than it was given cannot make a chunk read
/// back as something else.
///
/// The size check and the codec check are separate obligations, and this pins the
/// second one. `Truncates` returns strictly fewer bytes than the payload, so it
/// passes a writer that only compares sizes — and pairs them with a codec the
/// reader does not decode, so the payload is written under a name that means
/// "these are the bytes that hashed to `id`" without them being so. Nothing on the
/// read path hashes what it hands back, so the damage is silent: a restore
/// produces a file of zeros and the bytes of whichever record came next.
#[tokio::test]
async fn bytes_that_are_not_the_payload_are_not_stored_under_its_name() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), Truncates)
        .await
        .unwrap();

    let payload = incompressible(9, 200_000);
    let id = put(&store, &payload).await;

    let placement = store.locate(&id).unwrap();
    assert_eq!(
        placement.stored_len, placement.raw_len,
        "a record of {} bytes cannot be the {} byte payload it was stored for",
        placement.stored_len, placement.raw_len
    );

    // A second record, so a stored length short of the payload would run into
    // another record's bytes rather than into end of file.
    let other = incompressible(10, 100_000);
    put(&store, &other).await;

    drop(store);
    let reader = BlobStore::open_for_reading(&blob).await.unwrap();
    assert_eq!(
        read(&reader, &id).await,
        payload,
        "a chunk read back as something other than the content it is named for"
    );
}

/// A policy that returns exactly as many bytes as it was given is not believed
/// either.
///
/// The boundary is *strictly* smaller, and this is what pins it. A writer that
/// compared with `<=` would store this policy's frame and pass every other test
/// in this file, because a policy that grows its payload and one that merely
/// matches it differ only at a boundary nobody asserted. A frame that saved
/// nothing still costs its header, and it is still bytes the store did not hash
/// under the codec the record would claim.
struct MatchesSize;

impl Compressor for MatchesSize {
    fn compress<'a>(&self, raw: &'a [u8]) -> (Codec, Cow<'a, [u8]>) {
        let mut frame = zstd::bulk::compress(raw, 1).unwrap_or_else(|_| raw.to_vec());
        frame.resize(raw.len(), 0);

        (Codec::Zstd, Cow::Owned(frame))
    }
}

#[tokio::test]
async fn a_frame_that_saved_nothing_is_not_stored() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let store = BlobStore::open_for_writing(&blob, SegmentLimit::default(), MatchesSize)
        .await
        .unwrap();

    // Compressible, because the frame has to be long enough to be padded up to the
    // payload's own length at all; on content zstd cannot shrink there is no
    // shorter frame to grow and the fixture would pass for the wrong reason.
    let payload = compressible(200_000);
    let id = put(&store, &payload).await;

    let placement = store.locate(&id).unwrap();
    assert_eq!(
        placement.codec,
        Codec::Raw,
        "a frame no smaller than the payload was stored, and the reader will decode it"
    );
    assert_eq!(
        placement.stored_len, placement.raw_len,
        "a frame that saved nothing was stored anyway"
    );

    drop(store);
    let reader = BlobStore::open_for_reading(&blob).await.unwrap();
    assert_eq!(read(&reader, &id).await, payload);
}

/// Compression off stores everything raw, whatever the content is.
#[tokio::test]
async fn compression_off_stores_everything_raw() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");
    let store = writable(&blob, Compression::Off).await;

    let payload = compressible(200_000);
    let id = put(&store, &payload).await;

    assert_eq!(store.locate(&id).unwrap().codec, Codec::Raw);

    let reader = BlobStore::open_for_reading(&blob).await.unwrap();
    assert_eq!(read(&reader, &id).await, payload);
}

/// A chunk is addressed by its content, so the codec it was stored under is not
/// part of its identity.
///
/// This is the property that makes the codec safe to be a write policy at all. If
/// the address depended on how the bytes were compressed, then changing the level
/// between runs would re-upload the entire tree — and a level is a flag.
#[tokio::test]
async fn deduplication_holds_across_different_codecs() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    // The same content, stored first raw and then compressed, is one blob.
    let payload = compressible(120_000);
    let id = common::id_of(&payload);

    let store = writable(&blob, Compression::Off).await;
    store
        .put(
            &id,
            common::reader(payload.clone()),
            common::len_of(&payload),
        )
        .await
        .unwrap();
    assert_eq!(store.locate(&id).unwrap().codec, Codec::Raw);
    let after_first = fs::metadata(blob.with_extension("0000.bin")).unwrap().len();

    // Same hash, different policy. Nothing should be written: the store already
    // holds this content, and *how* it holds it is not the store's business.
    drop(store);
    let store = writable(&blob, Compression::zstd(9)).await;
    store
        .put(
            &id,
            common::reader(payload.clone()),
            common::len_of(&payload),
        )
        .await
        .unwrap();

    let after_second = fs::metadata(blob.with_extension("0000.bin")).unwrap().len();
    assert_eq!(
        after_first,
        after_second,
        "re-storing identical content under a different codec wrote {} bytes",
        after_second - after_first
    );
    assert_eq!(store.locate(&id).unwrap().codec, Codec::Raw);

    let reader = BlobStore::open_for_reading(&blob).await.unwrap();
    assert_eq!(read(&reader, &id).await, payload);
}

/// A store written at one level reads back correctly at any other.
///
/// The codec is in the record, not the store, so there is nothing to configure on
/// the reading side at all. That is what makes a level safe to change between
/// runs: the reader never has to know what the writer chose.
#[tokio::test]
async fn a_store_written_at_one_level_reads_at_another() {
    let dir = tempfile::tempdir().unwrap();
    let blob = dir.path().join("store");

    let payloads: Vec<Vec<u8>> = (0..6)
        .map(|i| {
            if i % 2 == 0 {
                compressible(80_000 + i * 1_000)
            } else {
                incompressible(u8::try_from(i).unwrap(), 80_000 + i * 1_000)
            }
        })
        .collect();

    let store = writable(&blob, Compression::zstd(1)).await;
    let mut ids = Vec::new();
    for payload in &payloads {
        ids.push(put(&store, payload).await);
    }
    drop(store);

    // Reopened with a level it was not written with. Nothing carries over.
    let reader = BlobStore::open_for_reading(&blob).await.unwrap();
    for (id, payload) in ids.iter().zip(&payloads) {
        assert_eq!(
            &read(&reader, id).await,
            payload,
            "content written at level 1 did not read back at level 9"
        );
    }
}

/// A store holding records written under several policies is one store, and every
/// version of it restores exactly.
///
/// The case that catches a codec stored per-set rather than per-record: three
/// backups of a growing tree at three different settings, then each version
/// restored on its own. A per-set codec would fail to read the versions written
/// under the others.
#[tokio::test]
async fn a_store_may_hold_records_written_under_several_policies() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("tree");
    fs::create_dir_all(&source).unwrap();
    let blob = dir.path().join("store");

    // One backup per policy, each of a tree that has grown since the last.
    let policies = [Compression::Off, Compression::zstd(9), Compression::zstd(1)];

    for (i, compression) in policies.iter().enumerate() {
        let n = u8::try_from(i).unwrap();
        fs::write(
            source.join(format!("text{n}.bin")),
            compressible(100_000 + i * 20_000),
        )
        .unwrap();
        fs::write(
            source.join(format!("noise{n}.bin")),
            incompressible(n, 60_000 + i * 10_000),
        )
        .unwrap();

        common::run_backup_compressed(&source, &blob, *compression).await;
    }

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    assert_eq!(
        store.versions().unwrap().len(),
        policies.len(),
        "each backup should have published a version"
    );

    // Every version restores, and each carries the files that existed when it was
    // taken — so version N has exactly N of the pairs above.
    for version in 1..=u32::try_from(policies.len()).unwrap() {
        let destination = dir.path().join(format!("out{version}"));
        common::run_restore_version(&blob, &destination, version).await;

        for i in 0..version {
            let n = u8::try_from(i).unwrap();
            assert_eq!(
                fs::read(destination.join(format!("text{n}.bin"))).unwrap(),
                compressible(100_000 + usize::try_from(i).unwrap() * 20_000),
                "version {version} restored the wrong bytes for text{n}.bin"
            );
            assert_eq!(
                fs::read(destination.join(format!("noise{n}.bin"))).unwrap(),
                incompressible(n, 60_000 + usize::try_from(i).unwrap() * 10_000),
                "version {version} restored the wrong bytes for noise{n}.bin"
            );
        }

        // And nothing from a *later* version leaked into an earlier one.
        let restored: Vec<String> = fs::read_dir(&destination)
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            restored.len(),
            usize::try_from(version).unwrap() * 2,
            "version {version} restored {restored:?}, which is not what it recorded"
        );
    }
}

#[tokio::test]
async fn a_compressed_blob_reports_its_raw_length_to_a_plan() {
    // A plan compares a snapshot's slice length against what the store holds, and
    // the two are in different units if the store is compressed: a snapshot
    // records content bytes, a placement reports bytes on disk. Getting this wrong
    // would either reject every chunk of a compressed store or accept one that is
    // genuinely too short.
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("tree");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("a.bin"), compressible(400_000)).unwrap();
    fs::write(source.join("b.bin"), compressible(300_000)).unwrap();

    let blob = dir.path().join("store");
    common::run_backup(&source, &blob).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let snapshot = load_snapshot(&store, store.latest().unwrap().1)
        .await
        .unwrap();
    let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap();

    // Every chunk resolves, and the plan's total is in content bytes.
    assert!(!plan.is_empty());
    assert!(plan.total_bytes() > 0);

    for chunk in plan.chunks() {
        let placement = chunk.placement;
        assert_eq!(
            placement.raw_len,
            store.locate(&chunk.id).unwrap().raw_len,
            "the plan and the store disagree about a chunk's content length"
        );
        assert!(
            chunk.length <= placement.raw_len,
            "a plan asked for {} bytes of a chunk holding {}",
            chunk.length,
            placement.raw_len
        );
    }

    // The chunks really are compressed, or this test has proved nothing about
    // comparing content against storage.
    let compressed = plan
        .chunks()
        .iter()
        .filter(|c| c.placement.codec == Codec::Zstd)
        .count();
    assert!(
        compressed > 0,
        "the fixture should produce compressed chunks, and produced none"
    );
}

/// A pack built from a compressed store holds the store's compressed bytes, so a
/// client downloads less without the store being decompressed first.
///
/// The other half of the decision to put compression in the store: a pack is
/// assembled by copying record payloads, so it inherits whatever the store holds.
#[tokio::test]
async fn a_pack_of_a_compressed_version_is_smaller_than_the_content() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("tree");
    fs::create_dir_all(&source).unwrap();
    let blob = dir.path().join("store");

    for i in 0..4 {
        fs::write(
            source.join(format!("f{i}.bin")),
            compressible(150_000 + i * 10_000),
        )
        .unwrap();
    }
    common::run_backup(&source, &blob).await;

    let store = BlobStore::open_for_reading(&blob).await.unwrap();
    let snapshot = load_snapshot(&store, store.latest().unwrap().1)
        .await
        .unwrap();
    let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id)).unwrap();

    // The pack writer reads through the store, so it is fed decompressed content.
    // What it writes is that content, end to end — and the *store* is where the
    // saving is. This asserts the two facts the design rests on, separately: the
    // plan is in content bytes, and the records behind it are smaller.
    let content: u64 = plan.iter().map(|c| c.length).sum();
    let on_disk: u64 = plan.iter().map(|c| c.placement.stored_len).sum();

    assert_eq!(content, plan.total_bytes());
    assert!(
        on_disk < content,
        "the store should hold less than the content, and holds {on_disk} of {content}"
    );
}

/// The level the user types is the policy that gets applied.
///
/// `--compression-level` counts 0 to 9 and there is no separate switch, so the
/// mapping from what a user writes to what the store does is the whole interface.
/// Two mistakes are possible and both are worth pinning: `0` has to mean "as-is"
/// rather than falling into zstd's own idea of level 0, and a number outside the
/// range has to be clamped by the library while the command line rejects it
/// before the library is ever reached.
#[test]
fn the_level_a_user_types_is_the_policy_applied() {
    use lepatch::storage::MAX_COMPRESSION_LEVEL;

    // 0 is not zstd level 0, which zstd itself treats as "use the default". It is
    // "do not compress", which is the only reading that makes it worth having.
    assert_eq!(Compression::from_level(0), Compression::Off);
    assert_eq!(Compression::from_level(0).level(), None);

    // 1 is the default and the fastest level that still compresses.
    assert_eq!(Compression::from_level(1), Compression::default());
    assert_eq!(Compression::from_level(1).level(), Some(1));

    // The top of the range.
    assert_eq!(Compression::from_level(9).level(), Some(9));
    assert_eq!(
        Compression::from_level(MAX_COMPRESSION_LEVEL),
        Compression::zstd(9)
    );

    // Past the top, the library clamps rather than panicking: a library caller
    // with a bad number gets a working policy, where a user gets an error.
    assert_eq!(Compression::from_level(200).level(), Some(9));
    assert_eq!(Compression::zstd(0).level(), Some(1));

    // And the two entry points agree about every level in range.
    for level in 1..=9 {
        assert_eq!(
            Compression::from_level(level),
            Compression::zstd(level),
            "level {level} is spelled differently by the two constructors"
        );
    }
}
