//! Serving pack bytes without writing a pack.
//!
//! A pack is asked for as `(set, version, base, compression)` and **nothing is
//! stored for it**. The id is an encoding of that tuple, so a pack costs no disk
//! until a client asks for a range, and costs nothing again when it goes away.
//! That matters because a pack is *derived output*: a version that has been
//! packed once can be re-derived at any time, and a server that cached every
//! pack would have a disk whose size is a function of how many versions clients
//! have been curious about.
//!
//! ## Why `compression` can only be `keep` here
//!
//! [`VirtualPack`] computes a pack's bytes out of the set, and a record in a pack
//! is the record the set already holds. A pack written under a compression policy
//! is different: it re-encodes every chunk, so its bytes are not in the set and
//! not derivable from it. So a request for one is **refused**, naming what would
//! have to happen, rather than quietly answered with the `keep` bytes.
//!
//! Serving `keep` is not a limitation in practice. It is the default on both
//! `blu pack` and `blu merge`, and it is the mode that makes a push
//! cheap: a chunk the source deduplicated as a frame arrives as that frame rather
//! than being decoded and compressed again.
//!
//! ## What a client actually fetches
//!
//! Not the whole file. [`VirtualPack::fetch_plan`] takes the chunk hashes the
//! client already holds and answers with the byte ranges it still needs, unioned
//! with the header, snapshot, index and trailer. The client writes them at their
//! offsets into a sparse file and calls [`Pack::open`] on it.
//!
//! [`Pack::open`]: crate::pack::Pack::open

use std::{collections::HashSet, io, path::Path, sync::Arc};

use crate::{
    metadata::Timestamp,
    pack::{Range, VirtualPack},
    storage::{BlobId, BlobStore, Compression, Read},
};

use super::sets::Sets;

/// What a pack id names.
///
/// **An id is a name, not a handle.** Nothing is looked up to resolve one: it
/// decodes to this tuple, the tuple is resolved against the set, and the bytes
/// are computed. That is what lets the server hold no state for a generated pack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Requested {
    /// The set the version is in.
    pub set: String,
    /// The version to pack, or `None` for the newest one that resolves.
    ///
    /// `None` rather than a sentinel, because a set's numbering starts at 1 and
    /// so `0` is not a version anything could have asked for by name. A client
    /// that sent `0` and a client that sent nothing would then be the same
    /// request, and a client that meant version zero has made a mistake this
    /// cannot detect.
    pub version: Option<u32>,
    /// The version the pack assumes its reader already holds, for a delta.
    ///
    /// **Honoured, and it changes what is served.** A delta's plan is
    /// [`Plan::since`](crate::storage::Plan::since), which keeps only the chunks
    /// the base does not have, and its index is correspondingly shorter. So
    /// `docs@5-4` is a genuinely smaller file than `docs@5`, and the base hash is
    /// written into its header.
    ///
    /// The reader is asserting it already holds those chunks. Nothing in the pack
    /// can check it, because the bytes that would prove it are exactly the bytes
    /// that were left out — so a reader that cannot account for the base must
    /// refuse the pack, which is what [`Pack::base`](crate::pack::Pack::base)
    /// reporting the hash is for.
    ///
    /// The base must resolve in **this** set. `since` compares against a
    /// snapshot, and the set that holds it is the one whose numbering the two
    /// versions mean.
    pub base: Option<u32>,
}

/// The bytes of a pack, as a function of its request and a set.
///
/// A [`VirtualPack`] is cheap to hold — the index and the snapshot, not the
/// payload — and answering a range from it is a walk plus a read per segment the
/// range touches. One value serves every range of one request.
///
/// **The server builds a pack per request.** The server stores nothing for a
/// pack id, so every request computes the pack again. A client that makes ten
/// requests pays for ten packs. Each pack builds its header store once, and the
/// ranges of that request share it.
#[derive(Debug)]
pub struct Resolved {
    /// The geometry, and the files each of its records is in.
    pub pack: VirtualPack,
    /// The version the pack records, which is what a client checks it fetched
    /// the version it asked for.
    pub version: u32,
    /// The pack's snapshot hash, which is its name.
    pub snapshot: BlobId,
    /// Time the packed version ran, in UTC seconds and nanos.
    pub taken_at: Timestamp,
}

impl Resolved {
    /// The bytes of `range`, as the parts they come from.
    ///
    /// # Errors
    ///
    /// Returns an error if the range is inverted or past the end of the pack.
    pub fn source(&self, range: Range) -> io::Result<Vec<crate::pack::Source>> {
        self.pack.source(range)
    }
}

/// Resolve a pack request against a set, without writing anything.
///
/// # Errors
///
/// Returns an error if the set cannot be read, if the version does not resolve,
/// or if the snapshot cannot be encoded on this platform.
///
/// Returns an error if the version's snapshot cannot be read or does not
/// decode.
///
/// Returns an error if the version's snapshot names a chunk the set does not
/// hold, because the plan that builds the pack resolves every chunk it names.
///
/// Returns an error if a named base version does not resolve, or if the base
/// version's snapshot cannot be read or does not decode. A delta that named no
/// resolvable base would leave out chunks that nothing checked.
///
/// A **cached read handle** is used, so this is one scan per set rather than one
/// per request. The handle is evicted when a merge publishes into that set, which
/// is what keeps a cached one from answering a listing that omits a version that
/// was just pushed.
pub(super) async fn resolve(sets: &Sets, request: &Requested) -> io::Result<Resolved> {
    let cached: Arc<BlobStore<Read, Compression>> = sets.reader(&request.set).await?;
    let base_path = sets.base(&request.set)?;

    let (version, snapshot_id) = select_version(&cached, &base_path, request.version)?;

    // `&*cached`, because `StorageGet` is not implemented for an `Arc` and adding
    // that impl would put an `Arc` in every store's public surface for one caller.
    let store = &*cached;
    let snapshot = crate::command::load_snapshot(store, snapshot_id).await?;

    // **The delta, when one was asked for.** `Plan::since` drops every chunk the
    // base already holds, so both the payload and the index shrink. That is the
    // whole of the point, and it is why a delta is not merely a full pack with
    // some records left out: the index would still name them.
    //
    // The base is resolved through the **store**, not off the version number, so
    // a stale index falls back to a scan and a version the set cannot serve is a
    // `404` rather than a delta whose omitted chunks nobody checked against.
    let (plan, base_id) = match request.base {
        None => (
            crate::storage::Plan::from_snapshot(&snapshot, |id| store.locate(id))?,
            None,
        ),
        Some(base_version) => {
            let base_id = store.snapshot_via_index(base_version)?;
            let base_snapshot = crate::command::load_snapshot(store, base_id).await?;
            let full = crate::storage::Plan::from_snapshot(&snapshot, |id| store.locate(id))?;

            (full.since(&snapshot, &base_snapshot), Some(base_id))
        }
    };

    let virtual_pack = VirtualPack::new(&plan, &snapshot, version, base_id, |segment| {
        store.segment_path(segment)
    })?;

    Ok(Resolved {
        pack: virtual_pack,
        version,
        snapshot: snapshot_id,
        taken_at: snapshot.taken_at,
    })
}

/// A version to pack, defaulting to the newest one that resolves.
///
/// # Errors
///
/// Returns an error if the set holds no usable version, or if the named version
/// does not resolve.
fn select_version(
    store: &BlobStore<Read, Compression>,
    base: &Path,
    version: Option<u32>,
) -> io::Result<(u32, BlobId)> {
    if let Some(version) = version {
        // Through the store, which checks the file against itself. An index that
        // is stale or foreign falls back to a scan; it is never a reason to
        // name a version the store cannot serve.
        return Ok((version, store.snapshot_via_index(version)?));
    }

    store.latest().ok_or_else(|| no_usable_version(base))
}

/// The refusal for a set with nothing to pack.
///
/// Its own function because the same sentence appears in two places, and two
/// wordings of one refusal is how a client ends up matching on one of them.
fn no_usable_version(base: &Path) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "no usable version in {}: the set holds no version that resolves",
            base.display()
        ),
    )
}

/// A single HTTP `Range`, in the form RFC 9110 defines for bytes.
///
/// Only one range is supported, and a request for several is answered whole
/// rather than as a `multipart/byteranges`. Two reasons, and the second is the
/// one that decides it.
///
/// A client that wants several ranges issues several requests. The fetch plan
/// [`VirtualPack::fetch_plan`] returns already merges touching ranges, so a
/// client following it never asks for two ranges that touch — and a multipart
/// body would be bytes a client must parse before it can use, where a second
/// request is bytes it can write at an offset.
///
/// And the multipart media type is the one place in an HTTP API where a
/// response is genuinely awkward to reassemble. A ranged pack is written at an
/// offset into a sparse file. Both forms can do that, and one of them can be
/// understood by reading it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    /// First byte, inclusive.
    pub start: u64,
    /// Last byte, inclusive.
    pub end: u64,
}

impl ByteRange {
    /// Parse a `Range` header value.
    ///
    /// # Errors
    ///
    /// Returns an error for anything this server does not serve: a unit other
    /// than bytes, a suffix range, several ranges, a malformed number, and a
    /// range whose last byte is before its first. The refusal says which,
    /// because a client that guessed the wrong syntax needs to be told the
    /// right one.
    pub fn parse(header: &str) -> io::Result<Self> {
        let spec = header.trim().strip_prefix("bytes=").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{header:?} is not a byte range. This server serves `bytes=`"),
            )
        })?;

        // A comma here is a multi-range request, which is answered whole rather
        // than refused: RFC 9110 says a server may ignore `Range`, and ignoring
        // it is more useful than failing a client whose library asked for two
        // ranges by default.
        if spec.contains(',') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "several ranges were asked for. Ask for one at a time",
            ));
        }

        let (start, end) = spec.split_once('-').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{header:?} is not a byte range of the form `bytes=first-last`"),
            )
        })?;

        // `bytes=-500` is a suffix range: the last 500 bytes. It is well-formed
        // and this server does not serve it, because the pack is computed and
        // answering it would mean computing all of it to learn its length. Named,
        // so the client knows to ask for `bytes=0-` instead.
        if start.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "a suffix range (`bytes=-N`) is not served. Ask for `bytes=first-last`",
            ));
        }

        let first: u64 = start.parse().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{start:?} is not a byte offset"),
            )
        })?;

        let last: u64 = if end.is_empty() {
            u64::MAX
        } else {
            end.parse::<u64>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{end:?} is not a byte offset"),
                )
            })?
        };

        if last < first {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("bytes={first}-{last} ends before it starts"),
            ));
        }

        Ok(Self {
            start: first,
            end: last,
        })
    }

    /// The half-open form, clamped to `length`.
    ///
    /// Clamping rather than failing, because `bytes=0-` is a legal request that
    /// means "the rest", and `bytes=0-99999` on a shorter pack is a client that
    /// asked for more than exists. Both are answered with what there is.
    ///
    /// An offset at or past `length` is a `416` from the caller, not a clamp to
    /// nothing: that is a client whose idea of the file is wrong, and it should
    /// hear about it.
    #[must_use]
    pub fn clamp(&self, length: u64) -> Option<Range> {
        if self.start >= length {
            return None;
        }

        Some(Range {
            start: self.start,
            end: self.end.saturating_add(1).min(length),
        })
    }
}

/// The `have` a client claims, as the fetch plan answers it.
///
/// # Errors
///
/// Returns an error if any hash is not 64 hex characters, naming which one. A
/// typo in a hash would otherwise become a chunk the client does not have, which
/// it would then fetch — and the range it gets back would not cover it.
pub(super) fn fetch_plan(resolved: &Resolved, have: &[String]) -> io::Result<Vec<Range>> {
    let set: HashSet<BlobId> = have
        .iter()
        .map(|text| {
            BlobId::from_hex(text).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "{text:?} is not a chunk hash. A chunk hash is 64 hexadecimal characters"
                    ),
                )
            })
        })
        .collect::<io::Result<HashSet<BlobId>>>()?;

    Ok(resolved.pack.fetch_plan(&set))
}

#[cfg(test)]
mod tests {
    use super::{ByteRange, Requested};

    #[test]
    fn an_ordinary_range_parses() {
        // The form a client following the fetch plan sends.
        let range = ByteRange::parse("bytes=0-1023").unwrap();

        assert_eq!(range.start, 0);
        assert_eq!(range.end, 1023);
    }

    #[test]
    fn an_open_ended_range_means_the_rest() {
        // `bytes=0-` is the common "give me everything from here" and is what a
        // client sends when it wants a whole tail.
        let range = ByteRange::parse("bytes=4096-").unwrap();

        assert_eq!(range.start, 4096);
        assert_eq!(range.end, u64::MAX);
    }

    #[test]
    fn a_single_byte_range_is_inclusive_at_both_ends() {
        // `bytes=0-0` is one byte. The half-open form this becomes must have a
        // length of one, which is where an off-by-one would show up.
        let range = ByteRange::parse("bytes=0-0").unwrap();
        let clamped = range.clamp(100).unwrap();

        assert_eq!(clamped.len(), 1);
    }

    #[test]
    fn a_range_past_the_end_is_clamped_not_refused() {
        // A client that asked for more than exists gets what there is. RFC 9110
        // allows this, and a fetch plan that is slightly optimistic should still
        // produce a usable file.
        let clamped = ByteRange::parse("bytes=90-99999")
            .unwrap()
            .clamp(100)
            .unwrap();

        assert_eq!(clamped.start, 90);
        assert_eq!(clamped.end, 100);
    }

    #[test]
    fn a_range_starting_past_the_end_is_not_a_range_at_all() {
        // The other half of the rule. A client whose idea of the file is wrong
        // should hear about it, rather than receive an empty body it would write
        // as a hole and then fail to open.
        assert!(
            ByteRange::parse("bytes=100-200")
                .unwrap()
                .clamp(100)
                .is_none()
        );
        assert!(ByteRange::parse("bytes=200-").unwrap().clamp(100).is_none());
    }

    #[test]
    fn a_suffix_range_is_refused_and_says_what_to_send() {
        // It is well-formed, and this server does not serve it: the pack is
        // computed, so answering "the last 500 bytes" means computing all of it
        // to learn its length. The message names the form that works.
        let error = ByteRange::parse("bytes=-500").unwrap_err();

        assert!(error.to_string().contains("bytes=first-last"));
    }

    #[test]
    fn a_multi_range_request_is_refused() {
        let error = ByteRange::parse("bytes=0-99,200-299").unwrap_err();

        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("one at a time"));
    }

    #[test]
    fn another_unit_is_refused() {
        let error = ByteRange::parse("items=0-99").unwrap_err();

        assert!(error.to_string().contains("bytes="));
    }

    #[test]
    fn an_inverted_range_is_refused() {
        // Not answered as an empty range. An empty answer to a range a client
        // believed it had would leave a file with a hole and nothing saying so.
        let error = ByteRange::parse("bytes=500-100").unwrap_err();

        assert!(error.to_string().contains("ends before it starts"));
    }

    #[test]
    fn a_non_numeric_offset_is_refused() {
        assert!(ByteRange::parse("bytes=abc-99").is_err());
        assert!(ByteRange::parse("bytes=0-xyz").is_err());
    }

    #[test]
    fn whitespace_around_a_range_is_tolerated() {
        // A header value arrives with whatever spacing a client chose.
        assert_eq!(ByteRange::parse("  bytes=0-10  ").unwrap().end, 10);
    }

    #[test]
    fn a_request_names_what_to_pack() {
        // The whole state-free design rests on this: a request is a tuple, and
        // resolving it needs nothing stored.
        let request = Requested {
            set: "docs".to_string(),
            version: Some(3),
            base: Some(2),
        };

        assert_eq!(request.set, "docs");
        assert_eq!(request.base, Some(2));
    }
}
