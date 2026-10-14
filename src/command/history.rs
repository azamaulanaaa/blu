// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! File history across versions: at which version a path appeared, changed, or left.
//!
//! [`report`] walks loaded snapshots oldest first and compares each one with
//! the one before it. A path present now and absent before was created here. A
//! path absent now and present before was deleted here. A path present in both
//! with a different fingerprint was modified here.
//!
//! Which paths count as requested is [`Filter`](super::filter::Filter)'s
//! answer, shared with the restores and applies that take the same flag.

use std::{
    collections::{HashMap, HashSet},
    io,
    path::PathBuf,
};

use super::filter::{Filter, dirs_of};
use crate::{metadata, storage};

/// What happened to one path at one version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// Present now and absent in the version before.
    Created,
    /// Present in both with a different fingerprint.
    Modified,
    /// Present before and absent now.
    Deleted,
}

impl Status {
    /// The word the command line prints and the server sends.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Modified => "modified",
            Self::Deleted => "deleted",
        }
    }

    /// The status a word names, or nothing for a word history never sends.
    #[must_use]
    pub fn parse(word: &str) -> Option<Self> {
        match word {
            "created" => Some(Self::Created),
            "modified" => Some(Self::Modified),
            "deleted" => Some(Self::Deleted),
            _ => None,
        }
    }
}

/// What a modification changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Detail {
    /// The bytes differ.
    Content,
    /// A file became a directory or a link, or the reverse.
    Type,
    /// The bytes are the same and the attributes differ.
    Metadata,
}

impl Detail {
    /// The word the server sends beside `modified`.
    #[must_use]
    pub const fn word(self) -> &'static str {
        match self {
            Self::Content => "content",
            Self::Type => "type",
            Self::Metadata => "metadata",
        }
    }

    /// The detail a word names, or nothing for a word history never sends.
    #[must_use]
    pub fn parse(word: &str) -> Option<Self> {
        match word {
            "content" => Some(Self::Content),
            "type" => Some(Self::Type),
            "metadata" => Some(Self::Metadata),
            _ => None,
        }
    }
}

/// One reported change: what happened to one path at one version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    /// The set's own number for the version.
    pub version: u32,
    /// The snapshot that version names.
    pub snapshot: storage::BlobId,
    /// The time that version ran.
    pub taken_at: metadata::Timestamp,
    /// The path relative to the backup root.
    pub path: PathBuf,
    /// What happened here.
    pub status: Status,
    /// What changed, set only for [`Status::Modified`].
    pub detail: Option<Detail>,
}

/// One version with its snapshot decoded and ready to compare.
#[derive(Debug, Clone)]
struct Loaded {
    /// The set's own number for the version.
    version: u32,
    /// The snapshot that version names.
    snapshot: storage::BlobId,
    /// The decoded snapshot.
    snapshot_body: metadata::Snapshot,
}

/// Loads one snapshot per version, oldest first.
///
/// Takes the version numbers with the snapshot hashes they name, so the caller
/// reads the numbering off the store that owns it. Every snapshot here is a
/// whole read: a snapshot fits in memory by design.
///
/// # Errors
///
/// Returns an error if a named snapshot cannot be read or decoded.
async fn load_snapshots<S: storage::StorageGet>(
    store: &S,
    versions: &[(u32, storage::BlobId)],
) -> io::Result<Vec<Loaded>> {
    let mut out = Vec::with_capacity(versions.len());
    for (version, snapshot) in versions {
        let body = super::load_snapshot(store, *snapshot).await?;
        out.push(Loaded {
            version: *version,
            snapshot: *snapshot,
            snapshot_body: body,
        });
    }
    Ok(out)
}

/// Reports the events for `raw` across the named versions, oldest first.
///
/// Loads each snapshot whole and compares each one with the one before it.
/// Only versions where at least one requested path changed appear. Within a
/// version the paths are sorted, so two runs report the same lines.
///
/// A file counts as changed when its bytes differ, not when its chunks do:
/// small files share chunks with their neighbours, so a neighbour's edit
/// re-hashes the chunk without touching the file. The report compares bytes
/// where the chunk layout leaves doubt, and reads only the files it reports
/// on.
///
/// # Errors
///
/// Returns an error if a `--files` value is empty, absolute, or holds `..`.
/// Returns an error if a named snapshot cannot be read, decoded, or
/// fingerprinted. Returns an error if a compared chunk cannot be read.
pub async fn report<S: storage::StorageGet>(
    store: &S,
    versions: &[(u32, storage::BlobId)],
    raw: &[PathBuf],
) -> io::Result<Vec<Event>> {
    let loaded = load_snapshots(store, versions).await?;
    let maps: Vec<HashMap<PathBuf, Entry>> = loaded
        .iter()
        .map(|item| entries_of(&item.snapshot_body))
        .collect::<io::Result<_>>()?;

    let mut dirs: HashSet<PathBuf> = HashSet::new();
    for item in &loaded {
        dirs.extend(dirs_of(&item.snapshot_body));
    }
    let filter = Filter::new(raw, &dirs)?;

    let mut out = Vec::new();
    let mut before: HashMap<PathBuf, Entry> = HashMap::new();
    for (item, current) in loaded.iter().zip(&maps) {
        let mut paths: Vec<PathBuf> = before
            .keys()
            .chain(current.keys())
            .filter(|path| filter.matches(path))
            .cloned()
            .collect::<HashSet<PathBuf>>()
            .into_iter()
            .collect();
        paths.sort();

        for path in paths {
            let found = match (before.get(&path), current.get(&path)) {
                (None, Some(_)) => Some(Event {
                    version: item.version,
                    snapshot: item.snapshot,
                    taken_at: item.snapshot_body.taken_at,
                    path: path.clone(),
                    status: Status::Created,
                    detail: None,
                }),
                (Some(_), None) => Some(Event {
                    version: item.version,
                    snapshot: item.snapshot,
                    taken_at: item.snapshot_body.taken_at,
                    path: path.clone(),
                    status: Status::Deleted,
                    detail: None,
                }),
                (Some(previous), Some(next)) => {
                    resolve(store, previous, next).await?.map(|detail| Event {
                        version: item.version,
                        snapshot: item.snapshot,
                        taken_at: item.snapshot_body.taken_at,
                        path: path.clone(),
                        status: Status::Modified,
                        detail: Some(detail),
                    })
                }
                (None, None) => None,
            };
            out.extend(found);
        }

        before.clone_from(current);
    }

    Ok(out)
}

/// Everything one comparison needs to know about one path.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Entry {
    /// A regular file: its length, its slices in file order, and its attributes.
    File {
        /// Length in bytes at backup time.
        len: u64,
        /// The chunk slices covering the file, in file order.
        slices: Vec<Slice>,
        /// The permissions and times to restore.
        attrs: metadata::Attributes,
    },
    /// A directory and its attributes.
    Dir {
        /// The permissions and times to restore.
        attrs: metadata::Attributes,
    },
    /// A symbolic link, its target, and its attributes.
    Symbolic {
        /// Where it points, as written.
        target: PathBuf,
        /// The permissions and times to restore.
        attrs: metadata::Attributes,
    },
    /// A hard link, the file it shares, and its attributes.
    Hard {
        /// The path within the backup whose contents this shares.
        shares: PathBuf,
        /// The permissions and times to restore.
        attrs: metadata::Attributes,
    },
}

/// One slice of a chunk inside a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Slice {
    /// The chunk holding these bytes.
    chunk: storage::BlobId,
    /// Byte offset of this slice within the chunk.
    offset: u32,
    /// Length of this slice in bytes.
    length: u32,
}

/// The fingerprint of every path a snapshot records, by normalized path.
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidData`] if a chunk slice names a chunk the
/// snapshot does not hold.
fn entries_of(snapshot: &metadata::Snapshot) -> io::Result<HashMap<PathBuf, Entry>> {
    let mut ordered: HashMap<u32, Vec<(u64, Slice)>> = HashMap::new();
    for slice in &snapshot.file_chunks {
        let id = snapshot
            .chunks
            .get(usize::try_from(slice.chunk_index).unwrap_or(usize::MAX))
            .copied()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "the snapshot names a chunk it does not hold",
                )
            })?;
        ordered.entry(slice.file_index).or_default().push((
            slice.file_offset,
            Slice {
                chunk: id,
                offset: slice.chunk_offset,
                length: slice.length,
            },
        ));
    }

    let mut out = HashMap::new();
    for (at, file) in snapshot.files.iter().enumerate() {
        let index = u32::try_from(at).unwrap_or(u32::MAX);
        let mut slices = ordered.remove(&index).unwrap_or_default();
        slices.sort_by_key(|(offset, _)| *offset);
        let slices: Vec<Slice> = slices.into_iter().map(|(_, slice)| slice).collect();
        out.insert(
            file.path.clone(),
            Entry::File {
                len: file.len,
                slices,
                attrs: file.attrs,
            },
        );
    }
    for dir in &snapshot.dirs {
        out.insert(dir.path.clone(), Entry::Dir { attrs: dir.attrs });
    }
    for link in &snapshot.links {
        match link {
            metadata::Link::Symbolic { path, target } => {
                out.insert(
                    path.clone(),
                    Entry::Symbolic {
                        target: target.clone(),
                        attrs: metadata::Attributes::default(),
                    },
                );
            }
            metadata::Link::Hard { path, shares } => {
                out.insert(
                    path.clone(),
                    Entry::Hard {
                        shares: shares.clone(),
                        attrs: metadata::Attributes::default(),
                    },
                );
            }
        }
    }
    Ok(out)
}

/// What a fast fingerprint comparison says about two entries of one path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// The bytes are the same and the attributes are the same.
    Same,
    /// The bytes differ.
    Content,
    /// The bytes are the same and the attributes differ.
    Metadata,
    /// A file became a directory or a link, or the reverse.
    Type,
    /// The chunk layout differs at the same length: small files share chunks
    /// with their neighbours, so only the bytes tell. Compare them.
    CheckContent,
}

/// What changed between two fingerprints of one path.
///
/// A different length is a different file: no byte comparison is needed. An
/// identical layout with identical attributes is the same file. Everything
/// else at the same length needs the bytes, which [`resolve`] reads.
fn verdict_of(previous: &Entry, next: &Entry) -> Verdict {
    match (previous, next) {
        (
            Entry::File {
                len: before_len,
                slices: before_slices,
                attrs: before_attrs,
            },
            Entry::File {
                len: after_len,
                slices: after_slices,
                attrs: after_attrs,
            },
        ) => {
            if before_len != after_len {
                Verdict::Content
            } else if before_slices != after_slices {
                Verdict::CheckContent
            } else if before_attrs != after_attrs {
                Verdict::Metadata
            } else {
                Verdict::Same
            }
        }
        (Entry::Dir { attrs: before }, Entry::Dir { attrs: after }) => {
            if before == after {
                Verdict::Same
            } else {
                Verdict::Metadata
            }
        }
        (Entry::Symbolic { target: before, .. }, Entry::Symbolic { target: after, .. })
        | (Entry::Hard { shares: before, .. }, Entry::Hard { shares: after, .. }) => {
            if before == after {
                Verdict::Same
            } else {
                Verdict::Content
            }
        }
        _ => Verdict::Type,
    }
}

/// Settles one fast verdict into a reported detail, or nothing for identical ones.
///
/// [`Verdict::CheckContent`] reads both files' bytes out of the store and
/// compares them. A neighbour's edit re-hashes a shared chunk without touching
/// the file, so the layout alone cannot tell a rewrite from a relayout.
///
/// # Errors
///
/// Returns an error if a compared chunk cannot be read.
async fn resolve<S: storage::StorageGet>(
    store: &S,
    previous: &Entry,
    next: &Entry,
) -> io::Result<Option<Detail>> {
    match verdict_of(previous, next) {
        Verdict::Same => Ok(None),
        Verdict::Content => Ok(Some(Detail::Content)),
        Verdict::Metadata => Ok(Some(Detail::Metadata)),
        Verdict::Type => Ok(Some(Detail::Type)),
        Verdict::CheckContent => {
            let (
                Entry::File {
                    slices: before_slices,
                    attrs: before_attrs,
                    ..
                },
                Entry::File {
                    slices: after_slices,
                    attrs: after_attrs,
                    ..
                },
            ) = (previous, next)
            else {
                return Ok(Some(Detail::Content));
            };
            let before = content_hash(store, before_slices).await?;
            let after = content_hash(store, after_slices).await?;
            if before != after {
                return Ok(Some(Detail::Content));
            }
            if before_attrs != after_attrs {
                return Ok(Some(Detail::Metadata));
            }
            Ok(None)
        }
    }
}

/// The BLAKE3 hash of a file's bytes, read slice by slice out of the store.
///
/// Reads each named chunk whole and hashes the recorded range, so a chunk
/// shared with other files is read once per file that names it.
///
/// # Errors
///
/// Returns an error if a named chunk cannot be read, or if a slice reaches
/// past the end of its chunk.
async fn content_hash<S: storage::StorageGet>(
    store: &S,
    slices: &[Slice],
) -> io::Result<blake3::Hash> {
    use tokio::io::AsyncReadExt as _;

    let mut hash = blake3::Hasher::new();
    for slice in slices {
        let mut reader = store.get(&slice.chunk).await?;
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await?;
        let start = usize::try_from(slice.offset).unwrap_or(usize::MAX);
        let len = usize::try_from(slice.length).unwrap_or(usize::MAX);
        let end = start.saturating_add(len);
        let Some(range) = bytes.get(start..end) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "chunk {} holds {} bytes and the snapshot reads past them",
                    slice.chunk,
                    bytes.len()
                ),
            ));
        };
        hash.update(range);
    }
    Ok(hash.finalize())
}

#[cfg(test)]
mod tests {
    use super::{Entry, Slice, Verdict, verdict_of};
    use crate::{metadata::Attributes, storage::BlobId};

    /// One chunk slice covering whole test content.
    fn slice(hash: &str) -> Slice {
        Slice {
            chunk: BlobId::from_hex(hash).unwrap(),
            offset: 0,
            length: 4,
        }
    }

    /// A four-byte file holding `hash`.
    fn file(hash: &str) -> Entry {
        Entry::File {
            len: 4,
            slices: vec![slice(hash)],
            attrs: Attributes::default(),
        }
    }

    const A: &str = "0101010101010101010101010101010101010101010101010101010101010101";
    const B: &str = "0202020202020202020202020202020202020202020202020202020202020202";

    #[test]
    fn identical_fingerprints_are_the_same_file() {
        assert_eq!(verdict_of(&file(A), &file(A)), Verdict::Same);
    }

    #[test]
    fn a_new_length_is_new_content_without_reading_bytes() {
        let longer = Entry::File {
            len: 8,
            slices: vec![slice(A)],
            attrs: Attributes::default(),
        };

        assert_eq!(verdict_of(&file(A), &longer), Verdict::Content);
    }

    #[test]
    fn a_relayout_at_the_same_length_needs_the_bytes() {
        assert_eq!(verdict_of(&file(A), &file(B)), Verdict::CheckContent);
    }

    #[test]
    fn changed_attributes_alone_are_metadata() {
        let before = file(A);
        let mut after = file(A);
        let Entry::File { attrs, .. } = &mut after else {
            unreachable!("the fixture builds a file");
        };
        attrs.mode = Some(0o755);

        assert_eq!(verdict_of(&before, &after), Verdict::Metadata);
    }

    #[test]
    fn a_changed_kind_is_a_type_change() {
        let dir = Entry::Dir {
            attrs: Attributes::default(),
        };

        assert_eq!(verdict_of(&file(A), &dir), Verdict::Type);
    }
}
