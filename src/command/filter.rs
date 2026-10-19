//! Keeping only the snapshot paths a `--files` filter names.
//!
//! One dialect in one place, read three ways. [`Filter::matches`] answers one
//! path. [`Filter::select`] answers a whole snapshot, returning the view a
//! partial restore or apply works from. History reports through the first and
//! restores and applies through the second, so the three cannot disagree about
//! what a value means.
//!
//! An exact value names one path. A value naming a directory names everything
//! under it. A value holding a metacharacter is a glob pattern. A backslash
//! names the next byte literally.

use std::{
    collections::{HashMap, HashSet},
    io,
    path::{Component, Path, PathBuf},
};

use crate::metadata;

/// Decides which snapshot paths a `--files` filter keeps.
///
/// Empty means everything: a run without `--files` builds [`Filter::all`],
/// which matches every path and returns the snapshot unchanged.
#[derive(Debug, Clone)]
pub struct Filter {
    queries: Vec<Query>,
}

impl Filter {
    /// A filter that keeps every path.
    ///
    /// What a run without `--files` uses. [`Filter::select`] returns the
    /// snapshot unchanged for one, so the common path pays nothing.
    #[must_use]
    pub fn all() -> Self {
        Self {
            queries: Vec::new(),
        }
    }

    /// Reads `--files` values against the directories a snapshot records.
    ///
    /// Takes the directories because a value naming one of them is a prefix,
    /// and only the snapshot knows which values those are.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] if a value is empty, absolute,
    /// or holds `..`.
    pub fn new(raw: &[PathBuf], dirs: &HashSet<PathBuf>) -> io::Result<Self> {
        for path in raw {
            check_query(path)?;
        }
        Ok(Self {
            queries: raw.iter().map(|path| parse_query(path, dirs)).collect(),
        })
    }

    /// Reads `--files` values against the snapshot they will select from.
    ///
    /// [`Filter::new`] with the directories read out of `snapshot`, for the
    /// caller that holds the snapshot already and should not have to name what
    /// it holds twice.
    ///
    /// # Errors
    ///
    /// As [`Filter::new`].
    pub fn for_snapshot(raw: &[PathBuf], snapshot: &metadata::Snapshot) -> io::Result<Self> {
        Self::new(raw, &dirs_of(snapshot))
    }

    /// Whether a snapshot-relative path is kept.
    ///
    /// Everything, where this filter holds no query. Otherwise whether any
    /// query answers it.
    #[must_use]
    pub fn matches(&self, path: &Path) -> bool {
        self.queries.is_empty() || self.queries.iter().any(|query| match_one(path, query))
    }

    /// The snapshot a partial run works from: the kept entries and nothing else.
    ///
    /// Keeps the matched files, links, and directories, plus the ancestors a
    /// kept entry needs above it and the recorded files a kept hard link
    /// shares with. Chunk slices are carried over and reindexed, and chunks no
    /// kept file names are dropped. The stamp is kept.
    ///
    /// Returns the snapshot unchanged where this filter holds no query, so a
    /// full run never pays for a view it does not need.
    #[must_use]
    pub fn select(&self, snapshot: &metadata::Snapshot) -> metadata::Snapshot {
        if self.queries.is_empty() {
            return snapshot.clone();
        }

        let kept = self.kept_positions(snapshot);

        let mut files = Vec::with_capacity(kept.files.len());
        let mut remapped = vec![usize::MAX; snapshot.files.len()];
        for old in kept.files {
            remapped[old] = files.len();
            files.push(snapshot.files[old].clone());
        }

        let mut chunk_used = vec![false; snapshot.chunks.len()];
        for slice in &snapshot.file_chunks {
            let old_file = usize::try_from(slice.file_index).unwrap_or(usize::MAX);
            if remapped.get(old_file).is_some_and(|new| *new != usize::MAX) {
                let old_chunk = usize::try_from(slice.chunk_index).unwrap_or(usize::MAX);
                if let Some(used) = chunk_used.get_mut(old_chunk) {
                    *used = true;
                }
            }
        }

        let mut chunk_remapped = vec![u32::MAX; snapshot.chunks.len()];
        let mut chunks = Vec::new();
        for (old, id) in snapshot.chunks.iter().enumerate() {
            if chunk_used[old] {
                chunk_remapped[old] = u32::try_from(chunks.len()).unwrap_or(u32::MAX);
                chunks.push(*id);
            }
        }

        let mut file_chunks = Vec::new();
        for slice in &snapshot.file_chunks {
            let old_file = usize::try_from(slice.file_index).unwrap_or(usize::MAX);
            let Some(new_file) = remapped.get(old_file).filter(|new| **new != usize::MAX) else {
                continue;
            };
            let old_chunk = usize::try_from(slice.chunk_index).unwrap_or(usize::MAX);
            let Some(new_chunk) = chunk_remapped
                .get(old_chunk)
                .filter(|new| **new != u32::MAX)
            else {
                continue;
            };
            file_chunks.push(metadata::FileChunk {
                file_index: u32::try_from(*new_file).unwrap_or(u32::MAX),
                chunk_index: *new_chunk,
                ..slice.clone()
            });
        }

        metadata::Snapshot {
            taken_at: snapshot.taken_at,
            files,
            dirs: kept
                .dirs
                .into_iter()
                .map(|at| snapshot.dirs[at].clone())
                .collect(),
            chunks,
            file_chunks,
            links: kept
                .links
                .into_iter()
                .map(|at| snapshot.links[at].clone())
                .collect(),
        }
    }
}

/// The snapshot positions a selection keeps, in snapshot order.
struct Kept {
    /// Files the filter names, with the recorded files kept hard links need.
    files: Vec<usize>,
    /// Links the filter names.
    links: Vec<usize>,
    /// Directories the filter names, with the ancestors kept entries need.
    dirs: Vec<usize>,
}

impl Filter {
    /// The snapshot positions this filter keeps.
    ///
    /// Files it names outright, links it names with the recorded files their
    /// hard targets need, and directories it names with the ancestors kept
    /// entries need above them. Split out of [`Filter::select`] so the choice
    /// of what is kept reads apart from the reindexing of what it chose.
    fn kept_positions(&self, snapshot: &metadata::Snapshot) -> Kept {
        let file_at: HashMap<&Path, usize> = snapshot
            .files
            .iter()
            .enumerate()
            .map(|(at, file)| (file.path.as_path(), at))
            .collect();

        let mut kept_files: HashSet<usize> = snapshot
            .files
            .iter()
            .enumerate()
            .filter(|(_, file)| self.matches(&file.path))
            .map(|(at, _)| at)
            .collect();

        let mut kept_links: Vec<usize> = Vec::new();
        for (at, link) in snapshot.links.iter().enumerate() {
            let (path, shares) = match link {
                metadata::Link::Symbolic { path, .. } => (path, None),
                metadata::Link::Hard { path, shares } => (path, Some(shares)),
            };
            if !self.matches(path) {
                continue;
            }
            kept_links.push(at);
            // A hard link writes no bytes of its own. Its target has to be
            // there, so the target joins the kept files where the snapshot
            // records it as one.
            if let Some(target) = shares
                && let Some(&found) = file_at.get(target.as_path())
            {
                kept_files.insert(found);
            }
        }

        let kept_paths: HashSet<&Path> = kept_files
            .iter()
            .map(|at| snapshot.files[*at].path.as_path())
            .chain(kept_links.iter().map(|at| match &snapshot.links[*at] {
                metadata::Link::Symbolic { path, .. } | metadata::Link::Hard { path, .. } => {
                    path.as_path()
                }
            }))
            .collect();

        let kept_dirs: Vec<usize> = snapshot
            .dirs
            .iter()
            .enumerate()
            .filter(|(_, dir)| {
                self.matches(&dir.path)
                    || kept_paths
                        .iter()
                        .any(|kept| *kept != dir.path && kept.starts_with(&dir.path))
            })
            .map(|(at, _)| at)
            .collect();

        let mut files: Vec<usize> = kept_files.into_iter().collect();
        files.sort_unstable();

        Kept {
            files,
            links: kept_links,
            dirs: kept_dirs,
        }
    }
}

/// The directories a snapshot records, for [`Filter::new`].
#[must_use]
pub(crate) fn dirs_of(snapshot: &metadata::Snapshot) -> HashSet<PathBuf> {
    snapshot.dirs.iter().map(|dir| dir.path.clone()).collect()
}

/// Refuses a `--files` value no backup could have recorded.
///
/// Empty, absolute, and `..` values never match a snapshot path. Refusing them
/// here keeps a typo from reading as a path no version holds.
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidInput`] naming the value.
fn check_query(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a `--files` value may not be empty. Pass a path relative to the backup root",
        ));
    }
    if path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{} is absolute. Pass a path relative to the backup root",
                path.display()
            ),
        ));
    }
    if path.components().any(|part| part == Component::ParentDir) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{} holds `..`. No backup records a path with `..`",
                path.display()
            ),
        ));
    }
    if path
        .components()
        .any(|part| matches!(part, Component::Prefix(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "{} is not relative to the backup root. Pass a relative path",
                path.display()
            ),
        ));
    }
    Ok(())
}

/// A `--files` value, read for what it asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Query {
    /// A glob pattern over the path bytes.
    Glob(Vec<u8>),
    /// A directory and everything under it.
    Prefix(PathBuf),
    /// One exact path.
    Exact(PathBuf),
}

/// Reads one `--files` value against the directories any version holds.
///
/// A value holding a metacharacter is a glob pattern. A value naming a
/// directory, or ending in a separator, is a prefix. Anything else is exact.
fn parse_query(raw: &Path, dirs: &HashSet<PathBuf>) -> Query {
    let bytes = raw.as_os_str().as_encoded_bytes();
    if bytes
        .iter()
        .any(|byte| matches!(byte, b'*' | b'?' | b'[' | b'\\' | b'{'))
    {
        return Query::Glob(bytes.to_vec());
    }
    let Some(normal) = normalize(raw) else {
        return Query::Exact(raw.to_path_buf());
    };
    if raw.as_os_str().as_encoded_bytes().ends_with(b"/") || dirs.contains(&normal) {
        return Query::Prefix(normal);
    }
    Query::Exact(normal)
}

/// Whether a snapshot path answers a query.
fn match_one(path: &Path, query: &Query) -> bool {
    match query {
        Query::Exact(wanted) => path == wanted,
        Query::Prefix(dir) => path == dir || path.starts_with(dir),
        Query::Glob(pattern) => glob_match(pattern, path.as_os_str().as_encoded_bytes()),
    }
}

/// Whether path bytes match glob pattern bytes.
///
/// `*` spans any bytes within one component. `**` as a whole component spans
/// nested components. `?` spans one byte. `[...]` spans one listed byte. A
/// backslash names the next byte literally.
fn glob_match(pattern: &[u8], path: &[u8]) -> bool {
    match_parts(&split_parts(pattern), &split_parts(path))
}

/// One path split at each separator.
fn split_parts(bytes: &[u8]) -> Vec<&[u8]> {
    bytes.split(|byte| *byte == b'/').collect()
}

/// Whether split pattern parts match split path parts.
fn match_parts(pattern: &[&[u8]], path: &[&[u8]]) -> bool {
    if pattern.is_empty() {
        return path.is_empty();
    }
    if pattern[0] == b"**" {
        for at in 0..=path.len() {
            if match_parts(&pattern[1..], &path[at..]) {
                return true;
            }
        }
        return false;
    }
    if path.is_empty() {
        return false;
    }
    component_match(pattern[0], path[0]) && match_parts(&pattern[1..], &path[1..])
}

/// Whether one pattern component matches one path component.
fn component_match(pattern: &[u8], text: &[u8]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }
    match pattern[0] {
        b'\\' => {
            if pattern.len() == 1 {
                return text == b"\\";
            }
            !text.is_empty() && text[0] == pattern[1] && component_match(&pattern[2..], &text[1..])
        }
        b'*' => {
            let mut rest = 1;
            while rest < pattern.len() && pattern[rest] == b'*' {
                rest += 1;
            }
            (0..=text.len()).any(|at| component_match(&pattern[rest..], &text[at..]))
        }
        b'?' => !text.is_empty() && component_match(&pattern[1..], &text[1..]),
        b'[' => match class_match(&pattern[1..], text.first().copied()) {
            Some((hit, rest)) => hit && component_match(rest, &text[1..]),
            None => {
                !text.is_empty() && text[0] == b'[' && component_match(&pattern[1..], &text[1..])
            }
        },
        byte => !text.is_empty() && text[0] == byte && component_match(&pattern[1..], &text[1..]),
    }
}

/// Whether the byte after `[` lists `byte`, and the pattern past `]`.
///
/// Reads an optional `!` or `^` negation, ranges, and backslash escapes. A `]`
/// in first position is a literal member. Returns nothing for an unclosed
/// class, which the caller reads as a literal `[`.
fn class_match(pattern: &[u8], byte: Option<u8>) -> Option<(bool, &[u8])> {
    let mut at = 0;
    let mut negated = false;
    if at < pattern.len() && (pattern[at] == b'!' || pattern[at] == b'^') {
        negated = true;
        at += 1;
    }
    let mut hit = false;
    let mut first = true;
    loop {
        if at >= pattern.len() {
            return None;
        }
        if pattern[at] == b']' && !first {
            return Some((negated != hit, &pattern[at + 1..]));
        }
        let (low, next) = class_byte(pattern, at)?;
        at = next;
        let mut high = low;
        if at + 1 < pattern.len() && pattern[at] == b'-' && pattern[at + 1] != b']' {
            let (end, next) = class_byte(pattern, at + 1)?;
            high = end;
            at = next;
        }
        if byte.is_some_and(|value| (low..=high).contains(&value)) {
            hit = true;
        }
        first = false;
    }
}

/// One class member byte and the offset past it.
fn class_byte(pattern: &[u8], at: usize) -> Option<(u8, usize)> {
    if at >= pattern.len() {
        return None;
    }
    if pattern[at] == b'\\' {
        if at + 1 >= pattern.len() {
            return None;
        }
        return Some((pattern[at + 1], at + 2));
    }
    Some((pattern[at], at + 1))
}

/// A path cleaned of `.` without changing what it names.
///
/// Returns nothing for an absolute path or one holding `..` or a prefix. The
/// caller refuses those before matching, so nothing here is one.
fn normalize(path: &Path) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for part in path.components() {
        match part {
            Component::Normal(piece) => out.push(piece),
            Component::CurDir => {}
            Component::RootDir | Component::ParentDir | Component::Prefix(_) => return None,
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::{Filter, class_match, component_match, glob_match};
    use crate::{
        metadata::{Attributes, Dir, File, FileChunk, Link, Snapshot, Timestamp},
        storage::BlobId,
    };
    use std::collections::HashSet;

    #[test]
    fn a_star_spans_bytes_within_one_component() {
        assert!(component_match(b"*.rs", b"main.rs"));
        assert!(!component_match(b"*.rs", b"main.txt"));
        assert!(component_match(b"*", b"anything"));
        assert!(!component_match(b"a*c", b"abbd"));
    }

    #[test]
    fn a_question_mark_spans_exactly_one_byte() {
        assert!(component_match(b"f?le", b"file"));
        assert!(!component_match(b"f?le", b"fle"));
        assert!(!component_match(b"f?le", b"fiile"));
    }

    #[test]
    fn a_class_lists_its_members_and_ranges() {
        assert!(component_match(b"f[ai]le", b"file"));
        assert!(!component_match(b"f[ai]le", b"fole"));
        assert!(component_match(b"f[a-z]le", b"fole"));
        assert!(component_match(b"f[!ai]le", b"fole"));
        assert!(!component_match(b"f[!ai]le", b"file"));
    }

    #[test]
    fn an_unclosed_class_is_a_literal_bracket() {
        assert!(component_match(b"f[le", b"f[le"));
        assert!(!component_match(b"f[le", b"file"));
    }

    #[test]
    fn a_backslash_names_the_next_byte_literally() {
        assert!(component_match(b"a\\*b", b"a*b"));
        assert!(!component_match(b"a\\*b", b"aab"));
    }

    #[test]
    fn a_star_does_not_span_a_separator() {
        assert!(!glob_match(b"*.rs", b"src/main.rs"));
        assert!(glob_match(b"*/*.rs", b"src/main.rs"));
    }

    #[test]
    fn a_double_star_spans_nested_components() {
        assert!(glob_match(b"src/**", b"src/a/b/c.rs"));
        assert!(glob_match(b"src/**", b"src"));
        assert!(glob_match(b"**/*.rs", b"a/b/main.rs"));
        assert!(!glob_match(b"src/**", b"other/main.rs"));
    }

    #[test]
    fn a_brace_is_a_literal_byte() {
        assert!(component_match(b"a{b", b"a{b"));
        assert!(class_match(b"a]", Some(b'a')).is_some());
    }

    /// A chunk hash, for a fixture with two distinct chunks.
    fn hash(byte: u8) -> BlobId {
        BlobId::from_hex(format!("{byte:02x}").repeat(32)).unwrap()
    }

    /// A snapshot holding two files on two chunks, one directory, and one hard
    /// link onto the first file.
    fn two_files() -> Snapshot {
        Snapshot {
            taken_at: Timestamp { secs: 0, nanos: 0 },
            files: vec![
                File {
                    path: std::path::PathBuf::from("a.txt"),
                    len: 4,
                    attrs: Attributes::default(),
                },
                File {
                    path: std::path::PathBuf::from("b.txt"),
                    len: 4,
                    attrs: Attributes::default(),
                },
            ],
            dirs: vec![Dir {
                path: std::path::PathBuf::from("sub"),
                attrs: Attributes::default(),
            }],
            chunks: vec![hash(0x01), hash(0x02)],
            file_chunks: vec![
                FileChunk {
                    chunk_index: 0,
                    file_index: 0,
                    chunk_offset: 0,
                    file_offset: 0,
                    length: 4,
                },
                FileChunk {
                    chunk_index: 1,
                    file_index: 1,
                    chunk_offset: 0,
                    file_offset: 0,
                    length: 4,
                },
            ],
            links: vec![Link::Hard {
                path: std::path::PathBuf::from("link"),
                shares: std::path::PathBuf::from("a.txt"),
            }],
        }
    }

    /// The filter under test, read against the fixture's own directories.
    fn select(snapshot: &Snapshot, values: &[&str]) -> Snapshot {
        let raw: Vec<std::path::PathBuf> = values.iter().map(std::path::PathBuf::from).collect();
        let dirs: HashSet<std::path::PathBuf> =
            snapshot.dirs.iter().map(|dir| dir.path.clone()).collect();
        Filter::new(&raw, &dirs).unwrap().select(snapshot)
    }

    #[test]
    fn select_keeps_the_named_file_and_prunes_the_rest() {
        let view = select(&two_files(), &["a.txt"]);

        assert_eq!(
            view.files
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>(),
            vec![std::path::PathBuf::from("a.txt")]
        );
        assert_eq!(view.chunks, vec![hash(0x01)]);
        assert_eq!(view.links.len(), 0);
        view.verify_file_lengths("the view must account for its files")
            .unwrap();
    }

    #[test]
    fn select_pulls_in_a_kept_hard_links_target() {
        let view = select(&two_files(), &["link"]);

        assert_eq!(view.links.len(), 1);
        assert_eq!(
            view.files
                .iter()
                .map(|file| file.path.clone())
                .collect::<Vec<_>>(),
            vec![std::path::PathBuf::from("a.txt")],
            "the link writes no bytes of its own, so its target joins the view"
        );
    }

    #[test]
    fn select_with_no_query_returns_the_snapshot() {
        let snapshot = two_files();
        let view = select(&snapshot, &[]);

        assert_eq!(view.files.len(), snapshot.files.len());
        assert_eq!(view.chunks.len(), snapshot.chunks.len());
        assert_eq!(view.file_chunks.len(), snapshot.file_chunks.len());
    }

    #[test]
    fn select_with_an_unknown_path_keeps_nothing() {
        let view = select(&two_files(), &["nope.txt"]);

        assert_eq!(view.files.len(), 0);
        assert_eq!(view.links.len(), 0);
        assert_eq!(view.dirs.len(), 0);
        assert_eq!(view.chunks.len(), 0);
        assert_eq!(view.file_chunks.len(), 0);
    }
}
