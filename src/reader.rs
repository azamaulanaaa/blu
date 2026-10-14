// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! Splitting a set of files into content-defined chunks.
//!
//! Files are read as if they had been concatenated into one stream, so a chunk
//! may span a file boundary. A [`FileRegistry`] records where each file sits in
//! that stream. That record lets a chunk's global range be mapped back onto the
//! one or more file slices it covers.

use std::{
    collections::VecDeque,
    fmt::Debug,
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

use fastcdc::{ronomon, v2020::StreamCDC};
use tracing::instrument;

/// The part of one file that a chunk covers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkSource {
    /// Path of the file, as it was passed to [`FileRegistry::new`].
    ///
    /// Shared, not owned. There is one of these per *file slice a chunk
    /// covers*, and a chunk spanning a file boundary has more than one. A backup
    /// therefore built a `PathBuf` per chunk, which allocated the whole path again,
    /// bytes and all. That named a file the registry had already recorded once. An
    /// [`Arc`] makes it a refcount bump. The allocation is not the point, the copy
    /// of the path is. That copy happened once per chunk rather than once per file.
    pub path: Arc<Path>,
    /// Byte offset of this slice within the file.
    pub offset: u64,
    /// Length of this slice in bytes.
    pub length: u32,
}

/// One chunk of the concatenated stream, and where it came from.
///
/// `Debug` is hand-written and omits `data`, because a debug log of a backup
/// that printed every chunk would print the user's files. `PartialEq` is derived
/// rather than hand-written, because comparing the contents is exactly what a
/// test of the chunker wants to do. Nothing in a log has to notice.
#[derive(Clone, PartialEq, Eq)]
pub struct Chunk {
    /// The file slices this chunk covers, in stream order. More than one entry
    /// means the chunk spans a file boundary.
    pub sources: Vec<ChunkSource>,
    /// The chunk's contents, owned rather than behind a reader. The chunker
    /// has already materialised them to find the boundary.
    pub data: Vec<u8>,
}

impl Debug for Chunk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Contents omitted, because a `Debug` that dumped a backup would be
        // a way to print the user's data. The length says everything useful
        // anyway.
        f.debug_struct("Chunk")
            .field("sources", &self.sources)
            .field("len", &self.data.len())
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct EntryFileRegistry {
    path: Arc<Path>,
    length: u64,
    global_offset: u64,
}

/// Where each file sits within the concatenated stream.
#[derive(Debug)]
pub struct FileRegistry {
    entries: Vec<EntryFileRegistry>,
}

impl FileRegistry {
    /// Record the length of each file, in the order given.
    ///
    /// # Errors
    ///
    /// Returns an error if any file cannot be stat'ed.
    #[instrument(level = "trace", skip(paths), err)]
    pub fn new<P: Into<PathBuf>, I: Iterator<Item = P>>(paths: I) -> io::Result<Self> {
        let mut global_offset = 0;

        let entries = paths
            .map(|path| {
                let path_buf = path.into();
                let length = std::fs::metadata(&path_buf)?.len();

                let entry = EntryFileRegistry {
                    // `into_boxed_path` then `Arc::from`, so the path bytes are
                    // moved into the `Arc` rather than copied into it.
                    path: Arc::from(path_buf.into_boxed_path()),
                    length,
                    global_offset,
                };

                global_offset += length;

                Ok(entry)
            })
            .collect::<io::Result<Vec<_>>>()?;

        Ok(Self { entries })
    }

    /// Map a global range of the concatenated stream onto the file slices it
    /// covers.
    ///
    /// An empty result means the range lies in a file that was not registered.
    /// For a well-formed chunker that means a file changed size between being
    /// registered and being read.
    #[instrument(level = "trace", skip(self))]
    pub fn resolve_chunk(&self, global_start: u64, length: u32) -> Vec<ChunkSource> {
        let global_end = global_start + u64::from(length);
        let mut mappings = Vec::new();

        let start_index = self
            .entries
            .partition_point(|entry| (entry.global_offset + entry.length) <= global_start);

        for entry in self.entries.iter().skip(start_index) {
            if entry.global_offset >= global_end {
                break;
            }

            let file_end_global = entry.global_offset + entry.length;

            let overlap_start_global = std::cmp::max(global_start, entry.global_offset);
            let overlap_end_global = std::cmp::min(global_end, file_end_global);

            // Both endpoints lie in `[global_start, global_end]`, so the
            // difference cannot exceed `length`. Clamping keeps that true rather
            // than wrapping to a plausible-looking wrong length.
            let overlap = overlap_end_global.saturating_sub(overlap_start_global);

            let overlap_len = u32::try_from(overlap).unwrap_or(length);

            if overlap_len > 0 {
                let file_local_offset = overlap_start_global - entry.global_offset;

                mappings.push(ChunkSource {
                    path: Arc::clone(&entry.path),
                    offset: file_local_offset,
                    length: overlap_len,
                });
            }
        }

        mappings
    }
}

/// The files in `paths` presented as one continuous byte stream, as if
/// concatenated in order.
#[derive(Debug)]
pub struct GlobalStream {
    paths: VecDeque<PathBuf>,
    current_file: Option<File>,
}

impl GlobalStream {
    /// Build a stream over `paths`, opening each one as it is reached.
    #[instrument(level = "trace", skip(paths))]
    pub fn new<P: Into<PathBuf>, I: Iterator<Item = P>>(paths: I) -> Self {
        let paths = paths.map(Into::into).collect();

        Self {
            paths,
            current_file: None,
        }
    }
}

impl Read for GlobalStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if let Some(ref mut file) = self.current_file {
                match file.read(buf) {
                    Ok(0) => {
                        self.current_file = None;
                        continue;
                    }
                    Ok(n) => return Ok(n),
                    Err(e) => return Err(e),
                }
            }

            match self.paths.pop_front() {
                Some(path) => {
                    let file = File::open(path)?;
                    self.current_file = Some(file);
                }
                None => return Ok(0),
            }
        }
    }
}

/// Content-defined chunking parameters.
///
/// Boundaries come from the content, so inserting or removing bytes shifts only
/// the chunks around the change rather than every chunk after it.
///
/// The three sizes are the whole public surface of the chunker's policy, and
/// [`ChunkerProfile`] is how they are chosen. Every backup the binary has written
/// used [`Default`], which is also what `--chunk-profile default` resolves to.
/// Keeping the values here rather than at the call site means a second
/// configuration cannot be a slightly different one by accident.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkerConfig {
    /// No boundary is taken before this many bytes.
    pub min_size: u32,
    /// The size the boundary probability curve is centred on.
    pub avg_size: u32,
    /// A boundary is forced once this many bytes have accumulated.
    pub max_size: u32,
}

impl Default for ChunkerConfig {
    fn default() -> Self {
        Self {
            min_size: 8 * 1024,
            avg_size: 16 * 1024,
            max_size: 64 * 1024,
        }
    }
}

impl ChunkerConfig {
    /// Check these parameters are ones fastcdc will actually chunk with.
    ///
    /// fastcdc asserts its own bounds with `debug_assert!`, so they are **gone
    /// from a release build**. A value it would refuse then reaches the chunker.
    /// It produces chunks of a nonsense shape rather than an error. Validating here
    /// is what makes a bad configuration a message instead of a corrupt backup.
    ///
    /// # Errors
    ///
    /// Returns an error naming the bound that was not met. It names a bound of
    /// fastcdc's own limits, or the ordering the three sizes must satisfy.
    pub fn validate(&self) -> io::Result<()> {
        let (min, avg, max) = (
            usize::try_from(self.min_size).unwrap_or(usize::MAX),
            usize::try_from(self.avg_size).unwrap_or(usize::MAX),
            usize::try_from(self.max_size).unwrap_or(usize::MAX),
        );

        let bounds = [
            ("min_size", min, ronomon::MINIMUM_MIN, ronomon::MINIMUM_MAX),
            ("avg_size", avg, ronomon::AVERAGE_MIN, ronomon::AVERAGE_MAX),
            ("max_size", max, ronomon::MAXIMUM_MIN, ronomon::MAXIMUM_MAX),
        ];

        for (name, value, lowest, highest) in bounds {
            if value < lowest || value > highest {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "{name} is {value} bytes, outside the {lowest}..={highest} that the \
                         chunker accepts"
                    ),
                ));
            }
        }

        // The ordering is not fastcdc's assertion but its algorithm's. A boundary
        // is only looked for above `min`, and `avg` sets the masks the gear
        // table is indexed with. So `avg <= min` chunks at `min` every time, and
        // `max < avg` can never reach the forced boundary. Both are silent, so
        // both are refused.
        if self.avg_size <= self.min_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "avg_size ({}) must be greater than min_size ({}), or every chunk comes out \
                     the same length",
                    self.avg_size, self.min_size
                ),
            ));
        }

        if self.max_size <= self.avg_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "max_size ({}) must be greater than avg_size ({}), or the forced boundary is \
                     never reached",
                    self.max_size, self.avg_size
                ),
            ));
        }

        Ok(())
    }
}

/// A named [`ChunkerConfig`]: three chunk sizes that were chosen rather than
/// calculated.
///
/// How big a chunk should be is a trade, and the trade depends on the tree.
/// Finer cuts deduplicate a *localised* edit better. A boundary is then more
/// likely to land on the same content in both the old and the new copy. Coarser
/// cuts mean fewer records, less framing and less to read back, which is what
/// matters once the content stops compressing at all. Almost nobody can place
/// themselves on that curve for a tree they have not measured. Being wrong is
/// only visible as a worse ratio weeks later. So the sizes live here and the
/// command line asks for one by name.
///
/// **A profile is not recorded in the store.** Chunks are named by their contents.
/// A set written at two different profiles is still one store. Every version of
/// it restores, and what differs is how much a run deduplicates against the
/// others. Moving a set to a different profile costs a full rewrite the first
/// time, and nothing but a worse ratio afterwards.
///
/// [`ChunkerConfig`] remains the way to state sizes that no name covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkerProfile {
    /// Fine cuts: 2 KiB minimum, 4 KiB average, 16 KiB maximum.
    ///
    /// For trees of many small files. A file shorter than one default chunk is a
    /// file stored whole, and an edit to it re-uploads all of it. At this size the
    /// edit costs the chunks around it instead. The price is records, four times
    /// as many of them, each with its own header and index entry.
    Small,
    /// The default: 8 KiB minimum, 16 KiB average, 64 KiB maximum.
    ///
    /// What every `blu backup` has used so far, and the safest thing to leave
    /// a set on. A profile nobody chose beats a profile chosen once and forgotten.
    Default,
    /// Coarse cuts: 32 KiB minimum, 64 KiB average, 256 KiB maximum.
    ///
    /// For content that does not deduplicate anyway, such as video or archives of
    /// already compressed files. A finer cut buys a marginally better match and
    /// costs a record for every chunk of every version. It also gives a pack the
    /// coarse ranges a client downloading a whole file wants.
    Large,
}

impl ChunkerProfile {
    /// Every profile, finest cut first, which is the order they are offered in.
    pub const ALL: [Self; 3] = [Self::Small, Self::Default, Self::Large];

    /// The name this profile is typed and written as.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Default => "default",
            Self::Large => "large",
        }
    }

    /// The chunker parameters this profile stands for.
    ///
    /// Each one validates. A profile that did not would be a bad backup rather
    /// than an error, because `fastcdc` asserts its bounds with `debug_assert!`. A
    /// release build does not contain them.
    #[must_use]
    pub fn config(self) -> ChunkerConfig {
        match self {
            // Not restated, deliberately. This profile *is* the default
            // configuration, so naming it that way is what keeps the two from
            // drifting into a pair of nearly equal answers.
            Self::Default => ChunkerConfig::default(),
            Self::Small => ChunkerConfig {
                min_size: 2 * 1024,
                avg_size: 4 * 1024,
                max_size: 16 * 1024,
            },
            Self::Large => ChunkerConfig {
                min_size: 32 * 1024,
                avg_size: 64 * 1024,
                max_size: 256 * 1024,
            },
        }
    }

    /// Every profile's name, comma-separated. This is what there was to choose
    /// from, for an error that has to be actionable on its own.
    #[must_use]
    pub fn names() -> String {
        Self::ALL.map(Self::name).join(", ")
    }
}

impl Default for ChunkerProfile {
    /// The profile a backup uses when nothing asks for another one.
    fn default() -> Self {
        Self::Default
    }
}

impl std::fmt::Display for ChunkerProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for ChunkerProfile {
    type Err = String;

    /// A profile name, in any case.
    ///
    /// The error is a [`String`] so that `clap`'s derived parser can turn it into
    /// a usage error. That error quotes what was typed and what there was instead.
    /// Failing the parse is the whole point. A backup cut at a size nobody
    /// asked for is not a thing to discover from a storage ratio weeks later.
    ///
    /// [`String`]: std::string::String
    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|profile| profile.name().eq_ignore_ascii_case(name))
            .ok_or_else(|| {
                format!(
                    "`{name}` is not a chunk profile; use one of {}",
                    Self::names()
                )
            })
    }
}

/// Splits a set of files into chunks, yielding each chunk's contents along with
/// the file slices it covers.
pub struct Chunker {
    registry: Arc<FileRegistry>,
    cdc_iter: StreamCDC<GlobalStream>,
}

impl Debug for Chunker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `StreamCDC` is not `Debug`.
        f.debug_struct("Chunker")
            .field("files", &self.registry.entries.len())
            .finish_non_exhaustive()
    }
}

impl Chunker {
    /// Chunk `paths` according to `config`.
    ///
    /// # Errors
    ///
    /// Returns an error if any file cannot be stat'ed while being registered, or
    /// if `config` is not one the chunker can chunk with. The second check is
    /// here rather than at the call site because it is the last point before the
    /// parameters stop being checked at all. `fastcdc` asserts its bounds with
    /// `debug_assert!`, which a release build does not contain.
    #[instrument(level = "trace", skip(paths, config), err)]
    pub fn new(paths: Vec<PathBuf>, config: ChunkerConfig) -> io::Result<Self> {
        config.validate()?;

        let registry = Arc::new(FileRegistry::new(paths.iter())?);

        // Bounds arrive as `u32` and are widened. A chunk longer than `u32::MAX`
        // is caught in `next`, where it can be reported against the chunk that
        // caused it.
        let widen = |value: u32| usize::try_from(value).unwrap_or(usize::MAX);

        let stream = GlobalStream::new(paths.iter());
        let cdc = StreamCDC::new(
            stream,
            widen(config.min_size),
            widen(config.avg_size),
            widen(config.max_size),
        );

        Ok(Self {
            registry,
            cdc_iter: cdc,
        })
    }
}

impl Iterator for Chunker {
    type Item = io::Result<Chunk>;

    #[instrument(level = "debug", skip_all)]
    fn next(&mut self) -> Option<Self::Item> {
        let cdc_chunk = self.cdc_iter.next()?;
        let cdc_chunk = match cdc_chunk {
            Ok(v) => v,
            Err(e) => return Some(Err(io::Error::other(e))),
        };

        // A configuration error, reported rather than narrowed. Truncating would
        // map the chunk to the wrong files.
        let Ok(length) = u32::try_from(cdc_chunk.length) else {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "chunker produced a {} byte chunk, over the u32 limit",
                    cdc_chunk.length
                ),
            )));
        };

        let sources = self.registry.resolve_chunk(cdc_chunk.offset, length);

        Some(Ok(Chunk {
            sources,
            data: cdc_chunk.data,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{ChunkerConfig, ChunkerProfile};
    use std::str::FromStr as _;

    /// A configuration the chunker accepts, for the cases that mutate one field.
    fn good() -> ChunkerConfig {
        ChunkerConfig {
            min_size: 8 * 1024,
            avg_size: 16 * 1024,
            max_size: 64 * 1024,
        }
    }

    /// A good configuration is accepted, so the cases below are testing the
    /// checks rather than a configuration that was never valid.
    #[test]
    fn an_ordinary_configuration_validates() {
        assert!(good().validate().is_ok());
    }

    /// Sizes below fastcdc's own floor are refused.
    #[test]
    fn a_min_size_below_the_floor_is_refused() {
        let config = ChunkerConfig {
            min_size: 1,
            ..good()
        };
        let error = config.validate().unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            error.to_string().contains("min_size is 1 bytes"),
            "the error should name the field and its value, got: {error}"
        );
    }

    /// So are sizes above its ceiling.
    #[test]
    fn an_avg_size_above_the_ceiling_is_refused() {
        let config = ChunkerConfig {
            avg_size: 512 * 1024 * 1024,
            max_size: 1024 * 1024 * 1024,
            ..good()
        };
        assert_eq!(
            config.validate().unwrap_err().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }

    /// `avg <= min` is silent in fastcdc, because every chunk comes out at `min`.
    /// It is therefore refused rather than left to produce a uniform-chunked
    /// backup.
    #[test]
    fn an_avg_at_or_below_min_is_refused() {
        let config = ChunkerConfig {
            min_size: 32 * 1024,
            avg_size: 32 * 1024,
            ..good()
        };
        let error = config.validate().unwrap_err();
        assert!(
            error.to_string().contains("must be greater than min_size"),
            "the error should say which way round they are, got: {error}"
        );
    }

    /// Likewise `max <= avg`. The forced boundary is never reached, so `max`
    /// does nothing and only the average is in play.
    #[test]
    fn a_max_at_or_below_avg_is_refused() {
        let config = ChunkerConfig {
            avg_size: 32 * 1024,
            max_size: 32 * 1024,
            ..good()
        };
        let error = config.validate().unwrap_err();
        assert!(
            error.to_string().contains("must be greater than avg_size"),
            "the error should say which way round they are, got: {error}"
        );
    }

    /// Every profile the command line offers is one the chunker can chunk with.
    ///
    /// Checked here rather than trusted, because a profile is a set of numbers
    /// typed into a match arm. Fastcdc's bounds are `debug_assert!`s, and a
    /// release build does not contain them. One out of range would be a bad
    /// backup rather than a message. [`ChunkerProfile::ALL`] is what `--help` and
    /// the parse error list, so anything not in it cannot be named at all.
    #[test]
    fn every_profile_is_a_configuration_the_chunker_accepts() {
        for profile in ChunkerProfile::ALL {
            profile
                .config()
                .validate()
                .unwrap_or_else(|e| panic!("profile {profile} is not chunkable: {e}"));
        }
    }

    /// The `default` profile is the default configuration, and cannot stop being
    /// it.
    ///
    /// [`ChunkerConfig::default`] is what a library caller gets without asking.
    /// `--chunk-profile default` is what the binary gets without asking. They are
    /// the same numbers, so a test that means "whatever the command line does"
    /// means one thing. This is what pins them together. `config()` reads the
    /// default rather than restating it, and this says why that is not a shortcut.
    #[test]
    fn the_default_profile_is_the_default_configuration() {
        assert_eq!(
            ChunkerProfile::default().config(),
            ChunkerConfig::default(),
            "the default profile and ChunkerConfig::default should be one configuration"
        );
        assert_eq!(
            ChunkerProfile::default(),
            ChunkerProfile::Default,
            "and the profile that should be the default is the one named default"
        );
    }

    /// The profiles are ordered by how finely they cut, and each says the same
    /// thing as its name.
    ///
    /// The ordering is the point of `ALL`, because it is what the flag's help and
    /// the parse error present. A list a reader cannot predict the order of is a
    /// list they cannot choose from.
    #[test]
    fn the_profiles_run_from_finest_to_coarsest() {
        let cuts: [ChunkerConfig; 3] = ChunkerProfile::ALL.map(ChunkerProfile::config);

        for pair in cuts.windows(2) {
            let (finer, coarser) = (&pair[0], &pair[1]);
            assert!(
                finer.avg_size < coarser.avg_size,
                "{finer:?} should cut more finely than {coarser:?}"
            );
        }

        for (profile, name) in ChunkerProfile::ALL.map(|p| (p, p.name())) {
            assert_eq!(
                profile.to_string(),
                name,
                "a profile should print as the name it is typed as"
            );
        }
    }

    /// A name that is not a profile says so, and says what there is instead.
    ///
    /// Both halves are the assertion. "unknown profile" alone leaves the user to
    /// run `--help` to find out what profiles exist. A message that lists the
    /// names without quoting the one that was refused cannot be matched against
    /// what they typed.
    #[test]
    fn an_unknown_profile_name_names_the_alternatives() {
        let error = ChunkerProfile::from_str("fine").unwrap_err();

        assert!(
            error.contains("`fine` is not a chunk profile"),
            "the error should quote the name that was refused, got: {error}"
        );
        for name in ChunkerProfile::ALL.map(ChunkerProfile::name) {
            assert!(
                error.contains(name),
                "the error should offer {name}, got: {error}"
            );
        }

        // And the profiles themselves parse, in any case, because a name typed
        // in caps is still the name.
        for profile in ChunkerProfile::ALL {
            let name = profile.name();
            assert_eq!(
                ChunkerProfile::from_str(name).unwrap(),
                profile,
                "{name} should parse as itself"
            );
            assert_eq!(
                ChunkerProfile::from_str(&name.to_uppercase()).unwrap(),
                profile,
                "{name} should parse however it is capitalised"
            );
        }
    }
}

/// Measure `resolve_chunk`, whose per-chunk work is one path clone.
///
/// The path in a [`ChunkSource`] is an [`Arc`] rather than a `PathBuf`. Building
/// one is therefore a refcount bump instead of an allocation plus a copy of the
/// path. That is the only thing `resolve_chunk` does that touches memory beyond
/// its output vector. That makes it measurable in a way an end-to-end backup is
/// not. A backup spends its time on per-record filesystem calls, and a saved
/// allocation per chunk does not show up there at all.
///
/// So this drives `resolve_chunk` directly, over a registry of enough files that a
/// chunk lands inside one of them.
///
/// **It asserts that every call resolved something. That assertion is the
/// point.** The first version of this benchmark built a registry of *empty* files
/// and reported 10ns per chunk for two milliseconds. The whole measurement was two
/// hundred real resolutions and a hundred and ninety-nine thousand iterations that
/// returned an empty vector. Those iterations found the global offset past the end
/// of the set. It looked like a working benchmark. Counting the sources is what
/// makes the difference visible, so the count is checked on every run.
///
/// Ignored by default. Run it with:
///
/// ```text
/// cargo test --release -- --ignored --nocapture resolve_chunk
/// ```
#[test]
#[ignore = "builds a registry and takes a second; run it by hand"]
fn resolve_chunk_cost() {
    /// Files in the registry. Enough that a chunk lands in one of them, so the
    /// clone is the per-chunk work rather than a special case.
    const FILES: usize = 200;
    /// Bytes per file, and per chunk, so a chunk lands inside one file rather than
    /// spanning several. A registry of empty files resolves to nothing at all.
    const FILE_BYTES: usize = 64 * 1024;
    /// Resolutions per round, wrapping over the registry so every one of them is
    /// a chunk that exists.
    const CHUNKS: u32 = 200_000;

    let dir = tempfile::tempdir().expect("a temp dir for the registry");
    let paths: Vec<PathBuf> = (0..FILES)
        .map(|i| {
            let path = dir.path().join(format!("f{i}.bin"));
            std::fs::write(&path, vec![0u8; FILE_BYTES]).expect("a file");
            path
        })
        .collect();

    let registry = FileRegistry::new(paths.into_iter()).expect("a registry");

    let mut best = f64::MAX;
    let mut sources = 0usize;
    for _ in 0..5 {
        let started = std::time::Instant::now();
        sources = 0;
        for n in 0..CHUNKS {
            let file = u64::from(n % u32::try_from(FILES).expect("fits in u32"));
            let at = file * u64::try_from(FILE_BYTES).expect("fits in u64");
            sources += registry
                .resolve_chunk(at, u32::try_from(FILE_BYTES).expect("fits in u32"))
                .len();
        }
        best = best.min(started.elapsed().as_secs_f64());
    }

    assert_eq!(
        sources,
        usize::try_from(CHUNKS).expect("fits in usize"),
        "only {sources} of {CHUNKS} calls resolved a file, so the rest measured an \
         empty return — this benchmark is not measuring what it claims to"
    );

    println!(
        "\n{FILES} files, {CHUNKS} chunks, {sources} sources: {:.1}ms ({:.0} ns/chunk)",
        best * 1000.0,
        best * 1e9 / f64::from(CHUNKS)
    );
}
