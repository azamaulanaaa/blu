//! The compression vocabulary shared by `pack` and `merge`.

use super::storage;

/// The compression policy from `--compression-level`, where 0 means "as-is".
///
/// clap rejects anything outside 0 to 9 as a usage error rather than letting it
/// be clamped. So a level is never silently turned into a store that disagrees
/// with the command that wrote it.
pub(crate) fn compression_policy(level: Option<u8>) -> storage::Compression {
    match level {
        Some(level) => storage::Compression::from_level(level),
        None => storage::Compression::default(),
    }
}

/// What a `--compression` flag asked for, on `pack` or on `merge`.
///
/// **One type and one parser for both commands, deliberately.** The two flags ask
/// the same question at the two ends of a transfer. That question is what the
/// records of this chunk should look like on the wire. A user who learns `keep`
/// on one of them has learned it on the other. Two types would let the
/// vocabularies drift. One command could come to accept a bare number. One
/// command could spell the default differently. The drift would then show up as a
/// flag that is accepted somewhere it is not meant. This is a little repetition
/// of the *routing*, which is genuinely different. A merge re-encodes on the way
/// into a set, and it has to open that set under a policy. A pack re-encodes on
/// the way out of a set, and it only reads that set. None of the repetition is
/// the parsing or the vocabulary. The parsing and the vocabulary are the part a
/// user meets.
///
/// Two answers rather than a policy on its own, because a level is both for a
/// merge. The level says which write path the run takes *and* what the receiving
/// set is opened with. The two cannot be allowed to disagree. A merge told to
/// re-encode into a set opened at a different level would store frames at a level
/// that nobody asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KeepOrLevel {
    /// Store what the pack carries, frame and all.
    Keep,
    /// Decode every chunk and store it at this zstd level, 0 meaning as-is.
    Level(u8),
}

impl KeepOrLevel {
    /// The policy the receiving set is opened with.
    ///
    /// The default under [`KeepOrLevel::Keep`], where it decides almost nothing.
    /// The verbatim path does not compress. So the only record that it writes
    /// through the store's own policy is the snapshot. This is still a real
    /// choice rather than `Off`. A set opened for writing has to be opened with
    /// something. The default is what every other command opens a set with.
    pub(crate) fn policy(self) -> storage::Compression {
        match self {
            Self::Keep => storage::Compression::default(),
            Self::Level(level) => storage::Compression::from_level(level),
        }
    }

    /// Which write path the merge takes per chunk.
    pub(crate) fn route(self) -> blulib::command::MergeCompression {
        match self {
            Self::Keep => blulib::command::MergeCompression::Keep,
            Self::Level(_) => blulib::command::MergeCompression::Reencode,
        }
    }

    /// The policy a pack writes each record under, or `None` to keep the form
    /// the set already holds.
    ///
    /// The one place the flag becomes a route for this command. `keep` is the
    /// absence of a policy, because it is the absence of any work. Both
    /// non-default answers are a policy that the pack's records are encoded
    /// under.
    pub(crate) fn pack_policy(self) -> Option<storage::Compression> {
        match self {
            Self::Keep => None,
            Self::Level(level) => Some(storage::Compression::from_level(level)),
        }
    }
}

/// The `--compression` argument of `pack` and `merge`: `keep`, or a level from 0
/// to 9.
///
/// Its own vocabulary rather than a bare number, so the default is a word
/// that says what it does. `keep` is the only non-numeric answer. Anything else
/// that is not a number in range is a usage error naming both. That is the same
/// treatment `backup --compression-level` gives an out-of-range level. A value
/// that this parser does not recognise must never become a policy that looks
/// like one the user chose.
///
/// The name says `keep` rather than either command, because it is both commands'.
pub(crate) fn parse_keep_or_level(what: &str) -> Result<KeepOrLevel, String> {
    if what.eq_ignore_ascii_case("keep") {
        return Ok(KeepOrLevel::Keep);
    }

    let level: u8 = what
        .parse()
        .map_err(|_| format!("`{what}` is not `keep` or a compression level from 0 to 9"))?;

    if level > storage::MAX_COMPRESSION_LEVEL {
        return Err(format!(
            "`{what}` is not `keep` or a compression level from 0 to {}",
            storage::MAX_COMPRESSION_LEVEL
        ));
    }

    Ok(KeepOrLevel::Level(level))
}
