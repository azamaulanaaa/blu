//! Exclude backup paths with gitignore rules.
//!
//! [`IgnoreMatcher`] reads gitignore lines and answers paths. Both
//! `.bluignore` and `.gitignore` share this matcher and differ only in
//! discovery. The backup walk stacks one matcher from parent ignore files down
//! to child ones and consults CLI lines last. See [`IgnoreOptions`] for the
//! switches behind that walk.

use std::path::{Path, PathBuf};

/// One parsed gitignore line with the directory it was read from.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Rule {
    /// Ignore file directory relative to the backup root. Empty names root.
    base: PathBuf,
    /// True for a `!` line. A match re-includes the path.
    negated: bool,
    /// True for a trailing slash line. A match needs a directory.
    dir_only: bool,
    /// True for a line holding a slash. A match starts at the base.
    anchored: bool,
    /// Pattern split at each slash.
    parts: Vec<Vec<u8>>,
}

impl Rule {
    /// Answer this rule for a backup relative path.
    ///
    /// Return true for exclude and false for re-include. Return nothing where
    /// the rule names another directory or misses the path.
    fn decide(&self, path: &[u8], is_dir: bool) -> Option<bool> {
        let base = self.base.as_os_str().as_encoded_bytes();
        let sub = if base.is_empty() {
            path
        } else {
            let rest = path.strip_prefix(base)?;
            let rest = rest.strip_prefix(b"/")?;
            if rest.is_empty() {
                return None;
            }
            rest
        };
        let parts: Vec<&[u8]> = sub.split(|byte| *byte == b'/').collect();
        let last_start = if self.anchored { 0 } else { parts.len() - 1 };
        for start in 0..=last_start {
            for end in start + 1..=parts.len() {
                if match_parts(&self.parts, &parts[start..end]) {
                    // A shorter match names an ancestor directory of the path.
                    // An excluded directory hides everything below it.
                    if end < parts.len() {
                        return Some(!self.negated);
                    }
                    if !self.dir_only || is_dir {
                        return Some(!self.negated);
                    }
                }
            }
        }
        None
    }
}

/// Answers whether a root relative path is excluded by gitignore rules.
///
/// Push lines in merge order with [`IgnoreMatcher::push_line`]. Later lines
/// win over earlier lines. [`IgnoreMatcher::decide`] reports the last matching
/// line. [`IgnoreMatcher::is_excluded`] answers exclusion directly.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IgnoreMatcher {
    rules: Vec<Rule>,
}

impl IgnoreMatcher {
    /// Build an empty matcher.
    ///
    /// Push lines before asking it anything. An empty matcher excludes nothing.
    #[must_use]
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Parse `line` read from `base` and keep the rule.
    ///
    /// Skip blank lines and `#` comments. Read `base` as the ignore file
    /// directory relative to the backup root. Pass an empty `base` for root
    /// level lines such as CLI `--exclude` values.
    pub fn push_line(&mut self, base: &Path, line: &str) {
        if let Some(rule) = parse_rule(base, line) {
            self.rules.push(rule);
        }
    }

    /// Parse `content` line by line.
    ///
    /// Treat `content` as read from `base`. Blank lines and `#` comments
    /// change nothing. See [`IgnoreMatcher::push_line`] for how `base` reads.
    pub fn push_str(&mut self, base: &Path, content: &str) {
        for line in content.lines() {
            self.push_line(base, line);
        }
    }

    /// Answer the last rule matching `path`.
    ///
    /// Return true for exclude and false for re-include. Return nothing where
    /// no rule matches. Read `path` relative to the backup root. Read `is_dir`
    /// as true for directories so trailing slash rules match them only.
    #[must_use]
    pub fn decide(&self, path: &Path, is_dir: bool) -> Option<bool> {
        let bytes = path.as_os_str().as_encoded_bytes();
        if bytes.is_empty() {
            return None;
        }
        let mut out = None;
        for rule in &self.rules {
            if let Some(hit) = rule.decide(bytes, is_dir) {
                out = Some(hit);
            }
        }
        out
    }

    /// Answer whether rules exclude `path`.
    ///
    /// Return false where no rule matches. Read `path` relative to the backup
    /// root. Read `is_dir` as true for directories so trailing slash rules
    /// match them only.
    #[must_use]
    pub fn is_excluded(&self, path: &Path, is_dir: bool) -> bool {
        self.decide(path, is_dir).unwrap_or(false)
    }
}

/// Which ignore files and CLI lines exclude paths from a backup.
///
/// The walk reads `.gitignore` and `.bluignore` in every visited directory by
/// default. Parent files come first and deeper files win. CLI lines come last
/// and win over every file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IgnoreOptions {
    /// Gitignore lines applied at the backup root.
    ///
    /// Later lines win over earlier lines and over ignore files. Fill from CLI
    /// `--exclude`.
    pub patterns: Vec<String>,
    /// Ignore files holding one gitignore line each.
    ///
    /// Lines read as if from the backup root. A missing file fails the run.
    /// Fill from CLI `--exclude-file`.
    pub files: Vec<PathBuf>,
    /// Read `.bluignore` files while walking.
    ///
    /// True by default. CLI `--no-bluignore` clears it.
    pub read_bluignore: bool,
    /// Read `.gitignore` files while walking.
    ///
    /// True by default. CLI `--no-gitignore` clears it.
    pub read_gitignore: bool,
}

impl Default for IgnoreOptions {
    /// Read both ignore file kinds and no CLI lines.
    ///
    /// This default matches what the CLI runs without flags.
    fn default() -> Self {
        Self {
            patterns: Vec::new(),
            files: Vec::new(),
            read_bluignore: true,
            read_gitignore: true,
        }
    }
}

impl IgnoreOptions {
    /// Read both ignore file kinds and no CLI lines.
    ///
    /// This constructor matches what the CLI runs without flags.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

/// Parse one gitignore line read from `base`.
///
/// Return nothing for blank lines and `#` comments. Handle `\#` and `\!`
/// escapes and trailing slash and leading slash forms here so matching stays
/// free of syntax.
fn parse_rule(base: &Path, line: &str) -> Option<Rule> {
    let mut bytes = line.as_bytes();
    if let Some(stripped) = bytes.strip_suffix(b"\r") {
        bytes = stripped;
    }
    bytes = trim_unescaped_trailing_spaces(bytes);
    if bytes.is_empty() {
        return None;
    }
    if bytes.starts_with(b"#") {
        return None;
    }
    let mut negated = false;
    if let Some(rest) = bytes.strip_prefix(b"!") {
        negated = true;
        bytes = rest;
        if bytes.is_empty() {
            return None;
        }
    }
    // An escaped marker names a literal byte. An unescaped one stays syntax.
    if bytes.len() >= 2 && bytes[0] == b'\\' && (bytes[1] == b'#' || bytes[1] == b'!') {
        bytes = &bytes[1..];
    }
    let mut anchored = false;
    if let Some(rest) = bytes.strip_prefix(b"/") {
        anchored = true;
        bytes = rest;
    }
    let mut dir_only = false;
    if let Some(rest) = bytes.strip_suffix(b"/") {
        dir_only = true;
        bytes = rest;
    }
    if bytes.is_empty() {
        return None;
    }
    if bytes.contains(&b'/') {
        anchored = true;
    }
    let mut parts: Vec<Vec<u8>> = bytes
        .split(|byte| *byte == b'/')
        .map(<[u8]>::to_vec)
        .collect();
    // A leading double star matches at every depth below the base. Drop it
    // and match the rest anywhere instead of anchoring it.
    let mut dropped = false;
    while parts.first().is_some_and(|part| part.as_slice() == b"**") {
        parts.remove(0);
        dropped = true;
    }
    if parts.is_empty() {
        parts.push(b"**".to_vec());
    } else if dropped {
        anchored = false;
    }
    Some(Rule {
        base: base.to_path_buf(),
        negated,
        dir_only,
        anchored,
        parts,
    })
}

/// Drop trailing spaces the pattern does not escape.
///
/// A backslash quotes the space after it. Two backslashes quote each other and
/// leave the space after them bare.
fn trim_unescaped_trailing_spaces(mut bytes: &[u8]) -> &[u8] {
    while bytes.last() == Some(&b' ') && !is_escaped(bytes, bytes.len() - 1) {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

/// Answer whether the byte at `at` is quoted by a backslash.
///
/// Count the backslashes directly before it. An odd count quotes it.
fn is_escaped(bytes: &[u8], at: usize) -> bool {
    let mut count = 0;
    let mut index = at;
    while index > 0 && bytes[index - 1] == b'\\' {
        count += 1;
        index -= 1;
    }
    count % 2 == 1
}

/// Whether split pattern parts match split path parts.
///
/// `**` as a whole part spans nested parts. Any other part matches exactly one
/// path part. See [`component_match`] for the byte dialect inside one part.
fn match_parts(pattern: &[Vec<u8>], path: &[&[u8]]) -> bool {
    if pattern.is_empty() {
        return path.is_empty();
    }
    if pattern[0].as_slice() == b"**" {
        return (0..=path.len()).any(|at| match_parts(&pattern[1..], &path[at..]));
    }
    if path.is_empty() {
        return false;
    }
    component_match(&pattern[0], path[0]) && match_parts(&pattern[1..], &path[1..])
}

/// Whether one pattern part matches one path part.
///
/// `*` spans any bytes inside the part. `?` spans one byte. `[...]` spans one
/// listed byte. A backslash names the next byte literally.
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
/// Read an optional `!` or `^` negation with ranges and backslash escapes. Read
/// a `]` in first position as a literal member. Return nothing for an
/// unclosed class. The caller reads that as a literal `[`.
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

#[cfg(test)]
mod tests {
    use super::IgnoreMatcher;
    use std::path::Path;

    /// A matcher holding `lines` at the backup root.
    fn root_matcher(lines: &[&str]) -> IgnoreMatcher {
        let mut matcher = IgnoreMatcher::new();
        for line in lines {
            matcher.push_line(Path::new(""), line);
        }
        matcher
    }

    /// Whether `lines` at the backup root exclude `path`.
    fn excluded(lines: &[&str], path: &str, is_dir: bool) -> bool {
        root_matcher(lines).is_excluded(Path::new(path), is_dir)
    }

    #[test]
    fn a_blank_line_and_a_comment_exclude_nothing() {
        assert!(!excluded(
            &["", "   ", "# build output", "#!kept"],
            "a.txt",
            false
        ));
        assert!(!excluded(&["", "   ", "# build output"], "build", true));
    }

    #[test]
    fn a_escaped_hash_matches_a_literal_hash_name() {
        assert!(excluded(&["\\#draft.txt"], "#draft.txt", false));
        assert!(!excluded(&["\\#draft.txt"], "draft.txt", false));
    }

    #[test]
    fn an_escaped_bang_matches_a_literal_bang_name() {
        assert!(excluded(&["\\!note.txt"], "!note.txt", false));
        assert!(!excluded(&["\\!note.txt"], "note.txt", false));
    }

    #[test]
    fn a_negation_restores_an_excluded_path() {
        assert!(excluded(&["*.log"], "debug.log", false));
        assert!(!excluded(&["*.log", "!keep.log"], "keep.log", false));
        assert!(excluded(&["*.log", "!keep.log"], "debug.log", false));
    }

    #[test]
    fn a_later_line_wins_over_an_earlier_line() {
        assert!(excluded(
            &["*.log", "!keep.log", "keep.log"],
            "keep.log",
            false
        ));
        assert!(!excluded(
            &["keep.log", "*.log", "!keep.log"],
            "keep.log",
            false
        ));
    }

    #[test]
    fn a_leading_slash_anchors_a_name_to_its_base() {
        assert!(excluded(&["/root.log"], "root.log", false));
        assert!(!excluded(&["/root.log"], "sub/root.log", false));
    }

    #[test]
    fn a_bare_name_matches_at_any_depth() {
        assert!(excluded(&["*.log"], "sub/deep/a.log", false));
        assert!(excluded(&["target"], "a/b/target", true));
    }

    #[test]
    fn a_slash_inside_anchors_a_pattern_to_its_base() {
        assert!(excluded(&["doc/*.log"], "doc/a.log", false));
        assert!(!excluded(&["doc/*.log"], "other/doc/a.log", false));
        assert!(excluded(&["sub/file.txt"], "sub/file.txt", false));
        assert!(!excluded(&["sub/file.txt"], "other/sub/file.txt", false));
    }

    #[test]
    fn a_trailing_slash_excludes_a_dir_tree_only() {
        assert!(excluded(&["build/"], "build", true));
        assert!(excluded(&["build/"], "build/a.o", false));
        assert!(excluded(&["build/"], "x/build/y.o", false));
    }

    #[test]
    fn a_trailing_slash_keeps_a_file_with_the_same_name() {
        assert!(!excluded(&["build/"], "build", false));
        assert!(!excluded(&["build/"], "notes.txt", false));
    }

    #[test]
    fn a_double_star_pair_spans_nested_dirs() {
        assert!(excluded(&["a/**/b"], "a/b", true));
        assert!(excluded(&["a/**/b"], "a/x/y/b", true));
        assert!(!excluded(&["a/**/b"], "a/x/c", true));
        assert!(!excluded(&["a/**/b"], "x/a/b", true));
    }

    #[test]
    fn a_leading_double_star_matches_at_every_depth() {
        assert!(excluded(&["**/foo"], "foo", false));
        assert!(excluded(&["**/foo"], "x/foo", false));
        assert!(excluded(&["**/foo"], "x/y/foo", false));
        assert!(!excluded(&["**/foo"], "x/foobar", false));
    }

    #[test]
    fn a_trailing_double_star_covers_inside_paths() {
        assert!(excluded(&["abc/**"], "abc", true));
        assert!(excluded(&["abc/**"], "abc/x", false));
        assert!(excluded(&["abc/**"], "abc/x/y", false));
        assert!(!excluded(&["abc/**"], "other", true));
    }

    #[test]
    fn a_star_stops_at_a_separator() {
        assert!(excluded(&["*.rs"], "src/main.rs", false));
        assert!(!excluded(&["/*.rs"], "src/main.rs", false));
        assert!(excluded(&["*/*.rs"], "src/main.rs", false));
        assert!(!excluded(&["src/*.rs"], "src/nested/main.rs", false));
        assert!(excluded(&["src/**/*.rs"], "src/nested/main.rs", false));
    }

    #[test]
    fn a_class_matches_one_listed_byte() {
        assert!(excluded(&["f[ai]le.txt"], "file.txt", false));
        assert!(!excluded(&["f[ai]le.txt"], "fole.txt", false));
        assert!(excluded(&["f[a-z]le.txt"], "fole.txt", false));
        assert!(excluded(&["f[!ai]le.txt"], "fole.txt", false));
        assert!(!excluded(&["f[!ai]le.txt"], "file.txt", false));
    }

    #[test]
    fn a_deeper_base_wins_over_a_parent_base() {
        let mut matcher = IgnoreMatcher::new();
        matcher.push_line(Path::new(""), "*.log");
        matcher.push_line(Path::new("sub"), "!*.log");

        assert!(matcher.is_excluded(Path::new("top.log"), false));
        assert!(!matcher.is_excluded(Path::new("sub/a.log"), false));
        assert!(matcher.is_excluded(Path::new("other/a.log"), false));
    }

    #[test]
    fn a_child_base_ignores_paths_outside_it() {
        let mut matcher = IgnoreMatcher::new();
        matcher.push_line(Path::new("sub"), "*.log");

        assert!(!matcher.is_excluded(Path::new("top.log"), false));
        assert!(matcher.is_excluded(Path::new("sub/a.log"), false));
        assert!(!matcher.is_excluded(Path::new("other/a.log"), false));
    }

    #[test]
    fn an_excluded_dir_hides_paths_below_it() {
        assert!(excluded(&["build"], "build/a.o", false));
        assert!(excluded(&["build"], "build/nested/a.o", false));
        assert!(!excluded(&["build"], "other/a.o", false));
    }
}
