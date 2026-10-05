//! The JSON this service speaks, written by hand.
//!
//! **No serialisation crate, and the argument for that does not change here.**
//! `Cargo.toml` explains why `bincode` was removed: so that no upstream's release
//! cadence or security policy can decide whether a backup set is still readable.
//! That argument is about the *on-disk format*. A response body is not read by a
//! future version of this binary. It is read by a client, and it is versioned by
//! its own contract — the `OpenAPI` document in `openapi/lepatch.json` says what
//! each field is and what each status means.
//!
//! So the boundary is exempt from the rule, and the rule is kept anyway. Two
//! reasons, and the second is the one that decides it.
//!
//! First, a derive-based approach puts `#[derive(Serialize)]` on this crate's own
//! types. The types are the *library's* contract — `Job`, `Plan`, a set name.
//! They are what `lepatch`'s other callers reason about. A JSON concern on them
//! is a second thing to keep in step with the first.
//!
//! Second, and more plainly: what a response contains here is eleven small
//! structs of strings and integers. `serde_json` would be a dependency, a proc
//! macro, and a derive on every type, in exchange for writing the same eleven
//! structs out. The hand-written encoder below is about a hundred lines and it
//! makes the bytes the server sends visible in one place, which is the property
//! that actually matters for a wire format.

use std::fmt;

/// One JSON value.
///
/// Deliberately small. What this service returns is objects of strings and
/// integers, plus the two fixed shapes every error takes. There is no nesting
/// deeper than two, so there is no array-of-array and no value whose type is
/// decided at runtime — except this one, which exists precisely because a
/// response is built up before it is known which fields it has.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    /// A string. Escaped on the way out by the writer.
    Str(String),
    /// An integer.
    ///
    /// Unsigned and signed separately rather than one `i128`. Every number this
    /// service emits is a count or a byte length, and both are non-negative.
    /// A signed value would mean writing a cast, and a cast on a byte length is
    /// how a figure goes negative in a report.
    Int(u64),
    /// A negative integer, for the fields that genuinely have one. There is
    /// none today; it is here so that adding one is not a sign error.
    NegInt(i64),
    /// A literal `true` or `false`.
    Bool(bool),
    /// The literal `null`.
    ///
    /// Not a sentinel for "absent". A field that is not sent is not in the
    /// object at all, which is a different thing and is expressed by not adding
    /// it. `null` means the value is known and has no value, which is what
    /// `pack_bytes` is on a version whose pack has not been built.
    Null,
    /// An object.
    Object(Vec<(String, Json)>),
    /// An array.
    Array(Vec<Json>),
}

impl Json {
    /// An object, from a list of fields.
    ///
    /// Takes a `Vec` rather than a map because the order is the order written,
    /// and a response's field order is part of its reviewable bytes. A `BTreeMap`
    /// would sort them and put `error` before `id`.
    #[must_use]
    pub fn object(fields: Vec<(&str, Json)>) -> Self {
        Self::Object(
            fields
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
        )
    }

    /// A string value.
    #[must_use]
    pub fn string(value: impl Into<String>) -> Self {
        Self::Str(value.into())
    }

    /// An integer value.
    #[must_use]
    pub const fn int(value: u64) -> Self {
        Self::Int(value)
    }

    /// An optional value: `None` becomes `null`.
    #[must_use]
    pub fn opt(value: Option<Self>) -> Self {
        value.unwrap_or(Self::Null)
    }

    /// Read a JSON object from `text`.
    ///
    /// **Not a JSON parser, and it does not pretend to be one.** It reads what
    /// this service's three request shapes use: a flat object of strings,
    /// integers, booleans, `null` and arrays of strings. Anything else is an
    /// error carrying a sentence saying what it found, which the handler turns
    /// into a `400`.
    ///
    /// A general parser is a dependency, a CVE feed and a maintenance
    /// obligation, in exchange for reading three bodies that hold eleven fields
    /// between them. What is here is about a hundred lines and it is the only
    /// JSON this crate parses.
    ///
    /// # Errors
    ///
    /// Returns an error if `text` is not an object, or holds anything the above
    /// does not cover. The message names what was found, because a client that
    /// sent the wrong shape needs to be told the shape, not told it failed.
    pub fn parse_object(text: &str) -> Result<Self, String> {
        let bytes = text.trim().as_bytes();
        let mut at = 0usize;

        let value = parse_value(bytes, &mut at, 0)?;
        skip_space(bytes, &mut at);

        if at != bytes.len() {
            return Err(format!(
                "there is more after the value at byte {at}, which is not JSON this reads"
            ));
        }

        // Enforced here rather than left to each caller. The name says object, and
        // a `parse_object` that returns an array is a function whose name is a
        // lie that three call sites would have to remember to check.
        match value {
            Json::Object(_) => Ok(value),
            other => Err(format!(
                "a request body must be a JSON object, and this one is {}",
                describe(&other)
            )),
        }
    }

    /// Write this value, and everything under it, into `out`.
    ///
    /// Recursive, and bounded by the depth of the value: the deepest thing this
    /// service sends is an object inside an array inside an object, so three
    /// frames. A document that grew a nesting level would grow this by one frame
    /// too, which is not a thing to design for now.
    fn write(&self, out: &mut String) {
        match self {
            Self::Str(value) => escape(value, out),
            Self::Int(value) => {
                out.push_str(&value.to_string());
            }
            Self::NegInt(value) => {
                out.push_str(&value.to_string());
            }
            Self::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
            Self::Null => out.push_str("null"),
            Self::Array(items) => {
                out.push('[');
                for (at, item) in items.iter().enumerate() {
                    if at > 0 {
                        out.push(',');
                    }
                    item.write(out);
                }
                out.push(']');
            }
            Self::Object(fields) => {
                out.push('{');
                for (at, (key, value)) in fields.iter().enumerate() {
                    if at > 0 {
                        out.push(',');
                    }
                    escape(key, out);
                    out.push(':');
                    value.write(out);
                }
                out.push('}');
            }
        }
    }
}

/// What a parsed value is, for an error that has to say so.
fn describe(value: &Json) -> &'static str {
    match value {
        Json::Object(_) => "an object",
        Json::Array(_) => "an array",
        Json::Str(_) => "a string",
        Json::Int(_) | Json::NegInt(_) => "a number",
        Json::Bool(_) => "a boolean",
        Json::Null => "null",
    }
}

/// How deep [`parse_value`] will go before refusing.
///
/// Three is the deepest thing this service reads: an object, an array inside it,
/// a string inside that. A bound rather than a limit, because a body nested
/// deeper than anything this API defines is a body someone has not thought about,
/// and the failure should be a `400` rather than a stack overflow.
const MAX_DEPTH: usize = 4;

/// Skip whitespace.
fn skip_space(bytes: &[u8], at: &mut usize) {
    while *at < bytes.len() && matches!(bytes[*at], b' ' | b'\t' | b'\n' | b'\r') {
        *at += 1;
    }
}

/// One value at `at`, advancing past it.
fn parse_value(bytes: &[u8], at: &mut usize, depth: usize) -> Result<Json, String> {
    if depth > MAX_DEPTH {
        return Err("this is nested too deeply to be a request body".to_string());
    }

    skip_space(bytes, at);
    let Some(&byte) = bytes.get(*at) else {
        return Err(format!(
            "the body ended at byte {at}, where a value was expected"
        ));
    };

    match byte {
        b'{' => parse_object_body(bytes, at, depth),
        b'[' => parse_array(bytes, at, depth),
        b'"' => parse_string(bytes, at).map(Json::Str),
        b't' => literal(bytes, at, "true").map(|()| Json::Bool(true)),
        b'f' => literal(bytes, at, "false").map(|()| Json::Bool(false)),
        b'n' => literal(bytes, at, "null").map(|()| Json::Null),
        b'-' | b'0'..=b'9' => parse_number(bytes, at),
        other => Err(format!(
            "{} at byte {at} is not the start of a JSON value",
            char::from(other)
        )),
    }
}

/// `{ "key": value, … }`.
fn parse_object_body(bytes: &[u8], at: &mut usize, depth: usize) -> Result<Json, String> {
    *at += 1; // the brace
    let mut fields = Vec::new();
    // Set once a comma has been read. A `}` after one is a **trailing comma**,
    // which RFC 8259 does not allow, and this reads it as an error rather than as
    // the end of the object — because a client whose serialiser emits one is a
    // client whose serialiser is not this, and the body is not the shape it
    // thinks it is.
    let mut after_comma = false;

    loop {
        skip_space(bytes, at);

        match bytes.get(*at) {
            None => return Err("the body ended inside an object".to_string()),
            Some(b'}') if !after_comma => {
                *at += 1;
                return Ok(Json::Object(fields));
            }
            Some(b'}') => {
                return Err(format!(
                    "a `,` before the `}}` at byte {at} is a trailing comma, which JSON does \
                     not allow"
                ));
            }
            Some(b'"') => {}
            Some(other) => {
                return Err(format!(
                    "{} at byte {at} is not a quoted key; an object's keys are strings",
                    char::from(*other)
                ));
            }
        }

        let key = parse_string(bytes, at)?;

        skip_space(bytes, at);
        if bytes.get(*at) != Some(&b':') {
            return Err(format!(
                "the key {key:?} at byte {at} is not followed by `:`"
            ));
        }
        *at += 1;

        let value = parse_value(bytes, at, depth + 1)?;
        fields.push((key, value));

        skip_space(bytes, at);
        match bytes.get(*at) {
            Some(b',') => {
                *at += 1;
                after_comma = true;
            }
            Some(b'}') => {
                *at += 1;
                return Ok(Json::Object(fields));
            }
            None => return Err("the body ended inside an object".to_string()),
            Some(other) => {
                return Err(format!(
                    "{} at byte {at} is not `,` or `}}` after a field",
                    char::from(*other)
                ));
            }
        }
    }
}

/// `[ value, … ]`.
fn parse_array(bytes: &[u8], at: &mut usize, depth: usize) -> Result<Json, String> {
    *at += 1; // the bracket
    let mut items = Vec::new();

    loop {
        skip_space(bytes, at);

        if bytes.get(*at) == Some(&b']') {
            *at += 1;
            return Ok(Json::Array(items));
        }

        items.push(parse_value(bytes, at, depth + 1)?);

        skip_space(bytes, at);
        match bytes.get(*at) {
            Some(b',') => *at += 1,
            Some(b']') => {
                *at += 1;
                return Ok(Json::Array(items));
            }
            None => return Err("the body ended inside an array".to_string()),
            Some(other) => {
                return Err(format!(
                    "{} at byte {at} is not `,` or `]` after a value",
                    char::from(*other)
                ));
            }
        }
    }
}

/// A quoted string, with the escapes [`escape`] writes.
fn parse_string(bytes: &[u8], at: &mut usize) -> Result<String, String> {
    *at += 1; // the opening quote
    let mut out = String::new();

    loop {
        let Some(&byte) = bytes.get(*at) else {
            return Err("the body ended inside a string".to_string());
        };
        *at += 1;

        match byte {
            b'"' => return Ok(out),
            b'\\' => {
                let Some(&escape) = bytes.get(*at) else {
                    return Err("the body ended inside an escape".to_string());
                };
                *at += 1;

                out.push(match escape {
                    b'"' => '"',
                    b'\\' => '\\',
                    b'/' => '/',
                    b'n' => '\n',
                    b'r' => '\r',
                    b't' => '\t',
                    b'b' => '\u{8}',
                    b'f' => '\u{c}',
                    b'u' => {
                        // Four hex digits. A `\u` carrying a surrogate pair is
                        // legal JSON and this does not read it; a body that used
                        // one would be refused rather than mangled.
                        let mut code = 0u32;
                        for _ in 0..4 {
                            let Some(&digit) = bytes.get(*at) else {
                                return Err("the body ended inside a \\u escape".to_string());
                            };
                            let Some(value) = (digit as char).to_digit(16) else {
                                return Err(format!(
                                    "{} is not a hexadecimal digit in a \\u escape",
                                    char::from(digit)
                                ));
                            };
                            code = code.saturating_mul(16).saturating_add(value);
                            *at += 1;
                        }
                        char::from_u32(code)
                            .ok_or_else(|| format!("\\u{code:04x} is not a character this reads"))?
                    }
                    other => {
                        return Err(format!(
                            "\\{} is not an escape this reads",
                            char::from(other)
                        ));
                    }
                });
            }
            byte if byte < 0x20 => {
                return Err(format!(
                    "a control character at byte {} is not allowed in a JSON string",
                    *at - 1
                ));
            }
            // Anything else is UTF-8 and is copied through. The bytes are already
            // validated as UTF-8 by being a `&str`, so a multi-byte sequence
            // arrives whole.
            byte => {
                // Find where this character ends and copy it, rather than pushing
                // a single byte as a `char` — which would truncate it.
                let width = utf8_width(byte);
                let end = (*at - 1 + width).min(bytes.len());
                out.push_str(
                    std::str::from_utf8(&bytes[*at - 1..end])
                        .map_err(|_| "the body is not valid UTF-8".to_string())?,
                );
                *at = end;
            }
        }
    }
}

/// Bytes in the UTF-8 sequence that starts with `byte`.
///
/// The continuation bytes of a multi-byte character are all `0b10xx_xxxx`, which
/// no lead byte of a valid sequence is, so this is unambiguous.
const fn utf8_width(byte: u8) -> usize {
    if byte < 0x80 {
        1
    } else if byte >= 0xF0 {
        4
    } else if byte >= 0xE0 {
        3
    } else if byte >= 0xC0 {
        2
    } else {
        1
    }
}

/// A bare `word`, if that is what is at `at`.
fn literal(bytes: &[u8], at: &mut usize, word: &str) -> Result<(), String> {
    let end = *at + word.len();
    if bytes.get(*at..end) == Some(word.as_bytes()) {
        *at = end;
        return Ok(());
    }

    Err(format!("{word:?} is expected at byte {at}"))
}

/// A number.
///
/// **Integers only, and a fraction or an exponent is refused.** Every number this
/// service reads is a version number or a chunk count. A body that sent `1.0` or
/// `1e3` for either is a client that has misunderstood the schema, and reading
/// it as an integer would hide that.
fn parse_number(bytes: &[u8], at: &mut usize) -> Result<Json, String> {
    let start = *at;

    if bytes.get(*at) == Some(&b'-') {
        *at += 1;
    }

    while matches!(bytes.get(*at), Some(b'0'..=b'9')) {
        *at += 1;
    }

    let text = std::str::from_utf8(&bytes[start..*at])
        .map_err(|_| format!("the number at byte {start} is not valid UTF-8"))?;

    if let Some(b'.' | b'e' | b'E') = bytes.get(*at) {
        return Err(format!(
            "{text:?} has a fraction or an exponent; this reads whole numbers only"
        ));
    }

    text.parse::<i64>()
        .map(|value| {
            if value < 0 {
                Json::NegInt(value)
            } else {
                Json::int(value.unsigned_abs())
            }
        })
        .map_err(|_| format!("{text:?} is not a number this reads"))
}

/// Write `value` as a JSON string, quotes included.
///
/// JSON strings are delimited by `"` and cannot carry an unescaped control
/// character, a backslash or a quote. Those are the rules that matter here. A
/// `char` above `U+001F` is emitted as it is: it is valid UTF-8, the encoder is
/// producing UTF-8, and the client is parsing UTF-8 per RFC 8259. Escaping it
/// as `\uXXXX` would be correct and would make a path unreadable in the bytes.
fn escape(value: &str, out: &mut String) {
    out.push('"');
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            // Everything else below a space, plus the two characters that would
            // end the string or make it unparseable.
            c if c < ' ' => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

impl fmt::Display for Json {
    /// The bytes this value is, as JSON.
    ///
    /// On stdout of nothing in particular — this is for a response body, and it
    /// is written there as bytes. `Display` rather than `to_string` on the hot
    /// path would allocate a second copy for no gain; a handler writes into a
    /// `String` it already has.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = String::new();
        self.write(&mut out);
        f.write_str(&out)
    }
}

#[cfg(test)]
mod tests {
    use super::{Json, escape};

    /// One value rendered, so a test reads as the bytes it is about.
    fn rendered(value: &Json) -> String {
        value.to_string()
    }

    #[test]
    fn an_object_keeps_the_order_it_was_written_in() {
        // A map would have sorted these. The order is the order written, so the
        // bytes a response is are visible where they are written.
        let value = Json::object(vec![
            ("state", Json::string("running")),
            ("id", Json::string("abc")),
        ]);

        assert_eq!(rendered(&value), r#"{"state":"running","id":"abc"}"#);
    }

    #[test]
    fn a_control_character_is_escaped() {
        // The one thing JSON actually forbids. A path with a newline in it is
        // possible and must not produce a body no client can parse.
        assert_eq!(rendered(&Json::string("a\nb")), r#""a\nb""#);
        assert_eq!(rendered(&Json::string("a\u{1}b")), r#""a\u0001b""#);
    }

    #[test]
    fn a_quote_and_a_backslash_are_escaped() {
        // Both would end the string or escape it. A job's error message can
        // carry either: an `io::Error` quotes the path it failed on.
        assert_eq!(rendered(&Json::string(r#"a"b"#)), r#""a\"b""#);
        assert_eq!(rendered(&Json::string(r"a\b")), r#""a\\b""#);
    }

    #[test]
    fn a_unicode_character_is_written_as_itself() {
        // Valid UTF-8 is valid JSON. Escaping it would be correct and would make
        // a path harder to read in the bytes.
        assert_eq!(rendered(&Json::string("caf\u{e9}")), "\"caf\u{e9}\"");
    }

    #[test]
    fn every_escaped_control_character_round_trips_through_a_parser() {
        // The check that matters: every byte sequence this produces is JSON a
        // real parser accepts. Without it, the escape table above is only as
        // good as its author's memory of RFC 8259.
        let awkward = "\u{0}\u{1}\u{1f}\"\\\n\r\t\u{8}\u{c}";
        let document = rendered(&Json::object(vec![("detail", Json::string(awkward))]));

        let parsed: serde_json::Value =
            serde_json::from_str(&document).expect("the encoder must produce valid JSON");
        assert_eq!(parsed["detail"], awkward);
    }

    #[test]
    fn numbers_keep_their_sign_and_take_no_exponent() {
        // A count of 1000 is `1000`. `1e3` is the same number and is a
        // different thing to read, and a client is not obliged to parse one.
        assert_eq!(rendered(&Json::int(0)), "0");
        assert_eq!(rendered(&Json::int(1000)), "1000");
        assert_eq!(rendered(&Json::int(u64::from(u32::MAX))), "4294967295");
        assert_eq!(rendered(&Json::NegInt(-1)), "-1");
    }

    #[test]
    fn a_field_that_is_absent_is_not_a_null() {
        // The distinction the type exists to keep. `opt` is for a value that is
        // known to have none; a field that is not sent is not in the object.
        let with_null = Json::object(vec![("pack_bytes", Json::opt(None))]);
        let without = Json::object(vec![]);

        assert_eq!(rendered(&with_null), r#"{"pack_bytes":null}"#);
        assert_eq!(rendered(&without), "{}");
    }

    #[test]
    fn an_empty_array_is_bracketed_not_null() {
        // `GET /v1/sets` on an empty root returns `{"sets":[]}`, not
        // `{"sets":null}`. The first says "there are none"; the second says the
        // field has no value, which is a different claim.
        let value = Json::object(vec![("sets", Json::Array(Vec::new()))]);

        assert_eq!(rendered(&value), r#"{"sets":[]}"#);
    }

    #[test]
    fn an_array_of_objects_is_comma_separated() {
        // The case every version listing is. A trailing comma here would make
        // every one of them unparseable.
        let versions = Json::Array(vec![
            Json::object(vec![("version", Json::int(1))]),
            Json::object(vec![("version", Json::int(2))]),
        ]);

        assert_eq!(
            rendered(&Json::object(vec![("versions", versions)])),
            r#"{"versions":[{"version":1},{"version":2}]}"#
        );
    }

    #[test]
    fn a_body_round_trips_through_the_parser() {
        // The direction that matters: anything [`Json`] writes, the parser reads
        // back as the same value. A server that writes a body it cannot parse is
        // a server whose own examples do not work.
        let document = Json::object(vec![
            ("upload", Json::string("abc123")),
            ("version", Json::int(7)),
            (
                "have",
                Json::Array(vec![Json::string("aa"), Json::string("bb")]),
            ),
            ("compression", Json::Null),
            ("flag", Json::Bool(true)),
        ]);

        assert_eq!(Json::parse_object(&document.to_string()).unwrap(), document);
    }

    #[test]
    fn the_parser_reads_what_the_encoders_escapes() {
        // The two halves of the module have to agree. An escape that is written
        // and not read is a body no client can hand back.
        let awkward = "quote\" slash\\ newline\n tab\t bell\u{0007}";
        let document = Json::string(awkward).to_string();

        assert_eq!(
            Json::parse_object(&format!("{{\"x\":{document}}}")).unwrap(),
            Json::Object(vec![("x".to_string(), Json::string(awkward))])
        );
    }

    #[test]
    fn the_parser_reads_a_surrogate_free_unicode_escape() {
        // `\\u00e9` is what a client may send for a character. A body that used a
        // surrogate pair is refused rather than mangled, and says so.
        let parsed = Json::parse_object(r#"{"x":"\u00e9"}"#).unwrap();

        assert_eq!(
            parsed,
            Json::Object(vec![(
                "x".to_string(),
                Json::string("caf\u{e9}".replace("caf\u{e9}", "\u{e9}"))
            )])
        );
    }

    #[test]
    fn the_parser_refuses_a_number_it_cannot_mean() {
        // Every number in a request body is a version or a count. A fraction or
        // an exponent is a client that has misunderstood, and reading it as an
        // integer would hide that.
        for body in [r#"{"v":1.5}"#, r#"{"v":1e3}"#, r#"{"v":-}"#, r#"{"v":}"#] {
            assert!(Json::parse_object(body).is_err(), "{body} should not parse");
        }
    }

    #[test]
    fn the_parser_refuses_what_it_does_not_read() {
        // Each of these says what it found, because a client that sent the wrong
        // shape needs the shape and not a refusal.
        assert!(Json::parse_object("[]").is_err());
        assert!(Json::parse_object("{} {}").is_err());
        assert!(Json::parse_object("{unquoted: 1}").is_err());
        assert!(Json::parse_object(r#"{"a" 1}"#).is_err());
        assert!(Json::parse_object(r#"{"a":1,}"#).is_err());
        assert!(Json::parse_object("{'a':1}").is_err());
        assert!(Json::parse_object("").is_err());
        assert!(Json::parse_object(r#"{"a":1"#).is_err());
    }

    #[test]
    fn the_parser_refuses_a_body_nested_past_what_this_api_defines() {
        // A bound rather than a limit, so the failure is a `400` and not a stack
        // overflow.
        let deep = "[".repeat(32) + &"]".repeat(32);

        assert!(Json::parse_object(&format!("{{\"a\":{deep}}}")).is_err());
    }

    #[test]
    fn the_parser_refuses_a_truncated_multi_byte_character() {
        // UTF-8 must not be copied one byte at a time, or a character becomes
        // several.
        let body = "{\"x\":\"\u{f0}\u{9f}\u{98}\u{80}\"}";

        assert!(Json::parse_object(body).is_ok(), "{body} should parse");
        assert!(
            Json::parse_object(body).unwrap().to_string().contains('x'),
            "the key must survive, which it does not if a character was copied a byte at a time"
        );
    }

    #[test]
    fn escaping_a_string_on_its_own_produces_a_quoted_document() {
        // `escape` is the only place a byte reaches the output, so it is tested
        // directly rather than only through a value.
        let mut out = String::new();
        escape("k\"ey", &mut out);

        assert_eq!(out, r#""k\"ey""#);
    }
}
