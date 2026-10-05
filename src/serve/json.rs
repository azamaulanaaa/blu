//! The JSON this service speaks, written by hand.
//!
//! **No serialisation crate, and the argument for that does not change here.**
//! `Cargo.toml` explains why `bincode` was removed: so that no upstream's release
//! cadence or security policy can decide whether a backup set is still readable.
//! That argument is about the *on-disk format*. A response body is not read by a
//! future version of this binary. It is read by a client, and it is versioned by
//! its own contract — the OpenAPI document in `openapi/lepatch.json` says what
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
    /// A string. Escaped on the way out by [`Json::write`].
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

    /// Write this value, and everything under it, into `out`.
    ///
    /// Recursive rather than iterative, and bounded by the depth of the value:
    /// the deepest thing this service sends is an object inside an array inside
    /// an object, so three frames. A document that grew a nesting level would
    /// grow this by one frame too, which is not a thing to design for now.
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
            c if c < ' ' => out.push_str(&format!("\\u{:04x}", u32::from(c))),
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
    fn rendered(value: Json) -> String {
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

        assert_eq!(rendered(value), r#"{"state":"running","id":"abc"}"#);
    }

    #[test]
    fn a_control_character_is_escaped() {
        // The one thing JSON actually forbids. A path with a newline in it is
        // possible and must not produce a body no client can parse.
        assert_eq!(rendered(Json::string("a\nb")), r#""a\nb""#);
        assert_eq!(rendered(Json::string("a\u{1}b")), r#""ab""#);
    }

    #[test]
    fn a_quote_and_a_backslash_are_escaped() {
        // Both would end the string or escape it. A job's error message can
        // carry either: an `io::Error` quotes the path it failed on.
        assert_eq!(rendered(Json::string(r#"a"b"#)), r#""a\"b""#);
        assert_eq!(rendered(Json::string(r"a\b")), r#""a\\b""#);
    }

    #[test]
    fn a_unicode_character_is_written_as_itself() {
        // Valid UTF-8 is valid JSON. Escaping it would be correct and would make
        // a path harder to read in the bytes.
        assert_eq!(rendered(Json::string("caf\u{e9}")), "\"caf\u{e9}\"");
    }

    #[test]
    fn every_escaped_control_character_round_trips_through_a_parser() {
        // The check that matters: every byte sequence this produces is JSON a
        // real parser accepts. Without it, the escape table above is only as
        // good as its author's memory of RFC 8259.
        let awkward = "\u{0}\u{1}\u{1f}\"\\\n\r\t\u{8}\u{c}";
        let document = rendered(Json::object(vec![("detail", Json::string(awkward))]));

        let parsed: serde_json::Value =
            serde_json::from_str(&document).expect("the encoder must produce valid JSON");
        assert_eq!(parsed["detail"], awkward);
    }

    #[test]
    fn numbers_keep_their_sign_and_take_no_exponent() {
        // A count of 1000 is `1000`. `1e3` is the same number and is a
        // different thing to read, and a client is not obliged to parse one.
        assert_eq!(rendered(Json::int(0)), "0");
        assert_eq!(rendered(Json::int(1000)), "1000");
        assert_eq!(
            rendered(Json::int(u64::from(u32::MAX))),
            "4294967295"
        );
        assert_eq!(rendered(Json::NegInt(-1)), "-1");
    }

    #[test]
    fn a_field_that_is_absent_is_not_a_null() {
        // The distinction the type exists to keep. `opt` is for a value that is
        // known to have none; a field that is not sent is not in the object.
        let with_null = Json::object(vec![("pack_bytes", Json::opt(None))]);
        let without = Json::object(vec![]);

        assert_eq!(rendered(with_null), r#"{"pack_bytes":null}"#);
        assert_eq!(rendered(without), "{}");
    }

    #[test]
    fn an_empty_array_is_bracketed_not_null() {
        // `GET /v1/sets` on an empty root returns `{"sets":[]}`, not
        // `{"sets":null}`. The first says "there are none"; the second says the
        // field has no value, which is a different claim.
        let value = Json::object(vec![("sets", Json::Array(Vec::new()))]);

        assert_eq!(rendered(value), r#"{"sets":[]}"#);
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
            rendered(Json::object(vec![("versions", versions)])),
            r#"{"versions":[{"version":1},{"version":2}]}"#
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