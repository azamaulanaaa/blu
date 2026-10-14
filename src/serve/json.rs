// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! The JSON this service speaks. Responses are written by hand; requests are
//! parsed by `serde_json`.
//!
//! **The no-serde rule is about the on-disk format, and does not reach here.**
//! `Cargo.toml` explains why `bincode` was removed: so that no upstream's release
//! cadence or security policy can decide whether a backup set is still readable.
//! That argument is about the *on-disk format*. A response body is not read by a
//! future version of this binary. It is read by a client, and it is versioned by
//! its own contract — the `OpenAPI` document in `openapi/blu.json` says what
//! each field is and what each status means.
//!
//! So the boundary is exempt from the rule, and the rule is kept anyway. Two
//! reasons, and the second is the one that decides it.
//!
//! First, a derive-based approach puts `#[derive(Serialize)]` on this crate's own
//! types. The types are the *library's* contract — `Job`, `Plan`, a set name.
//! They are what `blu`'s other callers reason about. A JSON concern on them
//! is a second thing to keep in step with the first.
//!
//! Second, and more plainly: what a response contains here is eleven small
//! structs of strings and integers, and `serde_json` **is** a direct dependency
//! — it parses every request body, and it was already in the graph under
//! `actix-web` before this crate named it. So the argument is no longer "no
//! dependency". It is what the dependency would be used *for*: writing a response
//! with it means a `#[derive(Serialize)]` on the types named above, in exchange
//! for encoding the same eleven structs out. The encoder below is about a hundred
//! lines, it puts a derive on nothing, and it makes the bytes the server sends
//! visible in one place — which is the property that actually matters for a wire
//! format.

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

    /// Read a request body, and refuse anything that is not the shape we accept.
    ///
    /// **Parsing is `serde_json`'s. Validating is ours**, and the split is the
    /// point of this function existing.
    ///
    /// A general parser accepts a whole grammar; a request body is a flat object
    /// of strings, non-negative integers, `null` and arrays of strings, and every
    /// other shape is a client that has misunderstood the API. So this rejects a
    /// nested object, a fraction, a negative number, a boolean and a non-string
    /// array element, **naming what it found** — a client that sent the wrong
    /// shape needs to be told the shape, not told it failed.
    ///
    /// That is what the hand-rolled parser was reaching for, and it is worth
    /// keeping. What is not worth keeping is owning the grammar underneath it.
    ///
    /// **Field order is not preserved.** `serde_json` stores an object in a
    /// `Map`, which sorts its keys, so the fields come back in name order rather
    /// than in the order they were sent. That matters for nothing here — every
    /// reader looks a field up by name — and it is worth stating because
    /// [`Json::Object`] otherwise promises an order. **Writing** preserves order,
    /// because a response's field order is part of its reviewable bytes; reading
    /// has no such obligation.
    ///
    /// # Errors
    ///
    /// Returns an error naming the problem: not JSON at all, not an object, or a
    /// field this does not read.
    pub fn checked(text: &str) -> Result<Self, String> {
        let value: serde_json::Value =
            serde_json::from_str(text).map_err(|e| format!("this is not JSON: {e}"))?;

        let serde_json::Value::Object(fields) = value else {
            return Err(format!(
                "a request body must be a JSON object, and this one is {}",
                describe(&value)
            ));
        };

        let mut out = Vec::with_capacity(fields.len());
        for (name, field) in fields {
            out.push((name.clone(), convert(&name, &field)?));
        }

        Ok(Json::Object(out))
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
            c if c < ' ' => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// What a parsed value is, for an error that has to say so.
fn describe(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Object(_) => "an object",
        serde_json::Value::Array(_) => "an array",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Null => "null",
    }
}

/// One field, converted or refused.
///
/// **No boolean.** No request field takes one — the three bodies hold versions, a
/// compression policy and a list of chunk hashes — so accepting one here would be
/// permissiveness with no reader behind it. It would be caught later, by the
/// field reader, with a message about that field rather than about its type.
/// `Json::Bool` still exists because responses carry booleans; it just cannot
/// arrive in a request.
fn convert(name: &str, value: &serde_json::Value) -> Result<Json, String> {
    match value {
        serde_json::Value::String(text) => Ok(Json::Str(text.clone())),
        serde_json::Value::Null => Ok(Json::Null),
        serde_json::Value::Number(number) => {
            // `as_u64` is the whole of the check: it is `None` for a negative, a
            // fraction, an exponent that is not integral, and anything past
            // `u64::MAX`. Every number in a request body is a version or a chunk
            // count, so all four are a client that has misunderstood.
            let Some(number) = number.as_u64() else {
                return Err(format!(
                    "`{name}` is {number}, and this reads whole numbers from 0 to u64::MAX only"
                ));
            };
            Ok(Json::Int(number))
        }
        serde_json::Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let serde_json::Value::String(text) = item else {
                    return Err(format!(
                        "`{name}` must be a list of strings, and it holds {}",
                        describe(item)
                    ));
                };
                out.push(Json::Str(text.clone()));
            }
            Ok(Json::Array(out))
        }
        // `describe` carries its own article, so this reads "is a boolean" rather
        // than "is a a boolean".
        other => Err(format!(
            "`{name}` is {}, and this reads strings, whole numbers, null and lists of strings",
            describe(other)
        )),
    }
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
        // A negative number is refused rather than rendered, so there is no
        // variant to render. See `convert`.
        assert!(Json::checked(r#"{"v":-1}"#).is_err());
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
    fn a_written_body_reads_back_as_the_same_fields() {
        // The direction that matters now that there are two halves: anything this
        // module writes must be something `checked` reads back. A server that
        // emits a body its own reader would refuse is a server whose examples do
        // not work.
        //
        // **Compared field by field, not as one `Json`.** `serde_json::Map` sorts
        // keys, so a parsed object's fields come back in name order while a
        // written one goes out in the order it was written. Comparing whole values
        // would be comparing the order rather than the content — which is the one
        // thing about a parsed body that does not matter.
        let document = Json::object(vec![
            ("upload", Json::string("abc123")),
            ("version", Json::int(7)),
            (
                "have",
                Json::Array(vec![Json::string("aa"), Json::string("bb")]),
            ),
            ("compression", Json::Null),
            ("absent", Json::Null),
            ("zero", Json::int(0)),
        ]);

        let Json::Object(read) = Json::checked(&document.to_string()).unwrap() else {
            unreachable!("`checked` yields an object or an error");
        };

        let field = |name: &str| {
            read.iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
        };

        assert_eq!(field("upload"), Some(Json::string("abc123")));
        assert_eq!(field("version"), Some(Json::int(7)));
        assert_eq!(
            field("have"),
            Some(Json::Array(vec![Json::string("aa"), Json::string("bb")]))
        );
        assert_eq!(field("compression"), Some(Json::Null));
        assert_eq!(field("absent"), Some(Json::Null));
        assert_eq!(field("zero"), Some(Json::int(0)));
        assert_eq!(read.len(), 6, "no field was lost or invented");
    }

    #[test]
    fn a_parsed_body_loses_field_order_and_that_is_fine() {
        // Stated because it is a behaviour change from the hand-rolled parser, and
        // a reader who assumed otherwise would write a lookup that depends on
        // position. Nothing here does: every field is fetched by name.
        let Json::Object(read) = Json::checked(r#"{"z":1,"a":2,"m":3}"#).unwrap() else {
            unreachable!("`checked` yields an object or an error");
        };

        let names: Vec<&str> = read.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(names, vec!["a", "m", "z"], "serde_json sorts its keys");
    }

    #[test]
    fn checked_refuses_what_a_request_body_cannot_be() {
        // The shape validation, which is what the hand-rolled parser was for and
        // what survived its removal. Each names the field and the problem.
        for (body, expects) in [
            (r#"{"v":-1}"#, "-1"),
            (r#"{"v":1.5}"#, "1.5"),
            (r#"{"v":1e3}"#, "whole numbers"),
            (r#"{"v":{"a":1}}"#, "`v`"),
            (r#"{"have":[1]}"#, "holds a number"),
            (r#"{"have":[{"a":1}]}"#, "holds an object"),
            (r#"{"v":true}"#, "`v` is a boolean"),
        ] {
            let error = Json::checked(body).unwrap_err();
            assert!(
                error.contains(expects),
                "{body} produced {error:?}, which does not mention {expects:?}"
            );
        }

        // Not JSON at all, and JSON that is not an object.
        assert!(Json::checked("{").is_err());
        assert!(Json::checked("[]").is_err());
        assert!(Json::checked("\"a\"").is_err());
    }

    #[test]
    fn checked_accepts_an_empty_object() {
        // Which every one of these endpoints treats as "use the defaults".
        assert_eq!(Json::checked("{}").unwrap(), Json::Object(Vec::new()));
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
