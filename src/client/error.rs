//! What a `lepatch serve` said, and what it refused.
//!
//! ## Why the refusals live in one module
//!
//! **Three modules of this client ask the server things, and all three have to
//! agree about what a refusal means.** A `404` on a version listing, on an
//! upload and on a job is one thing to a reader: that name is not here. A `422`
//! is the server saying the bytes are not what they claim to be, whether they
//! arrived as a version listing, as a pack or as a job document.
//!
//! So the status table, the problem-document reader and the strict field reader
//! are written once, here, and every module of this client answers through them.
//! A second table in `uploads.rs` would be a second opinion about the same
//! status, and it would be right until the server changed.
//!
//! ## The table is the server's, inverted
//!
//! `status_for` in `src/serve/error.rs` turns an [`io::ErrorKind`] into a status.
//! [`kind_for`] below turns that status back into a kind, so a refusal that would
//! have been a [`io::ErrorKind::NotFound`] on the command line arrives as a
//! [`io::ErrorKind::NotFound`] here. The two ends then agree about what a failure
//! is, and a user is not asked to learn a second vocabulary for one mistake.
//!
//! | Status | Kind here | What it means |
//! |---|---|---|
//! | `400` | `InvalidInput` | a request this server refuses, such as a compression value that is neither `keep` nor a level |
//! | `403` | `PermissionDenied` | |
//! | `404` | `NotFound` | no set, version, upload or job at that name |
//! | `409` | `AlreadyExists` | another run holds the claim, or the job has already finished |
//! | `416` | `InvalidInput` | a `Range` the file cannot answer; no endpoint here asks for one |
//! | `422` | `InvalidData` | the bytes are well-formed and are not what they claim to be |
//! | anything else, `500` included | `Other` | |
//!
//! ## The message is the server's own sentence
//!
//! `lepatch serve` answers every refusal with an RFC 9457 problem document whose
//! `title` and `detail` are the same `Display` of an [`io::Error`] this crate
//! would have printed locally. So a client shows that sentence verbatim: a set
//! that is not on the server reads like a set that is not on the disk, which is
//! the point of using the same tool on both sides.
//!
//! ## Why an answer this client cannot read is an error
//!
//! **The one way a client could quietly lie.** `awc` will hand back `200 OK` and
//! whatever bytes arrived. Read permissively — ask for `versions`, take what is
//! there, treat a missing object as nothing found — and a server that has
//! changed its shape produces an empty answer, and the command prints nothing
//! and exits 0. A script reading that believes the set is empty, or the job
//! produced nothing. That is a claim, and it is the wrong one.
//!
//! So [`Document`] asks for every field by name and checks its type, and a
//! document that is not the shape this module documents is
//! [`io::ErrorKind::InvalidData`] naming the address and what was wrong. Nothing
//! is inferred from silence. An empty list is a thing a server can say; it is
//! not a thing a client concludes.

use std::io;

use serde_json::Value;

use crate::storage::BlobId;

/// The most this client will read for one response body.
///
/// A pack is not one of these bodies — a pack is bytes this client sends — but a
/// job's `result` and a version's snapshot hash are, and both are small. This is
/// a ceiling rather than whatever arrives, because this client talks to a server
/// with no authentication and an unbounded body is a decision nobody took.
/// `awc` caps at 2 MiB by default; this is higher, and is still a ceiling: a body
/// past it is an error naming the limit, never a value truncated at whatever
/// happened to arrive first.
pub(crate) const PAYLOAD_LIMIT: usize = 64 * 1024 * 1024;

/// Read a response body whole, refusing one larger than [`PAYLOAD_LIMIT`].
pub(crate) async fn read(url: &str, response: &mut awc::ClientResponse) -> io::Result<Vec<u8>> {
    let read = response.body().limit(PAYLOAD_LIMIT).await;

    let bytes = read.map_err(|e| {
        if let awc::error::PayloadError::Overflow = e {
            return io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{url} is answering with more than the {PAYLOAD_LIMIT} bytes this client will \
                     read for one document"
                ),
            );
        }

        io::Error::other(format!("could not read the answer from {url}: {e}"))
    })?;

    Ok(bytes.to_vec())
}

/// The error a non-2xx answer becomes.
///
/// The problem document's `detail`, or its `title`, is the server's own message
/// and is carried through unchanged. `status_for` in `src/serve/error.rs` is the
/// table this inverts, so a `422` here is the `InvalidData` the server started
/// from.
pub(crate) fn refusal(status: u16, url: &str, body: &[u8]) -> io::Error {
    let said = said_by(body);
    let message = match said {
        Some(detail) => detail,
        // A body that is not a problem document is a body there is nothing to
        // quote, so the status and the address carry it. Never a bare "request
        // failed": the address is the one fact a reader cannot get elsewhere.
        None => format!("{url} answered {status} with no problem document in it"),
    };

    io::Error::new(kind_for(status), message)
}

/// The kind a status maps back to.
///
/// The inverse of [`crate::serve`]'s table, so the two ends agree. Anything
/// unmapped there is a `500` and so is anything unmapped here: inventing a kind
/// for a status nobody has classified would be a guess about what a client ought
/// to do about a failure nobody has seen.
pub(crate) fn kind_for(status: u16) -> io::ErrorKind {
    match status {
        // `400` and `416` together: the server sends `416` for a `Range` this
        // file cannot answer, and no endpoint this client calls asks for a range,
        // so both are "a request this client made that the server refused".
        400 | 416 => io::ErrorKind::InvalidInput,
        403 => io::ErrorKind::PermissionDenied,
        404 => io::ErrorKind::NotFound,
        409 => io::ErrorKind::AlreadyExists,
        422 => io::ErrorKind::InvalidData,
        _ => io::ErrorKind::Other,
    }
}

/// The server's own sentence, out of an RFC 9457 problem document.
///
/// `detail` first and `title` second, because this server writes the same
/// sentence into both. A document that is not JSON, or is JSON without either
/// field, has nothing to quote.
fn said_by(body: &[u8]) -> Option<String> {
    let document: Value = serde_json::from_slice(body).ok()?;

    ["detail", "title"]
        .into_iter()
        .find_map(|field| document.get(field)?.as_str().map(str::to_string))
        .filter(|said| !said.is_empty())
}

/// A response this client cannot read.
///
/// Said as one sentence about the address, because the user is looking at a
/// command that printed nothing and needs to know which server to ask about.
/// `what` names the document — `a job`, `an upload's commit` — so the sentence
/// says which answer was refused and not merely that one was.
pub(crate) fn drift(url: &str, what: &str, reason: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{url} answered with {what} this client does not understand: {reason}. Nothing is \
             reported rather than an empty value; `lepatch serve` documents the shape in \
             openapi/lepatch.json"
        ),
    )
}

/// One JSON object, read strictly.
///
/// Every accessor asks for a field **by name** and checks its type, and answers
/// [`io::ErrorKind::InvalidData`] naming the address and the field when it is
/// absent or is the wrong type. The accessors for a field the specification
/// declares nullable take `null` and a missing field alike as "no value"; every
/// other accessor demands the field.
///
/// The document owns its [`Value`], so it can be built from bytes in one call
/// and then handed about without a lifetime threaded through every accessor.
/// A nested document — a job's `result`, and nothing else in this API — holds a
/// copy of the object it describes, which is a handful of short strings and two
/// hashes; that is not a cost worth a lifetime on twelve accessors.
#[derive(Debug)]
pub(crate) struct Document {
    /// The address every message names.
    url: String,
    /// What this document is, for the drift message: `a job`, `an upload`.
    what: &'static str,
    /// Where inside the answer this object sits, for the field messages: empty at
    /// the root, `result` or `versions[2]` inside.
    path: String,
    /// The object itself.
    value: Value,
}

impl Document {
    /// Read `body` as an object.
    ///
    /// # Errors
    ///
    /// Returns an error if the body is not JSON, or is JSON that is not an
    /// object. `what` names the document in the message, so `an upload` and `a
    /// job` do not read alike.
    pub(crate) fn read(url: &str, what: &'static str, body: &[u8]) -> io::Result<Self> {
        let value: Value = serde_json::from_slice(body)
            .map_err(|e| drift(url, what, &format!("it is not JSON ({e})")))?;

        if !value.is_object() {
            return Err(drift(url, what, "it is not a JSON object"));
        }

        Ok(Self {
            url: url.to_string(),
            what,
            path: String::new(),
            value,
        })
    }

    /// The object at `name`, to read fields of.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no such field, or it is not an object.
    /// Nothing is read out of it either way.
    pub(crate) fn object(&self, name: &str) -> io::Result<Document> {
        let value = self.value.get(name).ok_or_else(|| self.missing(name))?;

        if !value.is_object() {
            return Err(self.wrong(name, "an object"));
        }

        Ok(Document {
            url: self.url.clone(),
            what: self.what,
            path: format!("`{name}`"),
            value: value.clone(),
        })
    }

    /// How many entries the array at `name` holds.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no such field, or it is not an array. **A
    /// missing array is not an empty one**: see this module's note on lying. An
    /// array with nothing in it is the one shape that legitimately says "nothing
    /// here", and it has to be written down to be believed.
    pub(crate) fn len(&self, name: &str) -> io::Result<usize> {
        self.entries(name).map(Vec::len)
    }

    /// The entry at `at` of the array at `name`, read as an object.
    ///
    /// # Errors
    ///
    /// Returns an error if the array is not there, if `at` is past its end, or
    /// if the entry is not an object.
    pub(crate) fn element(&self, name: &str, at: usize) -> io::Result<Document> {
        let value = self
            .entries(name)?
            .get(at)
            .ok_or_else(|| self.drift(&format!("`{name}` holds no entry {at}")))?;

        if !value.is_object() {
            return Err(drift(
                &self.url,
                self.what,
                &format!("`{name}` entry {at} is not an object"),
            ));
        }

        Ok(Document {
            url: self.url.clone(),
            what: self.what,
            path: format!("`{name}`[{at}]"),
            value: value.clone(),
        })
    }

    /// The array at `name`, checked to be one.
    fn entries(&self, name: &str) -> io::Result<&Vec<Value>> {
        self.value
            .get(name)
            .ok_or_else(|| self.missing(name))?
            .as_array()
            .ok_or_else(|| self.wrong(name, "an array"))
    }

    /// The whole-number counters in the object at `name`, in name order.
    ///
    /// **Sorted, because the same job polled twice has to compare equal when
    /// nothing moved.** The order of a JSON object is not part of its value, and
    /// a caller deciding "has this changed since I last printed it" by `==`
    /// against two polls must not be answering that question about field order.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no such field, if it is not an object, or if
    /// any value in it is not a whole number. An empty object is an empty list of
    /// counters, which is what a job that has reported nothing says.
    pub(crate) fn counters(&self, name: &str) -> io::Result<Vec<(String, u64)>> {
        let reported = self.object(name)?;
        let fields = reported
            .value
            .as_object()
            .ok_or_else(|| reported.wrong(name, "an object of counters"))?;

        let mut out: Vec<(String, u64)> = Vec::with_capacity(fields.len());
        for (counter, value) in fields {
            let counted = value.as_u64().ok_or_else(|| {
                reported.wrong(counter, "a whole number, since it is a running total")
            })?;

            out.push((counter.clone(), counted));
        }
        out.sort();
        Ok(out)
    }

    /// Whether `name` is there at all.
    ///
    /// For the one field this client must not require: `result` is `null` on a
    /// job that has not finished and an object on one that has, and which of the
    /// two it is decides which shape to read.
    pub(crate) fn has(&self, name: &str) -> bool {
        self.value.get(name).is_some_and(|value| !value.is_null())
    }

    /// The string at `name`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no such field, or it is not a string.
    pub(crate) fn text(&self, name: &str) -> io::Result<&str> {
        self.value
            .get(name)
            .ok_or_else(|| self.missing(name))?
            .as_str()
            .ok_or_else(|| self.wrong(name, "a string"))
    }

    /// The string at `name`, or `None` for `null` or for no such field.
    ///
    /// For the fields the specification declares nullable and no more: a caller
    /// that needs to tell an absent field from a `null` one is asking a question
    /// the API does not answer.
    ///
    /// # Errors
    ///
    /// Returns an error if the field is there and is neither a string nor `null`.
    pub(crate) fn optional_text(&self, name: &str) -> io::Result<Option<&str>> {
        match self.value.get(name) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_str()
                .map(Some)
                .ok_or_else(|| self.wrong(name, "a string or null")),
        }
    }

    /// The non-negative integer at `name`, as the count or byte length it is.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no such field, or it is not a non-negative
    /// integer. A fraction or a negative number is a server that has changed what
    /// it means by this field.
    pub(crate) fn count(&self, name: &str) -> io::Result<u64> {
        self.value
            .get(name)
            .ok_or_else(|| self.missing(name))?
            .as_u64()
            .ok_or_else(|| self.wrong(name, "a whole number"))
    }

    /// The count at `name`, or `None` for `null` or for no such field.
    ///
    /// # Errors
    ///
    /// Returns an error if the field is there and is not a non-negative integer
    /// or `null`.
    pub(crate) fn optional_count(&self, name: &str) -> io::Result<Option<u64>> {
        match self.value.get(name) {
            None | Some(Value::Null) => Ok(None),
            Some(value) => value
                .as_u64()
                .map(Some)
                .ok_or_else(|| self.wrong(name, "a whole number or null")),
        }
    }

    /// The number at `name`, as a set's own version number.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no such field, or it is not a number that
    /// fits in the `u32` a set numbers its versions with. A version number too
    /// large for one is drift rather than a truncation: a truncated version is
    /// another set's version.
    pub(crate) fn version(&self, name: &str) -> io::Result<u32> {
        let number = self.count(name)?;

        u32::try_from(number).map_err(|_| self.wrong(name, "a version number this crate uses"))
    }

    /// The version at `name`, or `None` for `null` or for no such field.
    ///
    /// # Errors
    ///
    /// As [`Document::version`], except that `null` and absence are both "no
    /// version named", which is what `base` means.
    pub(crate) fn optional_version(&self, name: &str) -> io::Result<Option<u32>> {
        match self.optional_count(name)? {
            None => Ok(None),
            Some(number) => u32::try_from(number)
                .map(Some)
                .map_err(|_| self.wrong(name, "a version number this crate uses")),
        }
    }

    /// The flag at `name`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no such field, or it is not `true` or
    /// `false`.
    pub(crate) fn flag(&self, name: &str) -> io::Result<bool> {
        self.value
            .get(name)
            .ok_or_else(|| self.missing(name))?
            .as_bool()
            .ok_or_else(|| self.wrong(name, "true or false"))
    }

    /// The hash at `name`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no such field, or it is not a string this
    /// crate's own [`BlobId`] accepts. The check is the crate's, so a server
    /// naming something this build cannot look up is refused here rather than
    /// failing against a store that has never heard of it.
    pub(crate) fn hash(&self, name: &str) -> io::Result<BlobId> {
        let hex = self.text(name)?;

        BlobId::from_hex(hex).map_err(|_| self.wrong(name, "a hash this crate wrote"))
    }

    /// The hash at `name`, or `None` for `null` or for no such field.
    ///
    /// # Errors
    ///
    /// As [`Document::hash`], except that `null` and absence are both "no base
    /// named", which is what a full pack says.
    pub(crate) fn optional_hash(&self, name: &str) -> io::Result<Option<BlobId>> {
        match self.optional_text(name)? {
            None => Ok(None),
            Some(hex) => BlobId::from_hex(hex)
                .map(Some)
                .map_err(|_| self.wrong(name, "a hash this crate wrote")),
        }
    }

    /// The error for a field this document does not carry at all.
    ///
    /// **The two messages are separate**, because "the field is not there" and
    /// "the field is there and is the wrong type" are different faults: the first
    /// is a server that stopped sending it or renamed it, and the second is one
    /// that changed what it means. A reader chasing one of them wants to know
    /// which.
    fn missing(&self, name: &str) -> io::Error {
        if self.path.is_empty() {
            return self.drift(&format!("it carries no `{name}`"));
        }

        self.drift(&format!("in {} it carries no `{name}`", self.path))
    }

    /// The error for a field this document carries and this client cannot read.
    ///
    /// `expectation` is what the field should have been — `a string`, `a whole
    /// number` — so the sentence names the field and what it should have been
    /// rather than only that something is wrong with it.
    fn wrong(&self, name: &str, expectation: &str) -> io::Error {
        if self.path.is_empty() {
            return self.drift(&format!("`{name}` is not {expectation}"));
        }

        self.drift(&format!("`{name}`, in {}, is not {expectation}", self.path))
    }

    /// A message about this document.
    fn drift(&self, reason: &str) -> io::Error {
        drift(&self.url, self.what, reason)
    }
}

#[cfg(test)]
mod tests {
    use super::{Document, kind_for, said_by};

    const ADDRESS: &str = "http://127.0.0.1:8480/v1/jobs/abc";

    #[test]
    fn a_four_oh_four_carries_the_servers_own_sentence() {
        // Not a paraphrase. The server's `detail` is what the command line would
        // have printed, and that is what the user reads.
        let body = br#"{"type":"about:blank","title":"no backup set at /srv/docs","status":404,"detail":"no backup set at /srv/docs (looked in /srv)"}"#;

        let error = super::refusal(404, ADDRESS, body);

        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        assert_eq!(
            error.to_string(),
            "no backup set at /srv/docs (looked in /srv)"
        );
    }

    #[test]
    fn a_four_oh_four_without_a_problem_document_still_names_the_server() {
        // A proxy in front, or a server that is not this one. The address is
        // then the only fact there is.
        let error = super::refusal(404, ADDRESS, b"<html>gone</html>");

        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        assert!(error.to_string().contains(ADDRESS));
        assert!(error.to_string().contains("404"));
    }

    #[test]
    fn every_mapped_status_keeps_its_meaning_in_both_directions() {
        // The inverse of the server's `status_for`. Each row here is a status
        // that table produces from a kind, so a change to either side shows up.
        assert_eq!(kind_for(400), std::io::ErrorKind::InvalidInput);
        assert_eq!(kind_for(403), std::io::ErrorKind::PermissionDenied);
        assert_eq!(kind_for(404), std::io::ErrorKind::NotFound);
        assert_eq!(kind_for(409), std::io::ErrorKind::AlreadyExists);
        assert_eq!(kind_for(416), std::io::ErrorKind::InvalidInput);
        assert_eq!(kind_for(422), std::io::ErrorKind::InvalidData);
        assert_eq!(kind_for(500), std::io::ErrorKind::Other);
    }

    #[test]
    fn the_title_is_quoted_when_there_is_no_detail() {
        let body = br#"{"title":"the set is gone"}"#;

        assert_eq!(said_by(body).as_deref(), Some("the set is gone"));
        assert_eq!(said_by(b"{}"), None);
        assert_eq!(said_by(b""), None);
        assert_eq!(said_by(br#"{"detail":""}"#), None);
    }

    /// The fields a job document cannot be read without.
    ///
    /// Each is asked for by name and by type, so a body missing one fails rather
    /// than decoding into a job of no state and no counters.
    fn read_identity(document: &Document) -> std::io::Result<()> {
        let _ = document.text("id")?;
        let _ = document.text("kind")?;
        let _ = document.text("state")?;
        let _ = document.count("started")?;
        let _ = document.counters("progress")?;
        Ok(())
    }

    #[test]
    fn every_field_is_asked_for_by_name_and_checked() {
        // Each of these is a server that has changed what it sends. Reading any
        // of them permissively would produce an empty job and a success, which is
        // the claim this module exists to prevent.
        for body in [
            &br"{}"[..],
            &br#"{"id":1,"kind":"merge","state":"running","started":1,"progress":{}}"#[..],
            &br#"{"id":"a","kind":["merge"],"state":"running","started":1,"progress":{}}"#[..],
            &br#"{"id":"a","kind":"merge","state":1,"started":1,"progress":{}}"#[..],
            &br#"{"id":"a","kind":"merge","state":"running","started":"soon","progress":{}}"#[..],
            &br#"{"id":"a","kind":"merge","state":"running","started":1,"progress":[]}"#[..],
            &br#"{"id":"a","kind":"merge","state":"running","started":1,"progress":{"chunks":"3"}}"#[..],
        ] {
            let document = Document::read(ADDRESS, "a job", body).unwrap();
            let error = read_identity(&document).unwrap_err();

            assert_eq!(
                error.kind(),
                std::io::ErrorKind::InvalidData,
                "body {body:?} should not have read"
            );
            assert!(
                error.to_string().contains(ADDRESS),
                "the error should name the address: {error}"
            );
        }

        // The one that reads, so the cases above are not passing because every
        // body fails for the same reason.
        let good = br#"{"id":"a","kind":"merge","state":"running","started":1,"progress":{}}"#;
        assert!(read_identity(&Document::read(ADDRESS, "a job", good).unwrap()).is_ok());
    }

    #[test]
    fn a_document_that_is_not_an_object_at_all_is_refused() {
        for body in [&b"[]"[..], &b"not json"[..], &b"\"a job\""[..], &b""[..]] {
            let error = Document::read(ADDRESS, "a job", body).unwrap_err();

            assert_eq!(
                error.kind(),
                std::io::ErrorKind::InvalidData,
                "body {body:?} should not have read"
            );
        }
    }

    #[test]
    fn a_well_formed_object_reads_every_accessor() {
        let body = br#"{"id":"abc","kind":"merge","state":"succeeded","started":7,"finished":9,
            "error":null,"result":null,"progress":{"chunks":3}}"#;
        let document = Document::read(ADDRESS, "a job", body).unwrap();

        assert_eq!(document.text("id").unwrap(), "abc");
        assert_eq!(document.count("started").unwrap(), 7);
        assert_eq!(document.optional_count("finished").unwrap(), Some(9));
        assert_eq!(document.optional_text("error").unwrap(), None);
        assert!(!document.has("result"));
        assert!(document.has("id"));
        // `progress` *is* an object, so reading its counters is the case that
        // has to work; reading a number as an object is the case that has to
        // fail.
        assert_eq!(
            document.counters("progress").unwrap(),
            vec![("chunks".to_string(), 3)]
        );
        assert_eq!(
            document.object("started").unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn a_missing_array_is_an_error_and_an_empty_one_is_an_empty_list() {
        // The two halves of the claim in this module's documentation: the server
        // has to *say* that a set is empty, and a parser that cannot tell the two
        // apart is the one that lies.
        let empty = Document::read(ADDRESS, "a job", br#"{"versions":[]}"#).unwrap();
        assert_eq!(empty.len("versions").unwrap(), 0);

        let missing = Document::read(ADDRESS, "a job", br"{}").unwrap();
        assert_eq!(
            missing.len("versions").unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn counters_are_read_in_a_stable_order_so_two_polls_compare_equal() {
        // Without the sort, two polls of one unchanged job would differ in field
        // order and report a line that had not moved.
        let body = br#"{"progress":{"written":2,"chunks":1}}"#;
        let document = Document::read(ADDRESS, "a job", body).unwrap();

        assert_eq!(
            document.counters("progress").unwrap(),
            vec![("chunks".to_string(), 1), ("written".to_string(), 2)]
        );

        // An empty object is a job that has reported nothing, which is a thing a
        // server can say. A missing field, or a value that is not a number, is
        // not that.
        assert!(document.counters("chunks").is_err());
        assert_eq!(
            Document::read(ADDRESS, "a job", br#"{"progress":{"chunks":"1"}}"#)
                .unwrap()
                .counters("progress")
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn an_entry_says_where_in_the_array_it_was() {
        let body = br#"{"versions":[{"version":1},{"version":2}]}"#;
        let document = Document::read(ADDRESS, "a version listing", body).unwrap();

        let error = document
            .element("versions", 1)
            .unwrap()
            .hash("snapshot")
            .unwrap_err();

        assert!(
            error.to_string().contains("`versions`[1]"),
            "the message says which entry: {error}"
        );
    }

    #[test]
    fn a_nested_object_says_where_it_is_in_the_message() {
        let body = br#"{"id":"abc","result":{"set":"docs","version":"1"}}"#;
        let document = Document::read(ADDRESS, "a job", body).unwrap();

        let error = document
            .object("result")
            .unwrap()
            .version("version")
            .unwrap_err();

        assert!(
            error.to_string().contains("`version`, in `result`"),
            "the message says which field of which object: {error}"
        );
    }

    #[test]
    fn a_version_number_too_large_is_drift_rather_than_a_truncation() {
        let body = br#"{"version":4294967296}"#;
        let document = Document::read(ADDRESS, "a job", body).unwrap();

        assert_eq!(
            document.version("version").unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }
}
