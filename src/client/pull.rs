//! `POST /v1/packs/{id}/plan`, and the ranged `GET`s that fetch what it names.
//!
//! ## What a pull is
//!
//! **A pack, assembled out of the ranges a client lacks.** The server does not
//! send pack bytes to `POST /v1/packs/{id}/plan`; it answers with a list of byte
//! ranges, which the client then fetches one request at a time and writes at
//! their offsets into a file of the pack's full length. See
//! [`crate::serve::pack`] for why the server can answer that question at all
//! without storing a pack.
//!
//! ## Why the file is sparse and never built in memory
//!
//! **A pack can be larger than the machine restoring it.** So the ranges are
//! written with a seek, at their offsets, into a file whose length is set before
//! the first byte arrives. Nothing is buffered across a range boundary, and
//! nothing is held whole: [`Remote::fetch_range`] streams a response body
//! straight into the file, a chunk of it at a time.
//!
//! ## Why every region is accounted for before the pack is opened
//!
//! **The gaps are the point.** A client that holds some chunks asks for the rest,
//! so the file it assembles has holes in it — and those holes are the chunks it
//! said it already had. [`Pull::open`] refuses anything else: every record the
//! fetched ranges do not cover must be one the client declared, or the run stops
//! before the file is handed to [`Pack::open`].
//!
//! Without that check a server that answered with less than it said it would
//! produce a file that *opens* — `Pack::open` reads a trailer and an index, and
//! both are in the fixed regions every plan includes — and then restores zeroes
//! where the missing chunks were. That is the one failure this protocol can
//! produce silently, so it is the one thing checked twice.
//!
//! ## Why a plan is checked before a byte is fetched
//!
//! **A `Range` header is built from the server's numbers.** A range past the end
//! of the file, an inverted one, or two that overlap are all values a server can
//! answer with, and writing one is how a pack file gets a hole that looks like a
//! chunk. So [`Remote::plan_pack`] reads the plan strictly — every field by name,
//! every offset as a whole number — and refuses a plan whose ranges are not
//! sorted, disjoint and inside the file, or whose own `bytes` disagrees with the
//! ranges it lists.
//!
//! ## What `have` is
//!
//! **The chunks the client already holds, and nothing else.** A hash in `have`
//! the client does not hold is not a saving; it is a hole in the file this
//! function then hands to a restore. An empty `have` is always correct and always
//! wasteful, and this client chooses between the two at the call site rather than
//! guessing here — see a `restore` of a serve target.

use std::{collections::HashSet, io, path::Path};

use actix_web::HttpMessage;
use futures_util::StreamExt as _;
use tokio::{
    fs::OpenOptions,
    io::{AsyncSeekExt, AsyncWriteExt},
};

use super::{
    Remote,
    error::{Document, read, refusal},
    set_path,
};
use crate::{
    pack::{Pack, Range},
    storage::BlobId,
};

/// What a client holding some chunks still needs to fetch.
///
/// **The server's own arithmetic, read strictly.** `length` is how long the pack
/// is, so the file is created at that length before anything arrives; `bytes` is
/// what the ranges add up to, which is what the run reports as what it fetched;
/// and `ranges` is the fetches themselves.
///
/// The ranges are half-open, as every [`Range`] in this crate is: `end` is one
/// past the last byte. **The `Range` header this client sends is inclusive at both
/// ends**, so it asks for `bytes={start}-{end - 1}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackPlan {
    /// Bytes the whole pack occupies.
    pub length: u64,
    /// Bytes the ranges below add up to.
    pub bytes: u64,
    /// Where to fetch, in ascending order and without overlaps.
    pub ranges: Vec<Range>,
}

/// What one fetch moved, for a caller that reports progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fetched {
    /// Ranges fetched so far.
    pub ranges: usize,
    /// Bytes written so far.
    pub bytes: u64,
    /// What the whole plan is.
    pub total: u64,
}

/// The id naming a pack of `version` of `set`, as a path segment.
///
/// **The set name is validated and encoded, and only the `@` and the number are
/// added.** A set name is the user's to type and the server's to refuse, so it
/// goes through [`set_path`] for both. A pack id is a compound of that name and
/// a version number, and a server that could not parse it back would be a server
/// answering about a different set than the one asked for.
///
/// # Errors
///
/// Returns an error if `set` is empty or is not one path component.
fn pack_id(set: &str, version: u32) -> io::Result<String> {
    Ok(format!("{}@{version}", set_path(set)?))
}

impl Remote {
    /// Ask what `version` of `set` still has to be fetched, given `have`.
    ///
    /// `have` is the chunk hashes the caller already holds **and can still read**.
    /// Every one of them is a region this answer will leave out, so a hash that is
    /// not really held becomes a hole in the caller's file rather than a saving.
    /// An empty slice is always right and always costs the whole payload.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot be reached, if it refuses — a set
    /// or version it does not hold is a `404` — if the answer is not the shape of
    /// a plan, or if the plan is not a set of ranges that could be written into a
    /// file of the length it names.
    ///
    /// Returns an error if `set` is empty or is not one path component, because
    /// the name goes into the request path before anything is sent.
    pub async fn plan_pack(
        &self,
        set: &str,
        version: u32,
        have: &[BlobId],
    ) -> io::Result<PackPlan> {
        tokio::task::LocalSet::new()
            .run_until(self.plan_pack_inner(set, version, have))
            .await
    }

    /// The request itself, for [`Remote::plan_pack`] to run on a
    /// [`tokio::task::LocalSet`].
    pub(super) async fn plan_pack_inner(
        &self,
        set: &str,
        version: u32,
        have: &[BlobId],
    ) -> io::Result<PackPlan> {
        let url = self.url(&format!("/v1/packs/{}/plan", pack_id(set, version)?))?;
        let held: Vec<String> = have.iter().map(|id| id.to_hex().to_string()).collect();
        let body = serde_json::json!({ "have": held });

        let mut response = self
            .authorize(self.client.post(&url))
            .send_json(&body)
            .await
            .map_err(|e| io::Error::other(format!("could not reach {url}: {e}")))?;

        let status = response.status();
        let answer = read(&url, &mut response).await?;

        if !status.is_success() {
            return Err(refusal(status.as_u16(), &url, &answer));
        }

        let plan = read_plan(&url, &answer)?;
        check_plan(&url, &plan)?;

        Ok(plan)
    }

    /// Fetch one range of a pack into `out`, and report how many bytes arrived.
    ///
    /// **Streamed, never buffered.** `out` is written a body chunk at a time, so
    /// a range as large as the whole pack costs one chunk of memory rather than
    /// the pack. The caller owns the file and the offsets; this owns the request.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot be reached, if it refuses, **if it
    /// answers anything but `206`** — a `200` would be the whole pack, and
    /// writing that at an offset would corrupt the file — or if the body is not
    /// exactly the range asked for.
    ///
    /// Returns an error if `set` is empty or is not one path component, or if
    /// `range` covers no bytes and so has no `Range` header this client sends.
    ///
    /// Returns an error if `out` cannot be written.
    pub async fn fetch_range<W>(
        &self,
        set: &str,
        version: u32,
        range: &Range,
        out: &mut W,
    ) -> io::Result<u64>
    where
        W: AsyncWriteExt + Unpin,
    {
        tokio::task::LocalSet::new()
            .run_until(self.fetch_range_inner(set, version, range, out))
            .await
    }

    /// The request itself, for [`Remote::fetch_range`] to run on a
    /// [`tokio::task::LocalSet`].
    pub(super) async fn fetch_range_inner<W>(
        &self,
        set: &str,
        version: u32,
        range: &Range,
        out: &mut W,
    ) -> io::Result<u64>
    where
        W: AsyncWriteExt + Unpin,
    {
        let url = self.url(&format!("/v1/packs/{}", pack_id(set, version)?))?;
        let header = header_for(range)?;

        let mut response = self
            .authorize(
                self.client
                    .get(&url)
                    .insert_header(("range", header.clone())),
            )
            .send()
            .await
            .map_err(|e| io::Error::other(format!("could not reach {url}: {e}")))?;

        let status = response.status();

        if !status.is_success() {
            let answer = read(&url, &mut response).await?;
            return Err(refusal(status.as_u16(), &url, &answer));
        }

        if status != actix_web::http::StatusCode::PARTIAL_CONTENT {
            // Deliberately without reading the body. A `200` here is the whole
            // pack, and the bytes of a pack this size are not something to pull
            // into memory to build an error message out of.
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{url} answered {status} to a range request, not to the 206 one. Its body \
                     is not the range asked for. Writing it at offset {} of the pack would \
                     leave a file that opens and is wrong",
                    range.start
                ),
            ));
        }

        let mut payload = response.take_payload();
        let mut written = 0u64;

        while let Some(part) = payload.next().await {
            let bytes = part.map_err(|e| {
                io::Error::other(format!("could not read the answer from {url}: {e}"))
            })?;
            out.write_all(&bytes).await?;
            written = written.saturating_add(bytes.len() as u64);
        }

        // The short body of a truncated transfer is the failure this whole
        // protocol can produce quietly, and this is where it is caught.
        if written != range.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "{url} sent {written} bytes for {header}, which is {} bytes. The file \
                     would have a hole in it",
                    range.len()
                ),
            ));
        }

        Ok(written)
    }
}

/// A pack being fetched from a server, and what the caller says it already has.
///
/// **One value for the whole download**, so the set, the version and the
/// `have` it was built from cannot be passed separately to the fetch and to the
/// check that follows it. The check is the reason: a pack is assembled with
/// holes in it wherever the caller said it held a chunk, and a hole anywhere
/// else is a chunk that would restore as zeroes.
#[derive(Debug)]
pub struct Pull<'a> {
    /// The server being asked.
    remote: &'a Remote,
    /// The set on it.
    set: &'a str,
    /// The version being fetched.
    version: u32,
    /// The chunks the caller already holds, and can still read.
    have: HashSet<BlobId>,
    /// Ranges already written into this file, and not to be fetched again.
    already: Vec<Range>,
}

impl<'a> Pull<'a> {
    /// A pull of `version` of `set`, given the chunks the caller already holds.
    ///
    /// **Every hash in `have` is a region this pull will not fetch.** A hash
    /// that is not really held is not a saving; it is a hole in the file, and the
    /// file is opened only over the holes the caller named.
    #[must_use]
    pub fn new(remote: &'a Remote, set: &'a str, version: u32, have: HashSet<BlobId>) -> Self {
        Self {
            remote,
            set,
            version,
            have,
            already: Vec::new(),
        }
    }

    /// A pull whose `path` already holds `written`, and needs no more of it.
    ///
    /// **The fixed regions a caller fetched to read the version's file table.**
    /// A plan names them again, because a plan is for a client that has fetched
    /// nothing, and fetching a header twice to save a third of a second is
    /// cheaper than being surprised about where a pack's bytes came from.
    #[must_use]
    pub fn already_fetched(mut self, written: Vec<Range>) -> Self {
        self.already = written;
        self
    }

    /// The chunks this pull will not fetch, for a caller that wants to say so.
    #[must_use]
    pub fn have(&self) -> &HashSet<BlobId> {
        &self.have
    }

    /// Start assembling the pack at `path`, empty apart from its length.
    ///
    /// **Any file already at `path` is removed first**, rather than truncated in
    /// place. A caller that fetched the fixed regions in one call and the rest in
    /// another is assembling one pack across both, and a file left by an earlier
    /// run at the same path would otherwise contribute bytes that no range in any
    /// plan covers — holes that no `have` names, in a file that opens.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be removed or created, or cannot be
    /// given its length.
    pub async fn begin(&self, length: u64, path: &Path) -> io::Result<()> {
        begin_file(length, path).await
    }

    /// Fetch `ranges` into the file at `path`, and report what moved.
    ///
    /// Each range is written at its own offset, so nothing larger than one
    /// response chunk is ever in memory and a range can be any size. A range
    /// already inside one this pull was told has been written is skipped.
    ///
    /// **The bytes are in the file when this returns.** The file at `path` is
    /// about to be read back through another descriptor — by [`Pull::open`], or
    /// by a caller that opens it itself — so a write still sitting in a handle's
    /// buffer when this call ended would be read as a hole.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened for writing or seeked, or if
    /// any range cannot be fetched or comes back the wrong length.
    pub async fn write<F>(
        &self,
        ranges: &[Range],
        path: &Path,
        mut on_progress: F,
    ) -> io::Result<Fetched>
    where
        F: FnMut(Fetched),
    {
        tokio::task::LocalSet::new()
            .run_until(self.write_inner(ranges, path, &mut on_progress))
            .await
    }

    /// Fetch every range `plan` names into a fresh file at `path`, and open it.
    ///
    /// [`Pull::begin`] then [`Pull::write`] then [`Pull::open`], which is what a
    /// caller with nothing to fetch in phases wants.
    ///
    /// **The file is left at `path`.** It is the caller's to remove: a run that
    /// fails leaves a file that is not a pack, and a caller that kept it would be
    /// keeping something that opens and is wrong.
    ///
    /// # Errors
    ///
    /// As [`Pull::begin`], [`Pull::write`] and [`Pull::open`].
    pub async fn fetch<F>(&self, plan: &PackPlan, path: &Path, on_progress: F) -> io::Result<Pack>
    where
        F: FnMut(Fetched),
    {
        self.begin(plan.length, path).await?;
        self.write(&plan.ranges, path, on_progress).await?;
        self.open(plan, path)
    }

    /// Open the assembled file, and refuse it if a record is missing from it.
    ///
    /// **This is the check the whole protocol turns on.** `Pack::open` reads a
    /// trailer and an index and no payload, and every plan includes both, so a
    /// file assembled over missing chunks opens perfectly well. What it must not
    /// do is open over a hole the caller did not name: those bytes would be
    /// restored as zeroes, and nothing downstream of a restore hashes a pack.
    ///
    /// # Errors
    ///
    /// Returns an error if `path` is not a pack, or if any record in it is
    /// outside every fetched range and is not one of this pull's `have`.
    pub fn open(&self, plan: &PackPlan, path: &Path) -> io::Result<Pack> {
        let pack = Pack::open(path)?;
        self.check_complete(&pack, plan)?;

        Ok(pack)
    }

    /// The work itself, for [`Pull::write`] to run on a
    /// [`tokio::task::LocalSet`].
    async fn write_inner<F>(
        &self,
        ranges: &[Range],
        path: &Path,
        on_progress: &mut F,
    ) -> io::Result<Fetched>
    where
        F: FnMut(Fetched),
    {
        let mut file = OpenOptions::new().write(true).open(path).await?;

        let wanted: Vec<Range> = ranges
            .iter()
            .flat_map(|range| uncovered(range, &self.already))
            .collect();

        let mut moved = Fetched {
            ranges: 0,
            bytes: 0,
            total: wanted
                .iter()
                .fold(0u64, |total, range| total.saturating_add(range.len())),
        };

        for range in wanted {
            file.seek(std::io::SeekFrom::Start(range.start)).await?;
            moved.bytes = moved.bytes.saturating_add(
                self.remote
                    .fetch_range_inner(self.set, self.version, &range, &mut file)
                    .await?,
            );
            moved.ranges = moved.ranges.saturating_add(1);
            on_progress(moved);
        }

        // **Before this returns, not after the handle is dropped.** A write on a
        // `tokio::fs::File` is a copy into the handle's own buffer, and the
        // `write(2)` behind it runs on a blocking task that has not necessarily
        // run when the copy is acknowledged. So `write_all` returning says the
        // bytes were accepted, not that they are in the file — and the caller
        // reads this file back through a *different* descriptor, which sees
        // whatever that task has got to. A pack whose trailer was in the last
        // write is then a file with a hole at the end of it, which is the one
        // thing the check in `Pull::open` cannot see, because the trailer is
        // read before it.
        //
        // Flushing here also closes the window between two writes: a second
        // `write` over the same path seeks a fresh handle, and seeks do not
        // order themselves against a write another handle has outstanding.
        file.flush().await?;

        Ok(moved)
    }

    /// Refuse a pack whose missing records are not the ones this pull claimed.
    fn check_complete(&self, pack: &Pack, plan: &PackPlan) -> io::Result<()> {
        let fetched = pack.chunks_covering(&plan.ranges);

        for chunk in pack.chunks() {
            if fetched.contains(&chunk.id) || self.have.contains(&chunk.id) {
                continue;
            }

            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "the ranges this server sent leave chunk {} out of the pack. This pull \
                     did not say it already held it. The file would restore zeroes for it",
                    chunk.id
                ),
            ));
        }

        Ok(())
    }
}

/// The parts of `wanted` that `already` does not cover.
///
/// **A plan merges touching ranges, so a fixed region a caller already fetched
/// is usually *inside* one of them rather than equal to it.** Dropping the whole
/// range because part of it is there would leave the rest a hole, and fetching it
/// whole because none of it is named would fetch the header twice. So the
/// difference is taken properly, and one run of records either side of the file's
/// fixed regions becomes two ranges rather than one.
///
/// Sorted and disjoint, because a caller hands `already` over in whatever order
/// it fetched and this is the answer a fetch loop walks.
fn uncovered(wanted: &Range, already: &[Range]) -> Vec<Range> {
    let mut out = vec![*wanted];

    for held in already {
        let mut next = Vec::with_capacity(out.len() + 1);

        for range in out {
            // No overlap: the range survives whole, which covers the touching
            // case and the case where `already` names something beyond it.
            if range.end <= held.start || held.end <= range.start {
                next.push(range);
                continue;
            }

            if range.start < held.start {
                next.push(Range {
                    start: range.start,
                    end: held.start,
                });
            }
            if held.end < range.end {
                next.push(Range {
                    start: held.end,
                    end: range.end,
                });
            }
        }

        out = next;
    }

    out.retain(|range| !range.is_empty());
    out
}

/// Remove whatever is at `path` and leave an empty file of `length` bytes there.
async fn begin_file(length: u64, path: &Path) -> io::Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await?;

    // **Before the first byte arrives.** The file is the pack's length from the
    // first moment, so a range can be written anywhere in it and a hole is a hole
    // rather than a file that ends early.
    file.set_len(length).await
}

/// Read a plan document, field by field.
fn read_plan(url: &str, body: &[u8]) -> io::Result<PackPlan> {
    let document = Document::read(url, "a fetch plan", body)?;

    let length = document.count("length")?;
    let bytes = document.count("bytes")?;
    let listed = document.len("ranges")?;

    let mut ranges = Vec::with_capacity(listed);
    for at in 0..listed {
        let one = document.element("ranges", at)?;
        ranges.push(Range {
            start: one.count("start")?,
            end: one.count("end")?,
        });
    }

    Ok(PackPlan {
        length,
        bytes,
        ranges,
    })
}

/// Refuse a plan whose own numbers could not be written into the file it names.
///
/// **Checked before a byte moves**, because every one of these is a way the
/// assembled file could end up with a hole nobody asked for. An inverted range
/// writes nothing where bytes were expected. A range past the end is past the
/// file. An overlapping pair writes the same bytes twice from two answers. And a
/// `bytes` that disagrees with the ranges is a server whose arithmetic this
/// client is about to report to a user.
fn check_plan(url: &str, plan: &PackPlan) -> io::Result<()> {
    let mut end_of_last = 0u64;

    for range in &plan.ranges {
        if range.end <= range.start {
            return Err(drift_plan(
                url,
                &format!("range {}..{} is not a range", range.start, range.end),
            ));
        }

        if range.end > plan.length {
            return Err(drift_plan(
                url,
                &format!(
                    "range {}..{} runs past the {}-byte pack it names",
                    range.start, range.end, plan.length
                ),
            ));
        }

        if range.start < end_of_last {
            return Err(drift_plan(
                url,
                &format!(
                    "range {}..{} overlaps the range before it. This client fetches ranges \
                     one at a time and writes each one at its own offset. Two answers for \
                     the same bytes would have one of them win",
                    range.start, range.end
                ),
            ));
        }

        end_of_last = range.end;
    }

    let listed: u64 = plan
        .ranges
        .iter()
        .fold(0u64, |total, range| total.saturating_add(range.len()));

    if listed != plan.bytes {
        return Err(drift_plan(
            url,
            &format!(
                "it says {listed} bytes of ranges, which is what its ranges add up to, and {} \
                 in `bytes`",
                plan.bytes
            ),
        ));
    }

    Ok(())
}

/// The `Range` header value for `range`.
///
/// **The half-open range becomes an inclusive one, and the off-by-one is the
/// whole risk.** A plan's `end` is one past the last byte, so `64..88` is
/// `bytes=64-87`. Sending `bytes=64-88` would fetch a byte of the record after
/// it; sending `bytes=64-` — which is what subtracting nothing leaves — asks for
/// the rest of the pack, and this client would write all of it at an offset into
/// a file that is not that long. So the conversion refuses an empty or inverted
/// range rather than producing either header.
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidInput`] if `range` covers no bytes.
fn header_for(range: &Range) -> io::Result<String> {
    let last = range.end.checked_sub(1).filter(|_| range.end > range.start);

    let Some(last) = last else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "refusing to fetch {}..{}, which is not a range. The header this client sends \
                 is inclusive. An empty range would read as `bytes={}-` and take the rest of \
                 the pack",
                range.start, range.end, range.start
            ),
        ));
    };

    Ok(format!("bytes={}-{last}", range.start))
}

/// The message for a plan this client could not act on.
fn drift_plan(url: &str, reason: &str) -> io::Error {
    super::error::drift(url, "a fetch plan", reason)
}

#[cfg(test)]
mod tests {
    use super::{PackPlan, check_plan, pack_id};
    use crate::{pack::Range, storage::BlobId};

    /// A plan document, as the server writes one for a client holding nothing.
    const WHOLE: &[u8] = br#"{"length":8192,"bytes":8168,"ranges":[
        {"start":0,"end":40},{"start":64,"end":8192}]}"#;

    #[test]
    fn a_plan_document_reads_its_three_fields() {
        let url = "http://127.0.0.1:8480/v1/packs/docs@1/plan";

        let plan = super::read_plan(url, WHOLE).unwrap();

        assert_eq!(
            plan,
            PackPlan {
                length: 8192,
                bytes: 8168,
                ranges: vec![
                    Range { start: 0, end: 40 },
                    Range {
                        start: 64,
                        end: 8192
                    }
                ],
            }
        );
        assert!(check_plan(url, &plan).is_ok());
    }

    #[test]
    fn a_range_is_half_open_here_and_inclusive_in_the_header() {
        // The off-by-one this crate's protocol lives or dies by. `end` is one past
        // the last byte, so a run of 24 bytes starting at 64 is `bytes=64-87`.
        let range = Range { start: 64, end: 88 };

        assert_eq!(range.len(), 24);
        assert_eq!(super::header_for(&range).unwrap(), "bytes=64-87");
    }

    #[test]
    fn a_pack_id_is_the_set_name_and_a_version() {
        assert_eq!(pack_id("docs", 3).unwrap(), "docs@3");
        // A set name that needs encoding is still one component to the server.
        assert_eq!(pack_id("photos 2024", 1).unwrap(), "photos%202024@1");
        assert!(pack_id("a/b", 1).is_err(), "a path is not a set name");
    }

    #[test]
    fn a_plan_that_could_not_be_written_into_its_file_is_refused() {
        let url = "http://127.0.0.1:8480/v1/packs/docs@1/plan";
        let good = Range { start: 0, end: 40 };

        // Inverted: it would write nothing where bytes were expected.
        let inverted = PackPlan {
            length: 8192,
            bytes: 0,
            ranges: vec![Range { start: 40, end: 40 }],
        };
        assert!(check_plan(url, &inverted).is_err());

        // Past the end: it would write beyond the file.
        let past = PackPlan {
            length: 100,
            bytes: 60,
            ranges: vec![
                Range { start: 0, end: 60 },
                Range {
                    start: 80,
                    end: 200,
                },
            ],
        };
        assert!(check_plan(url, &past).is_err());

        // Overlapping: two answers for one run of bytes.
        let overlapping = PackPlan {
            length: 100,
            bytes: 90,
            ranges: vec![
                Range { start: 0, end: 60 },
                Range {
                    start: 40,
                    end: 100,
                },
            ],
        };
        assert!(check_plan(url, &overlapping).is_err());

        // `bytes` disagreeing with the ranges is a server whose arithmetic is
        // about to be printed at a user.
        let disagreeing = PackPlan {
            length: 100,
            bytes: 1000,
            ranges: vec![good],
        };
        assert!(check_plan(url, &disagreeing).is_err());

        // Touching is fine — the server merges, and this client checks that it did.
        let touching = PackPlan {
            length: 100,
            bytes: 100,
            ranges: vec![
                Range { start: 0, end: 40 },
                Range {
                    start: 40,
                    end: 100,
                },
            ],
        };
        assert!(check_plan(url, &touching).is_ok());
    }

    #[test]
    fn a_plan_missing_a_field_is_an_error_rather_than_an_empty_fetch() {
        let url = "http://127.0.0.1:8480/v1/packs/docs@1/plan";

        for body in [
            &br#"{}"[..],
            &br#"{"length":10,"bytes":10}"#[..],
            &br#"{"length":10,"bytes":"10","ranges":[]}"#[..],
            &br#"{"length":10,"bytes":0,"ranges":[{"start":0}]}"#[..],
            &br#"{"length":10,"bytes":0,"ranges":[{"start":0,"end":"4"}]}"#[..],
        ] {
            let error = super::read_plan(url, body).unwrap_err();

            assert_eq!(
                error.kind(),
                std::io::ErrorKind::InvalidData,
                "body {body:?} should not have read"
            );
            assert!(
                error.to_string().contains(url),
                "the error names the address"
            );
        }
    }

    #[test]
    fn a_refused_range_never_becomes_an_open_ended_one() {
        // `bytes=40-` means "the rest of the file", so a range with nothing after
        // the dash has to be refused before it is ever built.
        for range in [Range { start: 40, end: 40 }, Range { start: 40, end: 0 }] {
            let error = super::header_for(&range).unwrap_err();

            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
            assert!(error.to_string().contains("bytes="));
        }

        assert_eq!(
            super::header_for(&Range { start: 0, end: 1 }).unwrap(),
            "bytes=0-0"
        );
    }

    #[test]
    fn a_hash_is_written_as_the_hex_the_server_reads() {
        // `have` is a list of hex strings, and a `BlobId` written any other way is
        // a hash the server refuses as `not a chunk hash`.
        let id = BlobId::from_hex("ab".repeat(32)).unwrap();

        assert_eq!(id.to_hex().to_string(), "ab".repeat(32));
    }
}
