# lepatch

A backup tool built on content-defined chunking. Files are split into chunks
whose boundaries follow the content rather than fixed offsets, so a small edit
shifts only the chunks around it. Repeated content is stored once, both within a
single backup and across every version that follows it.

> **Status: pre-release.** The on-disk format is versioned and refuses to read a
> store it does not understand, but the layout itself is not stable yet. Do not
> point this at anything you cannot afford to re-take.

## Usage

```console
$ lepatch backup ~/Documents docs      # creates docs.0000.bin, version 1
$ lepatch backup ~/Documents docs      # version 2, reusing chunks
$ lepatch versions docs                # list the versions
$ lepatch verify docs                  # check the stored chunks
$ lepatch restore /tmp/docs-restored docs
$ lepatch reindex docs                 # rebuild the per-version index files
```

`--version N` selects a specific version; without it, restore and verify use the
most recent one that completed successfully.

### Restoring

A restore **merges**: it writes everything the snapshot describes and leaves
anything else in the destination alone. That is the safer default — a restore
into a directory that turns out to be the wrong one should not have deleted it
first — but it does mean the destination may end up holding more than the
snapshot accounts for, and the run says so when it does.

`--clean` empties the destination first, so the result is exactly the snapshot:

```console
$ lepatch restore /tmp/docs-restored docs --clean
```

It also assembles the tree *beside* the destination and swaps it in, so a
failure or a Ctrl-C leaves the destination exactly as it was rather than half
written. The cost is the disk space for two trees at once.

That is the only operation here that destroys data, so it is deliberately hard to
talk into. It refuses a destination that holds the backup set being restored
from, your home directory, the working directory, and the filesystem root. A
restore also takes a claim beside the destination for its duration, so two
restores into one directory cannot interleave; a claim left by a killed run has
to be removed by hand.

Nothing is ever written outside the destination. Recorded paths that are
absolute, or that contain `..`, are refused before the first file is created —
a backup set is a file that gets synced and shared, so its contents are treated
as untrusted input. A symbolic link's *target* is exempt, because it is stored
verbatim and resolved by whoever follows it: a tree containing a link to
`/etc/passwd` is an ordinary tree.

### Layout

A backup set is named after a base path, which is treated as an opaque prefix:

```text
docs.0000.bin     segment 0: the first 4 GiB of records
docs.0001.bin     segment 1
...
docs.v0001.idx    where version 1's snapshot is, so a listing need not scan
docs.lock         held while a backup is writing
```

The base name is a prefix, not a stem, so a set called `docs.tar` produces
`docs.tar.0000.bin` — no part of the name is silently reinterpreted.

### Segments

A set is capped per file rather than being one file that grows without bound, so
it does not meet a limit that is somebody else's decision: a filesystem's maximum
file size, a tool that cannot read it, a transfer that has to succeed in one piece.
`lepatch backup --segment-size 1GiB` sets the cap; the default is 4 GiB, chosen to
sit under what other tools and formats will handle.

**A full segment is sealed and never written to again.** That is what makes this
safe rather than merely possible: a torn write — which the commit protocol already
anticipated as a tail that fails to verify — can only ever be in the *last*
segment. So damage anywhere else is permanent and reported, rather than something
the next run quietly trims away, and recovery only ever repairs one file.

Each segment's header records which segment it is, so a file renamed or moved
into another set is refused rather than read as though it belonged. A set missing a
segment in the middle is refused too: the segments are the only place the data is,
and a gap read as "those chunks were never stored" is a backup that lost data
without saying so.

The cap is a write policy, not part of the format, and is not recorded in the
header. Changing it mid-life is safe and never rewrites what is already there —
no record depends on where a boundary fell. A record larger than the whole cap is
an error naming the cap, not a segment that quietly grows past it; note that a
snapshot is a single record, so the cap has to exceed a snapshot's size.

Segments accumulate. Nothing is removed, because chunks are shared across
versions and so a segment cannot be dropped without breaking some other version's
manifest.

### Index files

One small file per version, saying which segment that version's snapshot is in:

```text
games.v0001.idx     version, segment, commit record within it, snapshot hash
```

They exist for one reason: without them, answering "what versions does this set
have" means reading the header of every record in every segment, which is felt on
the command line before a restore has written anything. With them it is a file
name per version.

**They are a cache, not a second source of truth.** The store's commit records are
the record of what versions exist; an index file is derived from them and can be
lost, stale, or wrong without the store disagreeing about anything. Delete them all
and the versions still restore — `lepatch reindex` puts the files back. That is the
inversion from the old layout, where an index file *was* the only place a version
existed, so losing one lost the backup and a power cut could leave the index
durable and the bytes it named unwritten.

An index that disagrees with the store is never used. It must name a version the
set holds, a segment that exists, and a commit record in that segment whose hash is
the one the file claims; anything else falls back to the store's own answer. So a
stale file costs a scan and a copied one costs an error, and neither can make a set
report a version it does not have.

`lepatch versions` reads the files, so its count is a **lower** bound: a version
whose index write was abandoned still exists and still restores. It never
over-reports.

### Packs

A store is the right shape for *holding* versions and the wrong shape for *serving*
one. Restoring a new version out of a store is a request per chunk, because each
chunk sits between two record headers and nothing makes the ones a client needs
adjacent. A **pack** is one version's chunks concatenated into a single file with no
framing between them, so the bytes a client needs are contiguous.

Measured on a synthetic tree of 8 binaries and 6 asset blobs, patching one 500 KB
binary:

```text
v1 from nothing      1,309,726 bytes in  1 request
v2 having v1           25,325 bytes in  1 request   (1.9% of the pack)
```

One request in both cases, including the patch. The same delta served out of the
store is 25,325 requests. That is the whole argument for packing, and it is why the
8 KiB average chunk size is not a problem: the concern about thousands of tiny
ranges was a property of the store's framing, which a pack does not have.

```text
magic | version | snapshot hash | chunk count | payload bytes
...payloads, end to end, in plan order...
...index: per chunk, hash | offset | length...
index length | magic
```

**The index is at the end.** A client fetches the last twelve bytes, learns where the
index is and how long it is, fetches *that* range, and now knows every chunk's
offset without having read a byte of payload. A header would be the wrong way round:
it would have to be read before anything, including before it is known whether the
pack is worth having at all. The trailing magic is what makes the reverse read
reliable — a length alone is indistinguishable from payload bytes, and a truncated
download would otherwise be read as a short index.

**A pack is a function of the tree, so it is byte-identical every time.** The order
is the snapshot's own chunk table, not the order chunks happened to be written, so
two backups of one tree produce the same bytes even though their stores lay the
chunks out differently. That is what makes a pack nameable by the hash of its own
contents: an immutable artifact a CDN can cache forever and a client can verify with
one comparison.

**A pack is derived, never authoritative.** It is output, regenerable from the store
and the snapshot alone, and losing one loses nothing. The store's commit records
remain the record of what versions exist, which is why nothing reads a pack to answer
that.

Where a header's counts and its own index could disagree, the pack is refused rather
than one of them believed: the index is what locates chunks, so if the two disagree
one is wrong, and picking the wrong one produces a pack that reads as valid and hands
out ranges covering the wrong bytes.

## What is preserved

Regular files, directories, symbolic links, hard links, permission bits and
modification times. Empty files and empty directories survive. Paths that are
not valid UTF-8 are stored as raw bytes rather than aborting the backup.

## What is not preserved

Ownership, ACLs, extended attributes, and hard-link relationships that point
outside the tree being backed up. Device nodes, FIFOs and sockets are skipped.

## Failure behaviour

The parts that matter when a backup runs unattended:

- **A version is only published if it completed.** A run writes its chunks, then
  its snapshot, then one commit record naming it — and the data is made durable
  *before* the commit record is. That ordering is the whole guarantee, and it is
  two syncs rather than one because pages within a file reach the device in no
  particular order: with a single sync the page holding the commit record can
  arrive while an earlier data page has not, and then the store holds a version
  whose data is not there. A run that stops before the commit leaves nothing
  published, and the next run's recovery discards the tail it left behind — from
  the last segment only, since a sealed one is never written to again. A killed
  run leaves no skipped version numbers to reason about, because a version number
  is a position in the store's own log.
- **A set that cannot be read whole is refused whole.** A segment with a tail that
  does not verify is reported as damaged rather than read up to the damage. Reading
  past it would mean a restore that quietly omits whatever those bytes held and
  reports success. Only a *run* repairs, and only the last segment.
- **Nothing is written from a corrupt store.** Every chunk is checked against
  the hash it is addressed by before restore writes a single byte. A chunk's
  address *is* the hash of its contents, so this is one check rather than two.
  `lepatch verify` runs the same check without restoring.
- **A file that is not a store is never modified.** Opening one for writing
  recovers it by discarding an unfinished run, so a store written by a build that
  predates this one would be destroyed by the very pass meant to read it. It is
  refused instead, with a message saying so, and left exactly as it was.
- **A file that changes mid-backup fails the backup.** Each file's chunks are
  checked against the length recorded when the tree was walked, so a file that
  grew or shrank while being read is an error rather than a snapshot whose
  chunks have silently migrated into the wrong files.
- **An unreadable directory fails the backup.** A permission error is never
  reported as an empty directory.
- **Two backups cannot interleave.** A backup holds `docs.lock` for its
  duration. If the process is killed the lock survives; the error message says
  to remove it once you are sure nothing is running.

Restore is a merge by default, so restoring twice into the same directory is
safe, and restoring over a file that is larger than the backed-up one replaces it
rather than leaving the old tail behind. See [Restoring](#restoring) for
`--clean` and the destination claim.

## Limits worth knowing

Stated rather than discovered:

- **Memory scales with the tree, not with the file being read.** A snapshot is
  held whole in memory while it is verified, restored and re-serialised. That is
  tens of megabytes for a tree of a million files and rather more beyond, which is
  comfortable for photographs and documents and not for a filesystem image.
  Chunk *data* is never all resident: the backup pipeline holds a bounded window
  of chunks and verify streams each one past a fixed scratch buffer, so this is a
  limit on the tree's shape rather than on its size. Removing it means streaming
  the snapshot, which is a change to the on-disk format rather than a tuning knob.
- **A backup is CPU-bound, and mostly on hashing.** Measured on 160 MB: 1.2s of CPU
  for 0.48s of wall clock, about half of it in the kernel. Reading and writing that
  much to a tmpfs costs about 70ms, so the run is not waiting on the disk at all.
  The reader hashes with blake3 on one thread while it walks the tree, and that
  thread is the busiest thing in the process. The obvious fix is hashing chunks
  across a thread pool, which the pipeline's current shape does not allow: the
  hash is needed to deduplicate, and deduplication is decided in commit order
  after every store has finished. It is a change to the pipeline rather than a
  tuning knob.
- **A merging restore is not atomic.** It writes in place, so one interrupted by a
  full disk or a kill leaves the destination partly written, and there is no
  journal to resume from. `--clean` *is* atomic: it assembles the tree beside the
  destination and swaps it in, so a failure or a kill leaves the destination
  exactly as it was. That costs the disk space for two trees at once. Making a
  merge atomic is possible and not worth it: the destination would have to be
  copied aside first, which for a restore into a large existing tree doubles both
  the disk and the time to say nothing that `--clean` does not already offer.
- **Ctrl-C stops the run and tidies up.** `SIGINT` and `SIGTERM` are both handled,
  the exit code follows the shell convention (130 and 143), and any claim on a
  backup set or a destination is released on the way out. What is *not* undone is
  work already done: a killed backup leaves no commit record, so no version is
  published, and the chunks it did store are reclaimed by the next run, and a
  killed merging restore leaves whatever it had written.
- **The format limits above are for restore.** A backup holds one bounded window
  of chunks regardless of tree size, so it streams.
- **The on-disk format is not stable, and changing it loses existing backup
  sets.** `FORMAT_VERSION` guards the snapshot encoding and a store carries its
  own version in its header, so a store from an older build is *reported* as
  unreadable rather than misread — but it is not converted, and the old bytes
  stay where they are. An older store is a bare concatenation of blobs with a
  separate index file per version, and there is nothing in it that says where one
  blob ends, so there is no way to walk one. Treat a format change as a reason to
  re-take your backups. The crate is not published, and the
  encoding is hand-written in `src/metadata/codec.rs` rather than delegated, so
  there is no upstream whose policy can decide whether your backup set can still
  be read — which is how `bincode` came to be on an advisory list in the first
  place.

## Library

The crate is usable directly:

```rust
use lepatch::{
    command::{Options, backup, restore},
    storage::{BlobStore, SegmentLimit},
};

let store = BlobStore::<true>::open("docs", SegmentLimit::default()).await?;
let first = backup("~/Documents", Options::default(), store).await?;

// Later backups need nothing passed to deduplicate: the store holds every
// version it has ever been given, so a chunk it already has is not written
// again however far back it was last seen.
let store = BlobStore::<true>::open("docs", SegmentLimit::default()).await?;
backup("~/Documents", Options::default(), store).await?;

let store = BlobStore::<false>::open("docs", SegmentLimit::default()).await?;
restore("~/Documents-restored", first.snapshot, store).await?;
```

`backup` returns a `Committed`: the version number the run was given and the hash
of the snapshot it published. A version is a position in the store's commit
records, so `store.versions()` and `store.snapshot(n)` address them directly.

`SegmentLimit` caps one file of the set; the default is 4 GiB and it can be changed
between runs without rewriting anything. A set is `base.0000.bin`,
`base.0001.bin` and so on, and `BlobStore::segments()` names them.

A pack is written from a `Plan`, which resolves a snapshot into an ordered list of
chunks with their locations, and asks `Pack` what a client holding some of them
still needs:

```rust
use std::collections::BTreeSet;
use lepatch::{
    metadata::Snapshot,
    pack::{Pack, chunks_covering},
    storage::{BlobStore, Plan, SegmentLimit, StorageGet},
};

// The plan is a function of the snapshot, so it is the same every run.
let store = BlobStore::<false>::open("docs", SegmentLimit::default()).await?;
let snapshot = read_snapshot(&store).await;
let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id))?;

let pack = Pack::write(&store, &plan, &snapshot_hash, "game.pack").await?;

// What a client holding the previous version's chunks does not have, as byte
// ranges of the pack. Adjacent ranges merge, which is the point of packing.
let ranges = pack.needed(&have_from_the_previous_version);

// After fetching: record what those ranges covered, and re-asking returns nothing.
let have = chunks_covering(&pack, &ranges);
```

`Pack::needed` is deliberately phrased as what a client *lacks*: one holding nothing
gets the whole payload as a single range, and one holding everything gets none.
`Pack::write` streams, so its memory is one chunk rather than one version.

The `WRITE` const generic on `BlobStore` is what stops a read-only restore from
being handed something that can append to the store.

## Releases

Releases are built by [cargo-dist](https://axodotdev.github.io/cargo-dist).
`dist-workspace.toml` holds the target matrix, and a tag push triggers
`.github/workflows/release.yml`, which publishes one archive per target with a
sha256 each plus a combined `sha256.sum`.

```console
$ mise run dist-plan    # what a tag would produce, without building
$ mise run dist-build   # build the archive for this machine
$ mise run dist-check   # confirm the release config and workflow gate hold
$ git tag v0.1.0 && git push --tags
```

A tag's build is gated on the full CI suite, so nothing is published from a
commit that does not pass. That gate is a hand edit to the generated release
workflow, and the `release-config` CI job fails if a regeneration drops it.

Archives only — no shell installer, MSI or Homebrew formula. That keeps a
release to files whose published checksums a user can verify by hand, which is
the property that matters for a tool whose job is to be trusted with a copy of
your data.

## Development

`mise` is the task runner. `mise install` fetches the pinned toolchain plus
`cargo-llvm-cov` and `cargo-deny`; every task then runs the same command CI
runs.

```console
$ mise run check       # every task below, in dependency order
$ mise run lint        # cargo fmt --check + clippy
$ mise run test        # the test suite
$ mise run cov         # coverage summary
$ mise run cov-html    # coverage report, opened in a browser
$ mise run msrv        # builds against the declared minimum Rust version
$ mise run licenses    # cargo deny: dependency licences
$ mise run advisories  # cargo deny: vulnerabilities and yanks
$ mise run bans        # cargo deny: wildcards, duplicates, unused crates
$ mise run doc         # rustdoc with warnings denied
```

`mise tasks` lists them with their descriptions.

The integration tests are the interesting ones: they build a tree, back it up,
restore it, and compare the two structures — including link identity — rather
than just checking that the command exited 0.

Lint levels live in `Cargo.toml` under `[lints]` rather than in CI, so they
apply to a local `cargo build` and not only to a push. `all`, `cargo` and
`pedantic` are all denied, along with `unwrap_used` and `expect_used`; the two
`#[allow]`s in library code say why in a comment.

To have the fast checks run on every commit:

```console
$ git config core.hooksPath .githooks
```

### CI

`.github/workflows/ci.yml` runs one job per check, named after the mise task that
runs the same command, so a red job is reproducible locally with `mise run <name>`.
Everything runs on Linux, including the test suite.

**The test suite runs on one platform, and the release ships five.** That gap is
deliberate and worth being exact about, because "tested on Linux" and "the macOS
and Windows binaries are correct" are different claims:

- **macOS is a second Unix.** Every `cfg(unix)` line of source and every
  `cfg(unix)` test runs unchanged on Linux, so dropping macOS did not stop any
  distinct code from being tested. What it stopped is testing the *same* code on
  a second filesystem: APFS rather than ext4, a case-insensitive volume by
  default, and different link and inode behaviour.
- **Windows is different code, and it is now not compiled at all.** The
  `cfg(windows)` branch of `symlink` in `src/fsattr.rs` and both `FileId`
  variants in `src/command/backup.rs` are the only places this crate does
  something the Unix path does not. With no Windows runner they are not merely
  untested — nothing type-checks them, so they can rot to the point of not
  compiling, and `cargo test` on Linux will not notice. The `experimental`
  feature that gates the Windows file-identity path is in the same position, and
  was already untested before this: the hard-link tests that would exercise it
  are `cfg(unix)`.
- **Releases still build all five targets**, so a Windows or macOS binary that
  does not compile fails the *release build* rather than shipping. What is no
  longer caught is a binary that compiles and is wrong.

If that trade is not the one you want, the cheap middle ground is to keep the
test job on Linux and add a non-blocking `cargo check --target
x86_64-pc-windows-msvc --all-features` so the Windows paths at least stay
compiling, or to cut the target list in `dist-workspace.toml` down to the two
Linux triples so nothing untested is published. Both are one small change; say
which and it is done.

Two things about the workflow are deliberate and worth knowing before changing
them:

- **The release gate is hand-edited, not generated.** `release.yml` is written by
  `dist generate` and then edited to make publishing depend on this workflow, so
  its bytes never match a regeneration and `dist generate --check` would always
  fail. What is protected instead is the *gate* — a `release-config` job greps for
  the two edits, so a regeneration that forgets them fails CI rather than
  quietly letting a tag publish without passing it.
- **The token is read-only here.** This workflow is called by the release workflow,
  which needs `contents: write`, and a called workflow inherits the caller's
  token. The explicit `permissions: contents: read` is what stops the lint, test
  and coverage jobs from running with write access to the repository every time a
  release is cut.

Actions are referenced by commit SHA rather than by tag, and only two of the three
are external: `actions/checkout` and `actions/upload-artifact`, both official and
both pinned. Everything else is in `.github/actions/setup-rust`, a composite
action in this repository that can be read in one sitting.

It follows two rules, which is the whole supply-chain argument:

- **Rust comes from the image.** The official GitHub-hosted runner images already
  ship Rust, Cargo and rustup, so there is no toolchain-fetching action here at
  all. The image has rustfmt but not clippy or llvm-tools, and those are
  `rustup component add` from `static.rust-lang.org`. The MSRV job installs
  exactly the version `Cargo.toml` declares.
- **Every other CLI comes from `cargo install --locked`.** `cargo-deny`,
  `cargo-llvm-cov` and `cargo-dist` are not on the image, so they are installed
  from crates.io at a pinned version — an unpinned install is a different tool the
  day someone cuts a release, which is the same weakness as a mutable action tag
  but slower to notice.

`release.yml` is the exception and is left unpinned deliberately: it is written by
`dist generate`, and `allow-dirty` in `dist-workspace.toml` scopes hand-edits to
the CI gate alone. Pinning it there would be undone by the next regeneration. It
does use only official `actions/*` entries, so it is not a third-party
dependency — just a tag rather than a SHA.

## License

GPL-2.0-or-later. See [LICENSE](LICENSE).
