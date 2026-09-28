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

There is no index file. Versions used to be recorded in one small index file each,
which made publishing a version a cross-file commit: an index named an offset
inside the store, and nothing could make the two land together. A power cut could
leave the index durable and the bytes it named unwritten, or the index's own
directory entry missing while its contents were on the device. Now the store
records its own versions, so publishing one is a single append to a single file
and there is no ordering between two files to get right. Losing an index used to
mean losing a backup whose bytes were still in the file, with no way to find them;
losing nothing now loses nothing.

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

## License

GPL-2.0-or-later. See [LICENSE](LICENSE).
