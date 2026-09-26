# lepatch

A backup tool built on content-defined chunking. Files are split into chunks
whose boundaries follow the content rather than fixed offsets, so a small edit
shifts only the chunks around it. Repeated content is stored once, both within a
single backup and across every version that follows it.

> **Status: pre-release.** The on-disk format is versioned and refuses to read a
> snapshot it does not understand, but the layout itself is not stable yet. Do
> not point this at anything you cannot afford to re-take.

## Usage

```console
$ lepatch backup ~/Documents docs      # creates docs.bin and docs.001.idx
$ lepatch backup ~/Documents docs      # creates docs.002.idx, reusing chunks
$ lepatch verify docs                  # check the stored chunks
$ lepatch restore /tmp/docs-restored docs
```

`--version N` selects a specific version; without it, restore and verify use the
most recent one that completed successfully.

### Layout

A backup set is named after a base path, which is treated as an opaque prefix:

```text
docs.bin          append-only blob store
docs.001.idx      one small index per version, holding that version's key
docs.002.idx
docs.lock         held while a backup is writing
```

The base name is a prefix, not a stem, so a set called `docs.tar` produces
`docs.tar.bin` — no part of the name is silently reinterpreted.

## What is preserved

Regular files, directories, symbolic links, hard links, permission bits and
modification times. Empty files and empty directories survive. Paths that are
not valid UTF-8 are stored as raw bytes rather than aborting the backup.

## What is not preserved

Ownership, ACLs, extended attributes, and hard-link relationships that point
outside the tree being backed up. Device nodes, FIFOs and sockets are skipped.
A restore overwrites files it describes and leaves anything else in the
destination alone, so it is a merge rather than a wipe.

## Failure behaviour

The parts that matter when a backup runs unattended:

- **A version is only published if it completed.** The index file is claimed
  before the work starts and removed again on failure, so an interrupted run
  never becomes the "latest" version. A process killed outright leaves a
  skipped version number and an empty index, which is ignored rather than
  mistaken for a finished one.
- **Nothing is written from a corrupt store.** Every chunk is checked against
  the hash recorded when it was written before restore writes a single byte.
  `lepatch verify` runs the same check without restoring.
- **A file that changes mid-backup fails the backup.** Each file's chunks are
  checked against the length recorded when the tree was walked, so a file that
  grew or shrank while being read is an error rather than a snapshot whose
  chunks have silently migrated into the wrong files.
- **An unreadable directory fails the backup.** A permission error is never
  reported as an empty directory.
- **Two backups cannot interleave.** A backup holds `docs.lock` for its
  duration. If the process is killed the lock survives; the error message says
  to remove it once you are sure nothing is running.

Restore is a merge, so restoring twice into the same directory is safe, and
restoring over a file that is larger than the backed-up one replaces it rather
than leaving the old tail behind.

## Library

The crate is usable directly:

```rust
use lepatch::{command::{Options, backup, restore}, storage::BlobFileStorage};

let store = BlobFileStorage::<true>::new("docs.bin").await?;
let key = backup("~/Documents", Options::default(), store).await?;

let store = BlobFileStorage::<false>::new("docs.bin").await?;
restore("~/Documents-restored", key, store).await?;
```

Pass the previous run's key as `Options::base_key` to deduplicate against it.
The `WRITE` const generic on `BlobFileStorage` is what stops a read-only restore
from being handed something that can append to the store.

## Development

```console
$ make check      # everything below
$ make lint       # cargo fmt --check + clippy
$ make test       # the test suite
$ make msrv       # builds against the declared minimum Rust version
$ make deps       # cargo deny: advisories, licences, sources
$ make doc        # rustdoc with warnings denied
```

Or through cargo directly: `cargo clippy --all-targets --all-features`,
`cargo test --all-features`, `cargo fmt --check`.

The integration tests are the interesting ones: they build a tree, back it up,
restore it, and compare the two structures — including link identity — rather
than just checking that the command exited 0.

Lint levels live in `Cargo.toml` under `[lints]` rather than in CI, so they
apply to a local `cargo build` and not only to a push. `missing_docs`,
`missing_errors_doc` and `clippy::pedantic` are all denied; the two places that
carry an `#[allow]` say why in a comment.

To have `make lint` run on every commit:

```console
$ git config core.hooksPath .githooks
```

## License

GPL-2.0-or-later. See [LICENSE](LICENSE).
