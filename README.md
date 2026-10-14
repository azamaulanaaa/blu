# lepatch

lepatch backs up directories with content-defined chunking. Chunk borders follow content, so a small edit moves only nearby chunks. lepatch stores repeated bytes once, within one version and across versions.

> **Status: pre-release.** The on-disk format carries a version and refuses unknown stores, but the layout is not stable yet. Do not use it for data you cannot recreate.

## Install

Download the archive for your platform from releases. Verify its checksum against `sha256.sum`. Or build from source with `cargo build --release`. Building needs the Rust version in `Cargo.toml`.

## Quick start

```console
$ lepatch backup ~/Documents docs
$ lepatch backup ~/Documents docs
$ lepatch versions docs
$ lepatch verify docs
$ lepatch restore /tmp/docs-restored docs
$ lepatch reindex docs
$ lepatch pack docs game.pack
$ lepatch merge game.pack other
$ lepatch serve ./backups
```

Every command takes one `TARGET`. A local path names a local set. An address with `://` names a serve set or a keyed store. Remote targets hold no local copy.

```console
$ lepatch backup ~/Documents docs
$ lepatch backup ~/Documents file:///mnt/drive/docs
$ lepatch backup ~/Documents http://127.0.0.1:8480/docs
```

Without `--version`, restore, verify, and pack use the most recent usable version. `stdout` is data. `stderr` is diagnostics. Every command is pipeable without `--quiet`. Use `--numbers-only` to print just the version numbers, one per line.

## Restore

Restore merges by default. It writes files from the snapshot and leaves other files alone. It reports extra files when it finishes.

Use `--clean` to hold exactly the snapshot and nothing else.

```console
$ lepatch restore /tmp/docs-restored docs --clean
```

`--clean` builds the tree beside the destination and then replaces the destination. A stopped `--clean` restore leaves the destination unchanged. `--clean` is the only operation that removes data.

It refuses the set itself, your home directory, the working directory, and the filesystem root. It refuses paths that escape the destination. Absolute paths and paths with `..` never reach the disk.

## Share versions

`pack` writes one version to a single distributable file. `apply` brings a directory up to the version a pack carries. `merge` merges a pack into a set as a new version of it. A pack carries its own file table, so apply and merge need no other input.

```console
$ lepatch pack docs game.pack
$ lepatch apply game.pack /tmp/docs-restored
$ lepatch merge game.pack laptop
```

`apply` writes only what is missing. Files that already match keep their inode and timestamps. Use `--prune` with apply to remove entries the version does not record. Without `--prune`, apply reports them and leaves them alone. Use `--recheck` with apply to re-read every file, ignoring the record of the last apply.

A full pack suits any set and creates one if there is none. A delta pack names the version its reader holds. A set with no such version refuses it. Use `pack --from N` to pack only what that version's holder is missing. Use `pack --full` to pack the whole version, ignoring `--from`. `pack --compression` and `merge --compression` take `keep` or `0` to `9`. `keep` writes the stored form and is the default. Merging the same pack twice writes nothing and reports the version that already holds it. A pack records the number of the version it was built from. Two sets number versions alone, so that number never chooses a version in the receiving set.

Each version carries its run time in UTC. `versions` shows it in local time. A new version never predates the latest one: `backup` and `merge` refuse an older stamp, and two pushes at once race the tip.

## Remote sets and serve

`file://` works in every build. `sftp://`, `ftp://`, and `s3://` need `--features remote`. `https://` endpoints need `remote-tls`. Remote targets hold no local copy. A `file://` command pulls the set into staging, runs locally, and pushes back its writes. Use `--staging DIR` for the staging copy. Without it, the system cache holds the copy.

`lepatch serve` serves the sets under a directory over HTTP. It binds `--bind`, `127.0.0.1:8480` by default. There is no authentication and no TLS. Put it behind something that does the authentication, if it needs it. The server moves packs and nothing else.

Run backup and pack locally. Upload the pack. Merge it on the server. Or fetch a pack from the server. Merge it locally. `versions` of an `http://` set prints the same lines as a local set.

```console
$ lepatch serve ./backups
$ lepatch versions http://127.0.0.1:8480/docs
```

## Tune storage

Three tuned profiles cut trees into chunks.

| profile | minimum | average | maximum | for |
|---|---|---|---|---|
| `small` | 2 KiB | 4 KiB | 16 KiB | trees of many small files |
| `default` | 8 KiB | 16 KiB | 64 KiB | most trees |
| `large` | 32 KiB | 64 KiB | 256 KiB | video and archives of compressed files |

Use `default` for most trees. Use `small` for many small files. Use `large` for video and archives of compressed files. A profile affects later writes only. Old chunks stay readable, but later runs deduplicate less well across profiles.

```console
$ lepatch backup ~/Documents docs
$ lepatch backup ~/src docs --chunk-profile small
$ lepatch backup ~/video docs --chunk-profile large
```

Record payloads compress at level 1 by default. Use `--compression-level 0` to store them as they are. Use `2` to `9` for the smallest output. A payload that does not compress is stored raw at any level. A level affects later writes only. Old records stay readable at any level. Same content stored at two levels still deduplicates to one chunk.

Use `--segment-size SIZE` to set how large a segment becomes before a new one starts. The default is 4 GiB. A new size affects later segments only.

Use `--dry-run` to say what a command would do. It writes none of it. A dry run reads what the command reads, so its figures are the real run's. `backup`, `restore`, `apply`, `pack`, `merge`, and `reindex` take `--dry-run`. `versions` and `verify` write nothing, so they take no such flag.

## Reindex

Use `reindex` to rebuild the per-version index files from the store's commit records. They are a cache. Nothing is lost if they are missing or wrong. The `versions` count is a lower bound. A version without its index file still exists and still restores.

## What is preserved

lepatch preserves regular files, directories, symbolic links, hard links, owner ids, permission bits, and modification times. Empty files and empty directories survive. lepatch stores paths without valid UTF-8 as raw bytes.

lepatch does not preserve ACLs, extended attributes, or hard links outside the tree. It skips device nodes, named pipes, and sockets, and it reports each skip. Opening a FIFO for reading waits for a writer, so reading one would stall the run.

An unprivileged restore cannot set owners it does not hold. It reports those entries, writes everything else, and still succeeds. Re-run as root to set them.

## Telemetry

`--telemetry` sends this run's spans and metrics to an OTLP collector. Nothing leaves the machine without it or without `OTEL_EXPORTER_OTLP_ENDPOINT`. A collector that cannot be built warns at startup. The command carries on.

```console
$ lepatch --telemetry backup ~/Documents docs
```

## Library

The crate also works as a library. Open a set for writing, store a version, then restore it through a read-only open.

```rust
# async fn example() -> std::io::Result<()> {
use lepatch::{
    command::{Options, backup, lock_destination, restore},
    storage::{BlobStore, Compression, SegmentLimit},
};

let _claim = lock_destination(std::path::Path::new("docs-restored"))?;

let store = BlobStore::open_for_writing(
    "docs",
    SegmentLimit::default(),
    Compression::default(),
).await?;
let first = backup("~/Documents", Options::default(), store, None).await?;

let store = BlobStore::open_for_reading("docs").await?;
restore("~/Documents-restored", first.snapshot, store).await?;
# Ok(())
# }
```

`backup` returns the version number and the snapshot hash. `store.versions()` and `store.snapshot(n)` address versions directly. See `docs.rs` output for `pack`, `apply`, and `merge`.

## Formats

| What | Version |
|---|---|
| Snapshot | 5 |
| Record | 2 |
| Pack | 7 |

This build reads only stores it wrote. It refuses older stores instead of converting them. A pack is derived output. Re-run `pack` to rebuild one from the current set.

```console
$ lepatch pack set v8.pack --version 8
```

## Development

`mise` runs each task with the same command CI runs.

```console
$ mise install
$ mise run check
$ mise run test
$ mise run lint
$ git config core.hooksPath .githooks
```

`mise run check` runs all tasks. `mise run test` runs tests. `mise run lint` runs style tasks. Set `core.hooksPath` to `.githooks` to run fast tasks on each commit. CI runs the same commands on Linux.

## License

The project uses GPL-2.0-or-later. See `LICENSE` for the full text.
