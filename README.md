# blu

blu backs up directories with content-defined chunking. Chunk borders follow content, so a small edit moves only nearby chunks. blu stores repeated bytes once, within one version and across versions.

> **Status: pre-release.** The on-disk format carries a version and refuses unknown stores, but the layout is not stable yet. Do not use it for data you cannot recreate.

## Install

Download the archive for your platform from releases. Verify its checksum against `sha256.sum`. Or install the `blu` binary from crates.io with `cargo install blulib`. Or build from source with `cargo build --release`. Building needs the Rust version in `Cargo.toml`.

## Quick start

```console
$ blu backup ~/Documents docs
$ blu backup ~/Documents docs
$ blu versions docs
$ blu verify docs
$ blu restore /tmp/docs-restored docs
$ blu reindex docs
$ blu pack docs game.pack
$ blu merge game.pack other
$ blu serve ./backups
```

Every command that works on a backup set takes one `TARGET`. A local path names a local set. An address with `://` names a serve set or a keyed store. Remote targets hold no local copy.

```console
$ blu backup ~/Documents docs
$ blu backup ~/Documents file:///mnt/drive/docs
$ blu backup ~/Documents http://127.0.0.1:8480/docs
```

Without `--version`, restore, verify, and pack use the most recent version. `stdout` is data. `stderr` is diagnostics. Every command is pipeable without `--quiet`. Use `--numbers-only` to print just the version numbers, one per line.

## Restore

Restore merges by default. It writes files from the snapshot and leaves other files alone. It reports what it wrote when it finishes, and points at `--clean` for an exact copy.

Use `--clean` to hold exactly the snapshot and nothing else.

```console
$ blu restore /tmp/docs-restored docs --clean
```

`--clean` builds the tree beside the destination and then replaces the destination. A stopped `--clean` restore leaves the destination unchanged. `--clean` is the only operation that removes data.

It refuses the set itself, your home directory, the working directory, and the filesystem root. It refuses paths that escape the destination. Absolute paths and paths with `..` never reach the disk.

## Share versions

`pack` writes one version to a single distributable file. `apply` brings a directory up to the version a pack carries. `merge` merges a pack into a set as a new version of it. A pack carries its own file table, so apply and merge need no other input.

```console
$ blu pack docs game.pack
$ blu apply game.pack /tmp/docs-restored
$ blu merge game.pack laptop
```

`apply` writes only what is missing. Files that already match keep their inode and timestamps. Use `--prune` with apply to remove entries the version does not record. Without `--prune`, apply reports them and leaves them alone. Use `--recheck` with apply to re-read every file, ignoring the record of the last apply.

A full pack suits any set and creates one if there is none. A delta pack names the version its reader holds. A set with no such version refuses it. Use `pack --from N` to pack only what that version's holder is missing. Use `pack --full` to pack the whole version, ignoring `--from`. `pack --compression` and `merge --compression` take `keep` or `0` to `9`. `keep` writes the stored form and is the default. Merging the same pack twice writes nothing and reports the version that already holds it. A pack records the number of the version it was built from. Two sets number versions alone, so that number never chooses a version in the receiving set.

Each version carries its run time in UTC. `versions` shows it in local time. A new version never predates the latest one: `backup` and `merge` refuse an older stamp, and two pushes at once race the tip.

## File history

`versions --files` reports the versions where the named paths changed, oldest first. One line per change: the version, `created`, `modified` or `deleted`, and the path.

```console
$ blu versions docs --files src/main.rs README.md
```

A value with `*`, `?`, `[`, `\` or `{` is a glob pattern: `*` spans one path level, `**` spans nested levels, and a backslash names the next byte literally. Quote patterns so the shell does not expand them. A value naming a directory matches everything under it.

## Restore and apply per file

`restore` and `apply` take the same `--files` to write only the selection. Parent directories are created as scaffolding, and everything else is left alone.

```console
$ blu restore /tmp/docs-restored docs --files src/main.rs
$ blu apply game.pack /tmp/docs-restored --files 'src/*.rs'
```

`restore --clean` is refused with `--files`: a clean restore promises exactly the snapshot. `apply --prune` scopes to the selection, removing only the unrecorded entries the filter keeps. A remote restore fetches only the chunks the selection needs.

## Remote sets and serve

Every build speaks `file://`, `ftp://` and `s3://`; unix builds also speak `sftp://` (`openssh` is unix-only). `s3` speaks plain HTTP and verified HTTPS. HTTPS verifies against the system roots. Remote targets hold no local copy. A `file://` command pulls the set into staging, runs locally, and pushes back its writes. Use `--staging DIR` for the staging copy. Without it, the system cache holds the copy.

`blu serve` serves the sets under a directory over HTTP. It binds `--bind`, `127.0.0.1:8480` by default. The server checks no auth. Put it behind a proxy (nginx, Caddy) when it needs auth. The proxy checks the bearer token. The client forwards it: pass `--token` or set `BLU_TOKEN`. The token travels as `Authorization: Bearer`. The server moves packs and nothing else. Clients speak plain HTTP and verified HTTPS.

Run backup and pack locally. Upload the pack. Merge it on the server. Or fetch a pack from the server. Merge it locally. `versions` of an `http://` or `https://` set prints the same lines as a local set.

```console
$ blu serve ./backups
$ blu versions http://127.0.0.1:8480/docs
```

## Tune storage

Three tuned profiles cut trees into chunks.

| profile | minimum | average | maximum | for |
|---|---|---|---|---|
| `small` | 2 KiB | 4 KiB | 16 KiB | trees of many small files |
| `default` | 8 KiB | 16 KiB | 64 KiB | most trees |
| `large` | 32 KiB | 64 KiB | 256 KiB | video and archives of compressed files |

Use `default` for most trees. Use `small` for many small files. Use `large` for video and archives of compressed files. A profile affects later writes only. Old chunks stay readable, but later runs deduplicate less well across profiles.

The scan uses SIMD Gear hashing where the CPU supports it. Cuts stay the same on every CPU. Cuts differ from releases before SIMD. Old chunks still restore.

```console
$ blu backup ~/Documents docs
$ blu backup ~/src docs --chunk-profile small
$ blu backup ~/video docs --chunk-profile large
```

Record payloads compress at level 1 by default. Use `--compression-level 0` to store them as they are. Use `2` to `9` for the smallest output. A payload that does not compress is stored raw at any level. A level affects later writes only. Old records stay readable at any level. Same content stored at two levels still deduplicates to one chunk.

Use `--segment-size SIZE` to set how large a segment becomes before a new one starts. The default is 4 GiB. A new size affects later segments only.

Use `--dry-run` to say what a command would do. It writes none of it. A dry run reads what the command reads, so its figures are the real run's. `backup`, `restore`, `apply`, `pack`, `merge`, and `reindex` take `--dry-run`. `versions` and `verify` write nothing, so they take no such flag.

## Reindex

Use `reindex` to rebuild the per-version index files from the store's commit records. They are a cache. Nothing is lost if they are missing or wrong. The `versions` count is a lower bound. A version without its index file still exists and still restores.

## What is preserved

blu preserves regular files, directories, symbolic links, hard links, owner ids, permission bits, modification times, extended attributes, and access lists. Empty files and empty directories survive. blu stores paths without valid UTF-8 as raw bytes.

On Linux blu keeps `user` extended attributes and POSIX access lists. It skips `security` and `trusted` names. Those need privilege to read and to write. On Windows blu keeps explicit allow and deny entries. Inherited entries stay with the parent. A restored file blocks inheritance from its parent. Symbolic links carry no extended attributes. Their target carries its own entry.

blu does not preserve hard links outside the tree. It skips device nodes, named pipes, and sockets, and it reports each skip. Opening a FIFO for reading waits for a writer, so reading one would stall the run.

An unprivileged restore cannot set owners it does not hold. It reports those entries, writes everything else, and still succeeds. Re-run as root to set them.

## Telemetry

`--telemetry` sends this run's spans and metrics to an OTLP collector. Nothing leaves the machine without it or without `OTEL_EXPORTER_OTLP_ENDPOINT`. A collector that cannot be built warns at startup. The command carries on.

```console
$ blu --telemetry backup ~/Documents docs
```

## Library

The crate also works as a library, published to crates.io as `blulib` (`cargo add blulib`). Open a set for writing, store a version, then restore it through a read-only open.

```rust
# async fn example() -> std::io::Result<()> {
use blulib::{
    command::{BackupBuilder, RestoreBuilder, lock_destination},
    storage::BlobStore,
};

let _claim = lock_destination(std::path::Path::new("docs-restored"))?;

let store = BlobStore::builder("docs").open_writer().await?;
let first = BackupBuilder::new("~/Documents", store)
    .latest(None)
    .run()
    .await?;

let store = BlobStore::builder("docs").open_reader().await?;
RestoreBuilder::new("~/Documents-restored", first.snapshot, store)
    .run()
    .await?;
# Ok(())
# }
```

`BackupBuilder::run` returns the version number and the snapshot hash. `store.versions()` and `store.snapshot(n)` address versions directly. See `docs.rs` output for `PackBuilder`, `ApplyBuilder`, and `MergeBuilder`.

## Formats

| What | Version |
|---|---|
| Snapshot | 6 |
| Record | 2 |
| Pack | 7 |

This build reads only stores it wrote. It refuses older stores instead of converting them. A pack is derived output. Re-run `pack` to rebuild one from the current set.

```console
$ blu pack set v8.pack --version 8
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

Copyright (C) 2025-2026 Aza Maulana.

All source, test, bench, and example files are `GPL-2.0-or-later` — version 2
of the GNU General Public License, or (at your option) any later version.
Each `.rs` file carries `SPDX-License-Identifier: GPL-2.0-or-later`; see
`REUSE.toml` for the machine-readable mapping. `Cargo.toml`, `openapi/blu.json`,
and `deny.toml` declare the same identifier.

`LICENSE` bundles the GPL-2.0 text. No GPL-3.0 text is bundled: later versions
apply by reference per section 9 of the license.

Exception: `.github/workflows/release.yml` is generated boilerplate from
`cargo-dist` and stays `MIT OR Apache-2.0` under its own header.
