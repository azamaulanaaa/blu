# Benchmark guide

> **Draft.** This guide describes the benchmark tools at commit `91d0e0e`. A maintainer must review it before the project uses it.

## 1. Purpose and scope

This guide explains every benchmark and measurement tool in the `blu` repository. It tells a contributor how to run each tool, how to read its output, and how to add a scenario. The reader knows Rust and does not know this benchmark system.

The guide covers the four Criterion targets and two measurement examples. It also covers the fixture generator, the paired runner, the two-revision driver, the scenario table, the tests, and the CI jobs. It does not describe the backup format or the command-line interface. The `README.md` covers those.

All paths are relative to the repository root unless the text says otherwise. The guide describes commit `91d0e0e`.

## 2. Choose a tool

The repository has four Criterion targets and two measurement examples. Use the target or example that matches the question.

| Target | What it measures | Command | Fixture |
| --- | --- | --- | --- |
| `hotpath` | Unit work: chunking, BLAKE3 hashing, snapshot encode and decode, bounded stream adapters. | `cargo bench --bench hotpath` | Own tree: 64 files of 512 KiB, plus in-memory samples. |
| `pack` | Pack open, locate, needed, covering, source, header, and zstd decode. | `cargo bench --bench pack` | Own tree: 96 files of 256 KiB, about 970 chunks. |
| `store` | Store put, open, and sync. | `cargo bench --bench store` | Own blobs: 256 of 64 KiB. |
| `throughput` | Whole backup at concurrency 1, 2, 4, 8, and 16, and whole restore. | `cargo bench --bench throughput` | Own tree: 200 files of 256 KiB. |

No target reads an external fixture. Each target builds its own fixture in a temporary directory before it starts to measure. The `hotpath` target also uses in-memory data for its hashing and serialisation benchmarks.

| Example | What it measures | Command | Fixture |
| --- | --- | --- | --- |
| `ab compare` | Backup throughput over concurrency 1, 2, 4, and 8 on a shared tree and a unique tree. | `cargo run --release --example ab -- compare` | Own trees: two sets of 200 files of 256 KiB. |
| `ab report` | One backup configuration, printed as one `RESULT` line. | `cargo run --release --example ab -- report LABEL 9` | Own unique tree: 200 files of 256 KiB. |
| `putbench` | Store put against a plain `std::fs` write, back to back. | `cargo run --release --example putbench` | Own blobs: 1500 of 64 KiB. |

The `ab` example writes no JSON report. It prints a table or one `RESULT` line. The `putbench` example prints a comparison table. Neither example takes a fixture argument. Each builds its own tree or store.

### 2.1 `hotpath`

`hotpath` measures the unit work of a backup. The groups are chunking, hashing, snapshot serialisation, and bounded stream adapters. The chunking group reports `read_and_cut` and `registering`. The difference between them is the chunking cost.

The header quotes about 46% chunking and about 27% hashing from a 200 MiB audit tree. It also quotes about 28% for plumbing, which this target does not measure. The header derives that share by subtraction, not by measurement. This target uses a 32 MiB tree, so do not treat those shares as this target's output.

The target builds a tree of 64 files of 512 KiB in a temporary directory. It needs no external fixture.

### 2.2 `pack`

`pack` measures the layer between chunking and the whole run. The benchmarks are `open`, `locate`, `needed`, `covering`, `source`, `header`, and `decode`. The fixture holds 96 files of 256 KiB. Every file shares half of its bytes with every other file. The pack holds about 970 chunks and both compressed and raw records.

The target opens the pack from a file, so the numbers include the bytes a client downloads. It builds the tree, the store, and the pack once before the first measurement. It needs no external fixture.

### 2.3 `store`

`store` measures the store layer. The benchmarks are `put`, `open`, and `sync`. `put` writes 256 blobs of 64 KiB at the concurrency a backup uses. It measures the write path: hashing, compression, and the append to the segment. `open` opens a store and builds its blob map. `sync` opens each dirty segment and flushes its pages. The fixture holds three stores because each benchmark needs a different one.

A segment header carries no record count, so the open caps its reservation for the blob map. The `open` benchmark checks that cap on every open. The target needs no external fixture.

### 2.4 `throughput`

`throughput` measures a whole backup and a whole restore. It runs the backup at concurrency 1, 2, 4, 8, and 16. The restore runs once per round. The tree holds 200 files of 256 KiB. Each backup round starts from an empty store, so the run cannot deduplicate against itself.

The absolute throughput is higher than the real binary's. The project does not explain the gap. The ratios between the rows are sound. The target header records that the curve still rises at 16 on a four-core box. CPU time falls as concurrency rises. The default is 8. Use `throughput` for the shape of the curve, and time the binary for a number to quote. The target needs no external fixture.

### 2.5 Read Criterion output

Criterion is reliable for a ratio within one run. It is unreliable for a comparison against a previous run on a shared machine. The `examples/ab.rs` header records a 16% shift between runs of one adapter benchmark. It concludes that an effect under about 20% is not measurable that way. Use the paired runner or the two-revision driver for a before/after claim.

Cargo builds the bench targets with the `bench` profile. The profile inherits `release`, uses one codegen unit, and keeps debug symbols. The crate disables Criterion's plotting features, so the run prints text and writes no HTML report.

`--test` runs one iteration of each benchmark and reports no number. CI uses it for the fast check. It shows that the targets build and run.

### 2.6 `ab` and `putbench`

`ab compare` measures four concurrency settings on two trees, one with no shared bytes and one with half of each file shared. It runs 9 interleaved rounds. It reports the minimum, the median, and the ratio between the median and the minimum. A ratio near 1.0 means a quiet machine.

`ab report LABEL 9` measures one configuration and prints one `RESULT` line. The line holds `min_ms`, `median_ms`, `contamination`, `stored_mib`, and `cpu_ms`. Use a shell loop to alternate two builds.

The absolute throughput from `ab` is above the real binary's. The project does not explain the gap. Use the ratios. `ab` also has `gen DIR`. It writes the two trees and prints their paths. `putbench` runs two warmup rounds, then five measured rounds. It compares the store put against a plain file write on the same machine. The gap between the two numbers is the store's own cost.

## 3. Fixture generator: `examples/mktree.rs`

`mktree` writes a tree of files with a known shape. The benchmark tools use it to build the same bytes for both arms of a comparison. It uses the repository's own byte generator, `benches/fixture.rs`. The test `tests/fixtures.rs` guards that generator.

The exact command forms are:

```console
cargo run --release --example mktree -- tree DIR FILES BYTES SHARED
cargo run --release --example mktree -- identical DIR FILES BYTES
cargo run --release --example mktree -- unique DIR FILES BYTES
cargo run --release --example mktree -- compressible DIR FILES BYTES
cargo run --release --example mktree -- mutate DIR
```

`DIR` is the destination directory. `FILES` is the number of files. `BYTES` is the number of bytes in each file. `SHARED` is the number of bytes at the start of every file.

| Shape | Arguments | What it writes |
| --- | --- | --- |
| `tree` | `DIR FILES BYTES SHARED` | A shared prefix, then bytes that no other file holds. |
| `identical` | `DIR FILES BYTES` | The same bytes in every file. |
| `unique` | `DIR FILES BYTES` | Bytes that no other file holds. |
| `compressible` | `DIR FILES BYTES` | A 1 KiB pattern that repeats in every 64 KiB block. Each block has a different pattern. |
| `mutate` | `DIR` | New bytes in one file of an existing tree. |

The `tree`, `identical`, `unique`, and `compressible` shapes empty `DIR` first. The `mutate` shape never removes anything. It rewrites the middle file in the sorted file list, and keeps the file length. The example derives the new bytes from the old bytes, so two runs on the same tree write the same new bytes. A second mutation changes the file again.

The shape and the parameters select every seed. The clock never reaches the bytes. Two runs with the same arguments write the same bytes. `tests/mktree.rs` pins this property.

`SHARED` must not exceed `BYTES`. It can be zero. The program stops with exit code 2 when an argument is not a number or when `SHARED` exceeds `BYTES`.

## 4. Paired runner: `examples/bench.rs`

`examples/bench.rs` has two modes. Pair mode measures two binaries with one command. Revision mode builds two revisions and measures them with the scenario table. This section covers pair mode. Section 5 covers revision mode.

Pair mode answers one question: is the head binary faster than the base binary? It runs both binaries in one process and interleaves them. One round runs the base arm, then the head arm. The next round runs the head arm first. A slow patch from a neighbour then affects both arms.

### 4.1 Command

The exact command form is:

```console
cargo run --release --example bench -- \
    --base ./blu-base --head ./blu-head [--rounds N] -- blu backup source store
```

A command after `--` selects pair mode. `--base` and `--head` name the two binaries. The command after `--` starts with a program name. The runner replaces that name with the binary of each arm. It passes the other arguments unchanged. The command runs in the current directory.

### 4.2 Rounds and warmups

`--rounds N` sets the rounds per arm. The default is 9. The runner rejects a count below 2. Pair mode does not create a fixture or a store. It repeats the command as written. If the command writes to a fixed store, later rounds read the store from earlier rounds. Use a command that clears its output, or use the two-revision driver for a fresh store per round.

The Criterion targets run a warmup before they measure. Pair mode does not. The runner has no warmup option and no warmup round. Every round enters the result. By default, three noise rounds run inside the same loop as the arms. They are not warmups, because the floor uses their differences.

### 4.3 Paired median

Each round gives one paired difference. The difference is `(head - base) / base * 100`. The runner sorts the differences and takes the middle one. For an even count, it takes the upper middle one. That value is the `paired median` in the table.

The table also shows `base median` and `head median`. Those are the medians of the raw values in each arm. They do not set the verdict. The paired median does.

### 4.4 Sign counts

A negative difference means the head value is lower. A positive difference means the head value is higher. The runner counts the rounds in each direction. A tie counts for neither arm. The report prints the head wins first, then the base wins.

### 4.5 Noise floor

By default, three rounds run the base binary in both arms. The paired differences of those rounds show what the machine does to one binary. The floor is the largest absolute difference of those rounds. The runner takes the largest, not the median, so the floor covers the worst noise the machine reached.

An effect at or below the floor is `unmeasurable`. The test `tests/bench.rs` pins the equality case: an effect equal to the floor is `unmeasurable`.

### 4.6 Verdicts

The runner checks the floor first. If the effect is above the floor, it checks the sign counts. The head arm must win at least 2 rounds more than the base arm. The base arm has the same margin in the other direction. The four verdicts are:

| Verdict | Condition |
| --- | --- |
| `head faster` | The effect is above the floor, and the head arm wins by at least 2 rounds. |
| `head slower` | The effect is above the floor, and the base arm wins by at least 2 rounds. |
| `no difference` | The effect is above the floor, and neither arm has a 2-round margin. |
| `unmeasurable` | The effect is at or below the floor. |

An effect above the floor is not automatically a result. The sign counts must also separate the arms. The pair report prints the wall-time verdict as the headline verdict. The table gives a verdict for each measurement.

The verdict uses the same four words for wall time, CPU time, and peak resident memory. For memory, `head faster` means the head arm used less memory.

### 4.7 Host noise

The report records the load average before and after the run, the number of cores, and the free memory. The host is noisy when the load average is greater than the number of cores, before or after the run. A noisy run has no verdict. The table prints `noisy` in the verdict column, and the JSON report writes `null` as the verdict.

### 4.8 JSON report

Pair mode writes `reports/bench/<base-stem>-vs-<head-stem>.json` in the current directory. For `./blu-base` and `./blu-head`, the file is `reports/bench/blu-base-vs-blu-head.json`. If both binaries have the same file stem, the file name is the same, so give them different names. The report holds the command, the rounds, the three measurements, the noise floor, the host state, and the verdict. It also holds the BLAKE3 hash of each binary.

The `reports/` directory is in `.gitignore`. The printed report is the table for a commit message. The JSON report is the full data.

### 4.9 Pair-mode limits

Pair mode does not check that the two binaries contain different bytes. It rejects only the same file or the same inode. Two copies of one binary pass the check, so the run measures one binary against itself. Read the two BLAKE3 hashes in the printed report before you trust a verdict.

## 5. Two-revision driver

Revision mode of `examples/bench.rs` builds two git revisions and measures them with the scenario table. It is the tool for a before/after claim on a real change. The parser selects revision mode when no command follows `--`.

The exact command form is:

```console
cargo run --release --example bench -- \
    --base REV --head REV [--scenario NAME] [--diff FROM..TO] \
    [--rounds N] [--keep-worktrees]
```

| Option | Meaning |
| --- | --- |
| `--base REV` | The base revision. |
| `--head REV` | The head revision. |
| `--scenario NAME` | One row by name. Repeat the option for more rows. |
| `--diff FROM..TO` | Select rows whose paths a changed file touches. |
| `--rounds N` | Rounds per arm. Default 9, minimum 2. |
| `--keep-worktrees` | Keep the worktrees and the run directory. |

The driver rejects any other option. There is no `--warmup` option and no warmup round. Every measured round enters the result. The noise rounds are not warmups. The driver rejects `--scenario`, `--diff`, and `--keep-worktrees` in pair mode.

### 5.1 Worktrees and the build

The driver adds one detached worktree for each revision. It puts them under the system temporary directory in a run directory named `blu-bench-<pid>-<base>-<head>-<stamp>`. Each worktree has its own `target` directory. The driver sets `CARGO_TARGET_DIR` to `<worktree>/target`, so cargo cannot reuse the other revision's artifact. It builds `blu` with `cargo build --release --bin blu -j 1`, then copies the two binaries into `<run-dir>/bin/`.

The inner build sets `CARGO_INCREMENTAL=0` and `CARGO_PROFILE_RELEASE_DEBUG=0`. The driver holds a host-wide lock around each inner cargo call. The lock file is `/tmp/blu-bench-cargo.lock` on this host. It releases the lock before it measures a binary. It removes the worktrees and the run directory at the end, unless you give `--keep-worktrees`. The JSON report records the run directory and whether the worktrees stayed.

### 5.2 A fresh store per round

For each sample, the driver makes a fresh directory under `<run-dir>/stores/<scenario>/<counter>/`. It sets `{store}`, `{dest}`, and `{pack}` inside that directory. It runs every `prepare` command, then the measured command. It removes the directory after the sample. The counter increases for every sample, so the base and head arms never share a store.

The driver does not time a `prepare` command. It shares one fixture between the two arms. Both arms read the same bytes.

### 5.3 The JSON report

The driver writes one JSON report per run. The path is `reports/bench/bench-<base-short>-vs-<head-short>-<unix-seconds>.json` under the repository root. The report holds every selected row and every skipped name. It also holds the stale names, the changed paths, the uncovered paths, the host state, and the per-row verdict. It also holds the full revision hash and the BLAKE3 hash of each binary.

`reports/` is in `.gitignore`. The report stays on the machine. The printed report is the table for a commit message.

### 5.4 Select rows

`--scenario NAME` selects one row. Repeat it for more rows. `--diff FROM..TO` runs `git diff --name-only FROM..TO`. It selects every row whose `paths` entry equals a changed path or is a parent directory of one. The two selectors combine with OR. A run with no selector runs all 18 rows.

`--diff` selects rows. It does not set the base or head revision. Give `--base` and `--head` separately. The driver checks stale paths at the `--head` revision, not at the `TO` revision.

The printed report lists the selected names and the skipped names. It lists every changed path that no runnable, non-stale row covers. A row marked `unmeasurable` does not cover a path for this list. Check that list before you read a verdict. The driver does not measure an uncovered path.

### 5.5 Compare one change

To compare one change, name the row:

```console
cargo run --release --example bench -- \
    --base origin/main --head HEAD --scenario pack.recompress
```

To select rows from the diff, use the range:

```console
cargo run --release --example bench -- \
    --base origin/main --head HEAD --diff origin/main..HEAD
```

Check the selection line before you read the verdicts. A row with no effect still prints a table. The base revision must be available as a local commit.

### 5.6 Measured examples

A measured run in the audit compared base `6e36724` with head `7434448` on `pack.recompress` with 5 rounds. The wall time moved from 3729.5 ms to 1902.8 ms, a paired median of -49.0%. The wall floor was 5.3%, so the verdict was `head faster`. The CPU time agreed at -47.1%. The peak resident memory did not move.

```text
  | measurement | base median | head median | paired median | floor | verdict |
  | --- | --- | --- | --- | --- | --- |
  | wall time (ms) | 3729.5 | 1902.8 | -49.0% | 5.3% | head faster |
  | cpu time (ms) | 2146.9 | 1137.7 | -47.1% | 7.6% | head faster |
  | peak resident memory (kB) | 32120.0 | 32120.0 | +0.0% | 0.0% | no difference |
```

The audit measured -56.5% for the same fixture shape with a different run. Do not treat the two numbers as the same measurement. The run took about 30 minutes on the 4-core, 4 GiB host. Most of that time was release builds.

A second audit run put the same revision `161823a` in both arms on `history.report` with 2 rounds. The wall time moved from 110.7 ms to 109.8 ms, a paired median of +1.5% against a floor of 0.3%. The verdict was `no difference`. The two binaries were byte-identical, so the run measured one binary against itself. The effect was above the floor, and the verdict was still `no difference`. The sign counts must also separate the arms.

### 5.7 Driver limits

The driver has no warmup option and no warmup round. Every round enters the result. A run with no selector runs all 18 rows, including 7 rows that cannot run. A run on one row can take about 30 minutes on the audit host, mostly for release builds.

The driver does not start a serve process. The `merge.push` row cannot run. The unmeasurable store and plan rows need commands that the `blu` binary does not have. The table marks those rows `unmeasurable` and gives the reason.

## 6. Scenario table

`benches/scenarios.toml` holds the rows the driver measures. Each `[[scenario]]` entry is one row. The table below lists all 18 rows. 11 rows are runnable. 7 rows are unmeasurable, and the last column gives the reason.

`shape` is not the fixture shape. It says whether the row needs a setting that the product does not use by default. `default` means the product's defaults reach the shape. `forced` means the row needs a non-default setting, such as `--compression 3` or `--segment-size 8MiB`.

The fixture column uses `files × bytes` and gives the shared prefix in bytes. Each command omits the leading `--quiet`, which every row uses.

| Scenario | Shape | Fixture | Command | Status |
| --- | --- | --- | --- | --- |
| `backup.many-small` | default | tree 5000 × 32768, shared 16384 | `backup {src} {store}` | runnable |
| `backup.one-large` | default | tree 1 × 268435456, shared 0 | `backup {src} {store}` | runnable |
| `backup.mixed` | default | tree 800 × 262144, shared 131072 | `backup {src} {store}` | runnable |
| `restore.default` | default | compressible 200 × 1048576 | `restore {dest} {store} --clean` | runnable |
| `restore.shared-chunks` | default | identical 400 × 1048576 | `restore {dest} {store} --clean` | runnable |
| `pack.default` | default | tree 800 × 262144, shared 131072 | `pack {store} {pack}` | runnable |
| `pack.recompress` | forced | compressible 250 × 1048576 | `pack {store} {pack} --compression 3` | runnable |
| `apply.default` | default | tree 800 × 262144, shared 131072 | `apply {pack} {dest}` | runnable |
| `merge.push` | default | tree 800 × 262144, shared 131072 | `merge {pack} http://127.0.0.1:8480/bench` | unmeasurable: the driver cannot start the serve process that receives the push |
| `store.put.8mib` | forced | unique 50 × 8388608 | `backup {src} {store}` | unmeasurable: the `blu` binary has no store-level put command, and a backup cuts records at 64 KiB |
| `store.open` | default | unique 4000 × 65536 | `versions {store}` | unmeasurable: the `blu` binary has no store-level open command |
| `store.sync.multi-segment` | forced | unique 24 × 8388608 | `backup {src} {store} --segment-size 8MiB` | unmeasurable: the `blu` binary has no sync command, and only a backup syncs a store |
| `plan.from-snapshot.sparse` | forced | tree 1 × 1048576, shared 0 | `pack {store} {pack}` | unmeasurable: the `blu` binary has no plan command, and only serve plans from a snapshot |
| `plan.since` | default | tree 2000 × 65536, shared 32768 | `pack {store} {pack} --from 1` | runnable |
| `pack.index.open` | default | tree 800 × 262144, shared 131072 | `apply {pack} {dest} --dry-run` | unmeasurable: the `blu` binary has no command that opens a pack and stops, because every command that opens one reads its chunks |
| `pack.covering` | default | tree 800 × 262144, shared 131072 | `apply {pack} {dest} --dry-run` | unmeasurable: the `blu` binary has no command that requests a range set, and only serve calls `chunks_covering` |
| `history.report` | default | unique 20000 × 256 | `versions {store} --files f0* --numbers-only` | runnable |
| `probe.check` | forced | identical 200 × 1048576 | `apply {pack} {dest} --dry-run --recheck` | runnable |

The table names `wall_ms` for every row except `store.open`, which names `rss_kb`. The driver prints all three measurements for every row that runs.

The `backup.one-large` row has no audit number. Treat its first run as a baseline. The audit did not smoke-check the 256 MiB one-file fixture. The other runnable rows have an audit number, but many use a near shape, not the exact fixture. Do not compare a new run against them.

### 6.1 Add a row

Add a `[[scenario]]` entry to `benches/scenarios.toml`. The loader reads these fields:

| Field | Required | Meaning |
| --- | --- | --- |
| `name` | yes | A unique name. The loader rejects a duplicate. |
| `paths` | yes | A non-empty array of repo-relative files or directories. |
| `shape` | yes | `default` or `forced`. |
| `fixture` | yes | A table that names the bytes. |
| `prepare` | no | An array of commands that fill the store before the measured command. |
| `command` | yes | A non-empty array. The driver measures this command. |
| `metric` | yes | `wall_ms`, `cpu_ms`, or `rss_kb`. The row's verdict uses this metric. |
| `unmeasurable` | no | A reason string. The driver skips the row and reports the reason. |

`fixture` takes `kind` and, for every kind except `mutate`, `files` and `bytes`. The `tree` kind also requires `shared`. The `shared` value can be zero, but it must not exceed `bytes`. `files` and `bytes` must be positive. The loader rejects a `tree` without `shared`. The `kind` value must be one that `mktree` accepts.

Use `shared` with the `tree` kind. The other kinds take no `shared` argument.

The driver replaces `{src}` with the fixture tree, `{store}` with the store path, `{dest}` with the destination, and `{pack}` with the pack path. It does this in `prepare` and in `command`. The fixture cache uses `kind-files-bytes-shared` as its key, so two rows with the same fixture share one tree.

A `prepare` command is not timed. The driver runs it once for each sample, before the measured command.

A minimal row has this form:

```toml
[[scenario]]
name = "pack.sample"
paths = ["src/pack.rs"]
shape = "default"
fixture = { kind = "tree", files = 800, bytes = 262144, shared = 131072 }
prepare = [["--quiet", "backup", "{src}", "{store}"]]
command = ["--quiet", "pack", "{store}", "{pack}"]
metric = "wall_ms"
```

Change the name and the paths for your scenario. Check the selection with `--scenario pack.sample` before you run a measurement.

### 6.2 The stale-path rule

The driver checks every `paths` entry at the `--head` revision. If one entry does not exist, the row is `stale` and the driver does not run it. The report prints the path and the reason. The driver checks all rows, not only the selected rows.

The driver checks existence only. It does not check the path type. A stale row does not cover a changed path. The `uncovered_paths` list includes a changed path when its only row is stale or unmeasurable. The audit found no stale row, so it did not exercise the stale path with a missing file.

## 7. Tests

Two integration tests protect the benchmark tools. Neither test measures a number. Both include the example by path, so the test and the tool cannot drift apart.

Run them with:

```console
cargo test --test bench
cargo test --test mktree
```

`tests/bench.rs` includes `examples/bench.rs`. It pins the paired statistics. These are the paired median from the round differences, the sign counts with a tie, and the four verdicts. The test also pins the floor as the largest paired difference and the equality case.

`tests/mktree.rs` includes `examples/mktree.rs`. It pins the five shapes. The properties are the shared prefix and unique tails, identical bytes, unique bytes, and the repeating pattern with its compression. The test also pins the one-file mutation, the same bytes from two runs, and the second mutation.

On the 4 GiB host, run each test with `CARGO_PROFILE_TEST_DEBUG=0` and `-j 1`:

```console
CARGO_PROFILE_TEST_DEBUG=0 cargo test --test bench -j 1
CARGO_PROFILE_TEST_DEBUG=0 cargo test --test mktree -j 1
```

In this environment, use the serialized wrapper:

```console
CARGO_PROFILE_TEST_DEBUG=0 /tmp/opencode/perf/cargo-bench.sh \
    <worktree> test --test bench -j 1
```

The wrapper waits for the host lock, so it does not start a second cargo.

## 8. CI

The workflow `.github/workflows/ci.yml` defines three benchmark jobs. All three set `continue-on-error: true`, so a failure does not change the pull request or release result. The workflow has `contents: read` permission.

| Job | When it runs | What it runs | Artifact | Retention |
| --- | --- | --- | --- | --- |
| `bench-fast` | Every pull request. | `cargo bench --locked --bench hotpath --bench pack -- --test` | None. | None. |
| `bench-pair` | A pull request with the `perf` label. | The driver with `--base origin/main --head HEAD --diff origin/main..HEAD`. | `bench-pair-report` | 30 days. |
| `bench-verify` | The weekly schedule (Monday 04:00 UTC) and a tag run. | The driver with the previous release tag as base and `HEAD` as head. | `bench-verify-report` | 90 days. |

`bench-fast` runs one iteration of each `hotpath` and `pack` benchmark. It reports no measurement. It writes a job summary that asks a maintainer for the `perf` label. It does not run `store` or `throughput`.

`bench-pair` and `bench-verify` upload `reports/bench/*.json` with `if: always()`. The job summary says the run is advisory. The `perf` label is the opt-in. The `bench-pair` job starts on the next push after a maintainer adds the label. `bench-verify` compares a tag with the tag before it, or the default branch with the last release.

**CI status:** This draft checks the job definitions against the workflow and the tools. This guide records no job run on GitHub. Treat the first run of each job as unchecked.

## 9. Host and safety rules

The benchmark host has 4 GiB of memory and 4 cores. A second cargo run can push a link over the memory limit and kill the linker. A killed link can leave a truncated binary. Follow these rules:

1. Run one cargo command at a time.
2. Do not start a second cargo while one is active.
3. Use `CARGO_PROFILE_TEST_DEBUG=0` for tests.
4. Use `-j 1` for tests.
5. Use the serialized wrapper for each cargo command that does not start another cargo.
6. Keep the machine otherwise idle.

The wrapper is `/tmp/opencode/perf/cargo-bench.sh`. Its form is:

```console
/tmp/opencode/perf/cargo-bench.sh WORKTREE CARGO_ARGS
```

The wrapper changes to `WORKTREE` and runs the cargo arguments. It holds `/tmp/blu-bench-cargo.lock` for the whole command. It sets `CARGO_TARGET_DIR=/tmp/opencode/perf/target-bench`, `CARGO_INCREMENTAL=0`, and `CARGO_BUILD_JOBS=1`. It touches `src/lib.rs`, so cargo rebuilds the local crate. A run that does not print `Compiling blulib` used a stale artifact.

The driver takes the same lock around each inner cargo call. Do not wrap the driver in `cargo-bench.sh`. The outer cargo would hold the lock, and the inner cargo would wait for it forever. Build the example with the wrapper, then run the built binary directly:

```console
/tmp/opencode/perf/cargo-bench.sh <worktree> build --release --example bench
/tmp/opencode/perf/target-bench/release/examples/bench \
    --base REV --head REV --scenario NAME
```

A direct cargo command does not take the lock. Run it only when no other cargo is active. A test run holds the lock for the whole command. A second wrapped cargo waits for it.

## 10. Troubleshooting

### 10.1 The two binaries are identical

Pair mode rejects the same file or the same inode. It does not reject two different files with the same bytes. If the two BLAKE3 hashes in the printed report match, the run measured one binary against itself. The driver warns when the two revisions build the same binary: `the two revisions built the same binary, so this run measures the noise floor only`.

Build each revision in its own worktree with its own target directory. The driver does this. A shared `CARGO_TARGET_DIR` can make cargo reuse one artifact. A build that does not print `Compiling blulib` is stale.

### 10.2 A row is stale

The report prints `status: stale` and `the path <path> does not exist at the head revision`. The row did not run. Fix the `paths` entry or remove the row.

The driver checks all rows, not only the selected rows. A stale row cannot cover a changed path.

### 10.3 The host is noisy

The report says `the host is noisy, so this run has no verdict`. The JSON verdict is `null`. The table prints `noisy` in the verdict column.

The check flags a load average greater than the number of cores, before or after the run. Wait for a quiet host and run again. Do not use `taskset`. The `examples/ab.rs` header records that pinning made the results worse.

### 10.4 A row or a verdict is unmeasurable

The word has two uses. A row marked `unmeasurable` cannot run. The table gives the reason. A verdict `unmeasurable` means the effect is at or below the floor.

For a row, read the reason. The `blu` binary has no command for the measured code. Do not add a fake command. Use a Criterion target or a direct example for the component. The `merge.push` row stays unmeasurable until the driver can start `blu serve`.

For a verdict, the machine reached that effect on its own. Run again on a quiet host. More rounds make the sign counts more stable, but they do not lower the floor.

### 10.5 An empty `--diff` selection

The current driver does not stop on an empty `--diff` selection. It builds both revisions, then reports `selection: 0 of 18 scenarios`. This wastes two release builds. Check the range and the `paths` values before the run.

Run `git diff --name-only FROM..TO` to see the changed paths. Compare them with the table. Use `--scenario NAME` to select a row directly.
