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
$ lepatch pack docs game.pack          # one version as a single distributable file
$ lepatch merge game.pack other        # and one version into another set
```

`--version N` selects a specific version; without it, restore, verify and pack use
the most recent one that completed successfully.

**stdout is data, stderr is diagnostics.** `pack` prints the pack's content hash and
`versions --numbers-only` prints a version per line, both meant to be captured, so
progress and summary lines go to stderr. Every command is pipeable without
`--quiet`.

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

The write itself runs in four phases, and the order is not incidental: directories
first, so every parent exists; regular files next, each created at its final length
before any content is written, which is what makes empty files and hard-link
targets exist; then links, which may only be created once their targets do; and
attributes last, because writing into a directory updates its modification time.
Files are removed rather than truncated in place, so a `link -> /etc` left by an
earlier restore into the same directory is not written through.

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

Inside a segment, each record is self-delimiting and self-checking:

```text
magic (4) | kind (1) | codec (1) | stored length (varint) | raw length (varint)
         | hash (32) | check (8) | payload
```

A scan stops at the first header whose magic or check does not verify and never
guesses past it, so a torn tail cannot make it read a record boundary that is not
one. The two lengths differ because a compressed payload is not as long as its
content: `stored` is what a scan skips, `raw` is what a reader is handed, and
`hash` is always the BLAKE3 of the *uncompressed* content — so two sets that
compressed the same bytes differently still agree on a chunk's name, and
deduplication survives a change of compression level.

The header's checksum is a truncated BLAKE3 rather than a CRC. CRC32C would be
cheaper, and the cost is invisible next to the seek and read each record already
does, but a cryptographic digest rejects *garbage* — a run of zeroes left by an
abandoned write, a length field from another format — with probability 2^-64
rather than 2^-32. It is not defending against an adversary who can write to the
file, who can compute a valid check for anything they like; it is against damage
and coincidence.

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

A set that is not there is an error rather than an empty listing — a mistyped name
must not come back as "no versions" — and a set that exists but commits nothing
lists as empty, which is a different thing. `lepatch reindex` refuses a name that
is not a set rather than creating one, since opening a store for writing creates
it and a repair command should not manufacture the thing it was asked to repair.

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

**The 1.9% is a best case; the one request is the real result.** That fixture is
built from a generator that is a function of position only, so every file in it is
a *prefix* of the next, and "rewriting" one leaves the original byte stream
truncated — the most favourable thing that can happen to content-defined chunking.
On a tree with no such relationship, the same one-file patch measures **6.1% of the
pack, still in a single request**. The request count is the property that holds; the
fraction is fixture-dependent, and `tests/pack.rs` asserts the robust one on
content that is not a special case.

```text
magic | version | snapshot hash | chunk count | payload bytes
...payloads, end to end, in plan order...
...the snapshot, encoded: the file table that gives the payload meaning...
...index: per chunk, hash | offset | length...
index length | snapshot length | magic
```

**The index is at the end.** A client fetches the last twenty bytes, learns where the
index is and how long it is, fetches *that* range, and now knows every chunk's
offset without having read a byte of payload. A header would be the wrong way round:
it would have to be read before anything, including before it is known whether the
pack is worth having at all. The trailing magic is what makes the reverse read
reliable — a length alone is indistinguishable from payload bytes, and a truncated
download would otherwise be read as a short index.

**A pack carries its own file table.** The chunk hashes in the index say what the
payload *is*; the snapshot says which bytes of which file each one belongs at. Both
are needed to place a chunk, so the pack carries both rather than expecting the
holder to have the store that produced it. That is what makes a single file
sufficient to hand a version to someone: no set, no server, no second request. The
snapshot sits between the payload and the index so the index length alone still
locates the index — the one-request property above does not depend on knowing where
the payload ends.

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

`lepatch pack <set> <out>` writes one, and prints its content hash — which is its
name, since the same tree packs to the same bytes. `--version N` packs a specific
version, and re-packing version 1 after version 2 exists reproduces the original
bytes exactly.

Where a header's counts and its own index could disagree, the pack is refused rather
than one of them believed: the index is what locates chunks, so if the two disagree
one is wrong, and picking the wrong one produces a pack that reads as valid and hands
out ranges covering the wrong bytes.

#### Delta packs

`lepatch pack <set> <out> --from 7` writes a pack carrying version 8's file table
and only the chunks a holder of version 7 lacks. `--full` (the default when no
`--from` is given) writes the whole version.

A chunk is left out only when the base places *every* one of the target's slices for
it identically — same path, same offset, same length. Comparing chunk hashes instead
would be wrong with shared content: a chunk the base holds somewhere is not thereby
held everywhere, and a new file whose content repeats something already in the tree
produces exactly that. Such a chunk is sent, even though the base has the bytes.

**A delta records what it assumes, and `apply` checks it.** The header names the base
version, and an apply refuses unless the directory's record says it is holding
exactly that. The reason is structural: verifying a slice means reading the chunk's
bytes, and a delta holds precisely the chunks its reader is asserted to have, so
nothing in the delta can confirm it. Refusing is the only honest answer — applying
what it happens to contain would leave a directory that is neither version and report
success.

The file-table side is unaffected: a delta carries the whole version's snapshot, so a
client can lay out every file from the one file it was sent.

#### Applying a pack

`lepatch apply <pack> <directory>` brings a directory up to the version. It does
not consult the backup set, and there is no `probe` command: what a directory holds
is what an apply has to work out before it can write anything, so it is a step of the
apply rather than something a user does separately. The apply's summary reports what
the fetch would have cost.

**An apply writes only what is missing.** A restore writes every byte a version
needs, because its bytes come out of a store one chunk at a time and it cannot know
which of them the destination already has. A pack can be asked, so a file with
nothing to write is not opened at all — it keeps its inode, its timestamps and
whatever the filesystem has cached about it. Applying version 1 and then version 2
costs the delta, not a tree.

**Working out what is missing answers two questions, and does not conflate them.**
The chunks a directory holds *correctly, at every place the version puts them* are
the answer a delta needs, and are per chunk and all-or-nothing: a chunk spanning two
files is held only if it is right in both. The *slices* that differ are per slice and
are what gets written. The second is not derivable from the first — a file missing
entirely has every slice reported absent even where its chunks are shared with another
file that is perfectly correct — and judging that file by the first creates it empty.

**A file that has grown is not the version, even though every slice of it is.**
Appending leaves every recorded byte exactly where it was, so every slice verifies
and every chunk is held. Slices cannot see past the end of a file, so lengths are
checked separately; without that, an apply would answer "holds this version" about a
file with somebody's data in it.

**An apply is not atomic, and does not pretend to be.** A failure partway leaves a
directory that is neither version. Because the check is idempotent, running the same
apply again finishes the job.

#### The record an apply leaves

`apply` writes `<dir>.lepatch-state` beside the destination — never inside it, so it
is not backed up as part of the tree. It records the version, and for each file the
length and modification time that version imposed.

The next apply stats those files and reads only the ones that have moved, which is
what makes a repeated apply cost a stat per file instead of a full read of the tree.
A chunk whose every file is untouched is not fetched from the pack at all.

**The record is a cache, and never believed on sight.** A directory keeps changing
after an apply writes it, so a record still claiming the old layout of an edited file
would be worse than no record: an apply that trusted it would skip the file and
leave the edit in place, reporting success. So every file is re-stat'd, and any that
moved is read properly. The fallback is per file, because the whole saving is the
one untouched file that need not be read.

A file that changed but kept both its length and its modification time is invisible
to this, and no stat-based scheme can see it. `apply --recheck` ignores the record
and reads the tree, for when something else has been restoring timestamps. It is
slower and reaches the same answer.

The record is also what makes a delta pack applicable: a delta names the version it
assumes, and the record beside the destination is how the client establishes that it
holds it. Without one, or with one naming another version, `apply` refuses rather
than guessing.

**Pruning is opt-in.** An entry the version does not record is as likely to be the
user's own work as a leftover from an older version, and nothing in the version can
tell those apart. Without `--prune`, such entries are named on stderr and left alone.

A corrupt pack must not reach the disk: every chunk about to be written is hashed
against the hash it is addressed by before the first byte is stored, so a damaged
pack makes everything look absent rather than overwriting a correct directory and
reporting success.

#### Merging a pack

`lepatch merge <pack> <name>` applies a pack *into a backup set*, as a new version
of it. It is the other half of `pack`: where `apply` brings a directory up to a
version, `merge` brings a *set* up to one. Nothing else is consulted to do it —
the pack carries its own file table — and the set ends up holding the version, so
it restores, verifies and lists like any other.

```console
$ lepatch pack docs game.pack
afd66e609e57fbceef225d03ab97bbfb1cd538a00be7245fed8d7e8b2a881f4f  game.pack

$ lepatch merge game.pack laptop
merged version 1: 44 chunks, 44 new, 0 already held, 850000 bytes stored from game.pack
afd66e60...  laptop
```

A **full pack** is correct against any set and **creates one** if there is none.
That is the case this command exists for: a new machine, a new location, or a set
rebuilt from packs after the original was lost. There is nothing to check a
self-contained pack against, so refusing a name with nothing behind it would mean
merge could only ever add to a set `backup` had made.

A **delta pack** is refused unless some committed version of the set is the version
it names. The check is *containment* — "is the base among the versions this set
holds" — and nothing more:

```console
$ lepatch merge v2.delta laptop
error: refusing to merge: v2.delta is a delta against the version whose snapshot
hashes to 0e2751a6..., and no committed version of the set holds that snapshot. A
delta omits the chunks that version has, so nothing here could tell you were
missing them — merge the full pack instead, or merge the base version first
```

A set at a **later** version still holds every chunk of an earlier one, so a set
ahead of the base is one the delta can be merged into. This is where `merge` and
`apply` deliberately disagree: an apply establishes its base against a *directory*,
which may hold extra files or be half-applied, so it has to check the tree is
exactly the base; a backup set is append-only, and a set holding the base at all is
a set that can merge the delta. Copying apply's check here would refuse merges that
are safe, and read as though the set had to be pristine.

**Atomic in the store's own sense, and there is no rollback.** Every chunk is
stored, then made durable, then one commit record publishes the version. A run that
fails before that commit leaves no version — only records in the last segment that
nothing references, which the next run's recovery reclaims. That is the guarantee
the store already gives a backup, and merge adds no mechanism on top of it: a
version either exists or does not, and the record that says so is written once.

**Merging the same pack twice is allowed**, and makes a second version with the
same snapshot. A store is a log, and refusing to record something that happened is
the wrong default for one; it would also make the command unsafe to retry, which is
what a transfer over a network needs. It costs one commit record, because a chunk
already held is not written again. Three ways to tell it happened: the report says
how many chunks were new, `versions` prints the snapshot hash beside each version,
and a note on stderr names the version already holding it.

**A damaged pack is refused, never half-merged.** Each chunk is hashed against the
hash it is addressed by as it is stored, so corrupted payload cannot be filed under
a chunk's name, and a snapshot naming a chunk that neither the pack nor the set
holds is refused before a byte is written. A refused merge leaves the name exactly
as it was.

### Chunk profiles

How big a chunk should be is a trade, and the trade depends on the tree. Fine cuts
deduplicate a *localised* edit better, because a boundary is more likely to land on
the same content in both the old and the new copy; coarse cuts mean fewer records,
less framing and less to read back, which is what matters once the content stops
compressing at all. Almost nobody can place themselves on that curve for a tree
they have not measured, and being wrong only shows up as a worse ratio weeks later,
so the sizes are chosen for you and referred to by name:

```console
$ lepatch backup ~/Documents docs                            # default: 16 KiB average
$ lepatch backup ~/src docs --chunk-profile small            # 4 KiB average
$ lepatch backup ~/video docs --chunk-profile large          # 64 KiB average
```

| profile | minimum | average | maximum | for |
|---|---|---|---|---|
| `small` | 2 KiB | 4 KiB | 16 KiB | trees of many small files, where one file is smaller than a default chunk and an edit to it re-uploads all of it |
| `default` | 8 KiB | 16 KiB | 64 KiB | everything else, and every version written so far |
| `large` | 32 KiB | 64 KiB | 256 KiB | content that does not deduplicate anyway — video, archives of compressed files |

**A name, not three numbers.** `--chunk-profile` takes one of those three names and
nothing else. There is deliberately no flag for an average chunk size, for the same
reason there is no `--no-compression`: two spellings of one policy are two ways to
forget which is in force, and a number typed here would be one almost nobody can
derive from their own data. A name the tool does not have is a usage error that quotes
what was typed and names the profiles that would have worked, so it can never be
quietly corrected into something nearby.

**It is a write policy, not a format property.** Nothing about the profile is written
into the store: chunks are named by their contents, so a set whose runs were cut at
different profiles is still one store and every version of it restores, byte for
byte — asserted in `tests/cli.rs`. What changes is deduplication, because a run stops
matching chunks cut under another profile. So moving a set to a different profile is
safe but costs a full rewrite the first time, and a worse ratio after that; a set that
wants a different profile forever wants re-taking.

Each run logs which profile cut it and the three sizes behind it, because that is the
one thing about a run that cannot be worked out from the store afterwards.

### Compression

Record payloads are stored as **zstd frames** by default, at level 1:

```console
$ lepatch backup ~/Documents docs                        # zstd level 1 (default)
$ lepatch backup ~/Documents docs --compression-level 9   # smaller, much slower
$ lepatch backup ~/Documents docs --compression-level 0   # stored as they are
```

One flag, counting 0 to 9, where **0 stores payloads as they are** and 1 to 9 are
zstd levels. There is deliberately no `--no-compression`: it would be a second
spelling of `--compression-level 0`, and two ways to say the same thing is two ways
to forget which one is in force. A value outside 0–9 is a usage error rather than
something silently corrected, so the store on disk always matches the command that
made it.

Measured on a mixed game tree — an executable, a texture, mesh JSON and config
text, 7.5 MB in total:

| | size | vs uncompressed |
|---|---|---|
| `--compression-level 0` | 7,519,423 | — |
| **level 1 (default)** | **4,358,018** | **42% smaller** |
| level 9 | 4,177,469 | 44% smaller |

**Level 1 is the default because the levels are not close.** On a 4 MB executable:
level 1 is 2.7x smaller at 468 MB/s and level 9 is 3.3x at 65 MB/s. zstd's own
levels run to 22, and the last thirteen of them are not offered: they buy a few
percent for a hundred times the time. A backup runs once and is read many times, so
write speed is the cost that matters.

**Already-compressed content gains nothing, and does not cost anything either.**
Textures, audio and video are entropy-coded, so zstd produces something no smaller
than what it was given — on a 4 MB file of random bytes, every level returns
100.0%. A payload that does not get smaller is therefore **stored raw** rather than
as a frame that would be a few bytes *larger* than the bytes it replaced. The win is
on executables, debug symbols, text and uncompressed audio; the rest is neutral.

**It is a write policy, not a format property.** Every record names the codec it was
written with, and a record's hash is always of its *content*, never of the compressed
bytes. So three things follow, and each is asserted in `tests/compression.rs`:

- Changing the level between runs is safe and never rewrites what is already there.
- A set whose records were written at different levels — or with compression off
  entirely — is one store, and every version of it restores.
- Deduplication holds across codecs. The same content stored raw and then
  compressed is **one** blob, so changing the flag does not re-upload the tree.

`FORMAT_VERSION` is **2**: a record header now carries a codec byte and two lengths,
and a store written by version 1 is *refused* rather than misparsed.

### Dry runs

`backup`, `restore`, `apply`, `pack`, `merge` and `reindex` take `--dry-run`. A dry
run reads and plans everything the real command reads and plans, prints what it
would change, and writes nothing:

```console
$ lepatch backup ~/Documents docs --dry-run
dry run: version 4 would be committed, snapshot 5b525e0f9a1c4e2d8b6f0a3c5d7e91b2c4a6e8d0f1b3c5d7e9a1c3e5d7f9b1d3
dry run: 128 files, 14 dirs, 2 links, 3104 chunks (12 new, 3092 already held), 48210 bytes to store

$ lepatch restore /tmp/docs-restored docs --dry-run
dry run: version 4 into /tmp/docs-restored
dry run: 128 files, 14 dirs, 2 links, 3104 chunks, 48210881 bytes would be written
dry run: anything already in /tmp/docs-restored would be left alone; pass --clean for an exact copy
```

`versions` and `verify` do not take it. They write nothing in the first place, and
a flag that did nothing there would be noise in `--help`; passing it there is a usage
error.

**A dry run reads everything.** That is the whole trade, and it is deliberate: a plan is
worth having *because* it is derived from the same walk, the same chunking and the
same comparison against what is already stored that the real run uses. So

- `backup --dry-run` still reads and hashes every file in the tree. It costs about
  what a backup costs and saves only the writing.
- `restore --dry-run` still reads the version out of the store and re-hashes every
  chunk against the hash it is addressed by, which is most of a restore's work.
- `apply --dry-run` still probes the destination against the version and reads every
  chunk back out of the pack to check it — so a damaged pack is still refused.
- `pack --dry-run` still resolves the version and locates every chunk.
- `merge --dry-run` still resolves the pack against the set and reads back every
  chunk a merge would store to check it, and still refuses a delta for a set that
  does not hold its base.

**It writes nothing at all.** Not a segment, not an index file, not a lock, not a
`.partial` or `.previous` beside a destination, not the manifest an apply records,
and not the store's directory when there is not one yet. The last of those is the
one that decides whether the flag is worth having: opening a set for writing *creates*
it, so a dry run opens for reading and reports a location backed by no set as an
all-new version rather than failing. No claim is taken either, so a plan can be run
against a set or a destination something else is using.

**The numbers are the real ones.** A backup's plan is computed by running the real
walk and the real pipeline against a store that answers what it is asked and keeps
none of it, so the snapshot it names is the one the next real backup commits, and the
chunk count is the one that will be written. The counts are pinned against the run
that follows them in `tests/cli.rs`.

**A dry pack cannot report two of its figures.** The size on disk includes a header
and a chunk index whose length depends on varint-encoded offsets, and the content
hash is the hash of the file itself; both are properties of a file that was not
written. Everything else — which chunks, how many, how much payload, how big the file
table — is known before a byte is fetched.

**A dry run is not a repair.** It opens the set for reading, so a store with a torn
tail in its last segment — which a real backup or `reindex` reclaims on the way
past — is refused rather than planned against. A plan reports what a run would do;
deciding that a store's tail is damage is itself a write, and it belongs to a
command that was asked to make one.

**`--clean` deserves the check.** A clean restore is the one operation here that
destroys what is already there, and a dry run of it empties nothing while still
saying what it would empty.

## What is preserved

Regular files, directories, symbolic links, hard links, ownership, permission bits
and modification times. Empty files and empty directories survive. Paths that are
not valid UTF-8 are stored as raw bytes rather than aborting the backup.

Ownership is the owning **user and group id**, not a name: a name means nothing on
any machine but the one it was read from, so recording one would be storing a
portability bug next to the answer. Changing an owner takes privilege, so a restore
by an unprivileged user cannot set one on a file it does not own — that is reported
on stderr, and everything else about the entry is still applied. See
[Ownership](#ownership) below.

## What is not preserved

ACLs, extended attributes, and hard-link relationships that point outside the tree
being backed up.

Device nodes, named pipes and sockets are **skipped, and reported**. They are not
skipped silently and they are not read: opening a FIFO for reading blocks until a
writer appears, so a tree containing one would hang the run — and a Ctrl-C would
not save it, because the signal is caught by the handler that releases claims and
that handler cannot unwind a thread parked in a read. The run says which paths it
left out, so a tree holding one is visible rather than quietly incomplete.

## Ownership

Each file and directory records its owning user and group as ids. On restore the
owner is applied **before the mode**, because Linux clears a file's set-user-ID and
set-group-ID bits when its owner changes — so the other order silently strips setuid
from everything restored, and does it invisibly: the bytes match, the lengths match,
and the only thing wrong is a bit nobody was looking for. Getting this right is the
entire point of recording ownership; a system restore has to bring back a working
`su` and `passwd`.

Changing an owner takes privilege. An unprivileged restore of a tree containing
files it does not own cannot set them, which is the ordinary case rather than an edge
one, so it is reported rather than refused:

```console
$ lepatch restore /tmp/out set
note: 2 entries in this restore recorded an owner this process could not set, which
takes privilege; their contents, permissions and times are correct. Re-run as root to
set them
```

The restore still succeeds. It is not refused, because refusing would leave a tree
half written with the offending file already on disk under the wrong owner and
nothing to retry — which is the outcome reporting an owner was supposed to prevent.

Snapshots taken before ownership was recorded are still readable, and read back with
no owner. That is a fact about them rather than a gap: they recorded nothing about
ownership, so they had none to record.

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
- **A snapshot is untrusted input, and is decoded as such.** A snapshot is a blob
  in a file that gets synced, shared and backed up like anything else, so the
  decoder checks every length against the bytes actually remaining *before*
  allocating for it, refuses trailing bytes, and bounds every varint. A count can
  therefore never request more than the blob holds. A compressed record's declared
  size is checked against the length its record header claims before anything is
  allocated, because that header is covered by a truncated hash rather than a MAC:
  a tampered length must not become a multi-gigabyte allocation.
- **A restore writes nothing until the whole snapshot is checked.** Every chunk
  is verified against its hash and every recorded path is checked to land inside
  the destination, *before* the first byte is written. Both because a snapshot is
  untrusted, and because finding out at entry 900,001 of a million that a path
  escapes would leave a tree that is neither the old one nor the new one.
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
- **A backup is CPU-bound, and mostly in the kernel.** Measured on a 153 MB tree of
  eight files, release build, tmpfs, 4 cores: **0.70s of wall clock for 1.54s of
  CPU** — 0.43s user and 1.11s system, so about **three quarters** of the work is
  kernel time in the syscalls rather than hashing. The run is not waiting on the
  disk: reading the same 153 MB costs 23ms and reading *and writing* it costs
  313ms, both to tmpfs, which is a fraction of the run. The reader hashes with
  blake3 on one thread while it walks the tree. The obvious fix is hashing chunks
  across a thread pool, which the pipeline's current shape does not allow: the
  hash is needed to deduplicate, and deduplication is decided in commit order after
  every store has finished. It is a change to the pipeline rather than a tuning
  knob. Treat these numbers as indicative of the *shape* — CPU-bound, mostly
  kernel, not I/O-bound — rather than as a figure to plan capacity against; they
  are from one machine and move with the kernel version and the filesystem.
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
  stay where they are. The store has had one layout so far: version 1, whose record
  headers had no codec and one length, is refused, and there is nothing in it that
  would let it be walked anyway. The snapshot encoding is at version 3; version 2
  recorded a per-chunk storage key alongside the content hash, and the store's own
  index replaced it, so a chunk is a bare hash. Treat a format change as a reason
  to re-take your backups. The crate is not published, and the encoding is
  hand-written in `src/metadata/codec.rs` rather than delegated, so there is no
  upstream whose policy can decide whether your backup set can still be read —
  which is how `bincode` came to be on an advisory list in the first place.

## Library

The crate is usable directly:

```rust
# async fn example() -> std::io::Result<()> {
use lepatch::{
    command::{Options, backup, lock_destination, restore},
    storage::{BlobStore, Compression, SegmentLimit},
};

// The claim excludes other runs. A backup takes no lock of its own, so this is
// the caller's to make and the caller's to hold.
let _claim = lock_destination(std::path::Path::new("docs-restored"))?;

// First backup. Four gibibytes per segment and zstd level 1, both the defaults.
let store = BlobStore::open_for_writing(
    "docs",
    SegmentLimit::default(),
    Compression::default(),
).await?;
let first = backup("~/Documents", Options::default(), store).await?;

// Later backups need nothing passed to deduplicate: the store holds every
// version it has ever been given, so a chunk it already has is not written
// again however far back it was last seen.
let store = BlobStore::open_for_writing(
    "docs",
    SegmentLimit::default(),
    Compression::default(),
).await?;
backup("~/Documents", Options::default(), store).await?;

// A read-only store takes no segment limit, because a read never rolls over.
let store = BlobStore::open_for_reading("docs").await?;
restore("~/Documents-restored", first.snapshot, store).await?;
# Ok(())
# }
```

`backup` returns a `Committed`: the version number the run was given and the hash
of the snapshot it published. A version is a position in the store's commit
records, so `store.versions()` and `store.snapshot(n)` address them directly.

`SegmentLimit` caps one file of the set; the default is 4 GiB and it can be changed
between runs without rewriting anything. A set is `base.0000.bin`,
`base.0001.bin` and so on, and `layout::IndexSet::segments()` names them.

A read-only store takes no `SegmentLimit`, because a read never rolls over: the
value a writer used is not needed to read what it wrote, and a parameter the
function ignores is worse than an asymmetry between the two constructors.

A pack is written from a `Plan`, which resolves a snapshot into an ordered list of
chunks with their locations, and asks `Pack` what a client holding some of them
still needs:

```rust
# async fn example() -> std::io::Result<()> {
use std::collections::HashSet;

use lepatch::{
    command::load_snapshot,
    pack::Pack,
    storage::{BlobStore, Plan},
};

let store = BlobStore::open_for_reading("docs").await?;

// The plan is a function of the snapshot, so it is the same every run.
let snapshot = load_snapshot(&store, store.snapshot(2)?).await?;
let plan = Plan::from_snapshot(&snapshot, |id| store.locate(id))?;

let pack = Pack::write(&store, &plan, &snapshot, std::path::Path::new("game.pack")).await?;

// What a client already holding some of it does not have, as byte ranges of the
// pack. Adjacent ranges merge, which is the point of packing: a client needs one
// range, not one per chunk.
let already: HashSet<_> = HashSet::new();
let ranges = pack.needed(&already);

// After fetching: record what those ranges covered, and re-asking returns nothing.
let have = pack.chunks_covering(&ranges);
# Ok(())
# }
```

`Pack::needed` is deliberately phrased as what a client *lacks*: one holding nothing
gets the whole payload as a single range, and one holding everything gets none.
`chunks_covering` is its inverse, and both are methods — the two halves of the
same exchange. `Pack::write` streams, so its memory is one chunk rather than one
version.

A pack is also a store, and the consuming half of the feature comes from that. There
is no `probe` subcommand because this is the question an apply has to answer anyway;
it is exposed as a function for anything that needs the answer *first*, such as a
client deciding what to fetch over HTTP:

```rust
# async fn example() -> std::io::Result<()> {
use lepatch::command::{ApplyOptions, apply, probe};

let pack = lepatch::pack::Pack::open(std::path::Path::new("game.pack"))?;

// What the directory holds, which decides both the ranges to fetch and the bytes
// to write.
let probed = probe("~/Games", &pack.snapshot()?, pack.clone()).await?;
let ranges = pack.needed(&probed.held);

// Writes only what is missing. `Applied::fetched` is what the fetch above would
// have cost, measured before the apply rather than after — afterwards the
// directory holds the version by construction and would report nothing.
let applied = apply("~/Games", &pack, ApplyOptions::default()).await?;
assert_eq!(ranges.len(), applied.ranges);
assert_eq!(pack.needed_bytes(&probed.held), applied.fetched);
# Ok(())
# }
```

`apply` takes that same probe internally, so calling it as well as `apply` costs a
second full read of the directory. A caller that only wants to write should call
`apply` and read `fetched` and `ranges` off the result; `Applied` reports both from
the probe it had to take anyway.

The `WRITE` const generic on `BlobStore` is what stops a read-only restore from
being handed something that can append to the store.

## Upgrading across a format change

Two on-disk formats changed in `0.3.0`, and both go one way. Neither direction
needs a re-backup.

**Sets are readable, but only forwards.** `0.3.0` reads snapshots it did not write
(`MIN_READABLE_VERSION` is 3), so your existing backup sets keep working. `0.2.0`
cannot read a `0.3.0` set — the owner field did not exist when it was written and it
has no way to interpret one.

**Packs are refused, and that costs nothing.** A pack is derived output: it is a
function of the tree, so an old-format one is regenerated by re-running
`lepatch pack` rather than recovered. Any version in the set can be packed at any
time, so nothing has to be backed up again to bring a client up to date.

```console
$ lepatch pack set v8.pack --version 8     # after upgrading the server
```

**Order matters if clients fetch packs from a server you are upgrading.** A `0.3.0`
client refuses a `0.2.0` pack, so the server has to go first:

1. Upgrade the server. It now writes snapshots and packs this build understands.
2. Re-run `lepatch pack` for whatever versions clients are fetching, and let them
   fetch those.
3. Upgrade the clients.

There is no window in which both builds can read each other's packs. That is
deliberate — a decoder that guessed at the wrong layout would read a chunk's length
as somebody's user id and produce a tree that looks right — but it does mean the
server goes first.

`lepatch probe` was removed. What a directory holds is what an apply has to work out
before it can write anything, so it is a step of the apply rather than a command;
`apply` reports the same figure on its summary line.

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
