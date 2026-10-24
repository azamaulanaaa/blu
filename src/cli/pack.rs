//! The `pack` command: write one version's chunks to a single file.

use super::{
    IndexSet, KeepOrLevel, Pack, PackBuilder, StoreTarget, Target, connect_store, remote_for,
    require_staged_set, select_version, storage,
};

/// Run `pack` against a local path, a keyed store, or a server.
pub(crate) async fn dispatch_pack(
    target: Target,
    out: &std::path::Path,
    version: Option<u32>,
    from: Option<u32>,
    compression: KeepOrLevel,
    dry_run: bool,
) -> std::io::Result<()> {
    match target {
        Target::Local(name) => run_pack(&name, out, version, from, compression, dry_run).await,
        Target::Store(target) => {
            run_pack_stored(&target, out, version, from, compression, dry_run).await
        }
        Target::Serve {
            address,
            set,
            token,
        } => {
            if compression != KeepOrLevel::Keep {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "an `http://` target packs `keep` only: the server serves the records it \
                     holds and re-encodes nothing",
                ));
            }
            run_pack_serve(
                &address,
                &set,
                out,
                version,
                from,
                dry_run,
                token.as_deref(),
            )
            .await
        }
    }
}

/// The base version a pack should omit, with `--full` overriding `--from`.
///
/// Two flags that contradict each other. The code resolves the conflict here
/// rather than in clap, so the message is ours. `--full` is the promise that the
/// artifact suits any client. Letting a stale `--from` in a script quietly narrow
/// that promise would break it without anyone noticing.
pub(crate) fn effective_from(from: Option<u32>, full: bool) -> Option<u32> {
    if full {
        if let Some(from) = from {
            say!("note: --full overrides --from {from}. This run packs the whole version").ok();
        }
        return None;
    }
    from
}

/// Write one version's chunks to a single file.
///
/// Opened for reading, and no lock taken. A pack does not touch the set. The
/// version is resolved before the plan is built. So a set that grows under the
/// run packs the version that was asked for, not whatever is newest when the run
/// finishes.
///
/// A dry run builds the same plan and stops before the file is touched. So the
/// run neither creates nor overwrites `<OUT>`. The size on disk and the content
/// hash are absent from a plan rather than guessed. The size depends on the
/// header and the chunk index. The hash is the hash of the file. A file that
/// this run does not write is the only thing either figure could be about.
async fn run_pack(
    name: &std::path::Path,
    out: &std::path::Path,
    version: Option<u32>,
    from: Option<u32>,
    compression: KeepOrLevel,
    dry_run: bool,
) -> std::io::Result<()> {
    let (version, packed) = write_pack(name, out, version, from, compression, dry_run).await?;

    // A plan reports and stops, and it wrote no pack, so there is nothing to
    // push — which is the other half of the refusal above, stated where the
    // run actually stops rather than where the flag is read.
    if dry_run {
        report_pack(version, out, &packed)?;
        return Ok(());
    }

    // Matched rather than unwrapped. The pair belongs to a written pack,
    // and this line is only reachable for one.
    let (Some(bytes), Some(content)) = (packed.bytes, packed.content) else {
        return Err(std::io::Error::other(
            "this run wrote the pack, and the pack reported neither a length nor a content hash",
        ));
    };

    tracing::info!(
        version,
        base = packed.base,
        chunks = packed.chunks,
        of = packed.of,
        bytes,
        snapshot = packed.table,
        content = %&content,
        "wrote pack"
    );

    // The content hash is the pack's name, so it goes to stdout for a script to
    // key a cache on.
    say!("{}  {}", &content, out.display())?;
    report_pack(version, out, &packed)?;

    Ok(())
}

/// Say what a pack holds, on stdout.
///
/// Both figures appear for a delta. The fraction depends on the fixture, and
/// the request count does not. The whole version is one range either way.
fn report_pack(
    version: u32,
    out: &std::path::Path,
    packed: &blulib::command::Packed,
) -> std::io::Result<()> {
    let held = match packed.base {
        Some(base_version) => format!(
            "version {version} since {base_version}: {} of {} chunks",
            packed.chunks, packed.of
        ),
        None => format!("version {version}: {} chunks", packed.chunks),
    };

    match packed.bytes {
        // No file, so no size on disk. Saying "0 bytes on disk" would be a claim
        // about a file that does not exist. So the figure is left out. What did
        // happen is said instead.
        None => say!(
            "dry run: {held}, {} bytes of payload, {} bytes of file table. This run wrote \
             nothing to {}",
            packed.payload,
            packed.table,
            out.display()
        ),
        Some(bytes) => say!(
            "{held}, {} bytes of payload, {} bytes of file table, {bytes} bytes on disk",
            packed.payload,
            packed.table
        ),
    }
}

/// Pack a version of a keyed store into `out`.
///
/// Pulls the set into staging and packs from there with exactly the code a
/// local run uses. Reads never push back.
async fn run_pack_stored(
    target: &StoreTarget,
    out: &std::path::Path,
    version: Option<u32>,
    from: Option<u32>,
    compression: KeepOrLevel,
    dry_run: bool,
) -> std::io::Result<()> {
    let backend = connect_store(&target.url)?;
    let staging = IndexSet::new(&target.staging);
    storage::pull_from_remote_async(&backend, &staging).await?;
    require_staged_set(&staging, target)?;

    run_pack(&target.staging, out, version, from, compression, dry_run).await
}

/// Pack a version of a `blu serve` set into `out`.
///
/// The pack is computed on the server and fetched whole: a pack is the one
/// artifact a client fetches in full, so the destination's record saves
/// nothing here.
///
/// A dry run reports the server's summary and fetches nothing: the counts
/// are the pack's own, and the size on disk and the content hash are left
/// out as properties of a file this run does not write.
#[expect(
    clippy::too_many_arguments,
    reason = "a pack's own arguments, plus the address"
)]
async fn run_pack_serve(
    address: &str,
    set: &str,
    out: &std::path::Path,
    version: Option<u32>,
    from: Option<u32>,
    dry_run: bool,
    token: Option<&str>,
) -> std::io::Result<()> {
    let remote = remote_for(address, token)?;
    blulib::client::check_set(set)?;

    let started = remote.start_pack(set, version, from).await?;
    let packed = remote
        .await_job_with(&started, |counters| {
            let shown = counters
                .iter()
                .map(|(name, value)| format!("{name} {value}"))
                .collect::<Vec<_>>()
                .join(" ");
            tracing::info!("packing on {}: {shown}", remote.address());
        })
        .await?;

    let Some(blulib::client::Outcome::Packed(summary)) = &packed.result else {
        return Err(std::io::Error::other(format!(
            "job {} finished without saying what it packed",
            packed.id
        )));
    };

    tracing::info!(
        server = %remote.address(),
        set,
        version = summary.version,
        snapshot = %summary.snapshot,
        chunks = summary.chunks,
        length = summary.length,
        out = %out.display(),
        dry_run,
        "packing a version from the server"
    );

    if dry_run {
        let since = match from {
            Some(base) => format!(" since {base}"),
            None => format!("version {}", summary.version),
        };
        return say!(
            "dry run: {}: {} chunks, {} bytes of payload. This run wrote nothing to {}",
            since,
            summary.chunks,
            summary.payload_bytes,
            out.display(),
        );
    }

    fetch_pack_serve(&remote, set, out, summary, from).await
}

/// Fetch a server-packed version whole into `out`, and report what it holds.
///
/// Split out of [`run_pack_serve`] only to keep that function readable; the
/// plan is checked against the summary before a byte is trusted, exactly as
/// the remote restore's own check does.
async fn fetch_pack_serve(
    remote: &blulib::client::Remote,
    set: &str,
    out: &std::path::Path,
    summary: &blulib::client::PackSummary,
    from: Option<u32>,
) -> std::io::Result<()> {
    let pull = blulib::client::Pull::new(
        remote,
        set,
        summary.version,
        std::collections::HashSet::default(),
    );
    pull.begin(summary.length, out).await?;
    let plan = remote.plan_pack(set, summary.version, &[]).await?;
    if plan.length != summary.length {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "{} planned a {}-byte pack and then described a {}-byte one. The file \
                 this run assembled would not be the file it fetched",
                remote.address(),
                summary.length,
                plan.length
            ),
        ));
    }
    pull.write(&plan.ranges, out, |moved| {
        tracing::info!(
            server = %remote.address(),
            ranges = moved.ranges,
            bytes = moved.bytes,
            total = moved.total,
            "fetched"
        );
    })
    .await?;
    let fetched = pull.open(&plan, out)?;
    drop(fetched);

    let pack = Pack::open(out)?;
    let content = blulib::pack::hash_file(out).await?;
    let packed = blulib::command::Packed {
        chunks: summary.chunks,
        of: summary.chunks,
        payload: pack.payload_len(),
        table: pack.snapshot_len(),
        base: from,
        bytes: Some(summary.length),
        content: Some(content),
    };
    tracing::info!(
        version = summary.version,
        chunks = summary.chunks,
        bytes = summary.length,
        snapshot = %summary.snapshot,
        content = %&content,
        "wrote pack"
    );
    say!("{}  {}", &content, out.display())?;
    report_pack(summary.version, out, &packed)
}

/// Pack a version of the set at `name` into `out`, and report what the pack holds.
///
/// **The one place a pack is written locally**, so `blu pack` and the pack
/// a backup to a serve target pushes are the same file built by the same code.
/// The run it is taken from does not matter: a serve backup hands the version
/// it just staged to this, and a `pack` hands the version its own `--version`
/// named, and both arrive here as one `Option<u32>`.
///
/// Returns the version it resolved, alongside what the pack holds. The caller
/// reports it, so `pack --version 2` says 2 and `pack` with no version says the
/// one the set resolved to — which is the number the report line is about, and
/// which a caller that only has the return value could not work out.
pub(crate) async fn write_pack(
    name: &std::path::Path,
    out: &std::path::Path,
    version: Option<u32>,
    from: Option<u32>,
    compression: KeepOrLevel,
    dry_run: bool,
) -> std::io::Result<(u32, blulib::command::Packed)> {
    let set = IndexSet::new(name);
    let store = storage::BlobStore::builder(set.base())
        .open_reader()
        .await?;
    let (version, _) = select_version(&set, &store, version)?;

    tracing::info!(
        name = %set.base().display(),
        version,
        from,
        out = %out.display(),
        compression = ?compression,
        dry_run,
        "packing"
    );

    let mut packing = PackBuilder::new(&store, out).version(version);
    if let Some(base) = from {
        let (base, _) = select_version(&set, &store, Some(base))?;
        packing = packing.from(base);
    }
    if let Some(policy) = compression.pack_policy() {
        packing = packing.compression(policy);
    }
    if dry_run {
        packing = packing.dry_run();
    }
    let packed = packing.run().await?;

    Ok((version, packed))
}
