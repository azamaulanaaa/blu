//! The `serve` command: serve the sets under a directory over HTTP.

/// Serve the sets under `root` until the process is asked to stop.
///
/// **The bound address goes to stdout**, and that is the same rule `pack` follows
/// when it prints a content hash. stdout is data; diagnostics are stderr. The
/// address is data a script needs — it wants to connect, and under `--bind
/// 127.0.0.1:0` it is the only way to learn the port. Diagnostics stay on
/// stderr, so `blu serve --bind 127.0.0.1:0 | head -1` is a working way to
/// get the address.
///
/// The server shuts down on the same stop signal every other command uses, so
/// Ctrl-C and a container stop both stop it cleanly — and the `Drop` guard on the
/// scratch directory removes whatever uploads were still held.
pub(crate) async fn run_serve(
    root: &std::path::Path,
    bind: Option<&str>,
    scratch: Option<&std::path::Path>,
    jobs: &blulib::serve::Jobs,
) -> std::io::Result<()> {
    let config = blulib::serve::Config::new(root, bind.unwrap_or(DEFAULT_BIND), scratch);

    // Reported before the listener is up, and to stdout, so a caller reading one
    // line knows the port. The address is resolved by actix, which is what makes
    // `--bind 127.0.0.1:0` mean "pick one".
    tracing::info!(
        root = %config.root.display(),
        bind = %config.bind,
        scratch = %config.scratch.display(),
        "serving"
    );

    let running = blulib::serve::bind(&config, jobs)?;
    say!("http://{}", running.addr())?;

    running.run().await
}

/// The address a server binds when `--bind` is not given.
///
/// Loopback, because there is no authentication and the two go together: a
/// service that grants every set on the machine to the network is a mistake, and
/// loopback is the default that prevents it by omission.
const DEFAULT_BIND: &str = "127.0.0.1:8480";
