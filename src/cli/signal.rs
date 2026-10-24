//! Stop signals and the error a stopped run reports.

/// Number of the signal that asked us to stop, or 0 if none did. `entry` reads it
/// to report `128 + signal`.
pub(crate) static STOP_SIGNAL: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// Resolve when the process is asked to stop. `SIGTERM` as well as Ctrl-C.
/// A supervisor sends that, and so does a container stop or a closing laptop.
///
/// A second Ctrl-C is left to the default handler, so a user who wants out
/// immediately can still have it.
#[cfg(unix)]
pub(crate) async fn stop_signal() -> std::io::Error {
    use std::sync::atomic::Ordering;

    let mut terminate =
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(_e) => {
                // No `SIGTERM` on this platform. Ctrl-C is all there is.
                let _ = tokio::signal::ctrl_c().await;
                STOP_SIGNAL.store(2, Ordering::Relaxed);
                return interrupted();
            }
        };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            STOP_SIGNAL.store(2, Ordering::Relaxed);
        }
        _ = terminate.recv() => {
            STOP_SIGNAL.store(15, Ordering::Relaxed);
        }
    }

    interrupted()
}

/// Ctrl-C only: the unix build has `SIGTERM` as well.
#[cfg(not(unix))]
pub(crate) async fn stop_signal() -> std::io::Error {
    use std::sync::atomic::Ordering;

    let _ = tokio::signal::ctrl_c().await;
    STOP_SIGNAL.store(2, Ordering::Relaxed);
    interrupted()
}

/// The error a stopped run reports. Not a success, and not a silent exit. The
/// work was abandoned. What is guaranteed is that nothing was *published*. A
/// backup's index is filled in only on completion. A restore's claim is released.
///
/// `Interrupted` is worth the scrutiny, because the crate is wary of naming a
/// kind it does not mean. `NotFound` is what the operating system returns for a
/// file that has gone. So a caller cannot tell one of ours from a real one. That
/// ambiguity has cost a swallowed error before. `Interrupted` has no such
/// collision. `std` retries `EINTR` itself, so no filesystem call in this crate
/// ever produces it. Whoever gets this error knows it means a deliberate stop,
/// and nothing else. That is exactly what the message says.
pub(crate) fn interrupted() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Interrupted,
        "interrupted. This run released its claim on a backup set or destination",
    )
}
