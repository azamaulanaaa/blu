//! A `lepatch serve` running as a real process, for a test to talk to.
//!
//! ## Why a subprocess
//!
//! Not `actix_web::test::call_service`. That would be faster and would run in one
//! process, and it would leave the router, the argument parsing, the bound
//! address, the JSON the server really writes and the `Content-Range` handling
//! untested — while the HTTP tests beside it would be testing those very things
//! through a different path.
//!
//! **One path is exercised instead of two.** The tests go through a socket to a
//! process started exactly as a user starts it, so what they check is what a
//! client gets.
//!
//! ## The port
//!
//! `--bind 127.0.0.1:0`, and the server prints its address on **stdout** as its
//! first line. So the test reads the line and learns the port. That is also why
//! the address goes to stdout rather than stderr: it is data a script needs, and
//! the same reason `lepatch pack` prints a content hash there.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    io::{BufRead, BufReader},
    net::SocketAddr,
    path::Path,
    process::{Child, Command, Stdio},
};

/// A running server, killed when this is dropped.
pub(crate) struct Server {
    child: Child,
    address: SocketAddr,
}

impl Server {
    /// Start `lepatch serve` on `root`, and wait for it to report its address.
    ///
    /// # Panics
    ///
    /// Panics if the process will not start or does not report an address. A
    /// server that never came up leaves nothing to assert about.
    pub(crate) fn start(root: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_lepatch"))
            // Before the subcommand, because `--quiet` is a global flag on `Args`
            // rather than one of `serve`'s own. `lepatch serve <root> --quiet` is
            // a parse error, and a parse error prints nothing to stdout — which
            // here looks exactly like a server that never came up.
            .arg("--quiet")
            .arg("serve")
            .arg(root)
            .args(["--bind", "127.0.0.1:0"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("lepatch serve should start");

        let stdout = child.stdout.take().expect("stdout was piped");
        let mut reader = BufReader::new(stdout);

        let mut line = String::new();
        reader
            .read_line(&mut line)
            .expect("the server should report its address");

        let address = line
            .trim()
            .strip_prefix("http://")
            .unwrap_or_else(|| panic!("the server printed {line:?}, not an address"))
            .parse()
            .expect("the address should parse");

        Self { child, address }
    }

    /// Where it is listening.
    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    /// Whether the process is still running.
    pub(crate) fn is_alive(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }
}

impl Drop for Server {
    /// Kill it.
    ///
    /// A `Drop` impl rather than a call at the end of each test, because a test
    /// that panics half-way would otherwise leave a process bound to a port for
    /// as long as the test binary lives. `kill` rather than `wait`, so a server
    /// that ignores the signal cannot hang the suite.
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
