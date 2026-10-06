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

use std::io::Write as _;

use std::{
    io::{BufRead, BufReader},
    net::{SocketAddr, TcpListener},
    path::Path,
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

/// A running server, killed when this is dropped.
pub(crate) struct Server {
    child: Child,
    address: SocketAddr,
}

impl Server {
    /// Start `lepatch serve` on `root`, and wait for it to report its address.
    ///
    /// Runs under `--quiet`, so **the terminal prints nothing**. A test that
    /// asserts on what a *client* received under this constructor is asserting on
    /// the filter as well as on the server, which is the right thing for most of
    /// them and the wrong thing for a test about progress — see
    /// [`Server::start_with_progress`].
    ///
    /// # Panics
    ///
    /// Panics if the process will not start or does not report an address. A
    /// server that never came up leaves nothing to assert about.
    pub(crate) fn start(root: &Path) -> Self {
        Self::spawn(root, &[])
    }

    /// Start `lepatch serve` with the terminal turned all the way down, **and the
    /// progress layer left working anyway**.
    ///
    /// **`--quiet` and `RUST_LOG=warn` together.** Both say the terminal prints
    /// nothing at all, so nothing a client sees can be coming from there. Naming
    /// both rather than `--quiet` alone because `--quiet` only sets the
    /// *default* directive, which `RUST_LOG` replaces — so `--quiet` on its own
    /// would still honour a `RUST_LOG` inherited from whatever is running the
    /// test, and a test about verbosity should not have that question open.
    ///
    /// **This is what makes it a test of progress rather than of the merge.**
    /// A progress event is an `INFO` event and the terminal's filter is at `WARN`,
    /// so if that filter were attached to the subscriber rather than to the writer
    /// layer it would refuse the callsite outright: the event would never be
    /// emitted, the progress layer would never be offered it, and a client polling
    /// the job would be told the merge reported nothing. That was the defect, it
    /// looked like a bug in the progress layer, and it was invisible to every
    /// other test in the file.
    ///
    /// # Panics
    ///
    /// Panics if the process will not start or does not report an address.
    pub(crate) fn start_with_progress(root: &Path) -> Self {
        Self::spawn(root, &[("RUST_LOG", "warn")])
    }

    /// The server both constructors above are, given the environment to add.
    ///
    /// # Panics
    ///
    /// Panics if the process will not start or does not report an address.
    fn spawn(root: &Path, env: &[(&str, &str)]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_lepatch"));
        command
            // Before the subcommand, because `--quiet` is a global flag on `Args`
            // rather than one of `serve`'s own. `lepatch serve <root> --quiet` is
            // a parse error, and a parse error prints nothing to stdout — which
            // here looks exactly like a server that never came up.
            .arg("--quiet")
            .arg("serve")
            .arg(root)
            .args(["--bind", "127.0.0.1:0"]);

        for (name, value) in env {
            command.env(name, value);
        }

        let mut child = command
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

/// A socket that answers every request with one fixed response.
///
/// ## Why this exists
///
/// **A client has to be tested against a server that has changed, and this
/// build cannot produce one.** The shape `lepatch versions --remote` reads is
/// the shape `lepatch serve` writes, so every response the real server can send
/// is a response the client understands — and a parser that read permissively
/// would pass against it forever. The only way to ask what a client does with
/// an answer it does not understand is to answer with one.
///
/// So this is not a `lepatch serve` and does not pretend to be: it reads a
/// request head, ignores what it says, and writes back bytes the test wrote
/// down. Enough HTTP/1.1 to satisfy `awc`, and nothing else — no `Server` above
/// has a second path through this file because only the remote tests need it.
pub(crate) struct Stub {
    address: SocketAddr,
    stop: Arc<AtomicBool>,
}

impl Stub {
    /// Answer every request `200 OK` with `body`.
    ///
    /// # Panics
    ///
    /// Panics if a loopback port cannot be bound. Nothing is left to assert
    /// about if it cannot be.
    pub(crate) fn answering_200(body: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port for the stub");
        let address = listener.local_addr().expect("the stub's own address");

        // Non-blocking rather than a shutdown channel: the thread has one job and
        // no state, so polling a flag is less machinery than a second socket
        // whose only purpose is to unblock `accept`.
        listener
            .set_nonblocking(true)
            .expect("the stub's accept to be non-blocking");

        let stop = Arc::new(AtomicBool::new(false));
        let finished = Arc::clone(&stop);

        // Built here rather than in the thread, so the body is owned by the
        // closure and the spawn does not need it to be `&'static str`. A test's
        // body is a literal, but saying so in the signature would be a promise
        // about the caller rather than a fact about this type.
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\
             connection: close\r\n\r\n{body}",
            body.len()
        );

        thread::spawn(move || {
            while !finished.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        read_head(&mut stream);
                        let _ = stream.write_all(response.as_bytes());
                        let _ = stream.flush();
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => return,
                }
            }
        });

        Self { address, stop }
    }

    /// Where it is listening.
    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }
}

impl Drop for Stub {
    /// Stop answering.
    ///
    /// The port is released when the thread returns and drops the listener, and
    /// the flag is what makes it return. Bounded by the poll interval rather
    /// than joined, because nothing a test asserts on depends on the thread
    /// having stopped by the time the next line runs.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Read a request head and throw it away.
///
/// Read rather than ignored, because a client writing a request into a socket
/// nobody is reading gets `EPIPE` on some platforms and a short write on
/// others — either of which would make this stub test the client's write
/// rather than its parse.
fn read_head(stream: &mut std::net::TcpStream) {
    use std::io::Read as _;

    let mut seen = Vec::new();
    let mut byte = [0_u8; 1];
    while !seen.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(0) | Err(_) => return,
            Ok(_) => seen.push(byte[0]),
        }
    }
}
