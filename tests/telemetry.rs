//! Does a span actually leave the process?
//!
//! # Why a subprocess and a socket
//!
//! Everything else about telemetry in this crate can be checked in process: a
//! recorder exporter, a span, a flush. That proves the pipeline works. **It does
//! not prove the run's spans are not thrown away on the way out**, and that is
//! the failure this file exists for.
//!
//! An OTLP exporter is asynchronous and batched. A finished span is handed to a
//! processor, which queues it and exports it later on a task of its own. A
//! `lepatch` run is a command that finishes and then exits — so the export
//! timer (five seconds by default) almost never fires, and *the only* thing that
//! ever flushes a run's spans is the shutdown call on the exit path. Drop that
//! call, or move it into the success arm of the match instead of after it, and
//! every span is discarded with no error, no warning, and an exit code that says
//! the run succeeded.
//!
//! So these tests run the real binary, with a socket standing in for the
//! collector, and read what actually arrives:
//!
//! * [`a_span_survives_a_successful_run`] — the ordinary case.
//! * [`a_span_survives_a_failed_run`] — the error arm, which is a different arm
//!   of the match in `main` and was the easy one to forget.
//! * [`a_span_survives_a_stopped_server`] — `SIGTERM`, on unix. A container stop
//!   is the most common way a server-side run ends.
//! * [`a_metric_survives_a_successful_run`] — the same argument for **metrics**,
//!   which are batched by the same kind of reader and were flushed by the same
//!   line of the same function, and were not covered here at all: deleting
//!   `metrics::shutdown()` from `telemetry::shutdown` used to leave every test
//!   in this file green.
//! * [`nothing_leaves_the_machine_when_telemetry_is_off`] — the default.
//!
//! The collector records the path each request was posted to, so a search is
//! scoped to one signal: a body is read as the **metrics** export only when it
//! arrived on `/v1/metrics`.
//!
//! # The collector
//!
//! A hundred and sixty lines of `std::net`, in this file, and it is not a mock.
//! The exporter is an HTTP client: whether it can reach a collector is a fact
//! about the socket, and a stub that answers anything would be testing that a
//! `Vec<SpanData>` can be pushed into another `Vec<SpanData>`. What is asserted
//! on is the bytes a collector would really have received — which is why the
//! assertions look for an *instrument name* and this build's *version string* in
//! the protobuf rather than for anything structured. Protobuf is not self-
//! describing, and this is the honest level at which to read it.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

/// The span name a command that writes a set produces.
const WRITE_SPAN: &[u8] = b"open_for_writing";

/// The span name a command that only reads a set produces — and a command that
/// failed before it could write one.
const READ_SPAN: &[u8] = b"open_for_reading";

/// The counter a backup's cut tree is reported through.
///
/// **An instrument name, and instrument names do not appear in a trace.** No
/// span in this crate carries `lepatch.backup.chunks_produced` — a backup's
/// `chunks` total is an `add` total, so it is a counter and specifically not a
/// span attribute (see `telemetry::metrics`) — so a body containing these bytes
/// is a body a collector received measurements through, and not a trace that
/// happened to contain the same words.
const BACKUP_CHUNKS_PRODUCED: &[u8] = b"lepatch.backup.chunks_produced";

/// The histogram every command's duration is reported on, successful or not.
const COMMAND_DURATION: &[u8] = b"lepatch.command.duration";

/// Where the exporter posts spans, and where it posts measurements.
const TRACES: &str = "/v1/traces";
const METRICS: &str = "/v1/metrics";

/// One request the collector received.
///
/// **The path is kept because OTLP posts the two signals to different paths**,
/// and a test about metrics that could be satisfied by a span export would be a
/// test about the socket rather than about the metric.
struct Request {
    /// The path it was posted to — `/v1/traces` or `/v1/metrics`.
    target: String,
    /// Its body, the protobuf a real collector would have decoded.
    body: Vec<u8>,
}

/// A socket standing in for an OTLP collector, in this process.
///
/// Bound before the binary starts, so the address it is given is one something is
/// listening on. `127.0.0.1`, no TLS — the same position the shipped binary
/// takes, and the reason the endpoint here is `http://`.
struct Collector {
    address: SocketAddr,
    bodies: Arc<Mutex<Vec<Request>>>,
}

impl Collector {
    /// Start listening, on a port the operating system picks.
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback listener");
        let address = listener.local_addr().expect("a bound address");
        let bodies: Arc<Mutex<Vec<Request>>> = Arc::new(Mutex::new(Vec::new()));

        let recorded = Arc::clone(&bodies);
        thread::spawn(move || {
            // Non-blocking, with a deadline, so a run that exports nothing costs
            // this thread its deadline and not the test's patience.
            listener
                .set_nonblocking(true)
                .expect("a non-blocking listener");
            let deadline = Instant::now() + Duration::from_secs(60);

            while Instant::now() < deadline {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if let Some(request) = read_request(stream) {
                            recorded.lock().expect("the collector's lock").push(request);
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(20));
                    }
                    Err(_) => break,
                }
            }
        });

        Self { address, bodies }
    }

    /// The endpoint the exporter should be pointed at.
    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    /// Every body received so far, concatenated, whichever signal carried it.
    fn received(&self) -> Vec<u8> {
        let requests = self.bodies.lock().expect("the collector's lock");
        let mut all = Vec::new();
        for request in requests.iter() {
            all.extend_from_slice(&request.body);
        }
        all
    }

    /// Every body posted to `target` so far, concatenated.
    ///
    /// One signal at a time, so a test about one signal cannot pass on the
    /// other's bytes.
    fn received_for(&self, target: &str) -> Vec<u8> {
        let requests = self.bodies.lock().expect("the collector's lock");
        let mut all = Vec::new();
        for request in requests.iter().filter(|request| request.target == target) {
            all.extend_from_slice(&request.body);
        }
        all
    }

    /// Wait up to `seconds` for a body posted to `target` containing `needle`.
    ///
    /// The export happens on another thread while the process is shutting down,
    /// so "not yet" is the normal answer for a moment and only becomes a
    /// failure if it stays that way.
    fn wait_for(&self, target: &str, needle: &[u8], seconds: u64) -> Option<Vec<u8>> {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        while Instant::now() < deadline {
            let received = self.received_for(target);
            if received
                .windows(needle.len())
                .any(|window| window == needle)
            {
                return Some(received);
            }
            thread::sleep(Duration::from_millis(50));
        }
        None
    }
}

/// Read one HTTP request off a connection, answer it, and return its path and
/// body.
///
/// Handles the two things an OTLP exporter's request can do that matter here:
/// `Expect: 100-continue`, and a body announced by `Content-Length`. The answer
/// is a `200` with no body, which is what a collector returns for a protobuf
/// export it accepted — and returning anything else would be a test asserting
/// against an error path nobody is testing.
fn read_request(mut stream: TcpStream) -> Option<Request> {
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("a read timeout");

    let mut head = Vec::new();
    let mut byte = [0_u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        // A closed connection or a read timeout both mean there is no request to
        // answer. Same answer, so one arm.
        match stream.read(&mut byte) {
            Ok(0) | Err(_) => return None,
            Ok(_) => head.push(byte[0]),
        }
    }

    let head = String::from_utf8_lossy(&head).to_ascii_lowercase();

    // The request line, before the head is used for anything else: the path is
    // what says which signal this body carries.
    let target = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or_default()
        .to_string();

    if head.contains("expect: 100-continue") {
        let _ = stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n");
    }

    let length = head
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse::<usize>().ok());

    let mut body = Vec::new();
    match length {
        Some(length) => {
            body.resize(length, 0);
            stream.read_exact(&mut body).ok()?;
        }
        None => {
            stream.read_to_end(&mut body).ok()?;
        }
    }

    let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
    let _ = stream.flush();

    Some(Request { target, body })
}

/// A scratch directory holding one small tree, so a backup has something to do.
///
/// A `TempDir` per test rather than a shared fixture directory: these tests
/// write a backup set, and a name collision between two of them would be one
/// more thing to explain when a span goes missing.
fn scratch() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("a scratch directory");
    fs::create_dir_all(dir.path().join("src/nested")).expect("a source tree");
    fs::write(
        dir.path().join("src/hello.txt"),
        b"a file worth deduplicating\n",
    )
    .expect("a file to write");
    fs::write(dir.path().join("src/nested/second.txt"), b"and another\n").expect("a file to write");
    dir
}

/// Run the binary with the collector as its endpoint, and wait for it to finish.
///
/// The endpoint variable is set *and* the flag is passed, so the test does not
/// depend on which of the two turns telemetry on; `Config::new` is tested for
/// that. Every other OTLP variable is removed, so a developer with one set does
/// not get a test that quietly points somewhere else — `OTEL_METRIC_EXPORT_INTERVAL`
/// in particular, which would turn the metric reader's sixty-second interval
/// into something shorter and let the metrics arrive on a timer instead of on
/// the exit path, which is the thing under test.
fn run_with_telemetry(dir: &Path, collector: &Collector, args: &[&str]) {
    let output = Command::new(env!("CARGO_BIN_EXE_lepatch"))
        .current_dir(dir)
        .args(["--telemetry"])
        .args(args)
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", collector.endpoint())
        .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .env_remove("OTEL_RESOURCE_ATTRIBUTES")
        .env_remove("OTEL_SERVICE_NAME")
        .env_remove("OTEL_METRIC_EXPORT_INTERVAL")
        .output()
        .expect("the lepatch binary should run");

    assert!(
        output.status.success(),
        "`lepatch {}` failed with {:?}\nstdout: {}\nstderr: {}",
        args.join(" "),
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The version this build reports, as bytes.
///
/// `GET /v1/version` reports the same string, and a trace has to carry it: a
/// dashboard that cannot tell two builds apart cannot tell a regression from a
/// rollout. Its presence in the body is also the check that the *resource* went
/// out, which a span name alone would not prove.
fn this_version() -> &'static [u8] {
    env!("CARGO_PKG_VERSION").as_bytes()
}

#[test]
fn a_span_survives_a_successful_run() {
    // **The ordinary case, end to end.** A backup writes a set, exits 0, and the
    // spans of the commands it ran are at a socket afterwards.
    //
    // Without the shutdown on the exit path this body is empty and the test
    // fails with an exit code of 0 and a perfectly successful backup behind it.
    let collector = Collector::start();
    let dir = scratch();

    run_with_telemetry(dir.path(), &collector, &["backup", "src", "photos"]);

    let body = collector
        .wait_for(TRACES, WRITE_SPAN, 20)
        .expect("a span must be received after a successful run exits");

    assert!(
        body.windows(this_version().len())
            .any(|window| window == this_version()),
        "the resource must name the build that produced the span: {:?}",
        String::from_utf8_lossy(&body)
    );
}

#[test]
fn a_span_survives_a_failed_run() {
    // **The other arm of the match in `main`.** A flush written into the success
    // arm would pass the test above and fail this one.
    //
    // The span waited for is the store open, not `verify`: the set is missing,
    // so the run fails before `verify` is ever called — and that is the point.
    // A flush that only ran on a command which got far enough to do some work
    // would still lose this run's spans, and this run is the common shape of a
    // failure: refused before there was anything to report.
    let collector = Collector::start();
    let dir = scratch();

    let output = Command::new(env!("CARGO_BIN_EXE_lepatch"))
        .current_dir(dir.path())
        .args(["--telemetry", "verify", "no-such-set"])
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", collector.endpoint())
        .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .output()
        .expect("the lepatch binary should run");

    assert!(
        !output.status.success(),
        "verifying a set that is not there is a failure\nstdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );

    let body = collector
        .wait_for(TRACES, READ_SPAN, 20)
        .unwrap_or_else(|| {
            panic!(
                "a span must be received after a failed run exits; the collector got {:?}",
                String::from_utf8_lossy(&collector.received())
            )
        });

    assert!(
        body.windows(this_version().len())
            .any(|window| window == this_version()),
        "the resource must name the build that produced the span"
    );
}

#[cfg(unix)]
#[test]
fn a_span_survives_a_stopped_server() {
    // **The `SIGTERM` path.** A container stop is how a server-side run usually
    // ends, and it is the ending most likely to be the only trace anybody
    // wanted.
    //
    // `sh -c "kill -TERM"` rather than `Command::kill`, which is `SIGKILL` and
    // would not go anywhere near `stop_signal`: a killed process is a process
    // that never reached its exit path, so it would prove nothing about it. The
    // signal is sent from a shell because `kill` is a shell builtin and this
    // crate has no libc to raise one with.
    let collector = Collector::start();
    let dir = scratch();

    // A set for the server to serve, made by an ordinary run with no telemetry
    // involved. It matters that the request below *reads* that set: `GET
    // /v1/sets` on an empty root answers 404 without opening a store, so a
    // server asked to stop right after it would have no span to flush and the
    // test would be asserting about an empty export.
    let prepared = Command::new(env!("CARGO_BIN_EXE_lepatch"))
        .current_dir(dir.path())
        .args(["backup", "src", "photos"])
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .output()
        .expect("the lepatch binary should run");
    assert!(
        prepared.status.success(),
        "the set the server serves should exist: {}",
        String::from_utf8_lossy(&prepared.stderr)
    );

    let mut child: Child = Command::new(env!("CARGO_BIN_EXE_lepatch"))
        .current_dir(dir.path())
        .args(["--telemetry", "serve", ".", "--bind", "127.0.0.1:0"])
        .env("OTEL_EXPORTER_OTLP_ENDPOINT", collector.endpoint())
        .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("lepatch serve should start");

    // The address, which the server prints on stdout as its first line.
    let stdout = child.stdout.take().expect("stdout was piped");
    let mut reader = BufReader::new(stdout);
    let mut address = String::new();
    reader
        .read_line(&mut address)
        .expect("the server should report its address");
    assert!(
        address.starts_with("http://127.0.0.1:"),
        "the server should report its bound address, got {address:?}"
    );

    // One request that reads the set, so the server has recorded a span before
    // it is asked to stop.
    //
    // Written by hand for the same reason `tests/common/http.rs` is: this file
    // must not pull a client in to make one request.
    let mut request = TcpStream::connect(
        address
            .trim()
            .strip_prefix("http://")
            .expect("an http address"),
    )
    .expect("the server should accept");
    request
        .write_all(
            b"GET /v1/sets/photos/versions HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .expect("the request should be written");
    let mut response = String::new();
    request
        .read_to_string(&mut response)
        .expect("the response should be read");
    assert!(
        response.starts_with("HTTP/1.1 200"),
        "the server should answer a version listing, got {response:?}"
    );

    let signalled = Command::new("sh")
        .arg("-c")
        .arg(format!("kill -TERM {}", child.id()))
        .status()
        .expect("sh should run");
    assert!(signalled.success(), "`kill -TERM` should succeed");

    let status = child.wait().expect("the server should exit");
    assert_eq!(
        status.code(),
        Some(128 + 15),
        "a SIGTERM is reported as 128 + the signal number"
    );

    // The server's own spans. A stopped server has to flush exactly like a
    // finished one: this is the path where a container is told to go away, and
    // it is the path with the most to lose.
    let body = collector
        .wait_for(TRACES, READ_SPAN, 20)
        .unwrap_or_else(|| {
            panic!(
                "a stopped server must still flush what it recorded; the collector got {:?}",
                String::from_utf8_lossy(&collector.received())
            )
        });

    assert!(
        body.windows(this_version().len())
            .any(|window| window == this_version()),
        "the resource must name the build that produced the span"
    );
}

#[test]
fn a_metric_survives_a_successful_run() {
    // **The metrics half of the same argument, and the half that was missing.**
    //
    // Everything above it is about spans, and spans were the only signal any
    // test here looked for: every one of them searched the received bytes for a
    // *span* name. So `let metrics = metrics::shutdown();` — the line that
    // flushes the meter provider on the exit path, and without which every
    // measurement a run made is discarded with the same silence a lost span is —
    // could be deleted and every test in this file stayed green. A backup that
    // reported its whole run through telemetry and lost every number it measured
    // is the failure this crate's telemetry design exists to prevent, and
    // nothing here would have noticed.
    //
    // The assertion names **instruments**, not spans. `lepatch.backup.
    // chunks_produced` is a counter and `lepatch.command.duration` is the
    // duration histogram, both built in `telemetry::metrics`; neither string
    // appears anywhere in a span this crate emits, so a body carrying them is a
    // body that arrived as a *metrics* export. The search is scoped to
    // `/v1/metrics` as well, so a span export could not satisfy it either.
    let collector = Collector::start();
    let dir = scratch();

    run_with_telemetry(dir.path(), &collector, &["backup", "src", "photos"]);

    let body = collector
        .wait_for(METRICS, BACKUP_CHUNKS_PRODUCED, 20)
        .unwrap_or_else(|| {
            let exported = collector.received_for(METRICS);
            panic!(
                "a metric must be received after a successful run exits: {} bytes arrived \
                 on {METRICS} ({:?}) and {} bytes in total. A reader whose queue is dropped \
                 on the way out exports nothing and reports nothing",
                exported.len(),
                String::from_utf8_lossy(&exported),
                collector.received().len()
            )
        });

    assert!(
        body.windows(COMMAND_DURATION.len())
            .any(|window| window == COMMAND_DURATION),
        "the command's own duration must be reported too: it is recorded on the \
         failure arm as well as this one, so its absence is about the exit path \
         rather than about this run\n{:?}",
        String::from_utf8_lossy(&body)
    );

    assert!(
        body.windows(this_version().len())
            .any(|window| window == this_version()),
        "the metrics' resource must name the build that produced them: {:?}",
        String::from_utf8_lossy(&body)
    );
}

#[test]
fn nothing_leaves_the_machine_when_telemetry_is_off() {
    // **The default, asserted at the socket rather than in a unit test.** No
    // flag, and the endpoint variable explicitly removed so the developer's own
    // environment cannot make this test assert nothing. A collector is listening
    // the whole time, so "nothing arrived" is a real observation.
    let collector = Collector::start();
    let dir = scratch();

    let output = Command::new(env!("CARGO_BIN_EXE_lepatch"))
        .current_dir(dir.path())
        .args(["backup", "src", "photos"])
        .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
        .env_remove("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
        .output()
        .expect("the lepatch binary should run");
    assert!(
        output.status.success(),
        "a backup with no telemetry must still work: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Nothing to wait for, so this is a pause with an assertion at the end of
    // it. Long enough that an export which *was* going to happen would have.
    thread::sleep(Duration::from_secs(2));

    assert!(
        collector.received().is_empty(),
        "a run with telemetry off must open no socket and send nothing: {:?}",
        String::from_utf8_lossy(&collector.received())
    );
}
