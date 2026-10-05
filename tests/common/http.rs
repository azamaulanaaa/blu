//! A hand-rolled HTTP/1.1 client, for the server tests.
//!
//! ## Why not `awc`
//!
//! `awc` is actix's own client, it is licence-clean, and it would do. It also
//! brings `h2` and optionally `tokio-native-tls` into the graph, and the surface
//! these tests need is: one method, one path, some headers, a body, and a status
//! with a body — over HTTP/1.1. That is about eighty lines over
//! [`tokio::net::TcpStream`], it pulls in nothing, and when something in it does
//! not fit there is nowhere to look but this file.
//!
//! **It is not a general client.** It follows no redirects, it does not chunk,
//! and it reads the body to the end or to `Content-Length`. Those are not
//! omissions to be sorry about: a server test that silently followed a redirect
//! would not be testing what it says it is testing.

#![allow(clippy::unwrap_used, clippy::expect_used)] // the helpers below are not `#[test]`s

use std::collections::HashMap;

use std::fmt::Write as _;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

/// A response, read whole.
#[derive(Debug, Clone)]
pub(crate) struct Response {
    /// The status line's number.
    pub status: u16,
    /// Header names lowercased, so a test does not have to know the casing.
    pub headers: HashMap<String, String>,
    /// The body, as bytes.
    pub body: Vec<u8>,
}

impl Response {
    /// The body as text, lossily.
    pub(crate) fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// A header by lowercased name.
    pub(crate) fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }

    /// Whether the content type is `type/subtype`, ignoring any parameters.
    pub(crate) fn is_type(&self, wanted: &str) -> bool {
        self.header("content-type")
            .is_some_and(|value| value.split(';').next().unwrap_or("").trim() == wanted)
    }

    /// A JSON field of the body, or `None` if the body is not JSON or has no such
    /// field.
    ///
    /// `serde_json` is a dev-dependency for the `OpenAPI` drift test; using it here
    /// too is why it is in the graph at all. The alternative is matching on
    /// substrings, which is a test that passes when the field is *mentioned*.
    pub(crate) fn field(&self, name: &str) -> Option<serde_json::Value> {
        let parsed: serde_json::Value = serde_json::from_slice(&self.body).ok()?;
        parsed.get(name).cloned()
    }
}

/// A request to make.
#[derive(Debug, Clone)]
pub(crate) struct Request<'a> {
    /// The method, uppercased by [`Request::new`].
    pub method: &'a str,
    /// The path, including any query string.
    pub path: &'a str,
    /// Headers, added or replaced.
    pub headers: Vec<(String, String)>,
    /// The body, sent with a `Content-Length`.
    pub body: Vec<u8>,
}

impl<'a> Request<'a> {
    /// A request with no body.
    pub(crate) fn new(method: &'a str, path: &'a str) -> Self {
        Self {
            method,
            path,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// A request with a body, sent as JSON.
    pub(crate) fn json(method: &'a str, path: &'a str, body: &serde_json::Value) -> Self {
        let mut request = Self::new(method, path);
        request
            .headers
            .push(("content-type".to_string(), "application/json".to_string()));
        request.body = serde_json::to_vec(body).expect("a `Value` always serialises");
        request
    }

    /// A request whose body is sent verbatim, as a pack is.
    pub(crate) fn bytes(
        method: &'a str,
        path: &'a str,
        content_type: &'a str,
        body: Vec<u8>,
    ) -> Self {
        let mut request = Self::new(method, path);
        request
            .headers
            .push(("content-type".to_string(), content_type.to_string()));
        request.body = body;
        request
    }

    /// Add a header.
    pub(crate) fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    /// Send it to `address` and read the response.
    ///
    /// # Panics
    ///
    /// Panics if the connection fails or the response is not HTTP/1.1 with a
    /// status line. A test that cannot connect has nothing to assert.
    pub(crate) async fn send(self, address: std::net::SocketAddr) -> Response {
        let mut stream = TcpStream::connect(address)
            .await
            .expect("the server should accept a connection");

        let mut head = format!(
            "{} {} HTTP/1.1\r\nhost: {}\r\nconnection: close\r\ncontent-length: {}\r\n",
            self.method.to_ascii_uppercase(),
            self.path,
            address,
            self.body.len()
        );
        for (name, value) in &self.headers {
            // `write!` rather than `push_str(&format!(..))`, which allocates a
            // `String` per header to append it to another one.
            let _ = write!(head, "{name}: {value}\r\n");
        }
        head.push_str("\r\n");

        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(&self.body).await.unwrap();
        stream.flush().await.unwrap();

        // `connection: close` above means the server closes when it is done, so
        // reading to end-of-stream is reading the whole body and nothing more.
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).await.unwrap();

        parse(&raw)
    }
}

/// Split a raw HTTP/1.1 response into its parts.
fn parse(raw: &[u8]) -> Response {
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("a response must have a blank line after its headers");

    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let mut body = raw[split + 4..].to_vec();

    let mut lines = head.lines();
    let status_line = lines.next().expect("a response must have a status line");
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("the status line must carry a number");

    let mut headers = HashMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    // `content-length` is authoritative when it is there, because a response may
    // be followed by nothing while a proxy's keep-alive bytes would not be part
    // of it. Absent it, whatever arrived is the body.
    if let Some(length) = headers
        .get("content-length")
        .and_then(|value| value.parse::<usize>().ok())
        && body.len() > length
    {
        body.truncate(length);
    }

    Response {
        status,
        headers,
        body,
    }
}
