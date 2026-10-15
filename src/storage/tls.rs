// Copyright (C) 2025-2026 Aza Maulana
// SPDX-License-Identifier: GPL-2.0-or-later
//! TLS transport for the operator.
//!
//! The transport uses rustls with ring.
//! It verifies against the system roots.
//! It pulls no Mozilla bundle.
//! It pulls no system OpenSSL.
//! It pulls no cmake.
//! One transport speaks both schemes.
//! Plain HTTP continues unchanged.
//! HTTPS verifies.
//! The installer runs process-wide once.
//! The first install wins.
//!
//! The transport follows redirects in place.
//! It follows eight hops at most.
//! 307 and 308 replay the request.
//! 301, 302 and 303 convert GET and HEAD only.
//! Other cases return the response for the backend.
//! Credentials never cross hosts.
//! A hop to another authority drops auth headers.
//!
//! The client dials endpoint addresses directly.
//! It uses no proxy.

use std::io;

use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response, StatusCode, Uri};
use http_body_util::{BodyExt, Full};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use hyper_util::{
    client::legacy::{Client, connect::HttpConnector},
    rt::TokioExecutor,
};
use opendal::{Buffer, Error, ErrorKind, HttpBody, HttpTransport, HttpTransporter, Result};

/// Count redirect hops per operation.
///
/// Eight hops cover endpoint moves and region moves.
/// A loop returns the last 3xx for the backend.
/// The transport never requests forever.
const MAX_REDIRECTS: u8 = 8;

/// HTTPS transport over hyper, rustls, ring and system roots.
#[derive(Debug, Clone)]
struct TlsTransport {
    client: Client<HttpsConnector<HttpConnector>, Full<Bytes>>,
}

/// Install the TLS transport process-wide.
///
/// The installer is idempotent.
/// The first install wins.
/// Later calls change nothing.
/// The installer reads the system roots.
/// A machine without roots fails here, not mid-run.
///
/// # Errors
///
/// The call fails when no system roots load.
pub(crate) fn install() -> io::Result<()> {
    let mut roots = rustls::RootCertStore::empty();
    let result = rustls_native_certs::load_native_certs();
    if !result.errors.is_empty() {
        tracing::warn!(errors = ?result.errors, "some system roots failed to load");
    }
    let (valid, _) = roots.add_parsable_certificates(result.certs);
    if valid == 0 {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "no system TLS roots loaded: HTTPS has nothing to verify against",
        ));
    }

    let provider = rustls::crypto::ring::default_provider();
    let config = rustls::ClientConfig::builder_with_provider(provider.into())
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .map_err(std::io::Error::other)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = HttpsConnectorBuilder::new()
        .with_tls_config(config)
        .https_or_http()
        .enable_http1()
        .build();
    let client = Client::builder(TokioExecutor::new()).build(connector);
    HttpTransporter::install_default(TlsTransport { client });
    Ok(())
}

/// Select what a redirect status does to a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Follow {
    /// Replay method and body at the new address.
    Replay,
    /// Retry as GET without a body.
    AsGet,
    /// Return the response for the backend.
    Return,
}

/// Decide how `status` moves a request with `method`.
///
/// 307 and 308 keep everything.
/// 301, 302 and 303 keep GET and HEAD.
/// 303 converts other methods to GET.
/// Other cases return.
/// Only the backend knows what 300 or 305 means.
fn follow(status: StatusCode, method: &Method) -> Follow {
    if status == StatusCode::TEMPORARY_REDIRECT || status == StatusCode::PERMANENT_REDIRECT {
        return Follow::Replay;
    }
    if method == Method::GET || method == Method::HEAD {
        if status == StatusCode::MOVED_PERMANENTLY
            || status == StatusCode::FOUND
            || status == StatusCode::SEE_OTHER
        {
            return Follow::Replay;
        }
    } else if status == StatusCode::SEE_OTHER {
        return Follow::AsGet;
    }
    Follow::Return
}

/// Build an absolute address for a redirect.
///
/// Absolute addresses pass through.
/// Absolute paths take the base scheme and authority.
/// Other values resolve against the base directory.
/// A bare `Location` never parses alone.
/// The resolver merges it as text and parses the whole once.
fn resolve_redirect(base: &Uri, location: &str) -> Result<Uri> {
    let failed = || {
        Error::new(
            ErrorKind::Unexpected,
            format!("redirect location {location:?} is not an address"),
        )
    };

    if let Some(after) = location.strip_prefix("//") {
        let (authority, path) = match after.find('/') {
            Some(at) => (&after[..at], &after[at..]),
            None => (after, "/"),
        };
        if authority.is_empty() {
            return Err(failed());
        }
        let scheme = base.scheme_str().ok_or_else(failed)?;
        return format!("{scheme}://{authority}{path}")
            .parse()
            .map_err(|_| failed());
    }

    if let Ok(target) = location.parse::<Uri>() {
        if target.authority().is_some() {
            if target.scheme().is_none() {
                return Err(failed());
            }
            return Ok(target);
        }
        if location.starts_with('/') {
            let scheme = base.scheme_str().ok_or_else(failed)?;
            let authority = base.authority().ok_or_else(failed)?;
            let path = target.path_and_query().map_or("/", |q| q.as_str());
            return format!("{scheme}://{authority}{path}")
                .parse()
                .map_err(|_| failed());
        }
    }

    if location.is_empty() || location.contains("://") {
        return Err(failed());
    }
    let scheme = base.scheme_str().ok_or_else(failed)?;
    let authority = base.authority().ok_or_else(failed)?;
    let dir = base
        .path()
        .rsplit_once('/')
        .map_or("/", |(dir, _)| if dir.is_empty() { "/" } else { dir });
    format!("{scheme}://{authority}{dir}/{location}")
        .parse()
        .map_err(|_| failed())
}

/// Check whether `next` shares scheme and authority with `base`.
///
/// Credentials move inside one origin only.
/// A scheme change counts as a move.
/// Sending secrets to a new scheme sends them somewhere new.
fn same_origin(base: &Uri, next: &Uri) -> bool {
    base.scheme() == next.scheme() && base.authority() == next.authority()
}

/// Drop credential headers before a cross-origin hop.
///
/// Authorization, proxy and cookies name one origin.
/// A target outside that origin must not receive them.
fn strip_credentials(headers: &mut HeaderMap) {
    headers.remove(http::header::AUTHORIZATION);
    headers.remove(http::header::PROXY_AUTHORIZATION);
    headers.remove(http::header::COOKIE);
}

/// Build an operator error for a transport failure.
fn net_err(doing: &str, error: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Unexpected, format!("could not {doing}: {error}"))
}

impl HttpTransport for TlsTransport {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        let (parts, body) = req.into_parts();
        let mut method = parts.method;
        let mut uri = parts.uri;
        let mut headers = parts.headers;
        let mut bytes = body.to_bytes();

        let mut hops = 0u8;
        let response = loop {
            let mut outgoing = Request::builder()
                .method(method.clone())
                .uri(uri.clone())
                .body(Full::new(bytes.clone()))
                .map_err(|e| net_err("build a request", &e))?;
            *outgoing.headers_mut() = headers.clone();

            let response = self
                .client
                .request(outgoing)
                .await
                .map_err(|e| net_err("send a request", &e))?;
            let status = response.status();
            if hops == MAX_REDIRECTS {
                break response;
            }
            match follow(status, &method) {
                Follow::Return => break response,
                Follow::AsGet => {
                    method = Method::GET;
                    bytes = Bytes::new();
                }
                Follow::Replay => {}
            }
            let Some(location) = response.headers().get(http::header::LOCATION).cloned() else {
                break response;
            };
            let location = location
                .to_str()
                .map_err(|e| net_err("read a redirect", &e))?;
            let next = resolve_redirect(&uri, location)?;
            if !same_origin(&uri, &next) {
                strip_credentials(&mut headers);
            }
            uri = next;
            hops += 1;
        };

        let (parts, body) = response.into_parts();
        let bytes = body
            .collect()
            .await
            .map_err(|e| net_err("read a response", &e))?
            .to_bytes();
        let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        let buffer = Buffer::from(bytes);
        let stream = futures_util::stream::iter(vec![Ok::<_, Error>(buffer)]);
        Ok(Response::from_parts(
            parts,
            HttpBody::new(stream, Some(size)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{Follow, follow, resolve_redirect, same_origin};
    use http::{Method, StatusCode, Uri};

    /// Check redirect rules.
    ///
    /// Replay keeps the request.
    /// GET keeps its form.
    /// Other cases return.
    /// No network runs here.
    #[test]
    fn check_redirect_rules() {
        use StatusCode as S;

        assert_eq!(follow(S::TEMPORARY_REDIRECT, &Method::PUT), Follow::Replay);
        assert_eq!(
            follow(S::PERMANENT_REDIRECT, &Method::DELETE),
            Follow::Replay
        );
        assert_eq!(follow(S::MOVED_PERMANENTLY, &Method::GET), Follow::Replay);
        assert_eq!(follow(S::FOUND, &Method::HEAD), Follow::Replay);
        assert_eq!(follow(S::SEE_OTHER, &Method::POST), Follow::AsGet);
        assert_eq!(follow(S::MOVED_PERMANENTLY, &Method::PUT), Follow::Return);
        assert_eq!(follow(S::FOUND, &Method::DELETE), Follow::Return);
        assert_eq!(follow(S::OK, &Method::GET), Follow::Return);
        assert_eq!(follow(S::NOT_FOUND, &Method::GET), Follow::Return);
    }

    /// Check redirect locations.
    ///
    /// Locations resolve four ways.
    /// Bad values fail.
    /// No network runs here.
    #[test]
    fn check_redirect_locations() {
        let base: Uri = "http://host:8080/a/b".parse().unwrap();

        let absolute = resolve_redirect(&base, "https://other/x").unwrap();
        assert_eq!(absolute, "https://other/x".parse::<Uri>().unwrap());

        let path = resolve_redirect(&base, "/c").unwrap();
        assert_eq!(path, "http://host:8080/c".parse::<Uri>().unwrap());

        let relative = resolve_redirect(&base, "c?d=1").unwrap();
        assert_eq!(relative, "http://host:8080/a/c?d=1".parse::<Uri>().unwrap());

        let scheme_relative = resolve_redirect(&base, "//other/c").unwrap();
        assert_eq!(scheme_relative, "http://other/c".parse::<Uri>().unwrap());

        assert!(resolve_redirect(&base, "http://[::1").is_err());
    }

    /// Check origin equality.
    ///
    /// The check compares scheme and authority.
    /// A scheme change counts as a move.
    /// No network runs here.
    #[test]
    fn check_origin_equality() {
        let base: Uri = "http://host/a".parse().unwrap();
        let same: Uri = "http://host/b".parse().unwrap();
        let upgrade: Uri = "https://host/a".parse().unwrap();
        let other: Uri = "http://elsewhere/a".parse().unwrap();

        assert!(same_origin(&base, &same));
        assert!(!same_origin(&base, &upgrade));
        assert!(!same_origin(&base, &other));
    }

    /// Check install idempotence.
    ///
    /// The first install wins.
    /// A second install changes nothing.
    /// The test needs system roots.
    /// Every machine that verifies HTTPS holds them.
    #[test]
    fn check_install_idempotence() {
        super::install().unwrap();
        super::install().unwrap();
    }
}
