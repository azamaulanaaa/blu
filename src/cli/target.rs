//! Resolving a `TARGET` into where a set lives.

use super::Target;

/// Resolve one `TARGET` into where the set lives.
///
/// Thin wrapper over [`blulib::target`]. The binary keeps its call sites.
/// The library owns the parsing.
pub(crate) fn resolve_target(
    command: &str,
    raw: &str,
    staging: Option<&std::path::Path>,
    token: Option<&str>,
) -> std::io::Result<Target> {
    let parsing = Target::parse(raw).command(command);
    let parsing = match staging {
        Some(dir) => parsing.staging(dir),
        None => parsing,
    };
    let parsing = match token {
        Some(value) => parsing.token(value),
        None => parsing,
    };
    parsing.resolve()
}

/// Build a serve client for `address`, with a bearer token when one is set.
///
/// Thin wrapper over [`blulib::target::remote_for`].
pub(crate) fn remote_for(
    address: &str,
    token: Option<&str>,
) -> std::io::Result<blulib::client::Remote> {
    blulib::target::remote_for(address, token)
}
