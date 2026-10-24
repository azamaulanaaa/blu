//! Keyed-store plumbing: connections, staging, and version selection.

use super::{IndexSet, StoreTarget, storage};

/// Resolve which version to operate on, or explain why there is none. A version
/// exists if and only if the store holds the commit record that published it.
pub(crate) fn select_version(
    set: &IndexSet,
    store: &storage::BlobStore<storage::Read>,
    requested: Option<u32>,
) -> std::io::Result<(u32, storage::BlobId)> {
    match requested {
        Some(version) => Ok((version, store.snapshot(version)?)),
        None => store.latest().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "no committed version found in {} (looked in {})",
                    set.base().display(),
                    set.directory().display()
                ),
            )
        }),
    }
}

/// A connected keyed store.
///
/// One type holds both backends because staged runners are generic.
/// The local arm holds a directory.
/// The remote arm holds a network operator.
pub(crate) enum StoreBackend {
    /// A local directory through `file://`.
    Local(storage::LocalDirStore),
    /// An operator over the network.
    Remote(opendal::Operator),
}

impl storage::AsyncObjectStore for StoreBackend {
    async fn keys(&self) -> std::io::Result<Vec<String>> {
        match self {
            Self::Local(backend) => backend.keys().await,
            Self::Remote(operator) => operator.keys().await,
        }
    }

    async fn stat(&self, key: &str) -> std::io::Result<Option<u64>> {
        match self {
            Self::Local(backend) => backend.stat(key).await,
            // Fully qualified because `Operator` holds an inherent `stat`.
            // Method resolution prefers the inherent one.
            Self::Remote(operator) => storage::AsyncObjectStore::stat(operator, key).await,
        }
    }

    async fn read_range(&self, key: &str, offset: u64, len: u64) -> std::io::Result<Vec<u8>> {
        match self {
            Self::Local(backend) => backend.read_range(key, offset, len).await,
            Self::Remote(operator) => operator.read_range(key, offset, len).await,
        }
    }

    async fn write_bytes(&self, key: &str, data: &[u8]) -> std::io::Result<()> {
        match self {
            Self::Local(backend) => backend.write_bytes(key, data).await,
            Self::Remote(operator) => operator.write_bytes(key, data).await,
        }
    }

    async fn import_file(&self, key: &str, path: &std::path::Path) -> std::io::Result<u64> {
        match self {
            Self::Local(backend) => backend.import_file(key, path).await,
            Self::Remote(operator) => operator.import_file(key, path).await,
        }
    }

    async fn export_file(&self, key: &str, path: &std::path::Path) -> std::io::Result<u64> {
        match self {
            Self::Local(backend) => backend.export_file(key, path).await,
            Self::Remote(operator) => operator.export_file(key, path).await,
        }
    }
}

/// Open the backend behind `url`.
///
/// `file://` opens a local directory.
/// Other schemes open a network operator.
/// `s3` accepts `http://` and `https://`.
/// HTTPS verifies against the system roots.
pub(crate) fn connect_store(url: &storage::RemoteUrl) -> std::io::Result<StoreBackend> {
    if url.scheme() == storage::RemoteScheme::File {
        return Ok(StoreBackend::Local(storage::LocalDirStore::new(
            url.location(),
        )));
    }
    connect_remote(url)
}

/// Open a network backend behind `url`.
fn connect_remote(url: &storage::RemoteUrl) -> std::io::Result<StoreBackend> {
    storage::connect(url).map(StoreBackend::Remote)
}

/// Refuse an empty staging copy with the store address.
///
/// A fresh pull from an empty remote leaves staging with no segments.
/// Local commands would then report the cache directory.
/// The user pointed at a URL and a set.
/// The error names those.
pub(crate) fn require_staged_set(staging: &IndexSet, target: &StoreTarget) -> std::io::Result<()> {
    if staging.segments()?.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "no set {} at {} (staged an empty copy)",
                target.set, target.url
            ),
        ));
    }
    Ok(())
}
