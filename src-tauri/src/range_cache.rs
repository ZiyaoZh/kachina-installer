use anyhow::{anyhow, bail, Context, Result};
use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub const ERR_MULTIPART_RANGE_UNSUPPORTED: &str = "ERR_MULTIPART_RANGE_UNSUPPORTED";

lazy_static::lazy_static! {
    static ref URL_LOCKS: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>> =
        Mutex::new(HashMap::new());
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheSource {
    Hit,
    Stored,
}

impl CacheSource {
    pub fn transport_label(self) -> &'static str {
        match self {
            CacheSource::Hit => "range-cache-full-200-hit",
            CacheSource::Stored => "range-cache-full-200-store",
        }
    }
}

pub struct CachedRange {
    pub reader: Box<dyn AsyncRead + Unpin + Send>,
    pub total_size: u64,
    pub source: CacheSource,
}

fn url_lock(url: &str) -> Result<Arc<tokio::sync::Mutex<()>>> {
    let mut locks = URL_LOCKS
        .lock()
        .map_err(|_| anyhow!("range cache lock table poisoned"))?;
    Ok(locks
        .entry(url.to_string())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone())
}

fn cache_root() -> PathBuf {
    std::env::temp_dir()
        .join("kachina-range-cache")
        .join(std::process::id().to_string())
}

fn cache_path(url: &str) -> PathBuf {
    let mut hasher = Sha256::new();
    hasher.update(url.as_bytes());
    let key = hex::encode(hasher.finalize());
    cache_root().join(format!("{key}.package"))
}

fn required_end(offset: usize, size: usize) -> Result<u64> {
    if size == 0 {
        bail!("range cache requires a non-empty range");
    }
    let end = offset
        .checked_add(size)
        .ok_or_else(|| anyhow!("range end overflows usize"))?;
    u64::try_from(end).context("range end overflows u64")
}

async fn metadata_len(path: &Path) -> Result<Option<u64>> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => Ok(Some(metadata.len())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("read range cache metadata: {path:?}")),
    }
}

async fn open_range_reader(
    path: &Path,
    offset: usize,
    size: usize,
) -> Result<Box<dyn AsyncRead + Unpin + Send>> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("open range cache file: {path:?}"))?;
    file.seek(std::io::SeekFrom::Start(offset as u64))
        .await
        .with_context(|| format!("seek range cache file: {path:?}"))?;
    Ok(Box::new(file.take(size as u64)))
}

pub async fn open_cached_range_if_available(
    url: &str,
    offset: usize,
    size: usize,
) -> Result<Option<CachedRange>> {
    let path = cache_path(url);
    let required_end = required_end(offset, size)?;

    let Some(total_size) = metadata_len(&path).await? else {
        return Ok(None);
    };

    if total_size < required_end {
        let _ = tokio::fs::remove_file(&path).await;
        return Ok(None);
    }

    Ok(Some(CachedRange {
        reader: open_range_reader(&path, offset, size).await?,
        total_size,
        source: CacheSource::Hit,
    }))
}

pub async fn open_range_from_200_response(
    url: &str,
    response: reqwest::Response,
    offset: usize,
    size: usize,
) -> Result<CachedRange> {
    let required_end = required_end(offset, size)?;
    if let Some(content_length) = response.content_length() {
        if content_length < required_end {
            bail!(
                "HTTP 200 body is too small for requested range: need at least {required_end} bytes, got {content_length}"
            );
        }
    }

    let lock = url_lock(url)?;
    let _guard = lock.lock().await;

    if let Some(cached) = open_cached_range_if_available(url, offset, size).await? {
        return Ok(cached);
    }

    let path = cache_path(url);
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("range cache path has no parent: {path:?}"))?;
    tokio::fs::create_dir_all(parent)
        .await
        .with_context(|| format!("create range cache dir: {parent:?}"))?;

    let part_path = path.with_extension("package.part");
    let _ = tokio::fs::remove_file(&part_path).await;

    let mut file = tokio::fs::File::create(&part_path)
        .await
        .with_context(|| format!("create range cache temp file: {part_path:?}"))?;
    let stream = response.bytes_stream().map_err(std::io::Error::other);
    let mut reader = tokio_util::io::StreamReader::new(stream);
    let written = tokio::io::copy(&mut reader, &mut file)
        .await
        .with_context(|| format!("write range cache temp file: {part_path:?}"))?;
    file.flush()
        .await
        .with_context(|| format!("flush range cache temp file: {part_path:?}"))?;
    drop(file);

    if written < required_end {
        let _ = tokio::fs::remove_file(&part_path).await;
        bail!(
            "HTTP 200 body is too small for requested range: need at least {required_end} bytes, got {written}"
        );
    }

    let _ = tokio::fs::remove_file(&path).await;
    tokio::fs::rename(&part_path, &path)
        .await
        .with_context(|| format!("promote range cache file: {part_path:?} -> {path:?}"))?;

    Ok(CachedRange {
        reader: open_range_reader(&path, offset, size).await?,
        total_size: written,
        source: CacheSource::Stored,
    })
}

pub async fn read_cached_range_if_available(
    url: &str,
    offset: usize,
    size: usize,
) -> Result<Option<Vec<u8>>> {
    let Some(cached) = open_cached_range_if_available(url, offset, size).await? else {
        return Ok(None);
    };
    read_cached_range(cached, size).await.map(Some)
}

pub async fn read_range_from_200_response(
    url: &str,
    response: reqwest::Response,
    offset: usize,
    size: usize,
) -> Result<Vec<u8>> {
    let cached = open_range_from_200_response(url, response, offset, size).await?;
    read_cached_range(cached, size).await
}

async fn read_cached_range(mut cached: CachedRange, expected_size: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(expected_size);
    cached
        .reader
        .read_to_end(&mut bytes)
        .await
        .context("read range cache slice")?;
    if bytes.len() != expected_size {
        bail!(
            "range cache slice length mismatch: expected {expected_size}, got {}",
            bytes.len()
        );
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn spawn_full_body_server(body: Vec<u8>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let body = body.clone();
                tokio::spawn(async move {
                    let mut request = [0_u8; 4096];
                    let _ = stream.read(&mut request).await;
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    stream.write_all(headers.as_bytes()).await.unwrap();
                    stream.write_all(&body).await.unwrap();
                });
            }
        });

        format!("http://{addr}/package.exe")
    }

    fn sample_body(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    #[tokio::test]
    async fn get_http_with_range_returns_requested_slice_from_200_full_body() {
        let body = sample_body(1024);
        let url = spawn_full_body_server(body.clone()).await;

        let (status, bytes) = crate::dfs::get_http_with_range(url, 0, 256).await.unwrap();

        assert_eq!(status, 206);
        assert_eq!(bytes, body[..256]);
    }

    #[tokio::test]
    async fn create_http_stream_reads_requested_slice_from_200_full_body() {
        let body = sample_body(2048);
        let url = spawn_full_body_server(body.clone()).await;

        let (mut reader, len, insight) = crate::fs::create_http_stream(&url, 512, 128, true)
            .await
            .unwrap();

        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();

        assert_eq!(len, 128);
        assert_eq!(bytes, body[512..640]);
        assert_eq!(
            insight.lock().unwrap().transport.as_deref(),
            Some(CacheSource::Stored.transport_label())
        );
    }

    #[tokio::test]
    async fn create_multi_http_stream_rejects_200_full_body() {
        let body = sample_body(2048);
        let url = spawn_full_body_server(body).await;

        let err = match crate::fs::create_multi_http_stream(&url, "0-127,512-639").await {
            Ok(_) => panic!("multi-range 200 response should be rejected"),
            Err(err) => err,
        };

        assert!(format!("{err:#}").contains(ERR_MULTIPART_RANGE_UNSUPPORTED));
    }
}
