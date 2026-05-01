// hls-transcoder — Railway-hosted Rust service
//
// Two endpoints:
//   GET /manifest?url=<encoded>&headers=<b64json>
//     → Fetches the HLS manifest, rewrites all segment/sub-manifest URLs
//       to point back to this service's /segment endpoint, returns M3U8.
//       Also registers the ordered segment list into the prefetch cache so
//       we know what to fetch ahead when a segment is requested.
//
//   GET /segment?url=<encoded>&headers=<b64json>&transcode=<0|1>
//     → Returns segment from in-memory cache if available (instant),
//       otherwise fetches from upstream. Either way, triggers background
//       prefetch of the next PREFETCH_AHEAD segments in the playlist.

use anyhow::{anyhow, Context, Result};
use axum::{
    extract::Query,
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use bytes::Bytes;
use serde::Deserialize;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tokio::process::Command;
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info, warn};

// ── Constants ─────────────────────────────────────────────────────────────────

/// How many segments ahead to prefetch after each request.
/// 8 segments × ~10s each = ~80s of buffer, enough for a 10s skip with margin.
const PREFETCH_AHEAD: usize = 8;

/// Max segments to keep in RAM. Each segment ~400 KB → 30 segments ≈ 12 MB.
/// Kept low for Koyeb free tier (512 MB RAM total).
const CACHE_MAX_SEGMENTS: usize = 30;

/// Evict cache entries older than this (covers a full episode).
const CACHE_TTL: Duration = Duration::from_secs(3600);

// ── Shared state ──────────────────────────────────────────────────────────────

#[derive(Clone)]
struct CacheEntry {
    data: Bytes,
    content_type: String,
    inserted: Instant,
}

/// In-memory segment cache keyed by upstream URL.
type SegmentCache = Arc<Mutex<HashMap<String, CacheEntry>>>;

/// Ordered list of upstream segment URLs parsed from the last manifest fetch,
/// keyed by the manifest URL. Used to know what to prefetch next.
type PlaylistIndex = Arc<Mutex<HashMap<String, Vec<String>>>>;

/// Cached rewritten manifest bodies, keyed by manifest URL.
/// Serves stale manifest to VLC when upstream has expired, preserving #EXT-X-ENDLIST.
type ManifestCache = Arc<Mutex<HashMap<String, (String, Instant)>>>;

/// URLs currently being fetched — prevents duplicate concurrent upstream requests
/// for the same segment when two prefetch waves overlap.
type InFlight = Arc<Mutex<std::collections::HashSet<String>>>;

struct AppState {
    cache: SegmentCache,
    playlists: PlaylistIndex,
    in_flight: InFlight,
    manifests: ManifestCache,
}

// ── Shared HTTP client ────────────────────────────────────────────────────────

static HTTP_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

fn http_client() -> &'static reqwest::Client {
    HTTP_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .danger_accept_invalid_certs(false)
            .pool_max_idle_per_host(16)
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .expect("failed to build HTTP client")
    })
}

// ── Config ────────────────────────────────────────────────────────────────────

fn service_base_url() -> String {
    std::env::var("SERVICE_BASE_URL")
        .unwrap_or_else(|_| "http://localhost:3001".to_string())
        .trim_end_matches('/')
        .to_string()
}

fn port() -> u16 {
    std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3001)
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "hls_transcoder=info,tower_http=warn".to_string()),
        )
        .init();

    let state = Arc::new(AppState {
        cache: Arc::new(Mutex::new(HashMap::new())),
        playlists: Arc::new(Mutex::new(HashMap::new())),
        in_flight: Arc::new(Mutex::new(std::collections::HashSet::new())),
        manifests: Arc::new(Mutex::new(HashMap::new())),
    });

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/manifest", get(handle_manifest))
        .route("/segment", get(handle_segment))
        .route("/health", get(|| async { "ok" }))
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port()));
    info!("hls-transcoder listening on {addr}");
    info!("Service base URL: {}", service_base_url());

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

// ── Query params ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct ProxyParams {
    url: String,
    headers: Option<String>,
    transcode: Option<String>,
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn decode_headers(b64: Option<&str>) -> HashMap<String, String> {
    b64.and_then(|s| {
        B64.decode(s).ok().and_then(|bytes| {
            serde_json::from_slice::<HashMap<String, String>>(&bytes).ok()
        })
    })
    .unwrap_or_default()
}

async fn upstream_get(url: &str, extra: &HashMap<String, String>) -> Result<reqwest::Response> {
    let mut req = http_client().get(url);
    for (k, v) in extra {
        req = req.header(k.as_str(), v.as_str());
    }
    req = req.header(
        "User-Agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 \
         (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    );
    let resp = req.send().await.context("upstream GET failed")?;
    if !resp.status().is_success() {
        return Err(anyhow!("upstream returned {}", resp.status()));
    }
    Ok(resp)
}

fn is_url_line(line: &str) -> bool {
    let t = line.trim();
    !t.is_empty() && !t.starts_with('#')
}

fn resolve_url(base: &str, href: &str) -> String {
    if href.starts_with("http://") || href.starts_with("https://") {
        return href.to_string();
    }
    if let Ok(base_url) = url::Url::parse(base) {
        if let Ok(resolved) = base_url.join(href) {
            return resolved.to_string();
        }
    }
    format!("https:{href}")
}

fn manifest_has_heaac(manifest: &str) -> bool {
    manifest.contains("mp4a.40.1") || manifest.contains("MP4A.40.1")
}

fn self_url(endpoint: &str, target: &str, headers_b64: &str, transcode: bool) -> String {
    let base = service_base_url();
    let encoded = urlencoding::encode(target);
    let t = if transcode { "1" } else { "0" };
    if headers_b64.is_empty() {
        format!("{base}/{endpoint}?url={encoded}&transcode={t}")
    } else {
        format!("{base}/{endpoint}?url={encoded}&headers={headers_b64}&transcode={t}")
    }
}

// ── Cache helpers ─────────────────────────────────────────────────────────────

async fn cache_get(cache: &SegmentCache, url: &str) -> Option<CacheEntry> {
    let map = cache.lock().await;
    map.get(url).and_then(|e| {
        if e.inserted.elapsed() < CACHE_TTL {
            Some(e.clone())
        } else {
            None
        }
    })
}

async fn cache_set(cache: &SegmentCache, url: String, entry: CacheEntry) {
    let mut map = cache.lock().await;
    // Evict oldest entries if over limit
    if map.len() >= CACHE_MAX_SEGMENTS {
        let oldest = map
            .iter()
            .min_by_key(|(_, v)| v.inserted)
            .map(|(k, _)| k.clone());
        if let Some(k) = oldest {
            map.remove(&k);
        }
    }
    map.insert(url, entry);
}

/// Fetch a segment from upstream and store it in the cache.
/// Uses an in-flight set to ensure only one fetch happens per URL at a time.
/// Silently ignores errors (prefetch is best-effort).
async fn fetch_and_cache(
    upstream_url: String,
    headers_b64: String,
    transcode: bool,
    cache: SegmentCache,
    in_flight: InFlight,
) {
    // Skip if already cached
    if cache_get(&cache, &upstream_url).await.is_some() {
        return;
    }

    // Skip if already being fetched by another task (prevents duplicate upstream requests)
    {
        let mut flying = in_flight.lock().await;
        if flying.contains(&upstream_url) {
            return;
        }
        flying.insert(upstream_url.clone());
    }

    let extra = decode_headers(Some(&headers_b64));
    let resp = match upstream_get(&upstream_url, &extra).await {
        Ok(r) => r,
        Err(e) => {
            warn!("prefetch failed for {upstream_url}: {e}");
            in_flight.lock().await.remove(&upstream_url);
            return;
        }
    };

    let content_length = resp.content_length().unwrap_or(0);
    let data = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            warn!("prefetch body read failed: {e}");
            in_flight.lock().await.remove(&upstream_url);
            return;
        }
    };

    let (data, content_type) = if data.len() == 16 || content_length == 16 {
        (data, "application/octet-stream".to_string())
    } else if transcode {
        match ffmpeg_transcode(data).await {
            Ok(t) => (t, "video/mp4".to_string()),
            Err(e) => {
                warn!("prefetch transcode failed: {e}");
                in_flight.lock().await.remove(&upstream_url);
                return;
            }
        }
    } else {
        (data, "video/mp4".to_string())
    };

    cache_set(
        &cache,
        upstream_url.clone(),
        CacheEntry { data, content_type, inserted: Instant::now() },
    )
    .await;

    in_flight.lock().await.remove(&upstream_url);
    info!("prefetched {upstream_url}");
}

/// After serving segment at `current_upstream_url`, kick off background
/// fetches for the next PREFETCH_AHEAD segments in the same playlist.
fn spawn_prefetch(
    current_upstream_url: String,
    headers_b64: String,
    transcode: bool,
    cache: SegmentCache,
    playlists: PlaylistIndex,
    in_flight: InFlight,
) {
    tokio::spawn(async move {
        // Find current segment's position in the playlist
        let segments = {
            let pl = playlists.lock().await;
            pl.values()
                .find(|segs| segs.contains(&current_upstream_url))
                .cloned()
        };

        let segments = match segments {
            Some(s) => s,
            None => return,
        };

        let pos = match segments.iter().position(|s| s == &current_upstream_url) {
            Some(p) => p,
            None => return,
        };

        // Fire-and-forget individual fetches — don't wait for them
        for url in segments.iter().skip(pos + 1).take(PREFETCH_AHEAD) {
            let u = url.clone();
            let h = headers_b64.clone();
            let c = cache.clone();
            let f = in_flight.clone();
            tokio::spawn(async move {
                fetch_and_cache(u, h, transcode, c, f).await;
            });
        }
    });
}

// ── /manifest ─────────────────────────────────────────────────────────────────

async fn handle_manifest(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    Query(params): Query<ProxyParams>,
) -> Response {
    match proxy_manifest(&params.url, params.headers.as_deref(), &state.playlists, &state.manifests).await {
        Ok((body, content_type)) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_str(&content_type).unwrap(),
            );
            headers.insert(
                axum::http::header::CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=3"),
            );
            headers.insert(
                HeaderName::from_static("access-control-allow-origin"),
                HeaderValue::from_static("*"),
            );
            (StatusCode::OK, headers, body).into_response()
        }
        Err(e) => {
            error!("manifest error for {}: {e}", params.url);
            (StatusCode::BAD_GATEWAY, e.to_string()).into_response()
        }
    }
}

async fn proxy_manifest(
    url: &str,
    headers_b64: Option<&str>,
    playlists: &PlaylistIndex,
    manifests: &ManifestCache,
) -> Result<(String, String)> {
    let extra = decode_headers(headers_b64);
    let resp = match upstream_get(url, &extra).await {
        Ok(r) => r,
        Err(e) => {
            // Upstream failed (expired token etc.) — serve cached manifest if we have one
            let cached = manifests.lock().await;
            if let Some((body, _)) = cached.get(url) {
                info!("upstream manifest failed ({e}), serving cached manifest for {url}");
                return Ok((body.clone(), "application/vnd.apple.mpegurl".to_string()));
            }
            return Err(e);
        }
    };
    let body = resp.text().await?;

    if !body.trim_start().starts_with("#EXTM3U") {
        warn!("manifest URL did not return #EXTM3U: {url}");
        return Ok((body, "application/vnd.apple.mpegurl".to_string()));
    }

    let needs_transcode = manifest_has_heaac(&body);
    if needs_transcode {
        info!("HE-AAC detected in manifest — segments will be transcoded");
    }

    let body = body.replace("mp4a.40.1", "mp4a.40.2");

    let headers_b64_str = headers_b64.unwrap_or("");
    let mut out = String::with_capacity(body.len() + 512);
    let mut next_line_is_stream = false;

    // Collect upstream segment URLs in order for the prefetch index
    let mut segment_urls: Vec<String> = Vec::new();

    for line in body.lines() {
        let trimmed = line.trim();

        if trimmed.starts_with("#EXT-X-STREAM-INF") || trimmed.starts_with("#EXT-X-I-FRAME-STREAM-INF") {
            next_line_is_stream = true;
        }

        if trimmed.starts_with('#') && trimmed.contains("URI=\"") {
            let rewritten = rewrite_uri_attrs(line, url, headers_b64_str, needs_transcode);
            out.push_str(&rewritten);
            out.push('\n');
            continue;
        }

        if is_url_line(trimmed) {
            let resolved = resolve_url(url, trimmed);
            // Detect sub-manifests: either flagged by #EXT-X-STREAM-INF above,
            // or the URL explicitly contains a manifest path marker.
            let is_sub_manifest = next_line_is_stream
                || resolved.contains(".m3u8")
                || resolved.contains("playlist")
                || resolved.contains("index.m3u8");
            next_line_is_stream = false;

            let endpoint = if is_sub_manifest { "manifest" } else { "segment" };

            // Record segment URLs for prefetch index
            if endpoint == "segment" {
                segment_urls.push(resolved.clone());
            }

            let rewritten = self_url(endpoint, &resolved, headers_b64_str, needs_transcode);
            out.push_str(&rewritten);
            out.push('\n');
            continue;
        }

        next_line_is_stream = false;
        out.push_str(line);
        out.push('\n');
    }

    // Store ordered segment list keyed by manifest URL
    if !segment_urls.is_empty() {
        let mut pl = playlists.lock().await;
        pl.insert(url.to_string(), segment_urls);
    }

    // Cache the rewritten manifest so VLC gets a consistent response on re-polls
    {
        let mut mc = manifests.lock().await;
        mc.insert(url.to_string(), (out.clone(), Instant::now()));
    }

    Ok((out, "application/vnd.apple.mpegurl".to_string()))
}

fn rewrite_uri_attrs(line: &str, base_url: &str, headers_b64: &str, _transcode: bool) -> String {
    let mut result = String::new();
    let mut rest = line;
    while let Some(start) = rest.find("URI=\"") {
        result.push_str(&rest[..start + 5]);
        rest = &rest[start + 5..];
        if let Some(end) = rest.find('"') {
            let inner = &rest[..end];
            let resolved = resolve_url(base_url, inner);
            let rewritten = self_url("segment", &resolved, headers_b64, false);
            result.push_str(&rewritten);
            result.push('"');
            rest = &rest[end + 1..];
        }
    }
    result.push_str(rest);
    result
}

// ── /segment ──────────────────────────────────────────────────────────────────

async fn handle_segment(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    Query(params): Query<ProxyParams>,
) -> Response {
    let transcode = params.transcode.as_deref() == Some("1");
    let headers_b64 = params.headers.clone().unwrap_or_default();

    match proxy_segment(
        &params.url,
        params.headers.as_deref(),
        transcode,
        &state.cache,
    )
    .await
    {
        Ok((data, content_type)) => {
            // Kick off background prefetch of next segments
            spawn_prefetch(
                params.url.clone(),
                headers_b64,
                transcode,
                state.cache.clone(),
                state.playlists.clone(),
                state.in_flight.clone(),
            );

            let data_len = data.len();
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_str(&content_type)
                    .unwrap_or_else(|_| HeaderValue::from_static("video/mp4")),
            );
            headers.insert(
                axum::http::header::CONTENT_LENGTH,
                HeaderValue::from_str(&data_len.to_string()).unwrap(),
            );
            headers.insert(
                axum::http::header::ACCEPT_RANGES,
                HeaderValue::from_static("bytes"),
            );
            headers.insert(
                axum::http::header::CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=3600, immutable"),
            );
            headers.insert(
                HeaderName::from_static("access-control-allow-origin"),
                HeaderValue::from_static("*"),
            );
            (StatusCode::OK, headers, data).into_response()
        }
        Err(e) => {
            error!("segment error for {}: {e}", params.url);
            (StatusCode::BAD_GATEWAY, e.to_string()).into_response()
        }
    }
}

async fn proxy_segment(
    url: &str,
    headers_b64: Option<&str>,
    transcode: bool,
    cache: &SegmentCache,
) -> Result<(Bytes, String)> {
    // Serve from cache if available (instant for already-prefetched segments)
    if let Some(entry) = cache_get(cache, url).await {
        info!("cache hit: {url}");
        return Ok((entry.data, entry.content_type));
    }

    info!("cache miss, fetching: {url}");
    let extra = decode_headers(headers_b64);
    let resp = upstream_get(url, &extra).await?;

    let content_length = resp.content_length().unwrap_or(0);
    let data = resp.bytes().await.context("reading segment body")?;

    // AES-128 keys are exactly 16 bytes
    if data.len() == 16 || content_length == 16 {
        return Ok((data, "application/octet-stream".to_string()));
    }

    let (data, content_type) = if transcode {
        match ffmpeg_transcode(data).await {
            Ok(t) => (t, "video/mp4".to_string()),
            Err(e) => {
                warn!("FFmpeg transcode failed ({e}), returning error");
                return Err(e);
            }
        }
    } else {
        (data, "video/mp4".to_string())
    };

    // Store in cache for future seeks
    cache_set(
        cache,
        url.to_string(),
        CacheEntry {
            data: data.clone(),
            content_type: content_type.clone(),
            inserted: Instant::now(),
        },
    )
    .await;

    Ok((data, content_type))
}

// ── FFmpeg ────────────────────────────────────────────────────────────────────

async fn ffmpeg_transcode(input: Bytes) -> Result<Bytes> {
    let mut child = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel", "error",
            "-i", "pipe:0",
            "-c:v", "copy",
            "-c:a", "aac",
            "-profile:a", "aac_low",
            "-b:a", "128k",
            "-movflags", "frag_keyframe+empty_moov+default_base_moof",
            "-f", "mp4",
            "pipe:1",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .context("failed to spawn ffmpeg — is it installed?")?;

    let stdin = child.stdin.take().ok_or_else(|| anyhow!("no stdin"))?;
    tokio::io::AsyncWriteExt::write_all(
        &mut tokio::io::BufWriter::new(stdin),
        &input,
    )
    .await
    .context("writing to ffmpeg stdin")?;

    let output = child.wait_with_output().await.context("ffmpeg wait")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("ffmpeg exited {}: {stderr}", output.status));
    }

    Ok(Bytes::from(output.stdout))
}
