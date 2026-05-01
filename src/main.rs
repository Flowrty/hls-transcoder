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
    /// Direct upstream URL. Optional when `manifest_url`+`seq` are present.
    url: Option<String>,
    headers: Option<String>,
    transcode: Option<String>,
    /// Segment sequence index within the playlist (0-based).
    /// When present together with `manifest_url`, the actual upstream segment
    /// URL is re-resolved at fetch time so stale signed tokens are never used.
    seq: Option<usize>,
    /// Media-playlist URL used to re-resolve the segment at fetch time.
    manifest_url: Option<String>,
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

/// Build a segment URL that embeds the media-playlist URL + sequence index
/// instead of the raw upstream segment URL. This lets /segment re-fetch a
/// fresh signed token at request time rather than using a baked-in stale one.
fn self_segment_url_by_seq(playlist_url: &str, seq: usize, headers_b64: &str, transcode: bool) -> String {
    let base = service_base_url();
    let encoded_playlist = urlencoding::encode(playlist_url);
    let t = if transcode { "1" } else { "0" };
    if headers_b64.is_empty() {
        format!("{base}/segment?manifest_url={encoded_playlist}&seq={seq}&transcode={t}")
    } else {
        format!("{base}/segment?manifest_url={encoded_playlist}&seq={seq}&headers={headers_b64}&transcode={t}")
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
    let raw = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            warn!("prefetch body read failed: {e}");
            in_flight.lock().await.remove(&upstream_url);
            return;
        }
    };

    // Kiwi / owocdn.top: unwrap base64-JSON segment envelope (same logic as proxy_segment)
    let data = if raw.first() == Some(&b'{') {
        if let Ok(text) = std::str::from_utf8(&raw) {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(text) {
                if let Some(b64_str) = val.get("data").and_then(|v| v.as_str()) {
                    B64.decode(b64_str).map(Bytes::from).unwrap_or(raw)
                } else { raw }
            } else { raw }
        } else { raw }
    } else { raw };

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

/// After serving a seq-based segment, kick off background fetches for the next
/// PREFETCH_AHEAD segments by re-resolving their URLs from the playlist.
/// Each segment gets a fresh signed token at fetch time.
fn spawn_prefetch_by_seq(
    playlist_url: String,
    current_seq: usize,
    headers_b64: String,
    transcode: bool,
    cache: SegmentCache,
    in_flight: InFlight,
) {
    tokio::spawn(async move {
        for offset in 1..=PREFETCH_AHEAD {
            let seq = current_seq + offset;
            let pl = playlist_url.clone();
            let hb = headers_b64.clone();
            let c = cache.clone();
            let f = in_flight.clone();
            tokio::spawn(async move {
                let upstream_url = match resolve_segment_url_from_playlist(&pl, seq, &hb).await {
                    Ok(u) => u,
                    Err(e) => {
                        // seq out of range (end of playlist) — normal, stop silently
                        if e.to_string().contains("out of range") {
                            return;
                        }
                        warn!("prefetch seq={seq} resolve failed: {e}");
                        return;
                    }
                };
                fetch_and_cache(upstream_url, hb, transcode, c, f).await;
            });
        }
    });
}

// ── /manifest ─────────────────────────────────────────────────────────────────

/// Decide whether a resolved URL from a manifest line is a sub-manifest (→ /manifest)
/// or a media segment (→ /segment).
///
/// Rules (in priority order):
///  1. Contains ".m3u8"              — always a manifest
///  2. Contains "playlist"           — always a manifest
///  3. Same host as the base URL AND path is just "/" or empty AND has a query string
///     → proxy servers like zaza.animex.one serve every resource (manifest or segment)
///       via query params on the root path; when a line resolves to the same host with
///       only a query string it is a quality-variant sub-manifest, never raw media.
///     ⚠ Rule 3 is SUPPRESSED when `is_media_playlist` is true. A media playlist
///       contains #EXTINF tags so every URL line is a real segment — routing them to
///       /manifest would cause an infinite loop.
///  4. Everything else               — treat as a segment
fn url_is_sub_manifest(base_url: &str, resolved: &str, is_media_playlist: bool) -> bool {
    // Rule 1 & 2 — explicit manifest path markers
    if resolved.contains(".m3u8") || resolved.contains("playlist") {
        return true;
    }

    // Rule 3 — same-host, query-only (e.g. zaza.animex.one quality variants).
    // SUPPRESSED for media playlists: if the manifest contains #EXTINF, every URL
    // line is a real segment — same-host/query-only URLs are NOT sub-manifests here.
    // Without this guard, zaza.animex.one segment URLs (same host, query-only path)
    // would be routed to /manifest instead of /segment, creating an infinite loop.
    if !is_media_playlist {
        if let (Ok(base), Ok(res)) = (url::Url::parse(base_url), url::Url::parse(resolved)) {
            let same_host = base.host_str() == res.host_str();
            let query_only = (res.path() == "/" || res.path().is_empty()) && res.query().is_some();
            if same_host && query_only {
                return true;
            }
        }
    }

    false
}

async fn handle_manifest(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    Query(params): Query<ProxyParams>,
) -> Response {
    let url = match params.url.as_deref() {
        Some(u) => u.to_string(),
        None => return (StatusCode::BAD_REQUEST, "missing url parameter").into_response(),
    };
    match proxy_manifest(&url, params.headers.as_deref(), &state.playlists, &state.manifests).await {
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
            error!("manifest error for {url}: {e}");
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
    let resp_content_type = resp.headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(';').next().unwrap_or(s).trim().to_string());
    let body = resp.text().await?;

    if !body.trim_start().starts_with("#EXTM3U") {
        // Upstream returned a 200 but non-HLS body — token probably expired.
        // Serve the cached rewritten manifest if we have one so the player
        // keeps working with already-proxied segment URLs.
        {
            let cached = manifests.lock().await;
            if let Some((cached_body, _)) = cached.get(url) {
                info!("manifest URL did not return #EXTM3U (token expired?), serving cached manifest for {url}");
                return Ok((cached_body.clone(), "application/vnd.apple.mpegurl".to_string()));
            }
        }
        // No cache yet — nothing we can do but warn. Proxy the raw body so
        // the caller at least gets *something* rather than a 502.
        warn!("manifest URL did not return #EXTM3U: {url}");
        let content_type = resp_content_type.unwrap_or_else(|| "video/mp4".to_string());
        return Ok((body, content_type));
    }

    let needs_transcode = manifest_has_heaac(&body);
    if needs_transcode {
        info!("HE-AAC detected in manifest — segments will be transcoded");
    }

    let body = body.replace("mp4a.40.1", "mp4a.40.2");

    let headers_b64_str = headers_b64.unwrap_or("");
    let mut out = String::with_capacity(body.len() + 512);
    let mut next_line_is_stream = false;

    // If the manifest contains #EXTINF it is a media playlist (actual segments),
    // not a master playlist. In that case, same-host query-only URLs are segments,
    // not quality-variant sub-manifests. Rule 3 of url_is_sub_manifest must be
    // suppressed so we don't recurse /manifest→/manifest→… forever.
    let is_media_playlist = body.contains("#EXTINF");

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
            // do NOT reset next_line_is_stream here — a URI= tag line is not a URL line
            continue;
        }

        if is_url_line(trimmed) {
            let resolved = resolve_url(url, trimmed);
            let is_sub_manifest = next_line_is_stream || url_is_sub_manifest(url, &resolved, is_media_playlist);
            next_line_is_stream = false;

            let rewritten = if is_sub_manifest {
                self_url("manifest", &resolved, headers_b64_str, needs_transcode)
            } else {
                // For media-playlist segments, use seq-based indirection so the
                // actual upstream URL (and its signed token) is re-resolved fresh
                // at fetch time rather than baked into the rewritten manifest.
                let seq = segment_urls.len();
                segment_urls.push(resolved.clone());
                self_segment_url_by_seq(url, seq, headers_b64_str, needs_transcode)
            };
            out.push_str(&rewritten);
            out.push('\n');
            continue;
        }

        // Only reset the flag for non-special comment lines — not for #EXT-X-STREAM-INF
        // which sets the flag and must keep it set until the next URL line.
        if !trimmed.starts_with("#EXT-X-STREAM-INF") && !trimmed.starts_with("#EXT-X-I-FRAME-STREAM-INF") {
            next_line_is_stream = false;
        }
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
    // Detect if this is an EXT-X-KEY line — its URI is a binary key, not a playlist.
    let is_key_line = line.trim_start().starts_with("#EXT-X-KEY");
    while let Some(start) = rest.find("URI=\"") {
        result.push_str(&rest[..start + 5]);
        rest = &rest[start + 5..];
        if let Some(end) = rest.find('"') {
            let inner = &rest[..end];
            let resolved = resolve_url(base_url, inner);
            let rewritten = if is_key_line {
                // AES-128 key: proxy as a raw segment (16 bytes), not a manifest.
                self_url("segment", &resolved, headers_b64, false)
            } else {
                // URIs in #EXT-X-MEDIA etc. are media playlists — rewrite via /manifest.
                self_url("manifest", &resolved, headers_b64, false)
            };
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

    // Resolve the actual upstream segment URL.
    // If manifest_url+seq are present, re-fetch the playlist for a fresh signed
    // token; otherwise fall back to the direct url= parameter.
    let (upstream_url, playlist_url_for_prefetch, seq_for_prefetch) =
        match (params.manifest_url.as_deref(), params.seq) {
            (Some(playlist_url_encoded), Some(seq)) => {
                let playlist_url = urlencoding::decode(playlist_url_encoded)
                    .unwrap_or_else(|_| std::borrow::Cow::Borrowed(playlist_url_encoded))
                    .to_string();
                match resolve_segment_url_from_playlist(&playlist_url, seq, &headers_b64).await {
                    Ok(u) => (u, Some(playlist_url.clone()), Some(seq)),
                    Err(e) => {
                        error!("failed to resolve segment seq={seq} from playlist {playlist_url}: {e}");
                        return (StatusCode::BAD_GATEWAY, e.to_string()).into_response();
                    }
                }
            }
            _ => {
                let u = match params.url.as_deref() {
                    Some(u) => u.to_string(),
                    None => return (StatusCode::BAD_REQUEST, "missing url or manifest_url+seq").into_response(),
                };
                (u, None, None)
            }
        };

    match proxy_segment(
        &upstream_url,
        params.headers.as_deref(),
        transcode,
        &state.cache,
    )
    .await
    {
        Ok((data, content_type)) => {
            // Kick off background prefetch of next segments.
            // For seq-based requests, prefetch by seq index (fresh token per segment).
            // For direct-URL requests, use the playlist index lookup.
            if let (Some(pl_url), Some(seq)) = (playlist_url_for_prefetch, seq_for_prefetch) {
                spawn_prefetch_by_seq(
                    pl_url,
                    seq,
                    headers_b64.clone(),
                    transcode,
                    state.cache.clone(),
                    state.in_flight.clone(),
                );
            } else {
                spawn_prefetch(
                    upstream_url.clone(),
                    headers_b64,
                    transcode,
                    state.cache.clone(),
                    state.playlists.clone(),
                    state.in_flight.clone(),
                );
            }

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
            error!("segment error for {}: {e}", upstream_url);
            (StatusCode::BAD_GATEWAY, e.to_string()).into_response()
        }
    }
}

/// Re-fetch the media playlist and extract the Nth segment URL (0-based).
/// This gives us a fresh signed token every time, avoiding 404s from expired URLs.
async fn resolve_segment_url_from_playlist(
    playlist_url: &str,
    seq: usize,
    headers_b64: &str,
) -> Result<String> {
    let extra = decode_headers(Some(headers_b64));
    let resp = upstream_get(playlist_url, &extra).await
        .context("re-fetching media playlist for fresh segment URL")?;
    let body = resp.text().await.context("reading media playlist body")?;

    let segment_urls: Vec<String> = body
        .lines()
        .filter(|l| is_url_line(l.trim()))
        .map(|l| resolve_url(playlist_url, l.trim()))
        .collect();

    segment_urls
        .into_iter()
        .nth(seq)
        .ok_or_else(|| anyhow!("seq={seq} out of range in playlist ({playlist_url})"))
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
    let raw = resp.bytes().await.context("reading segment body")?;

    // ── Kiwi / owocdn.top base64-JSON unwrapping ─────────────────────────────
    // owocdn.top (used by kiwi provider) wraps fMP4 segments in a JSON envelope:
    //   {"data":"AAAAIGZ0eXBpc..."}
    // where the value is standard base64-encoded binary. HLS.js gets garbage
    // if we pass the JSON through directly — detect and unwrap it here.
    let data = if raw.first() == Some(&b'{') {
        // Looks like JSON — try to parse {"data": "<base64>"}
        if let Ok(text) = std::str::from_utf8(&raw) {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(text) {
                if let Some(b64_str) = val.get("data").and_then(|v| v.as_str()) {
                    match B64.decode(b64_str) {
                        Ok(decoded) => {
                            info!("kiwi: unwrapped base64-JSON segment ({} → {} bytes)", raw.len(), decoded.len());
                            Bytes::from(decoded)
                        }
                        Err(e) => {
                            warn!("kiwi: JSON looked like base64-wrapper but decode failed: {e}");
                            raw
                        }
                    }
                } else {
                    raw
                }
            } else {
                raw
            }
        } else {
            raw
        }
    } else {
        raw
    };

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
