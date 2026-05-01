// hls-transcoder — Railway-hosted Rust service
//
// Two endpoints:
//   GET /manifest?url=<encoded>&headers=<b64json>
//     → Fetches the HLS manifest, rewrites all segment/sub-manifest URLs
//       to point back to this service's /segment endpoint, returns M3U8.
//
//   GET /segment?url=<encoded>&headers=<b64json>&transcode=<0|1>
//     → Fetches the fMP4 segment. If transcode=1, pipes it through FFmpeg
//       to remux HE-AAC (mp4a.40.1) → AAC-LC (mp4a.40.2). Returns the
//       (possibly transcoded) segment with correct content-type.
//
// The caller (streaming.js) points kiwi/animepahe streams at this service
// instead of /api/proxy-stream. The service handles everything from there.

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
use std::{collections::HashMap, net::SocketAddr};
use tokio::process::Command;
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info, warn};

// ── Config ────────────────────────────────────────────────────────────────────

/// The public URL of this Railway service (set via Railway env var).
/// Used to rewrite segment URLs in manifests back to ourselves.
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

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/manifest", get(handle_manifest))
        .route("/segment", get(handle_segment))
        .route("/health", get(|| async { "ok" }))
        .layer(cors);

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
    /// Base64-encoded JSON object of extra request headers e.g. {"Referer":"https://kwik.cx/"}
    headers: Option<String>,
    /// Whether to transcode audio (segment endpoint only). "1" = yes.
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
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(false)
        .build()?;

    let mut req = client.get(url);
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
    // The base URL here is the upstream manifest URL (e.g. zaza.animex.one/?u=...)
    // NOT the transcoder's own SERVICE_BASE_URL. Resolve relative hrefs against it.
    if let Ok(base_url) = url::Url::parse(base) {
        if let Ok(resolved) = base_url.join(href) {
            return resolved.to_string();
        }
    }
    // Last resort: treat as absolute
    format!("https:{href}")
}

fn manifest_has_heaac(manifest: &str) -> bool {
    manifest.contains("mp4a.40.1") || manifest.contains("MP4A.40.1")
}

fn self_url(endpoint: &str, target: &str, headers_b64: &str, transcode: bool) -> String {
    let base = service_base_url();
    let encoded = urlencoding::encode(target);
    let t = if transcode { "1" } else { "0" };
    format!("{base}/{endpoint}?url={encoded}&headers={headers_b64}&transcode={t}")
}

// ── /manifest ─────────────────────────────────────────────────────────────────

async fn handle_manifest(Query(params): Query<ProxyParams>) -> Response {
    match proxy_manifest(&params.url, params.headers.as_deref()).await {
        Ok((body, content_type)) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_str(&content_type).unwrap(),
            );
            headers.insert(
                axum::http::header::CACHE_CONTROL,
                HeaderValue::from_static("no-store"),
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

async fn proxy_manifest(url: &str, headers_b64: Option<&str>) -> Result<(String, String)> {
    let extra = decode_headers(headers_b64);
    let resp = upstream_get(url, &extra).await?;
    let body = resp.text().await?;

    if !body.trim_start().starts_with("#EXTM3U") {
        warn!("manifest URL did not return #EXTM3U: {url}");
        return Ok((body, "application/vnd.apple.mpegurl".to_string()));
    }

    let needs_transcode = manifest_has_heaac(&body);
    if needs_transcode {
        info!("HE-AAC detected in manifest — segments will be transcoded");
    }

    // Patch CODECS= declaration so browser opens the right SourceBuffer
    let body = body.replace("mp4a.40.1", "mp4a.40.2");

    let headers_b64_str = headers_b64.unwrap_or("");
    let mut out = String::with_capacity(body.len() + 512);
    let mut next_line_is_stream = false;

    for line in body.lines() {
        let trimmed = line.trim();

        // Track whether the next URL line is a sub-manifest (rendition playlist)
        if trimmed.starts_with("#EXT-X-STREAM-INF") || trimmed.starts_with("#EXT-X-I-FRAME-STREAM-INF") {
            next_line_is_stream = true;
        }

        // Rewrite URI="..." inside tags (EXT-X-KEY, EXT-X-MAP, etc.)
        if trimmed.starts_with('#') && trimmed.contains("URI=\"") {
            let rewritten = rewrite_uri_attrs(line, url, headers_b64_str, needs_transcode);
            out.push_str(&rewritten);
            out.push('\n');
            continue;
        }

        // Rewrite bare segment / sub-manifest lines
        if is_url_line(trimmed) {
            let resolved = resolve_url(url, trimmed);
            // A line is a sub-manifest if:
            //  - preceded by #EXT-X-STREAM-INF, OR
            //  - URL contains .m3u8 or "playlist"
            let is_sub_manifest = next_line_is_stream
                || resolved.contains(".m3u8")
                || resolved.contains("playlist");
            next_line_is_stream = false;

            let endpoint = if is_sub_manifest { "manifest" } else { "segment" };
            let rewritten = self_url(endpoint, &resolved, headers_b64_str, needs_transcode);
            out.push_str(&rewritten);
            out.push('\n');
            continue;
        }

        next_line_is_stream = false;
        out.push_str(line);
        out.push('\n');
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
            // Keys/init segments: never transcode, just proxy
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

async fn handle_segment(Query(params): Query<ProxyParams>) -> Response {
    let transcode = params.transcode.as_deref() == Some("1");
    match proxy_segment(&params.url, params.headers.as_deref(), transcode).await {
        Ok((data, content_type)) => {
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_str(&content_type)
                    .unwrap_or_else(|_| HeaderValue::from_static("video/mp4")),
            );
            headers.insert(
                axum::http::header::CACHE_CONTROL,
                HeaderValue::from_static("no-store"),
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
) -> Result<(Bytes, String)> {
    let extra = decode_headers(headers_b64);
    let resp = upstream_get(url, &extra).await?;
    let data = resp.bytes().await.context("reading segment body")?;

    if !transcode {
        return Ok((data, "video/mp4".to_string()));
    }

    match ffmpeg_transcode(data).await {
        Ok(transcoded) => Ok((transcoded, "video/mp4".to_string())),
        Err(e) => {
            warn!("FFmpeg transcode failed ({e}), returning error");
            Err(e)
        }
    }
}

/// Pipe raw fMP4 bytes through FFmpeg, remuxing audio HE-AAC → AAC-LC.
/// Video stream is copied as-is. Output is fragmented MP4.
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
