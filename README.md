# hls-transcoder

A lightweight Rust + FFmpeg service that proxies HLS streams and transcodes HE-AAC (`mp4a.40.1`) audio to AAC-LC (`mp4a.40.2`) on the fly — enabling playback on browsers that don't support HE-AAC via MSE (notably Firefox).

## How it works

```
Client → GET /manifest?url=<m3u8 url>&headers=<b64 json>
  → fetches the HLS manifest from the upstream CDN
  → rewrites all segment/sub-manifest URLs to route through this service
  → patches mp4a.40.1 → mp4a.40.2 in CODECS= declarations
  → returns the patched M3U8

Client → GET /segment?url=<segment url>&headers=<b64 json>&transcode=1
  → fetches the fMP4 segment from the upstream CDN
  → pipes through FFmpeg: -c:v copy -c:a aac -profile:a aac_low
  → returns a remuxed fMP4 with AAC-LC audio
```

## Endpoints

| Endpoint | Params | Description |
|----------|--------|-------------|
| `GET /manifest` | `url`, `headers` | Proxy + rewrite an HLS manifest |
| `GET /segment` | `url`, `headers`, `transcode` | Proxy (and optionally transcode) a segment |
| `GET /health` | — | Health check, returns `ok` |

**`headers`** — base64-encoded JSON object of request headers to forward upstream, e.g. `btoa(JSON.stringify({ Referer: "https://example.com/" }))`.

**`transcode`** — `1` to transcode audio via FFmpeg, `0` to pass through raw (default `0`).

## Running locally

Requires [Rust](https://rustup.rs) and [FFmpeg](https://ffmpeg.org/download.html) on your PATH.

```bash
git clone https://github.com/Flowrty/hls-transcoder
cd hls-transcoder
cargo run
# Server listening on http://localhost:3001
curl http://localhost:3001/health  # → ok
```

## Deploying to Render

1. Fork or clone this repo and push to GitHub
2. [Render](https://render.com) → New → Web Service → connect your repo
3. Runtime is auto-detected as **Docker** via `render.yaml`
4. Set the environment variable `SERVICE_BASE_URL` to your Render service URL (e.g. `https://hls-transcoder.onrender.com`)
5. Deploy

> **Free tier note:** Render's free tier spins down after 15 minutes of inactivity. To prevent cold starts, set up a free [UptimeRobot](https://uptimerobot.com) monitor pinging `/health` every 5 minutes.

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVICE_BASE_URL` | `http://localhost:3001` | Public URL of this service — used to rewrite segment URLs in manifests |
| `PORT` | `3001` | Port to listen on |
| `RUST_LOG` | `hls_transcoder=info` | Log level |

## Tech stack

- [Axum](https://github.com/tokio-rs/axum) — async HTTP server
- [Reqwest](https://github.com/seanmonstar/reqwest) — upstream HTTP client
- [Tokio](https://tokio.rs) — async runtime
- [FFmpeg](https://ffmpeg.org) — audio transcoding

## License

MIT
