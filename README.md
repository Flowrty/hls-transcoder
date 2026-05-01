# hls-transcoder

Rust + FFmpeg service that proxies HLS streams and transcodes HE-AAC (mp4a.40.1) → AAC-LC (mp4a.40.2) on the fly. Fixes kiwi/animepahe playback on all browsers.

## Local dev

```bash
# Requires FFmpeg on PATH
cargo run
# → http://localhost:3001/health
```

## Deploy to Render

1. Push this folder to a GitHub repo
2. Render → New → Web Service → connect repo
3. Runtime: Docker (auto-detected from render.yaml)
4. Add env var: `SERVICE_BASE_URL=https://<your-name>.onrender.com`
5. Deploy

### Prevent cold starts (free tier sleeps after 15min)
Set up a free UptimeRobot monitor:
- URL: `https://<your-name>.onrender.com/health`
- Interval: every 5 minutes
- This keeps the container warm so HLS.js never hits a cold start mid-stream.

## Wire into HighBludsTV

In `streaming.js`, once deployed, the kiwi path in `getMiruroStream` will be
updated to route through this transcoder instead of `/api/proxy-stream`.

## How it works

```
Browser → /manifest?url=<kiwi m3u8>&headers=<b64>
  → fetches manifest from kiwi CDN
  → rewrites all segment URLs to point to /segment on this service
  → patches mp4a.40.1 → mp4a.40.2 in CODECS= declarations
  → returns patched M3U8

Browser → /segment?url=<segment>&headers=<b64>&transcode=1
  → fetches fMP4 segment from kiwi CDN
  → pipes through FFmpeg: -c:v copy -c:a aac -profile:a aac_low
  → returns remuxed fMP4 with AAC-LC audio
```
