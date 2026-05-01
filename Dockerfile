# ── Build stage ───────────────────────────────────────────────────────────────
FROM rust:1.86-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM debian:bookworm AS runtime

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/hls-transcoder /app/hls-transcoder

ENV PORT=3001
ENV RUST_LOG=hls_transcoder=info

EXPOSE 3001

CMD ["/app/hls-transcoder"]
