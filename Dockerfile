# ── Build stage ───────────────────────────────────────────────────────────────
FROM rust:1.78-slim-bookworm AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Cache dependencies first
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs
RUN cargo build --release
RUN rm src/main.rs

# Build the real binary
COPY src ./src
RUN touch src/main.rs && cargo build --release

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

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
