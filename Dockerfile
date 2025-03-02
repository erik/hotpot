FROM rust:1-slim-bookworm as builder

WORKDIR /build

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    libssl-dev \
    libsqlite3-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

FROM debian:bookworm-slim

# Debian slim images don't have certs available.
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libsqlite3-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/hotpot /usr/local/bin/hotpot

ENTRYPOINT ["hotpot"]
CMD ["--db", "/data/hotpot.sqlite3", "serve", "--host", "0.0.0.0", "--strava-webhook"]

EXPOSE 8080

# REQUIRED (--strava-webhook)
#  - ENV STRAVA_CLIENT_ID      1234567890
#  - ENV STRAVA_CLIENT_SECRET  abc123
#  - ENV STRAVA_WEBHOOK_SECRET xyz123
#
# OPTIONAL (--upload)
#  - ENV HOTPOT_UPLOAD_TOKEN   unset
