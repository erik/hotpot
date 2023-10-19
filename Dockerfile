FROM rust:1-slim-buster as builder

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

FROM debian:buster-slim

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

ENV STRAVA_CLIENT_ID=unset
ENV STRAVA_CLIENT_SECRET=unset
ENV STRAVA_WEBHOOK_SECRET=unset

# ENV HOTPOT_UPLOAD_TOKEN=unset