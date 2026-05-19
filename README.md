# VideoJanitor

A Rust-based tool for collecting, archiving, and tracking Nostr video events.

## Features

- **Multi-Relay Collection**: Fetches video events (kinds 21, 22, 34235, 34236) from multiple Nostr relays
- **Event Archival**: Forwards events to a target relay for backup
- **URL Tracking**: Extracts and tracks video URLs from `imeta` tags
- **URL Validation**: Checks video URL availability with HTTP HEAD requests
- **Smart Sync**: Initial backfill + incremental updates (tracks last sync per relay)
- **Flexible Deployment**: Run as one-shot job or daemon with scheduled intervals

## Architecture

VideoJanitor uses a 4-stage pipeline architecture:

1. **Stage 1 - Event Collection**: Parallel fetching from multiple relays
2. **Stage 2 - Event Processing**: Database storage + relay forwarding
3. **Stage 3 - URL Validation**: Concurrent HTTP availability checks
4. **Stage 4 - Filter Generation**: Creates BinaryFuse16 filter of failed events

See [design document](docs/plans/2025-12-05-video-janitor-design.md) for details.

## Prerequisites

- Rust 1.70+
- PostgreSQL 12+
- Access to Nostr relays

## Installation

```bash
git clone <repository>
cd video-janitor
cargo build --release
```

## Configuration

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
```

### Required Variables

```bash
# Database connection
DATABASE_URL=postgresql://user:password@localhost:5432/video_janitor

# Relays (comma-separated)
SOURCE_RELAYS=wss://relay.damus.io,wss://nos.lol,wss://relay.nostr.band
TARGET_RELAY=wss://localhost:7777

# Job settings
JOB_INTERVAL_HOURS=1
URL_CHECK_CONCURRENCY=10
URL_CHECK_TIMEOUT_SECS=10

# Backfill limits
BACKFILL_BATCH_SIZE=500
BACKFILL_MAX_EVENTS=10000

# Logging
RUST_LOG=info
```

## Database Setup

The application automatically runs migrations on startup. Just ensure PostgreSQL is running:

```bash
# Using Docker
docker run --name video-janitor-db \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=video_janitor \
  -p 5432:5432 \
  -d postgres

# Or create database manually
createdb video_janitor
```

## Usage

### One-Shot Mode

Run a single collection cycle:

```bash
cargo run
# or
./target/release/video-janitor
```

### Daemon Mode

Run as a service with scheduled intervals:

```bash
cargo run -- --daemon
# or
./target/release/video-janitor --daemon
```

In daemon mode:
- Initial job runs immediately on startup
- Subsequent jobs run every `JOB_INTERVAL_HOURS` hours
- Press Ctrl+C to gracefully shutdown

## How It Works

### First Run (Backfill)

When a relay is synced for the first time:
1. Paginates backwards through all historical video events
2. Respects `BACKFILL_MAX_EVENTS` safety limit
3. Saves last seen timestamp to `relay_state` table

### Subsequent Runs (Update)

On later runs:
1. Queries only events since last known timestamp
2. Processes new events only
3. Updates relay state with latest timestamp

### URL Status Tracking

Each video URL gets checked and tracked:
- `pending`: Not yet checked
- `available`: HTTP 2xx response
- `not_found`: HTTP 404
- `server_error`: HTTP 5xx
- `timeout`: Request timed out

Additional metadata tracked:
- HTTP status code
- Last check timestamp
- Error count
- Last error message

### Failed Events Filter

After URL validation, Stage 4 generates a BinaryFuse16 probabilistic filter containing event IDs where **all** video URLs have failed. The filter is:
- Serialized using bincode (with serde support from xorf)
- Base64 encoded
- Saved to `failed_events_filter.json` with metadata (event count, timestamp)
- Space-efficient for quick lookups of failed events

## Logging

Set `RUST_LOG` environment variable:

```bash
RUST_LOG=debug cargo run    # Verbose
RUST_LOG=info cargo run     # Default
RUST_LOG=warn cargo run     # Warnings only
```

## Database Schema

### `events`
Stores Nostr video events with metadata.

### `video_urls`
Tracks video URLs extracted from events (1:N relationship).

### `relay_state`
Tracks sync progress per relay for incremental updates.

See [design document](docs/plans/2025-12-05-video-janitor-design.md) for full schema.

## Development

### Run Tests

```bash
cargo test
```

### Check Code

```bash
cargo clippy
cargo fmt
```

## Deployment

### Docker

```dockerfile
FROM rust:1.70 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates
COPY --from=builder /app/target/release/video-janitor /usr/local/bin/
CMD ["video-janitor", "--daemon"]
```

### Systemd Service

One-shot via timer:

```ini
# /etc/systemd/system/video-janitor.service
[Unit]
Description=VideoJanitor - Nostr video event collector
After=network.target postgresql.service

[Service]
Type=oneshot
EnvironmentFile=/etc/video-janitor/config
ExecStart=/usr/local/bin/video-janitor
User=video-janitor
```

```ini
# /etc/systemd/system/video-janitor.timer
[Unit]
Description=Run VideoJanitor hourly

[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target
```

Or as long-running daemon:

```ini
# /etc/systemd/system/video-janitor.service
[Unit]
Description=VideoJanitor Daemon
After=network.target postgresql.service

[Service]
Type=simple
EnvironmentFile=/etc/video-janitor/config
ExecStart=/usr/local/bin/video-janitor --daemon
Restart=on-failure
User=video-janitor

[Install]
WantedBy=multi-user.target
```

## Error Handling

**Critical Errors (Job Aborts):**
- Database connection failure
- Invalid configuration
- Database transaction errors

**Non-Critical Errors (Logged, Job Continues):**
- Individual relay timeouts
- Target relay forwarding failures
- Individual URL validation errors

## License

[Your License Here]

## Contributing

[Your Contributing Guidelines Here]
