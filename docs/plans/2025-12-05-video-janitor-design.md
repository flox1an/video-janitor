# VideoJanitor Design

**Date:** 2025-12-05
**Purpose:** Nostr video event collection, archival, and URL tracking system

## Overview

VideoJanitor is a Rust-based tool that collects Nostr video events from multiple relays, archives them to a target relay, and tracks video URL availability in PostgreSQL.

**Core Capabilities:**
- Fetch video events (kinds: 21, 22, 34235, 34236) from multiple relays
- Forward events to a target relay for backup
- Extract and track video URLs from imeta tags
- Validate URL availability with HTTP checks
- Run as one-shot job or daemon with scheduled intervals

## Architecture

**Pipeline Architecture (3 Stages)**

The system uses a channel-based pipeline with three concurrent stages:

### Stage 1: Event Collection (Relay Fetcher)
- Spawns parallel async tasks per source relay
- Uses `nostr-sdk::Client` for relay communication
- Implements two modes:
  - **Backfill Mode:** First sync - paginate backwards through all history
  - **Update Mode:** Subsequent syncs - fetch only new events since last sync
- Pushes events to `mpsc::channel` → Stage 2
- **Error Handling:** Relay offline/timeout → log warning, continue to next relay

### Stage 2: Event Processing
- Consumes events from Stage 1 channel
- Deduplicates by event ID (check against database)
- Extracts video URLs from `imeta` tags (filters for `video/*` mime types)
- Stores event + URLs in PostgreSQL (single transaction)
- Forwards event to target relay (best-effort)
- Pushes URLs to channel → Stage 3
- **Error Handling:** Database error → abort job (critical)

### Stage 3: URL Validation
- Pool of N concurrent tasks (configurable, default: 10)
- HEAD request per URL with timeout
- Updates database with status, HTTP code, timestamp, error details
- **Error Handling:** Individual URL failures → log only (non-critical)

## Database Schema

### Table: `events`
Stores core Nostr event data.

```sql
CREATE TABLE events (
    event_id TEXT PRIMARY KEY,
    pubkey TEXT NOT NULL,
    kind INTEGER NOT NULL,
    created_at BIGINT NOT NULL,
    content TEXT,
    raw_event JSONB,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    relay_source TEXT NOT NULL
);

CREATE INDEX idx_events_pubkey ON events(pubkey);
CREATE INDEX idx_events_created_at ON events(created_at DESC);
CREATE INDEX idx_events_kind ON events(kind);
```

### Table: `video_urls`
1:N relationship with events. Tracks video URLs and their availability.

```sql
CREATE TABLE video_urls (
    id SERIAL PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    url TEXT NOT NULL,
    url_type TEXT NOT NULL,               -- 'original' or 'derived'
    mime_type TEXT,

    -- Status tracking
    status TEXT NOT NULL DEFAULT 'pending',
    http_status_code SMALLINT,
    last_checked_at TIMESTAMPTZ,
    error_count INTEGER NOT NULL DEFAULT 0,
    last_error_message TEXT,

    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(event_id, url)
);

CREATE INDEX idx_video_urls_event_id ON video_urls(event_id);
CREATE INDEX idx_video_urls_status ON video_urls(status);
CREATE INDEX idx_video_urls_last_checked ON video_urls(last_checked_at);
```

**Status Values:**
- `pending`: Not yet checked
- `available`: HTTP 2xx response
- `not_found`: HTTP 404
- `server_error`: HTTP 5xx
- `timeout`: Request timed out

### Table: `relay_state`
Tracks sync progress per relay. Enables incremental updates.

```sql
CREATE TABLE relay_state (
    relay_url TEXT PRIMARY KEY,
    last_event_timestamp BIGINT NOT NULL,
    last_sync_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    total_events_fetched INTEGER NOT NULL DEFAULT 0
);
```

## IMETA Tag Parsing

Video events contain multiple `imeta` tags, each representing a video variant:

```json
{
  "kind": 34235,
  "tags": [
    ["imeta", "url", "https://cdn.video/file.mp4", "m", "video/mp4", "size", "52428800"],
    ["imeta", "url", "https://cdn.video/file.webm", "m", "video/webm", "size", "41943040"],
    ["imeta", "url", "https://cdn.video/thumb.jpg", "m", "image/jpeg"]
  ]
}
```

**Extraction Logic:**
1. Iterate through all tags with kind `imeta`
2. Parse key-value pairs: `url` → URL, `m` → mime type
3. Filter: only include if mime type starts with `video/`
4. Mark all extracted URLs as `url_type: 'original'`
5. `derived` type reserved for future transcoded/processed variants

## Configuration

**Environment Variables (with .env support):**

```bash
# Database
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/video_janitor

# Relays
SOURCE_RELAYS=wss://relay.damus.io,wss://nos.lol,wss://relay.nostr.band
TARGET_RELAY=wss://localhost:7777

# Job settings
JOB_INTERVAL_HOURS=1
URL_CHECK_CONCURRENCY=10
URL_CHECK_TIMEOUT_SECS=10

# Backfill limits
BACKFILL_BATCH_SIZE=500
BACKFILL_MAX_EVENTS=10000
```

**Configuration Loading:**
- Uses `dotenvy` to load `.env` file (optional)
- Falls back to system environment variables
- Environment variables override `.env` values

**CLI Interface:**
```bash
video-janitor              # One-shot mode (single job run)
video-janitor --daemon     # Service mode (runs every N hours)
```

## State Management & Sync Strategy

### First Sync (Backfill Mode)

When a relay is encountered for the first time:

1. Query events with `until: now()` and `limit: BATCH_SIZE`
2. Process batch and move cursor backward: `until = oldest_event.created_at`
3. Repeat until no more events or hit `BACKFILL_MAX_EVENTS`
4. Save final state to `relay_state` table

**Safety:** `BACKFILL_MAX_EVENTS` prevents infinite loops on massive relays.

### Subsequent Syncs (Update Mode)

When relay has existing state:

1. Query events with `since: last_known_timestamp`
2. Process all new events
3. Update `relay_state` with newest event timestamp

**Efficiency:** Only fetches new events, no re-processing of historical data.

## Error Handling

**Hybrid Strategy:**

**Critical Errors (Abort Job):**
- Database connection failure
- Invalid configuration
- Database transaction errors

**Non-Critical Errors (Log & Continue):**
- Relay timeout/offline
- Target relay forwarding failure
- Individual URL validation errors

**Implementation:**
```rust
#[derive(Debug, thiserror::Error)]
enum VideoJanitorError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Relay error: {0}")]
    Relay(String),

    #[error("URL validation error: {0}")]
    UrlValidation(String),
}
```

## Logging

**Framework:** `tracing` + `tracing-subscriber`

**Log Levels:**
- `ERROR`: Critical failures (DB down, config invalid)
- `WARN`: Non-critical issues (relay timeout, URL 404)
- `INFO`: Job progress (started, completed, statistics)
- `DEBUG`: Detailed operations (each event, each URL check)

**Configuration:** `RUST_LOG` environment variable

**Example Output:**
```
INFO  video_janitor: Job started
INFO  video_janitor: Syncing relay wss://relay.damus.io
DEBUG video_janitor: Fetched 500 events (batch 1)
INFO  video_janitor: Processed 1,234 events, extracted 3,456 URLs
WARN  video_janitor: URL check failed: https://dead.link/video.mp4 (404)
INFO  video_janitor: Job completed in 3m 42s
```

## Technology Stack

**Core Dependencies:**
- `tokio` - Async runtime
- `nostr-sdk` - Nostr protocol implementation
- `sqlx` - PostgreSQL driver with compile-time query checking
- `reqwest` - HTTP client for URL validation
- `tokio-cron-scheduler` - Job scheduling (daemon mode)
- `tracing` / `tracing-subscriber` - Structured logging
- `thiserror` - Error handling
- `serde` / `serde_json` - Serialization
- `dotenvy` - Environment variable loading
- `envy` - Environment → struct deserialization
- `clap` - CLI argument parsing

## Deployment Modes

### Development
```bash
# Create .env file with configuration
echo "DATABASE_URL=postgresql://postgres:postgres@localhost:5432/video_janitor" > .env
echo "SOURCE_RELAYS=wss://relay.damus.io" >> .env
echo "TARGET_RELAY=wss://localhost:7777" >> .env

# Run migrations
sqlx migrate run

# Run one-shot
cargo run

# Run as daemon
cargo run -- --daemon
```

### Production (Docker)
```bash
# Pass env vars directly
docker run -e DATABASE_URL=... -e SOURCE_RELAYS=... video-janitor --daemon
```

### Production (systemd)
```bash
# One-shot via timer
systemctl enable --now video-janitor.timer

# Or as long-running service
systemctl enable --now video-janitor.service
```

## Future Enhancements

Potential additions not in initial scope:

- **Derived URLs:** Track transcoded/processed video variants
- **Thumbnail tracking:** Add support for image/jpeg imeta tags
- **Metrics:** Prometheus exporter for monitoring
- **Rate limiting:** Configurable delays between relay requests
- **Retry logic:** Exponential backoff for failed URL checks
- **Webhook notifications:** Alert on URL availability changes
- **GraphQL API:** Query interface for collected data
