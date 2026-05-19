# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

VideoJanitor is a Rust-based Nostr video event collector that fetches video events from multiple relays, archives them to a target relay, validates video URL availability, and generates probabilistic filters of failed events.

**Video Event Kinds:** 21, 22, 34235, 34236

## Development Commands

### Building and Running
```bash
# Build the project
cargo build

# Build release version
cargo build --release

# Run in one-shot mode (single collection cycle)
cargo run

# Run in daemon mode (scheduled intervals)
cargo run -- --daemon

# Run the reprocess_urls utility binary
cargo run --bin reprocess_urls
```

### Testing and Code Quality
```bash
# Run all tests
cargo test

# Run clippy linter
cargo clippy

# Format code
cargo fmt

# Check formatting without modifying files
cargo fmt -- --check
```

### Database Operations
```bash
# Database is automatically migrated on startup
# Migrations are in migrations/001_initial_schema.sql

# To manually run migrations, use sqlx CLI:
# cargo install sqlx-cli
# sqlx migrate run
```

## Architecture

### 4-Stage Pipeline

The system uses a **channel-based pipeline architecture** with four sequential stages:

1. **Stage 1 - Event Collection** (`stage1_collection.rs`)
   - Spawns parallel async tasks per source relay
   - Uses `nostr-sdk::Client` for relay communication
   - Two modes: **Backfill** (first sync, paginate backwards) and **Update** (fetch events since last sync)
   - Sends events to Stage 2 via `mpsc::channel`

2. **Stage 2 - Event Processing** (`stage2_processing.rs`)
   - Consumes events from Stage 1
   - Deduplicates by event ID
   - Extracts video URLs from `imeta` tags (filters for `video/*` mime types)
   - Stores events and URLs in PostgreSQL in a single transaction
   - Forwards events to target relay (best-effort)

3. **Stage 3 - URL Validation** (`stage3_validation.rs`)
   - Runs **after** Stage 1 and 2 complete
   - Pool of N concurrent tasks (configurable via `URL_CHECK_CONCURRENCY`)
   - Makes HEAD requests to check URL availability
   - Updates database with status: `pending`, `available`, `not_found`, `server_error`, `timeout`

4. **Stage 4 - Filter Generation** (`stage4_filter_generation.rs`)
   - Runs **after** Stage 3 completes
   - Generates BinaryFuse16 probabilistic filter containing event IDs where **all** video URLs have failed
   - Serializes using **MessagePack** (binary format), encodes as base64
   - Saves to `failed_events_filter.json`

**Pipeline execution:** Stages 1 and 2 run concurrently. Stage 3 waits for both to complete. Stage 4 waits for Stage 3 to complete.

### Key Modules

- `config.rs` - Configuration loading from environment variables
- `db.rs` - Database connection and query operations
- `parser.rs` - IMETA tag parsing for extracting video URLs
- `pipeline.rs` - Orchestrates the 4-stage pipeline
- `main.rs` - CLI entry point, daemon/one-shot mode handling

### Database Schema

Three main tables:

- **`events`** - Stores Nostr video events with `event_id` (PK), `pubkey`, `kind`, `created_at`, `content`, `raw_event` (JSONB), `first_seen_at`, `relay_source`

- **`video_urls`** - 1:N relationship with events. Tracks `url`, `url_type` (original/derived), `mime_type`, `status`, `http_status_code`, `last_checked_at`, `error_count`, `last_error_message`

- **`relay_state`** - Tracks sync progress per relay: `relay_url` (PK), `last_event_timestamp`, `last_sync_at`, `total_events_fetched`

### IMETA Tag Parsing

Video events contain multiple `imeta` tags representing video variants:

```json
{
  "kind": 34235,
  "tags": [
    ["imeta", "url", "https://cdn.video/file.mp4", "m", "video/mp4", "size", "52428800"],
    ["imeta", "url", "https://cdn.video/file.webm", "m", "video/webm"]
  ]
}
```

The parser extracts key-value pairs and filters for `video/*` mime types only.

## Configuration

All configuration via environment variables (`.env` file supported):

```bash
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/video_janitor
SOURCE_RELAYS=wss://relay.damus.io,wss://nos.lol,wss://relay.nostr.band
TARGET_RELAY=wss://localhost:7777
JOB_INTERVAL_HOURS=1
URL_CHECK_CONCURRENCY=10
URL_CHECK_TIMEOUT_SECS=10
URL_CHECK_DELAY_MS=100
BACKFILL_BATCH_SIZE=500
BACKFILL_MAX_EVENTS=10000
RUST_LOG=info
```

**Configuration loading:** Uses `dotenvy` to load `.env`, falls back to system environment variables.

## Error Handling Strategy

**Critical errors (abort job):**
- Database connection failure
- Invalid configuration
- Database transaction errors

**Non-critical errors (log and continue):**
- Individual relay timeouts
- Target relay forwarding failures
- Individual URL validation errors

## State Management

**First sync (Backfill Mode):**
- Query events with `until: now()`, paginate backwards
- Safety limit: `BACKFILL_MAX_EVENTS` prevents infinite loops
- Save final state to `relay_state`

**Subsequent syncs (Update Mode):**
- Query events with `since: last_known_timestamp` from `relay_state`
- Only fetch and process new events

## Dependencies

Key crates:
- `tokio` - Async runtime
- `nostr-sdk` - Nostr protocol implementation
- `sqlx` - PostgreSQL driver with compile-time checking
- `reqwest` - HTTP client for URL validation
- `tokio-cron-scheduler` - Job scheduling for daemon mode
- `tracing` / `tracing-subscriber` - Structured logging
- `xorf` - BinaryFuse16 probabilistic filter (Stage 4)
- `clap` - CLI argument parsing

## Running Locally

```bash
# 1. Start PostgreSQL
docker run --name video-janitor-db \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=video_janitor \
  -p 5432:5432 \
  -d postgres

# 2. Configure environment
cp .env.example .env
# Edit .env with your relay URLs

# 3. Run (migrations happen automatically)
cargo run
```

## Failed Events Filter (Stage 4 Output)

Stage 4 generates a `failed_events_filter.json` file containing a space-efficient probabilistic filter.

### Filter Structure

```json
{
  "filter_type": "BinaryFuse16",
  "filter_base64": "...",
  "filter_json": {
    "seed": 1234567890,
    "segment_length": 256,
    "segment_length_mask": 255,
    "segment_count_length": 1024,
    "fingerprints": [12345, 67890, ...]
  },
  "event_count": 1234,
  "generated_at": "2025-12-06T12:34:56.789Z"
}
```

### Filter Representations

The output contains **two representations** of the same BinaryFuse16 filter:

**1. Binary (Compact) - `filter_base64` field:**

The `filter_base64` field contains a bincode-serialized BinaryFuse16 filter:

1. **Event ID to Hash Conversion:**
   - Event IDs (hex strings) are hashed using `DefaultHasher` to produce `u64` values
   - These hashes are inserted into the BinaryFuse16 filter

2. **Serialization:**
   - The BinaryFuse16 filter structure is serialized using **bincode**
   - The serialized bytes are then base64-encoded for JSON compatibility
   - **Most compact representation** - use this for storage or transmission

**2. Plain JSON - `filter_json` field:**

The `filter_json` field contains the filter's complete internal structure as plain JSON:
- `seed`: 64-bit unsigned integer used for hash randomization
- `segment_length`: Length of each segment in the filter
- `segment_length_mask`: Bit mask derived from segment_length (typically segment_length - 1)
- `segment_count_length`: Combined segment count and length parameter
- `fingerprints`: Array of 16-bit unsigned integers representing the filter's fingerprint array
- **Human-readable** and easier to work with in environments without binary serialization
- **Larger size** than the base64 version but can be used directly in JSON-based systems
- **Complete reconstruction**: Contains ALL fields needed to fully reconstruct a working BinaryFuse16 filter

### Filter Properties

- **False positive rate:** ~0.4% (BinaryFuse16 characteristic)
- **No false negatives:** If an event ID is in the set, the filter will always match
- **Space efficient:** ~18 bits per entry (much smaller than storing full event IDs)

### Using the Filter

**Option 1: Using the binary (base64) representation:**

```rust
// 1. Decode base64
let filter_bytes = BASE64.decode(&filter_base64)?;

// 2. Deserialize with bincode
let filter: BinaryFuse16 = bincode::deserialize(&filter_bytes)?;

// 3. Hash the event ID
let mut hasher = DefaultHasher::new();
event_id.hash(&mut hasher);
let hash = hasher.finish();

// 4. Check filter membership
if filter.contains(&hash) {
    // This event likely has all URLs failed (0.4% false positive rate)
}
```

**Option 2: Using the JSON representation:**

```rust
use serde_json::json;

// 1. Parse the JSON and reconstruct the filter
let filter_value = json!({
    "seed": filter_json.seed,
    "segment_length": filter_json.segment_length,
    "segment_length_mask": filter_json.segment_length_mask,
    "segment_count_length": filter_json.segment_count_length,
    "fingerprints": filter_json.fingerprints
});

// 2. Deserialize back into BinaryFuse16
let filter: BinaryFuse16 = serde_json::from_value(filter_value)?;

// 3. Use the filter normally
let mut hasher = DefaultHasher::new();
event_id.hash(&mut hasher);
let hash = hasher.finish();

if filter.contains(&hash) {
    // This event likely has all URLs failed (0.4% false positive rate)
}
```

The JSON representation contains all necessary fields (seed, segment parameters, and fingerprints) to fully reconstruct a working BinaryFuse16 filter.

## Logging

Controlled via `RUST_LOG` environment variable:
- `RUST_LOG=debug` - Verbose (each event, each URL check)
- `RUST_LOG=info` - Default (job progress, statistics)
- `RUST_LOG=warn` - Warnings only (relay timeouts, URL 404s)
