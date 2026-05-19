# Design: Related Events, Delete Propagation & Publication Logging

**Date:** 2026-05-19  
**Status:** Approved

## Overview

Extend VideoJanitor with three capabilities:

1. **Related Events Collection (Pipeline B)** — fetch reactions, comments, notes, delete events, zap requests/receipts for known video events from source relays; store and publish to target relays.
2. **Delete Propagation** — kind:5 delete events are broadcast to ALL known relays (source + target) to maximize deletion coverage.
3. **Publication & Sighting Logging** — track where every event was seen (source relays) and where it was published (target relays) in the database.

## Database Schema

### Migration 002: `relay_state` → `relays` (Master Relay Table)

`relay_state` is replaced by `relays`, which becomes the canonical registry for all known relays (source and target). A surrogate `id` key is used for all FK references.

```sql
CREATE TABLE relays (
    id          SERIAL PRIMARY KEY,
    relay_url   TEXT UNIQUE NOT NULL,
    relay_type  TEXT NOT NULL CHECK (relay_type IN ('source', 'target', 'both')),
    -- Sync state: only populated for source/both relays
    last_event_timestamp  BIGINT,
    last_sync_at          TIMESTAMPTZ,
    total_events_fetched  INTEGER NOT NULL DEFAULT 0
);

INSERT INTO relays (relay_url, relay_type, last_event_timestamp, last_sync_at, total_events_fetched)
SELECT relay_url, 'source', last_event_timestamp, last_sync_at, total_events_fetched
FROM relay_state;

DROP TABLE relay_state;
```

The `relay_source TEXT` column in `events` is removed (it contained only `"unknown"`, no data loss).

### Migration 003: New Tables

```sql
-- Links related events back to their video event
CREATE TABLE event_relations (
    related_event_id  TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    video_event_id    TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    relation_type     TEXT NOT NULL CHECK (relation_type IN (
                        'reaction', 'comment', 'note', 'delete',
                        'zap_request', 'zap_receipt'
                      )),
    PRIMARY KEY (related_event_id, video_event_id)
);
CREATE INDEX idx_event_relations_video_id ON event_relations(video_event_id);

-- Where we have seen an event (source relays)
CREATE TABLE event_sightings (
    event_id      TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    relay_id      INTEGER NOT NULL REFERENCES relays(id),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, relay_id)
);

-- Where we have published an event (target relays)
CREATE TABLE relay_publications (
    event_id     TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    relay_id     INTEGER NOT NULL REFERENCES relays(id),
    published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, relay_id)
);
CREATE INDEX idx_relay_publications_relay_id ON relay_publications(relay_id);
```

## Pipeline Architecture

### Pipeline A (existing, Stage 1–4) — extended

Stages 1–4 remain structurally unchanged. Two modifications:

- **Stage 1 → Stage 2 channel** changes from `mpsc::Sender<Event>` to `mpsc::Sender<(Event, String)>` to carry the source relay URL alongside each event.
- **Stage 2** gains publication logging: target relays are upserted into `relays` on startup; after each successful `send_event()` a row is written to `relay_publications`; `event_sightings` is populated from the relay URL carried through the channel.

### Pipeline B (new, Stage 5)

Runs after Stage 4 completes. Fetches all events related to known video events.

```
Pipeline A:  Stage1 → Stage2 → Stage3 → Stage4
                                               ↓
Pipeline B:                              Stage5 (Related Events)
```

**Stage 5 steps:**

1. Connect to all source relays.
2. Load video event IDs from `events` (kinds 21, 22, 34235, 34236) in pages of `RELATED_EVENTS_BATCH_SIZE`.
3. For each batch, query source relays with:
   - `kinds: [1, 5, 7, 1111, 9734, 9735]`
   - `#e: [video_event_ids]`
4. For each returned event:
   - Skip if already in DB (`event_exists()`).
   - Insert into `events` + `event_relations` (with appropriate `relation_type`).
   - Insert `event_sightings` row.
5. Publish all collected events to target relays; write `relay_publications` rows.
6. For kind:5 events: additionally broadcast to **all** source relays.

Stage 5 is fully idempotent — every insert uses `ON CONFLICT DO NOTHING`.

## Relation Type Mapping

| Nostr kind | `relation_type`  |
|------------|-----------------|
| 1          | `note`          |
| 5          | `delete`        |
| 7          | `reaction`      |
| 1111       | `comment`       |
| 9734       | `zap_request`   |
| 9735       | `zap_receipt`   |

## Configuration

One new optional environment variable:

```bash
RELATED_EVENTS_BATCH_SIZE=100   # Video event IDs per relay query (default: 100)
```

All other configuration (source/target relays, concurrency, timeouts) is reused from the existing `Config` struct.

## Module Structure

### New files

| File | Purpose |
|------|---------|
| `src/stage5_related_events.rs` | Pipeline B implementation |
| `migrations/002_relays_master_table.sql` | relay_state → relays migration |
| `migrations/003_related_events.sql` | event_relations, event_sightings, relay_publications |

### Changed files

| File | Changes |
|------|---------|
| `src/db.rs` | `upsert_relay()`, `insert_event_sighting()`, `insert_publication()`, `is_published()`, `insert_related_event()`, `get_video_event_ids_paginated()`, relay_state queries → relays |
| `src/stage1_collection.rs` | Channel type: `Event` → `(Event, String)` |
| `src/stage2_processing.rs` | Upsert target relays, log sightings + publications |
| `src/pipeline.rs` | Append Stage 5 after Stage 4 |
| `src/config.rs` | Add `related_events_batch_size: usize` |

## Error Handling

### Critical (abort pipeline)
- DB connection failure
- Schema migration mismatch
- No video events in DB (Stage 5 logs a warning and exits cleanly — not an error)

### Non-critical (log and continue)
- Individual source relay timeout during related events fetch
- `send_event()` failure on a target relay → no `relay_publications` entry written → next run retries automatically
- kind:5 broadcast failure on an individual relay → logged, no abort

### Retry Strategy

No retry framework. **Idempotency is the retry mechanism**: a missing `relay_publications` entry means the next pipeline run will attempt publication again. This keeps the code simple and correct.

## Idempotency Guarantees

| Operation | Mechanism |
|-----------|-----------|
| Event already in DB | `event_exists()` check → skip |
| Sighting already recorded | `ON CONFLICT DO NOTHING` |
| Publication already logged | `ON CONFLICT DO NOTHING` |
| Relation already exists | `PRIMARY KEY` conflict → ignored |

## Stage 1 → Stage 2 Channel Note

When multiple source relays return the same event concurrently, the first arrival is recorded as the sighting. Subsequent arrivals for the same `(event_id, relay_id)` pair are silently dropped via `ON CONFLICT DO NOTHING`. This is intentional — `event_sightings` records "seen on", not "first and only seen on".
