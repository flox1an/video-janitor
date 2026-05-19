# Related Events, Delete Propagation & Publication Logging — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend VideoJanitor with related-event collection (reactions, comments, notes, zaps, deletes), delete broadcasting to all relays, and full publication/sighting logging in the database.

**Architecture:** Migrations 002 and 003 extend the schema (relay master table, event_relations, event_sightings, relay_publications). The existing pipeline gains publication logging in Stage 2 and a new Stage 5 that fetches related events for all known video events in a separate post-pipeline pass. Stage 1's channel carries the source relay URL so Stage 2 can log sightings.

**Tech Stack:** Rust, sqlx 0.8, nostr-sdk 0.35, PostgreSQL, tokio

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `migrations/002_relays_master_table.sql` | relay_state → relays, drop relay_source column |
| Create | `migrations/003_related_events.sql` | event_relations, event_sightings, relay_publications |
| Create | `src/stage5_related_events.rs` | Pipeline B: fetch + store + publish related events |
| Modify | `src/db.rs` | Relay/sighting/publication/relation queries, remove relay_source |
| Modify | `src/stage1_collection.rs` | Channel type: `Event` → `(Event, String)` |
| Modify | `src/stage2_processing.rs` | Sighting + publication logging |
| Modify | `src/config.rs` | Add `related_events_batch_size` |
| Modify | `src/pipeline.rs` | Append Stage 5 |

---

## Task 1: Migration 002 — relay_state → relays

**Files:**
- Create: `migrations/002_relays_master_table.sql`

- [ ] **Step 1: Write the migration file**

```sql
-- 002_relays_master_table.sql
-- Replaces relay_state with a general relays master table.
-- Idempotent: wrapped in DO blocks that check for existence before acting.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM information_schema.tables WHERE table_name = 'relays'
    ) THEN
        CREATE TABLE relays (
            id          SERIAL PRIMARY KEY,
            relay_url   TEXT UNIQUE NOT NULL,
            relay_type  TEXT NOT NULL CHECK (relay_type IN ('source', 'target', 'both')),
            last_event_timestamp  BIGINT,
            last_sync_at          TIMESTAMPTZ,
            total_events_fetched  INTEGER NOT NULL DEFAULT 0
        );

        IF EXISTS (
            SELECT FROM information_schema.tables WHERE table_name = 'relay_state'
        ) THEN
            INSERT INTO relays (relay_url, relay_type, last_event_timestamp, last_sync_at, total_events_fetched)
            SELECT relay_url, 'source', last_event_timestamp, last_sync_at, total_events_fetched
            FROM relay_state;

            DROP TABLE relay_state;
        END IF;
    END IF;
END
$$;

-- Remove relay_source column from events (contained only "unknown", no data loss)
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM information_schema.columns
        WHERE table_name = 'events' AND column_name = 'relay_source'
    ) THEN
        ALTER TABLE events DROP COLUMN relay_source;
    END IF;
END
$$;
```

- [ ] **Step 2: Verify migration file exists**

```bash
ls migrations/
```
Expected: `001_initial_schema.sql  002_relays_master_table.sql`

- [ ] **Step 3: Commit**

```bash
git add migrations/002_relays_master_table.sql
git commit -m "feat: migration 002 — relay_state to relays master table"
```

---

## Task 2: Migration 003 — New Tables

**Files:**
- Create: `migrations/003_related_events.sql`

- [ ] **Step 1: Write the migration file**

```sql
-- 003_related_events.sql

CREATE TABLE IF NOT EXISTS event_relations (
    related_event_id  TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    video_event_id    TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    relation_type     TEXT NOT NULL CHECK (relation_type IN (
                        'reaction', 'comment', 'note', 'delete',
                        'zap_request', 'zap_receipt'
                      )),
    PRIMARY KEY (related_event_id, video_event_id)
);

CREATE INDEX IF NOT EXISTS idx_event_relations_video_id ON event_relations(video_event_id);

CREATE TABLE IF NOT EXISTS event_sightings (
    event_id      TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    relay_id      INTEGER NOT NULL REFERENCES relays(id),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, relay_id)
);

CREATE TABLE IF NOT EXISTS relay_publications (
    event_id     TEXT NOT NULL REFERENCES events(event_id) ON DELETE CASCADE,
    relay_id     INTEGER NOT NULL REFERENCES relays(id),
    published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, relay_id)
);

CREATE INDEX IF NOT EXISTS idx_relay_publications_relay_id ON relay_publications(relay_id);
```

- [ ] **Step 2: Verify migration file exists**

```bash
ls migrations/
```
Expected: three files: `001_initial_schema.sql  002_relays_master_table.sql  003_related_events.sql`

- [ ] **Step 3: Commit**

```bash
git add migrations/003_related_events.sql
git commit -m "feat: migration 003 — event_relations, event_sightings, relay_publications"
```

---

## Task 3: Update db.rs — Relay Operations

**Files:**
- Modify: `src/db.rs`

All changes in this task are in `src/db.rs`. The `RelayState` struct gains an `id` field. Relay-state queries are rewritten to use `relays`. A new `upsert_relay()` function returns the relay's surrogate id.

- [ ] **Step 1: Update `run_migrations` to run all three migrations**

Replace the existing `run_migrations` method (lines 44–52) with:

```rust
pub async fn run_migrations(&self) -> Result<(), DatabaseError> {
    let m001 = include_str!("../migrations/001_initial_schema.sql");
    let m002 = include_str!("../migrations/002_relays_master_table.sql");
    let m003 = include_str!("../migrations/003_related_events.sql");

    sqlx::raw_sql(m001).execute(&self.pool).await.ok();
    sqlx::raw_sql(m002).execute(&self.pool).await.ok();
    sqlx::raw_sql(m003).execute(&self.pool).await.ok();

    Ok(())
}
```

- [ ] **Step 2: Add `id` field to `RelayState` struct**

Replace the existing `RelayState` struct (lines 27–31):

```rust
#[derive(Debug, Clone)]
pub struct RelayState {
    pub id: i32,
    pub relay_url: String,
    pub last_event_timestamp: i64,
    pub total_events_fetched: i32,
}
```

- [ ] **Step 3: Update `get_relay_state` to query `relays` table**

Replace the existing `get_relay_state` method:

```rust
pub async fn get_relay_state(
    &self,
    relay_url: &str,
) -> Result<Option<RelayState>, DatabaseError> {
    let result = sqlx::query_as::<_, (i32, String, i64, i32)>(
        r#"
        SELECT id, relay_url, last_event_timestamp, total_events_fetched
        FROM relays
        WHERE relay_url = $1 AND last_event_timestamp IS NOT NULL
        "#,
    )
    .bind(relay_url)
    .fetch_optional(&self.pool)
    .await?;

    Ok(result.map(|(id, relay_url, last_event_timestamp, total_events_fetched)| RelayState {
        id,
        relay_url,
        last_event_timestamp,
        total_events_fetched,
    }))
}
```

- [ ] **Step 4: Update `upsert_relay_state` to write to `relays` table**

Replace the existing `upsert_relay_state` method:

```rust
pub async fn upsert_relay_state(
    &self,
    relay_url: &str,
    last_event_timestamp: i64,
    events_count: i32,
) -> Result<(), DatabaseError> {
    sqlx::query(
        r#"
        INSERT INTO relays (relay_url, relay_type, last_event_timestamp, last_sync_at, total_events_fetched)
        VALUES ($1, 'source', $2, NOW(), $3)
        ON CONFLICT (relay_url) DO UPDATE
        SET last_event_timestamp = EXCLUDED.last_event_timestamp,
            last_sync_at = NOW(),
            total_events_fetched = relays.total_events_fetched + $3,
            relay_type = CASE
                WHEN relays.relay_type = 'target' THEN 'both'
                ELSE relays.relay_type
            END
        "#,
    )
    .bind(relay_url)
    .bind(last_event_timestamp)
    .bind(events_count)
    .execute(&self.pool)
    .await?;

    Ok(())
}
```

- [ ] **Step 5: Add `upsert_relay` — ensures any relay exists, returns its id**

Add this method after `upsert_relay_state`:

```rust
pub async fn upsert_relay(&self, relay_url: &str, relay_type: &str) -> Result<i32, DatabaseError> {
    let result: (i32,) = sqlx::query_as(
        r#"
        INSERT INTO relays (relay_url, relay_type)
        VALUES ($1, $2)
        ON CONFLICT (relay_url) DO UPDATE
        SET relay_type = CASE
            WHEN relays.relay_type = EXCLUDED.relay_type THEN relays.relay_type
            ELSE 'both'
        END
        RETURNING id
        "#,
    )
    .bind(relay_url)
    .bind(relay_type)
    .fetch_one(&self.pool)
    .await?;

    Ok(result.0)
}
```

- [ ] **Step 6: Build to verify no compile errors**

```bash
cargo build 2>&1 | head -40
```
Expected: may see errors about `relay_source` in `insert_event` — those are fixed in the next task.

- [ ] **Step 7: Commit**

```bash
git add src/db.rs
git commit -m "feat: update db relay operations to use relays master table"
```

---

## Task 4: Update db.rs — Sightings, Publications, Relations, Event Insert

**Files:**
- Modify: `src/db.rs`

- [ ] **Step 1: Remove `relay_source` from `insert_event`**

Replace the existing `insert_event` method:

```rust
pub async fn insert_event(
    &self,
    event_id: &str,
    pubkey: &str,
    kind: i32,
    created_at: i64,
    content: Option<&str>,
    raw_event: &serde_json::Value,
) -> Result<(), DatabaseError> {
    sqlx::query(
        r#"
        INSERT INTO events (event_id, pubkey, kind, created_at, content, raw_event)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (event_id) DO NOTHING
        "#,
    )
    .bind(event_id)
    .bind(pubkey)
    .bind(kind)
    .bind(created_at)
    .bind(content)
    .bind(raw_event)
    .execute(&self.pool)
    .await?;

    Ok(())
}
```

- [ ] **Step 2: Add `insert_event_sighting`**

Add after `insert_event`:

```rust
pub async fn insert_event_sighting(
    &self,
    event_id: &str,
    relay_id: i32,
) -> Result<(), DatabaseError> {
    sqlx::query(
        r#"
        INSERT INTO event_sightings (event_id, relay_id)
        VALUES ($1, $2)
        ON CONFLICT (event_id, relay_id) DO NOTHING
        "#,
    )
    .bind(event_id)
    .bind(relay_id)
    .execute(&self.pool)
    .await?;

    Ok(())
}
```

- [ ] **Step 3: Add `insert_publication`**

```rust
pub async fn insert_publication(
    &self,
    event_id: &str,
    relay_id: i32,
) -> Result<(), DatabaseError> {
    sqlx::query(
        r#"
        INSERT INTO relay_publications (event_id, relay_id)
        VALUES ($1, $2)
        ON CONFLICT (event_id, relay_id) DO NOTHING
        "#,
    )
    .bind(event_id)
    .bind(relay_id)
    .execute(&self.pool)
    .await?;

    Ok(())
}
```

- [ ] **Step 4: Add `insert_event_relation`**

```rust
pub async fn insert_event_relation(
    &self,
    related_event_id: &str,
    video_event_id: &str,
    relation_type: &str,
) -> Result<(), DatabaseError> {
    sqlx::query(
        r#"
        INSERT INTO event_relations (related_event_id, video_event_id, relation_type)
        VALUES ($1, $2, $3)
        ON CONFLICT (related_event_id, video_event_id) DO NOTHING
        "#,
    )
    .bind(related_event_id)
    .bind(video_event_id)
    .bind(relation_type)
    .execute(&self.pool)
    .await?;

    Ok(())
}
```

- [ ] **Step 5: Add `get_video_event_ids_paginated`**

```rust
pub async fn get_video_event_ids_paginated(
    &self,
    limit: i64,
    offset: i64,
) -> Result<Vec<String>, DatabaseError> {
    let results = sqlx::query_as::<_, (String,)>(
        r#"
        SELECT event_id FROM events
        WHERE kind IN (21, 22, 34235, 34236)
        ORDER BY created_at ASC
        LIMIT $1 OFFSET $2
        "#,
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(&self.pool)
    .await?;

    Ok(results.into_iter().map(|(id,)| id).collect())
}
```

- [ ] **Step 6: Build to verify no compile errors (expect errors from callers of old `insert_event` signature)**

```bash
cargo build 2>&1 | head -40
```

- [ ] **Step 7: Commit**

```bash
git add src/db.rs
git commit -m "feat: add sighting, publication, relation db operations; remove relay_source from insert_event"
```

---

## Task 5: Update Stage 1 — Channel Carries Relay URL

**Files:**
- Modify: `src/stage1_collection.rs`

The channel changes from `mpsc::Sender<Event>` to `mpsc::Sender<(Event, String)>`. Every `event_tx.send(event)` becomes `event_tx.send((event, relay_url.to_string()))`.

- [ ] **Step 1: Update `run` function signature**

In `src/stage1_collection.rs`, change line 18:
```rust
// Before:
    event_tx: mpsc::Sender<Event>,
// After:
    event_tx: mpsc::Sender<(Event, String)>,
```

- [ ] **Step 2: Update `sync_relay` signature and calls**

Change line 85:
```rust
// Before:
    event_tx: mpsc::Sender<Event>,
// After:
    event_tx: mpsc::Sender<(Event, String)>,
```

- [ ] **Step 3: Update `backfill_relay` signature**

Change line 114:
```rust
// Before:
    event_tx: mpsc::Sender<Event>,
// After:
    event_tx: mpsc::Sender<(Event, String)>,
```

- [ ] **Step 4: Update send call in `backfill_relay`**

In `backfill_relay`, find the `event_tx.send(event)` call (around line 172) and replace:
```rust
// Before:
            if let Err(e) = event_tx.send(event).await {
// After:
            if let Err(e) = event_tx.send((event, relay_url.to_string())).await {
```

- [ ] **Step 5: Update `fetch_new_events` signature**

Change line 204:
```rust
// Before:
    event_tx: mpsc::Sender<Event>,
// After:
    event_tx: mpsc::Sender<(Event, String)>,
```

- [ ] **Step 6: Update send call in `fetch_new_events`**

Find `event_tx.send(event)` in `fetch_new_events` (around line 239) and replace:
```rust
// Before:
        if let Err(e) = event_tx.send(event).await {
// After:
        if let Err(e) = event_tx.send((event, relay_url.to_string())).await {
```

- [ ] **Step 7: Build to verify**

```bash
cargo build 2>&1 | head -40
```
Expected: errors in `pipeline.rs` (channel type mismatch) and `stage2_processing.rs` (destructure) — fixed in next tasks.

- [ ] **Step 8: Commit**

```bash
git add src/stage1_collection.rs
git commit -m "feat: stage1 channel carries source relay URL alongside event"
```

---

## Task 6: Update Stage 2 — Sighting + Publication Logging

**Files:**
- Modify: `src/stage2_processing.rs`

Stage 2 now receives `(Event, String)` tuples, upserts target relays on startup to get their IDs, records a sighting per event, and records a publication per target relay that accepts the event.

- [ ] **Step 1: Replace `src/stage2_processing.rs` entirely**

```rust
use crate::config::Config;
use crate::db::Database;
use crate::parser;
use nostr_sdk::{Client, Event};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

pub async fn run(
    config: Config,
    db: Database,
    mut event_rx: mpsc::Receiver<(Event, String)>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Stage 2: Starting event processing");

    // Register target relays in the relays table and cache their IDs
    let mut target_relay_ids: HashMap<String, i32> = HashMap::new();
    for relay_url in &config.target_relays {
        match db.upsert_relay(relay_url, "target").await {
            Ok(id) => {
                let key = relay_url.trim_end_matches('/').to_string();
                target_relay_ids.insert(key, id);
            }
            Err(e) => warn!("Failed to upsert target relay {}: {}", relay_url, e),
        }
    }

    info!(
        "Stage 2: Connecting to {} target relay(s)",
        config.target_relays.len()
    );
    let target_client = Client::default();
    for relay in &config.target_relays {
        target_client.add_relay(relay).await?;
    }
    target_client.connect().await;

    // Cache source relay IDs to avoid a DB round-trip per event
    let mut source_relay_id_cache: HashMap<String, i32> = HashMap::new();

    let mut processed_count = 0;
    let mut skipped_count = 0;
    let mut url_count = 0;

    while let Some((event, source_relay_url)) = event_rx.recv().await {
        debug!("Processing event: {}", event.id);

        if db.event_exists(&event.id.to_hex()).await? {
            debug!("Event {} already exists, skipping", event.id);
            skipped_count += 1;
            continue;
        }

        let urls = parser::extract_video_urls(&event);

        let event_id = event.id.to_hex();
        let pubkey = event.pubkey.to_hex();
        let kind = event.kind.as_u16() as i32;
        let created_at = event.created_at.as_u64() as i64;
        let content = if event.content.is_empty() {
            None
        } else {
            Some(event.content.as_str())
        };
        let raw_event = serde_json::to_value(&event)?;

        db.insert_event(&event_id, &pubkey, kind, created_at, content, &raw_event)
            .await?;

        // Record where we saw this event
        let source_relay_id = if let Some(&id) = source_relay_id_cache.get(&source_relay_url) {
            id
        } else {
            match db.upsert_relay(&source_relay_url, "source").await {
                Ok(id) => {
                    source_relay_id_cache.insert(source_relay_url.clone(), id);
                    id
                }
                Err(e) => {
                    warn!("Failed to upsert source relay {}: {}", source_relay_url, e);
                    -1
                }
            }
        };
        if source_relay_id >= 0 {
            db.insert_event_sighting(&event_id, source_relay_id)
                .await
                .ok();
        }

        for url in urls {
            match db.insert_video_url(&url).await {
                Ok(id) => {
                    debug!("Inserted URL: {} (id={})", url.url, id);
                    url_count += 1;
                }
                Err(e) => {
                    warn!("Failed to insert URL {}: {}", url.url, e);
                }
            }
        }

        debug!(
            "Forwarding event {} (kind {}) to target relays",
            event.id,
            event.kind.as_u16()
        );
        match target_client.send_event(event.clone()).await {
            Ok(output) => {
                for relay_url in &output.success {
                    let url_str = relay_url.to_string();
                    let url_key = url_str.trim_end_matches('/');
                    if let Some(&relay_id) = target_relay_ids.get(url_key) {
                        db.insert_publication(&event_id, relay_id).await.ok();
                    }
                }
                debug!("Forwarded event {} to {} relay(s)", event.id, output.success.len());
            }
            Err(e) => {
                warn!("Failed to forward event {} to target relays: {}", event.id, e);
            }
        }

        processed_count += 1;
        if processed_count % 25 == 0 {
            info!(
                "Stage 2: {} new events stored ({} skipped, {} URLs so far)",
                processed_count, skipped_count, url_count
            );
        }
    }

    target_client.disconnect().await?;

    info!(
        "Stage 2: Event processing completed — {} processed, {} skipped, {} URLs extracted",
        processed_count, skipped_count, url_count
    );

    Ok(())
}
```

- [ ] **Step 2: Build to verify**

```bash
cargo build 2>&1 | head -40
```
Expected: errors in `pipeline.rs` about channel type — fixed in Task 8.

- [ ] **Step 3: Commit**

```bash
git add src/stage2_processing.rs
git commit -m "feat: stage2 logs event sightings and relay publications"
```

---

## Task 7: Update config.rs — Add `related_events_batch_size`

**Files:**
- Modify: `src/config.rs`

- [ ] **Step 1: Add field to `Config` struct**

In `src/config.rs`, add to the `Config` struct after `relay_concurrency`:
```rust
    pub related_events_batch_size: usize,
```

- [ ] **Step 2: Parse the new env var in `Config::from_env`**

Add before the closing `Ok(Config {` block:
```rust
        let related_events_batch_size = env::var("RELATED_EVENTS_BATCH_SIZE")
            .unwrap_or_else(|_| "100".to_string())
            .parse::<usize>()
            .map_err(|e: std::num::ParseIntError| {
                ConfigError::InvalidValue("RELATED_EVENTS_BATCH_SIZE".to_string(), e.to_string())
            })?;
```

- [ ] **Step 3: Add field to the `Ok(Config { ... })` return**

Add `related_events_batch_size,` to the struct initialisation.

- [ ] **Step 4: Build to check**

```bash
cargo build 2>&1 | head -20
```

- [ ] **Step 5: Commit**

```bash
git add src/config.rs
git commit -m "feat: add RELATED_EVENTS_BATCH_SIZE config option"
```

---

## Task 8: New Stage 5 — Related Events Collection

**Files:**
- Create: `src/stage5_related_events.rs`
- Modify: `src/lib.rs` (add `pub mod stage5_related_events;`)

- [ ] **Step 1: Add module declaration to `src/lib.rs`**

Open `src/lib.rs` and add:
```rust
pub mod stage5_related_events;
```

- [ ] **Step 2: Create `src/stage5_related_events.rs`**

```rust
use crate::config::Config;
use crate::db::Database;
use nostr_sdk::{Client, Event, EventId, Filter, Kind};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Maps a Nostr kind number to the relation_type string stored in event_relations.
fn relation_type_for_kind(kind: u16) -> &'static str {
    match kind {
        1 => "note",
        5 => "delete",
        7 => "reaction",
        1111 => "comment",
        9734 => "zap_request",
        9735 => "zap_receipt",
        _ => "note",
    }
}

/// Stage 5: Related Events Collection
///
/// Fetches reactions, comments, notes, delete events, and zaps for all known
/// video events. Stores them in the database and publishes to target relays.
/// kind:5 delete events are additionally broadcast to all source relays.
pub async fn run(
    config: Config,
    db: Database,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Stage 5: Starting related events collection");

    let related_kinds = vec![
        Kind::from(1),
        Kind::from(5),
        Kind::from(7),
        Kind::from(1111),
        Kind::from(9734),
        Kind::from(9735),
    ];

    // Connect to all source relays for fetching
    let source_client = Client::default();
    for relay in &config.source_relays {
        source_client.add_relay(relay).await?;
    }
    source_client.connect().await;
    info!("Stage 5: Connected to {} source relay(s)", config.source_relays.len());

    // Connect target relays for publishing
    let target_client = Client::default();
    for relay in &config.target_relays {
        target_client.add_relay(relay).await?;
    }
    target_client.connect().await;

    // Register target relays in DB and cache IDs
    let mut target_relay_ids: HashMap<String, i32> = HashMap::new();
    for relay_url in &config.target_relays {
        match db.upsert_relay(relay_url, "target").await {
            Ok(id) => {
                let key = relay_url.trim_end_matches('/').to_string();
                target_relay_ids.insert(key, id);
            }
            Err(e) => warn!("Failed to upsert target relay {}: {}", relay_url, e),
        }
    }

    // Connect all-relays client for delete broadcasting (source + target, deduplicated)
    let all_relay_urls: Vec<String> = {
        let mut urls: Vec<String> = config.source_relays.clone();
        for url in &config.target_relays {
            if !urls.contains(url) {
                urls.push(url.clone());
            }
        }
        urls
    };
    let broadcast_client = Client::default();
    for relay in &all_relay_urls {
        broadcast_client.add_relay(relay).await?;
    }
    broadcast_client.connect().await;

    let batch_size = config.related_events_batch_size as i64;
    let mut offset: i64 = 0;
    let mut total_fetched = 0usize;
    let mut total_stored = 0usize;

    loop {
        let video_ids = db.get_video_event_ids_paginated(batch_size, offset).await?;
        if video_ids.is_empty() {
            break;
        }

        info!(
            "Stage 5: Processing batch of {} video events (offset {})",
            video_ids.len(),
            offset
        );

        // Parse hex IDs into EventId values; skip any that fail
        let event_ids: Vec<EventId> = video_ids
            .iter()
            .filter_map(|id| EventId::from_hex(id).ok())
            .collect();

        let filter = Filter::new()
            .kinds(related_kinds.clone())
            .events(event_ids);

        let related_events = match tokio::time::timeout(
            Duration::from_secs(30),
            source_client.get_events_of(
                vec![filter],
                nostr_sdk::EventSource::relays(None),
            ),
        )
        .await
        {
            Ok(Ok(events)) => events,
            Ok(Err(e)) => {
                warn!("Stage 5: Relay query failed for batch at offset {}: {}", offset, e);
                offset += batch_size;
                continue;
            }
            Err(_) => {
                warn!("Stage 5: Relay query timed out for batch at offset {}", offset);
                offset += batch_size;
                continue;
            }
        };

        total_fetched += related_events.len();
        debug!(
            "Stage 5: Fetched {} related events for batch at offset {}",
            related_events.len(),
            offset
        );

        let mut delete_events: Vec<Event> = Vec::new();

        for event in related_events {
            let event_id_hex = event.id.to_hex();
            let kind = event.kind.as_u16();

            if db.event_exists(&event_id_hex).await? {
                debug!("Related event {} already in DB, skipping insert", event_id_hex);
            } else {
                let pubkey = event.pubkey.to_hex();
                let created_at = event.created_at.as_u64() as i64;
                let content = if event.content.is_empty() {
                    None
                } else {
                    Some(event.content.as_str())
                };
                let raw_event = serde_json::to_value(&event)?;

                db.insert_event(
                    &event_id_hex,
                    &pubkey,
                    kind as i32,
                    created_at,
                    content,
                    &raw_event,
                )
                .await?;

                total_stored += 1;
            }

            // Link to all video events in this batch referenced by the event's e-tags
            let relation = relation_type_for_kind(kind);
            for tag in event.tags.iter() {
                let tag_vec = tag.as_slice();
                if tag_vec.len() >= 2 && (tag_vec[0] == "e" || tag_vec[0] == "E") {
                    let referenced_id = tag_vec[1].as_str();
                    if video_ids.contains(&referenced_id.to_string()) {
                        db.insert_event_relation(&event_id_hex, referenced_id, relation)
                            .await
                            .ok();
                    }
                }
            }

            // Publish to target relays and log
            match target_client.send_event(event.clone()).await {
                Ok(output) => {
                    for relay_url in &output.success {
                        let url_str = relay_url.as_str();
                        if let Some(&relay_id) = target_relay_ids.get(url_str) {
                            db.insert_publication(&event_id_hex, relay_id).await.ok();
                        }
                    }
                }
                Err(e) => warn!("Stage 5: Failed to publish event {}: {}", event_id_hex, e),
            }

            if kind == 5 {
                delete_events.push(event);
            }
        }

        // Broadcast delete events to all known relays
        for event in delete_events {
            match broadcast_client.send_event(event.clone()).await {
                Ok(output) => {
                    info!(
                        "Stage 5: Broadcast delete event {} to {}/{} relays",
                        event.id,
                        output.success.len(),
                        all_relay_urls.len()
                    );
                }
                Err(e) => warn!("Stage 5: Delete broadcast failed for {}: {}", event.id, e),
            }
        }

        offset += batch_size;
    }

    source_client.disconnect().await?;
    target_client.disconnect().await?;
    broadcast_client.disconnect().await?;

    info!(
        "Stage 5: Completed — {} related events fetched, {} newly stored",
        total_fetched, total_stored
    );

    Ok(())
}
```

- [ ] **Step 3: Build to verify**

```bash
cargo build 2>&1 | head -40
```

- [ ] **Step 4: Commit**

```bash
git add src/stage5_related_events.rs src/lib.rs
git commit -m "feat: add stage5 — related events collection, delete broadcast, publication logging"
```

---

## Task 9: Update pipeline.rs — Wire Up All Stages

**Files:**
- Modify: `src/pipeline.rs`

- [ ] **Step 1: Replace `src/pipeline.rs` entirely**

```rust
use crate::config::Config;
use crate::db::Database;
use crate::{stage1_collection, stage2_processing, stage3_validation, stage4_filter_generation, stage5_related_events};
use tokio::sync::mpsc;
use tracing::info;

/// Run the complete pipeline.
///
/// Pipeline A (Stages 1–4): collect video events, store, validate URLs, generate filter.
/// Pipeline B (Stage 5):    fetch related events (reactions, comments, deletes, zaps),
///                          store, publish, broadcast deletes.
pub async fn run(
    config: Config,
    db: Database,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Starting VideoJanitor pipeline");

    let (event_tx, event_rx) = mpsc::channel::<(nostr_sdk::Event, String)>(1000);

    let stage1 = tokio::spawn({
        let config = config.clone();
        let db = db.clone();
        async move { stage1_collection::run(config, db, event_tx).await }
    });

    let stage2 = tokio::spawn({
        let config = config.clone();
        let db = db.clone();
        async move { stage2_processing::run(config, db, event_rx).await }
    });

    let (r1, r2) = tokio::try_join!(stage1, stage2)?;
    r1?;
    r2?;

    info!("Stages 1 and 2 completed, starting Stage 3");
    stage3_validation::run(config.clone(), db.clone()).await?;

    info!("Stage 3 completed, starting Stage 4");
    stage4_filter_generation::run(config.clone(), db.clone()).await?;

    info!("Stage 4 completed, starting Stage 5 (related events)");
    stage5_related_events::run(config, db).await?;

    info!("Pipeline completed successfully");
    Ok(())
}
```

- [ ] **Step 2: Full build**

```bash
cargo build 2>&1
```
Expected: clean build, zero errors.

- [ ] **Step 3: Run clippy**

```bash
cargo clippy 2>&1
```
Fix any warnings before committing.

- [ ] **Step 4: Commit**

```bash
git add src/pipeline.rs
git commit -m "feat: wire stage5 related events into pipeline"
```

---

## Task 10: Smoke Test

- [ ] **Step 1: Ensure a local Postgres is running**

```bash
docker run --name video-janitor-db \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=video_janitor \
  -p 5432:5432 \
  -d postgres
```

- [ ] **Step 2: Run with a minimal `.env`**

```bash
# .env
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/video_janitor
SOURCE_RELAYS=wss://relay.damus.io
TARGET_RELAYS=wss://relay.damus.io
BACKFILL_MAX_EVENTS=50
RELATED_EVENTS_BATCH_SIZE=25
RUST_LOG=info
```

```bash
cargo run 2>&1 | head -80
```

Expected log sequence:
```
Starting VideoJanitor pipeline
Stage 1: Starting event collection from 1 relays
Stage 2: Starting event processing
Stage 2: Event processing completed
Stage 1 and 2 completed, starting Stage 3
Stage 3 completed, starting Stage 4
Stage 4 completed, starting Stage 5 (related events)
Stage 5: Starting related events collection
Stage 5: Completed
Pipeline completed successfully
```

- [ ] **Step 3: Verify new tables exist in Postgres**

```bash
docker exec video-janitor-db psql -U postgres -d video_janitor -c "\dt"
```
Expected: `event_relations`, `event_sightings`, `relay_publications`, `relays` listed.

- [ ] **Step 4: Spot-check relay and publication data**

```bash
docker exec video-janitor-db psql -U postgres -d video_janitor \
  -c "SELECT relay_url, relay_type FROM relays LIMIT 10;"

docker exec video-janitor-db psql -U postgres -d video_janitor \
  -c "SELECT COUNT(*) FROM relay_publications;"

docker exec video-janitor-db psql -U postgres -d video_janitor \
  -c "SELECT COUNT(*) FROM event_sightings;"
```

- [ ] **Step 5: Final commit**

```bash
git add .
git commit -m "feat: complete related events pipeline — smoke test passed"
```
