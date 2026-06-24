use crate::parser;
use sqlx::postgres::{PgPool, PgPoolOptions};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
}

#[derive(Debug, Clone)]
pub struct Database {
    pool: PgPool,
}

#[derive(Debug, Clone)]
pub struct VideoUrl {
    #[allow(dead_code)]
    pub id: Option<i32>,
    pub event_id: String,
    pub url: String,
    pub url_type: String,
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RelayState {
    pub id: i32,
    pub relay_url: String,
    pub last_event_timestamp: i64,
    pub total_events_fetched: i32,
}

#[derive(Debug, Clone)]
pub struct VideoEventReference {
    pub event_id: String,
    pub pubkey: String,
    pub kind: i32,
    pub identifier: Option<String>,
    pub video_hashes: Vec<String>,
}

impl VideoEventReference {
    pub fn address(&self) -> Option<String> {
        if !matches!(self.kind, 34235 | 34236) {
            return None;
        }

        let identifier = self.identifier.as_ref()?;
        Some(format!("{}:{}:{}", self.kind, self.pubkey, identifier))
    }
}

impl Database {
    pub async fn connect(database_url: &str) -> Result<Self, DatabaseError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(database_url)
            .await
            .map_err(|e| DatabaseError::ConnectionFailed(e.to_string()))?;

        Ok(Database { pool })
    }

    pub async fn run_migrations(&self) -> Result<(), DatabaseError> {
        let m001 = include_str!("../migrations/001_initial_schema.sql");
        let m002 = include_str!("../migrations/002_relays_master_table.sql");
        let m003 = include_str!("../migrations/003_related_events.sql");
        let m004 = include_str!("../migrations/004_relay_video_relation_sync.sql");
        let m005 = include_str!("../migrations/005_lifecycle_status.sql");
        let m006 = include_str!("../migrations/006_relay_active_flag.sql");

        sqlx::raw_sql(m001).execute(&self.pool).await.ok();
        sqlx::raw_sql(m002).execute(&self.pool).await.ok();
        sqlx::raw_sql(m003).execute(&self.pool).await.ok();
        sqlx::raw_sql(m004).execute(&self.pool).await.ok();
        sqlx::raw_sql(m005).execute(&self.pool).await.ok();
        sqlx::raw_sql(m006).execute(&self.pool).await.ok();

        Ok(())
    }

    // Event operations
    pub async fn event_exists(&self, event_id: &str) -> Result<bool, DatabaseError> {
        let result: Option<(bool,)> =
            sqlx::query_as("SELECT EXISTS(SELECT 1 FROM events WHERE event_id = $1)")
                .bind(event_id)
                .fetch_optional(&self.pool)
                .await?;

        Ok(result.map(|(exists,)| exists).unwrap_or(false))
    }

    pub async fn insert_event(
        &self,
        event_id: &str,
        pubkey: &str,
        kind: i32,
        created_at: i64,
        content: Option<&str>,
        raw_event: &serde_json::Value,
        lifecycle_status: &str,
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
        identifier: Option<&str>,
    ) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"
            INSERT INTO events
                (event_id, pubkey, kind, created_at, content, raw_event,
                 lifecycle_status, expires_at, identifier)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (event_id) DO NOTHING
            "#,
        )
        .bind(event_id)
        .bind(pubkey)
        .bind(kind)
        .bind(created_at)
        .bind(content)
        .bind(raw_event)
        .bind(lifecycle_status)
        .bind(expires_at)
        .bind(identifier)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// For addressable event kinds (34235, 34236), returns the (event_id, created_at)
    /// of the current active event for the given pubkey + kind + d-tag identifier.
    pub async fn get_active_addressable_event(
        &self,
        pubkey: &str,
        kind: i32,
        identifier: &str,
    ) -> Result<Option<(String, i64)>, DatabaseError> {
        let result = sqlx::query_as::<_, (String, i64)>(
            r#"
            SELECT event_id, created_at FROM events
            WHERE pubkey = $1 AND kind = $2 AND identifier = $3
              AND lifecycle_status = 'active'
            LIMIT 1
            "#,
        )
        .bind(pubkey)
        .bind(kind)
        .bind(identifier)
        .fetch_optional(&self.pool)
        .await?;

        Ok(result)
    }

    /// Mark a specific event as replaced by a newer version of the same address.
    pub async fn mark_event_replaced(&self, event_id: &str) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"
            UPDATE events SET lifecycle_status = 'replaced'
            WHERE event_id = $1 AND lifecycle_status = 'active'
            "#,
        )
        .bind(event_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Mark all video events as 'deleted' where a kind:5 event from the same pubkey
    /// references them in event_relations. Returns the number of events newly marked.
    ///
    /// NIP-09: only the event's own author can delete it, enforced by the pubkey join.
    pub async fn mark_events_deleted(&self) -> Result<u64, DatabaseError> {
        let result = sqlx::query(
            r#"
            UPDATE events AS vid
            SET lifecycle_status = 'deleted'
            FROM event_relations er
            JOIN events del ON del.event_id = er.related_event_id
            WHERE er.video_event_id = vid.event_id
              AND er.relation_type = 'delete'
              AND del.pubkey = vid.pubkey
              AND vid.lifecycle_status = 'active'
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Sweep all events whose NIP-40 expiration timestamp has passed and mark them
    /// as 'expired'. Returns the number of events newly marked.
    pub async fn mark_expired_events(&self) -> Result<u64, DatabaseError> {
        let result = sqlx::query(
            r#"
            UPDATE events
            SET lifecycle_status = 'expired'
            WHERE expires_at <= NOW()
              AND lifecycle_status = 'active'
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

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

    #[allow(dead_code)]
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

    /// Returns relays that have at least one video event sighting and are read-enabled.
    /// Read-disabled relays are excluded so stage 5 doesn't
    /// waste time fetching related events from retired endpoints.
    pub async fn get_relays_with_video_sightings(
        &self,
    ) -> Result<Vec<(i32, String)>, DatabaseError> {
        let results = sqlx::query_as::<_, (i32, String)>(
            r#"
            SELECT DISTINCT r.id, r.relay_url
            FROM relays r
            JOIN event_sightings es ON es.relay_id = r.id
            JOIN events e ON e.event_id = es.event_id
            WHERE e.kind IN (21, 22, 34235, 34236)
              AND r.read_enabled = TRUE
            ORDER BY r.relay_url
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(results)
    }

    /// Returns the URLs of all read-enabled source relays in the database.
    /// Used by the pipeline to filter out read-disabled relays after the initial
    /// upsert registration, before stage 1 starts fetching.
    pub async fn get_read_enabled_source_relay_urls(&self) -> Result<Vec<String>, DatabaseError> {
        let results = sqlx::query_as::<_, (String,)>(
            r#"
            SELECT relay_url FROM relays
            WHERE relay_type IN ('source', 'both')
              AND read_enabled = TRUE
            ORDER BY relay_url
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(results.into_iter().map(|(url,)| url).collect())
    }

    pub async fn get_write_enabled_relay_urls(
        &self,
        relay_urls: &[String],
    ) -> Result<Vec<String>, DatabaseError> {
        if relay_urls.is_empty() {
            return Ok(Vec::new());
        }

        let results = sqlx::query_as::<_, (String,)>(
            r#"
            SELECT relay_url FROM relays
            WHERE relay_url = ANY($1)
              AND write_enabled = TRUE
            ORDER BY relay_url
            "#,
        )
        .bind(relay_urls)
        .fetch_all(&self.pool)
        .await?;

        Ok(results.into_iter().map(|(url,)| url).collect())
    }

    /// Returns video event IDs that were sighted on a specific relay, paginated.
    #[allow(dead_code)]
    pub async fn get_video_event_ids_for_relay_paginated(
        &self,
        relay_id: i32,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<String>, DatabaseError> {
        let results = sqlx::query_as::<_, (String,)>(
            r#"
            SELECT e.event_id
            FROM events e
            JOIN event_sightings es ON es.event_id = e.event_id
            WHERE es.relay_id = $1
              AND e.kind IN (21, 22, 34235, 34236)
            ORDER BY e.created_at ASC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(relay_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(results.into_iter().map(|(id,)| id).collect())
    }

    /// Returns video event IDs plus address metadata for events sighted on a relay.
    pub async fn get_video_event_references_for_relay_paginated(
        &self,
        relay_id: i32,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<VideoEventReference>, DatabaseError> {
        let results = sqlx::query_as::<_, (String, String, i32, serde_json::Value)>(
            r#"
            SELECT e.event_id, e.pubkey, e.kind, e.raw_event
            FROM events e
            JOIN event_sightings es ON es.event_id = e.event_id
            WHERE es.relay_id = $1
              AND e.kind IN (21, 22, 34235, 34236)
            ORDER BY e.created_at ASC
            LIMIT $2 OFFSET $3
            "#,
        )
        .bind(relay_id)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        Ok(results
            .into_iter()
            .map(|(event_id, pubkey, kind, raw_event)| VideoEventReference {
                video_hashes: extract_video_hashes_from_raw_event(&raw_event),
                event_id,
                pubkey,
                kind,
                identifier: extract_identifier_from_raw_event(&raw_event),
            })
            .collect())
    }

    // Video URL operations
    pub async fn insert_video_url(&self, video_url: &VideoUrl) -> Result<i32, DatabaseError> {
        let result: (i32,) = sqlx::query_as(
            r#"
            INSERT INTO video_urls (event_id, url, url_type, mime_type, status)
            VALUES ($1, $2, $3, $4, 'pending')
            ON CONFLICT (event_id, url) DO NOTHING
            RETURNING id
            "#,
        )
        .bind(&video_url.event_id)
        .bind(&video_url.url)
        .bind(&video_url.url_type)
        .bind(&video_url.mime_type)
        .fetch_one(&self.pool)
        .await?;

        Ok(result.0)
    }

    pub async fn update_url_status(
        &self,
        url_id: i32,
        status: &str,
        http_status_code: Option<i16>,
        error_message: Option<&str>,
    ) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"
            UPDATE video_urls
            SET status = $2,
                http_status_code = $3,
                last_checked_at = NOW(),
                error_count = CASE
                    WHEN $2 = 'available' THEN 0
                    ELSE error_count + 1
                END,
                last_error_message = $4
            WHERE id = $1
            "#,
        )
        .bind(url_id)
        .bind(status)
        .bind(http_status_code)
        .bind(error_message)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_pending_urls(
        &self,
        max_retries: i32,
    ) -> Result<Vec<(i32, String)>, DatabaseError> {
        let results = sqlx::query_as::<_, (i32, String)>(
            r#"
            SELECT vu.id, vu.url
            FROM video_urls vu
            JOIN events e ON e.event_id = vu.event_id
            WHERE e.lifecycle_status = 'active'
              AND (vu.status = 'pending'
               OR (vu.status IN ('not_found', 'server_error', 'timeout') AND vu.error_count < $1))
            ORDER BY vu.added_at ASC
            "#,
        )
        .bind(max_retries)
        .fetch_all(&self.pool)
        .await?;

        Ok(results)
    }

    pub async fn get_fully_failed_event_ids(
        &self,
        max_retries: i32,
    ) -> Result<Vec<String>, DatabaseError> {
        let results = sqlx::query_as::<_, (String,)>(
            r#"
            SELECT e.event_id
            FROM events e
            JOIN video_urls vu ON vu.event_id = e.event_id
            WHERE e.lifecycle_status = 'active'
            GROUP BY e.event_id
            HAVING COUNT(CASE WHEN vu.status IN ('not_found', 'server_error', 'timeout')
                              AND vu.error_count >= $1 THEN 1 END) = COUNT(*)
            "#,
        )
        .bind(max_retries)
        .fetch_all(&self.pool)
        .await?;

        Ok(results.into_iter().map(|(id,)| id).collect())
    }

    pub async fn get_all_raw_events(&self) -> Result<Vec<serde_json::Value>, DatabaseError> {
        let results = sqlx::query_as::<_, (serde_json::Value,)>(
            "SELECT raw_event FROM events ORDER BY created_at ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(results.into_iter().map(|(v,)| v).collect())
    }

    // Relay state operations
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

        Ok(result.map(
            |(id, relay_url, last_event_timestamp, total_events_fetched)| RelayState {
                id,
                relay_url,
                last_event_timestamp,
                total_events_fetched,
            },
        ))
    }

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

    pub async fn upsert_relay(
        &self,
        relay_url: &str,
        relay_type: &str,
    ) -> Result<i32, DatabaseError> {
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

    pub async fn disable_relay_read_write(
        &self,
        relay_url: &str,
        disable_reason: &str,
    ) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"
            INSERT INTO relays (
                relay_url, relay_type,
                read_enabled, write_enabled,
                read_disabled_reason, write_disabled_reason,
                read_disabled_at, write_disabled_at,
                is_active, disable_reason, disabled_at
            )
            VALUES ($1, 'source', FALSE, FALSE, $2, $2, NOW(), NOW(), FALSE, $2, NOW())
            ON CONFLICT (relay_url) DO UPDATE
            SET read_enabled = FALSE,
                write_enabled = FALSE,
                read_disabled_reason = EXCLUDED.read_disabled_reason,
                write_disabled_reason = EXCLUDED.write_disabled_reason,
                read_disabled_at = NOW(),
                write_disabled_at = NOW(),
                is_active = FALSE,
                disable_reason = EXCLUDED.disable_reason,
                disabled_at = NOW(),
                relay_type = CASE
                    WHEN relays.relay_type = 'target' THEN 'both'
                    ELSE relays.relay_type
                END
            "#,
        )
        .bind(relay_url)
        .bind(disable_reason)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn disable_relay_write(
        &self,
        relay_url: &str,
        disable_reason: &str,
    ) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"
            INSERT INTO relays (
                relay_url, relay_type,
                write_enabled, write_disabled_reason, write_disabled_at
            )
            VALUES ($1, 'target', FALSE, $2, NOW())
            ON CONFLICT (relay_url) DO UPDATE
            SET write_enabled = FALSE,
                write_disabled_reason = EXCLUDED.write_disabled_reason,
                write_disabled_at = NOW(),
                relay_type = CASE
                    WHEN relays.relay_type = 'source' THEN 'both'
                    ELSE relays.relay_type
                END
            "#,
        )
        .bind(relay_url)
        .bind(disable_reason)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_relay_video_sync_states(
        &self,
        relay_id: i32,
        video_ids: &[String],
    ) -> Result<std::collections::HashMap<String, i64>, DatabaseError> {
        if video_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }

        let results = sqlx::query_as::<_, (String, i64)>(
            r#"
            SELECT video_event_id, last_sync_timestamp
            FROM relay_video_relation_sync
            WHERE relay_id = $1 AND video_event_id = ANY($2)
            "#,
        )
        .bind(relay_id)
        .bind(video_ids)
        .fetch_all(&self.pool)
        .await?;

        Ok(results.into_iter().collect())
    }

    pub async fn upsert_relay_video_sync_states(
        &self,
        relay_id: i32,
        video_ids: &[String],
        timestamp: i64,
    ) -> Result<(), DatabaseError> {
        if video_ids.is_empty() {
            return Ok(());
        }

        sqlx::query(
            r#"
            INSERT INTO relay_video_relation_sync (relay_id, video_event_id, last_sync_timestamp)
            SELECT $1, unnest($2), $3
            ON CONFLICT (relay_id, video_event_id) DO UPDATE
            SET last_sync_timestamp = EXCLUDED.last_sync_timestamp
            "#,
        )
        .bind(relay_id)
        .bind(video_ids)
        .bind(timestamp)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

fn extract_identifier_from_raw_event(raw_event: &serde_json::Value) -> Option<String> {
    raw_event
        .get("tags")?
        .as_array()?
        .iter()
        .filter_map(|tag| tag.as_array())
        .find_map(|tag| {
            if tag.first()?.as_str()? == "d" {
                tag.get(1)?.as_str().map(ToString::to_string)
            } else {
                None
            }
        })
}

fn extract_video_hashes_from_raw_event(raw_event: &serde_json::Value) -> Vec<String> {
    serde_json::from_value(raw_event.clone())
        .map(|event| parser::extract_video_hashes(&event))
        .unwrap_or_default()
}
