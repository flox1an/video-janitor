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
    pub id: Option<i32>,
    pub event_id: String,
    pub url: String,
    pub url_type: String,
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RelayState {
    pub id: i32,
    pub relay_url: String,
    pub last_event_timestamp: i64,
    pub total_events_fetched: i32,
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

        sqlx::raw_sql(m001).execute(&self.pool).await.ok();
        sqlx::raw_sql(m002).execute(&self.pool).await.ok();
        sqlx::raw_sql(m003).execute(&self.pool).await.ok();

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

    pub async fn get_pending_urls(&self) -> Result<Vec<(i32, String)>, DatabaseError> {
        let results = sqlx::query_as::<_, (i32, String)>(
            r#"
            SELECT id, url
            FROM video_urls
            WHERE status = 'pending'
            ORDER BY added_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(results)
    }

    pub async fn get_fully_failed_event_ids(&self) -> Result<Vec<String>, DatabaseError> {
        let results = sqlx::query_as::<_, (String,)>(
            r#"
            SELECT e.event_id
            FROM events e
            WHERE (
                SELECT COUNT(*)
                FROM video_urls vu
                WHERE vu.event_id = e.event_id
            ) > 0
            AND (
                SELECT COUNT(*)
                FROM video_urls vu
                WHERE vu.event_id = e.event_id
                AND vu.status IN ('not_found', 'server_error', 'timeout')
            ) = (
                SELECT COUNT(*)
                FROM video_urls vu
                WHERE vu.event_id = e.event_id
            )
            "#,
        )
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

        Ok(result.map(|(id, relay_url, last_event_timestamp, total_events_fetched)| RelayState {
            id,
            relay_url,
            last_event_timestamp,
            total_events_fetched,
        }))
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
}
