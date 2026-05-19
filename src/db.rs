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
        // Read and execute migration file
        let migration_sql = include_str!("../migrations/001_initial_schema.sql");

        // Execute migration (this is idempotent-safe if tables already exist)
        sqlx::raw_sql(migration_sql).execute(&self.pool).await.ok(); // Ignore errors if tables already exist

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
        relay_source: &str,
    ) -> Result<(), DatabaseError> {
        sqlx::query(
            r#"
            INSERT INTO events (event_id, pubkey, kind, created_at, content, raw_event, relay_source)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (event_id) DO NOTHING
            "#
        )
        .bind(event_id)
        .bind(pubkey)
        .bind(kind)
        .bind(created_at)
        .bind(content)
        .bind(raw_event)
        .bind(relay_source)
        .execute(&self.pool)
        .await?;

        Ok(())
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
        let result = sqlx::query_as::<_, (String, i64, i32)>(
            "SELECT relay_url, last_event_timestamp, total_events_fetched FROM relay_state WHERE relay_url = $1"
        )
        .bind(relay_url)
        .fetch_optional(&self.pool)
        .await?;

        Ok(result.map(
            |(relay_url, last_event_timestamp, total_events_fetched)| RelayState {
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
            INSERT INTO relay_state (relay_url, last_event_timestamp, total_events_fetched)
            VALUES ($1, $2, $3)
            ON CONFLICT (relay_url) DO UPDATE
            SET last_event_timestamp = $2,
                last_sync_at = NOW(),
                total_events_fetched = relay_state.total_events_fetched + $3
            "#,
        )
        .bind(relay_url)
        .bind(last_event_timestamp)
        .bind(events_count)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
