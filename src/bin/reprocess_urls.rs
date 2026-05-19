/// Utility to re-extract video URLs from existing events
///
/// This script reads all events from the database and re-extracts video URLs
/// using the corrected parser. Useful after fixing the imeta tag parsing logic.
use sqlx::postgres::PgPoolOptions;
use std::env;
use video_janitor::parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables
    dotenvy::dotenv().ok();

    // Setup logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    tracing::info!("Connecting to database...");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    tracing::info!("Fetching all events...");
    let events = sqlx::query!(
        r#"
        SELECT event_id, raw_event
        FROM events
        ORDER BY created_at DESC
        "#
    )
    .fetch_all(&pool)
    .await?;

    tracing::info!("Found {} events to process", events.len());

    let mut total_urls = 0;
    let mut events_with_urls = 0;
    let mut inserted = 0;
    let mut already_exists = 0;

    for (idx, record) in events.iter().enumerate() {
        if idx % 1000 == 0 && idx > 0 {
            tracing::info!(
                "Processed {}/{} events, found {} URLs ({} new, {} existing)",
                idx,
                events.len(),
                total_urls,
                inserted,
                already_exists
            );
        }

        // Parse the raw event JSON
        let event: nostr_sdk::Event = serde_json::from_value(record.raw_event.clone())?;

        // Extract video URLs using the fixed parser
        let urls = parser::extract_video_urls(&event);

        if !urls.is_empty() {
            events_with_urls += 1;
            total_urls += urls.len();

            // Insert each URL into database
            for url in urls {
                let result = sqlx::query!(
                    r#"
                    INSERT INTO video_urls (event_id, url, url_type, mime_type, status)
                    VALUES ($1, $2, $3, $4, 'pending')
                    ON CONFLICT (event_id, url) DO NOTHING
                    "#,
                    url.event_id,
                    url.url,
                    url.url_type,
                    url.mime_type
                )
                .execute(&pool)
                .await?;

                if result.rows_affected() > 0 {
                    inserted += 1;
                } else {
                    already_exists += 1;
                }
            }
        }
    }

    tracing::info!("✅ Reprocessing complete!");
    tracing::info!("   Total events: {}", events.len());
    tracing::info!("   Events with video URLs: {}", events_with_urls);
    tracing::info!("   Total URLs found: {}", total_urls);
    tracing::info!("   New URLs inserted: {}", inserted);
    tracing::info!("   Already existed: {}", already_exists);

    Ok(())
}
