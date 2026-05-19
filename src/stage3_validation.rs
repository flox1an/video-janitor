use crate::config::Config;
use crate::db::Database;
use futures::stream::{self, StreamExt};
use reqwest::Client;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Stage 3: URL Validation
///
/// Queries the database for pending URLs and performs HTTP HEAD requests
/// to check availability. Updates database with status, HTTP code, and error details.
pub async fn run(
    config: Config,
    db: Database,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(
        "Stage 3: Starting URL validation with {} concurrent checks",
        config.url_check_concurrency
    );

    // Fetch all pending URLs from database
    let pending_urls = db.get_pending_urls().await?;

    if pending_urls.is_empty() {
        info!("Stage 3: No pending URLs to validate");
        return Ok(());
    }

    let total_urls = pending_urls.len();
    info!("Stage 3: Found {} pending URLs to validate", total_urls);

    let http_client = Client::builder()
        .timeout(Duration::from_secs(config.url_check_timeout_secs))
        .build()?;

    let checked = Arc::new(AtomicUsize::new(0));

    stream::iter(pending_urls)
        .for_each_concurrent(config.url_check_concurrency, |(url_id, url)| {
            let db = db.clone();
            let client = http_client.clone();
            let delay_ms = config.url_check_delay_ms;
            let checked = checked.clone();

            async move {
                debug!("Checking URL: {}", url);

                let (status, http_code, error_msg) = match client.head(&url).send().await {
                    Ok(resp) => {
                        let code = resp.status().as_u16();
                        let status = if resp.status().is_success() {
                            "available"
                        } else if resp.status().is_client_error() {
                            "not_found"
                        } else {
                            "server_error"
                        };
                        (status, Some(code as i16), None)
                    }
                    Err(e) if e.is_timeout() => ("timeout", None, Some(e.to_string())),
                    Err(e) => ("server_error", None, Some(e.to_string())),
                };

                if let Err(e) = db
                    .update_url_status(url_id, status, http_code, error_msg.as_deref())
                    .await
                {
                    warn!("Failed to update URL status in database: {}", e);
                }

                debug!(
                    "URL {} -> {}{}",
                    url,
                    status,
                    http_code
                        .map(|c| format!(" (HTTP {})", c))
                        .unwrap_or_default()
                );

                let n = checked.fetch_add(1, Ordering::Relaxed) + 1;
                if n % 50 == 0 {
                    info!("Stage 3: {}/{} URLs checked", n, total_urls);
                }

                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        })
        .await;

    info!("Stage 3: URL validation completed");

    Ok(())
}
