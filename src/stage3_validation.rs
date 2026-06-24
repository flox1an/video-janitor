use crate::config::Config;
use crate::db::Database;
use futures::stream::{self, StreamExt};
use reqwest::Client;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
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
    let pending_urls = db
        .get_pending_urls(config.url_check_max_retries as i32)
        .await?;

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
    let available = Arc::new(AtomicUsize::new(0));
    let not_found = Arc::new(AtomicUsize::new(0));
    let server_error = Arc::new(AtomicUsize::new(0));
    let timed_out = Arc::new(AtomicUsize::new(0));
    // Track recent errors for context in progress logs
    let last_errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    stream::iter(pending_urls)
        .for_each_concurrent(config.url_check_concurrency, |(url_id, url)| {
            let db = db.clone();
            let client = http_client.clone();
            let delay_ms = config.url_check_delay_ms;
            let checked = checked.clone();
            let available = available.clone();
            let not_found = not_found.clone();
            let server_error = server_error.clone();
            let timed_out = timed_out.clone();
            let last_errors = last_errors.clone();

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

                match status {
                    "available" => { available.fetch_add(1, Ordering::Relaxed); }
                    "not_found" => { not_found.fetch_add(1, Ordering::Relaxed); }
                    "server_error" => {
                        server_error.fetch_add(1, Ordering::Relaxed);
                        if let Some(ref msg) = error_msg {
                            if let Ok(mut errs) = last_errors.lock() {
                                if errs.len() < 3 {
                                    errs.push(format!("{}: {}", url, msg));
                                }
                            }
                        }
                    }
                    "timeout" => { timed_out.fetch_add(1, Ordering::Relaxed); }
                    _ => {}
                }

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
                    let pct = n * 100 / total_urls;
                    info!(
                        "Stage 3: {}/{} URLs checked ({}%) — ok={}, not_found={}, err={}, timeout={}",
                        n, total_urls, pct,
                        available.load(Ordering::Relaxed),
                        not_found.load(Ordering::Relaxed),
                        server_error.load(Ordering::Relaxed),
                        timed_out.load(Ordering::Relaxed),
                    );
                }

                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        })
        .await;

    let total_checked = checked.load(Ordering::Relaxed);
    let ok = available.load(Ordering::Relaxed);
    let nf = not_found.load(Ordering::Relaxed);
    let se = server_error.load(Ordering::Relaxed);
    let to = timed_out.load(Ordering::Relaxed);
    info!(
        "Stage 3: URL validation completed — {}/{} checked: {} available, {} not_found, {} server_error, {} timeout",
        total_checked, total_urls, ok, nf, se, to
    );

    Ok(())
}
