mod config;
mod db;
mod parser;
mod pipeline;
mod stage1_collection;
mod stage2_processing;
mod stage3_validation;
mod stage4_filter_generation;
mod stage5_related_events;

use clap::Parser;
use config::Config;
use db::Database;
use nostr_sdk::{Client, Event};
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "video-janitor")]
#[command(about = "Nostr video event collector and URL tracker", long_about = None)]
struct Args {
    /// Run as daemon with scheduled jobs
    #[arg(long)]
    daemon: bool,

    /// Re-publish all events from the local database to the target relays
    #[arg(long)]
    republish: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .init();

    // Load .env file if present
    dotenvy::dotenv().ok();

    // Parse CLI args
    let args = Args::parse();

    // Load configuration
    let config = Config::from_env()?;

    // Connect to database
    info!("Connecting to database...");
    let db = Database::connect(&config.database_url).await?;

    // Run migrations
    info!("Running database migrations...");
    db.run_migrations().await?;

    if args.republish {
        run_republish(config, db).await?;
    } else if args.daemon {
        run_daemon(config, db).await?;
    } else {
        run_once(config, db).await?;
    }

    Ok(())
}

/// Run once (one-shot mode)
async fn run_once(config: Config, db: Database) -> Result<(), Box<dyn std::error::Error>> {
    info!("Running in one-shot mode");

    let start = std::time::Instant::now();

    match pipeline::run(config, db).await {
        Ok(_) => {
            info!("Job completed successfully in {:?}", start.elapsed());
            Ok(())
        }
        Err(e) => {
            error!("Job failed: {}", e);
            Err(e)
        }
    }
}

/// Run as daemon with scheduled jobs
async fn run_daemon(config: Config, db: Database) -> Result<(), Box<dyn std::error::Error>> {
    info!(
        "Running in daemon mode (interval: {} hours)",
        config.job_interval_hours
    );

    let scheduler = JobScheduler::new().await?;

    // Create cron expression (every N hours)
    // Format: "0 0 */N * * *" = every N hours
    let cron_expr = format!("0 0 */{} * * *", config.job_interval_hours);

    // Clone for use in closure
    let config_for_job = config.clone();
    let db_for_job = db.clone();

    let job = Job::new_async(cron_expr.as_str(), move |_uuid, _lock| {
        let config = config_for_job.clone();
        let db = db_for_job.clone();

        Box::pin(async move {
            info!("Starting scheduled job");
            let start = std::time::Instant::now();

            match pipeline::run(config, db).await {
                Ok(_) => {
                    info!(
                        "Scheduled job completed successfully in {:?}",
                        start.elapsed()
                    );
                }
                Err(e) => {
                    error!("Scheduled job failed: {}", e);
                }
            }
        })
    })?;

    scheduler.add(job).await?;
    scheduler.start().await?;

    info!("Scheduler started. Press Ctrl+C to stop.");

    // Run first job immediately
    info!("Running initial job...");
    let start = std::time::Instant::now();
    match pipeline::run(config, db).await {
        Ok(_) => {
            info!(
                "Initial job completed successfully in {:?}",
                start.elapsed()
            );
        }
        Err(e) => {
            error!("Initial job failed: {}", e);
        }
    }

    // Keep running until interrupted
    tokio::signal::ctrl_c().await?;
    info!("Shutting down...");

    Ok(())
}

async fn run_republish(config: Config, db: Database) -> Result<(), Box<dyn std::error::Error>> {
    info!("Re-publish mode: loading all events from database...");

    let raw_events = db.get_all_raw_events().await?;
    let total = raw_events.len();
    info!(
        "Found {} events in database, publishing to {} target relay(s): {}",
        total,
        config.target_relays.len(),
        config.target_relays.join(", ")
    );

    let client = Client::default();
    for relay in &config.target_relays {
        client.add_relay(relay).await?;
    }
    client.connect().await;

    let mut success = 0;
    let mut failed = 0;

    for (i, raw) in raw_events.into_iter().enumerate() {
        let event: Event = match serde_json::from_value(raw) {
            Ok(e) => e,
            Err(e) => {
                warn!("[{}/{}] Failed to deserialize event: {}", i + 1, total, e);
                failed += 1;
                continue;
            }
        };

        match client.send_event(event).await {
            Ok(_) => {
                success += 1;
                if success % 50 == 0 {
                    info!(
                        "[{}/{}] Published {} events so far...",
                        i + 1,
                        total,
                        success
                    );
                }
            }
            Err(e) => {
                warn!("[{}/{}] Failed to publish event: {}", i + 1, total, e);
                failed += 1;
            }
        }
    }

    client.disconnect().await?;
    info!(
        "Re-publish complete: {} succeeded, {} failed (total: {})",
        success, failed, total
    );

    Ok(())
}
