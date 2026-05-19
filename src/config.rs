use std::env;
use std::fs;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Missing required environment variable: {0}")]
    MissingVariable(String),
    #[error("Invalid value for {0}: {1}")]
    InvalidValue(String, String),
    #[error("Failed to read relay file {0}: {1}")]
    RelayFileError(String, String),
}

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub source_relays: Vec<String>,
    pub target_relays: Vec<String>,
    pub job_interval_hours: u64,
    pub url_check_concurrency: usize,
    pub url_check_timeout_secs: u64,
    pub url_check_delay_ms: u64,
    pub backfill_batch_size: usize,
    pub backfill_max_events: usize,
    pub relay_concurrency: usize,
    pub related_events_batch_size: usize,
}

fn is_valid_relay_url(url: &str) -> bool {
    if !url.starts_with("wss://") {
        return false;
    }
    let host = &url["wss://".len()..];
    // reject credential syntax (user@host)
    if host.contains('@') {
        return false;
    }
    // reject bare hex strings (pubkeys/hashes mistakenly used as relay URLs)
    let host_part = host
        .split('/')
        .next()
        .unwrap_or("")
        .split(':')
        .next()
        .unwrap_or("");
    if host_part.len() >= 40 && host_part.chars().all(|c| c.is_ascii_hexdigit()) {
        return false;
    }
    true
}

fn load_relays_from_file(path: &str) -> Result<Vec<String>, ConfigError> {
    let content = fs::read_to_string(path)
        .map_err(|e| ConfigError::RelayFileError(path.to_string(), e.to_string()))?;
    let relays: Vec<String> = content
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| is_valid_relay_url(l))
        .collect();
    Ok(relays)
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        let database_url = env::var("DATABASE_URL")
            .map_err(|_| ConfigError::MissingVariable("DATABASE_URL".to_string()))?;

        let source_relays = if let Ok(file_path) = env::var("SOURCE_RELAYS_FILE") {
            let relays = load_relays_from_file(&file_path)?;
            tracing::info!("Loaded {} relays from file: {}", relays.len(), file_path);
            relays
        } else {
            let source_relays_str = env::var("SOURCE_RELAYS").map_err(|_| {
                ConfigError::MissingVariable("SOURCE_RELAYS or SOURCE_RELAYS_FILE".to_string())
            })?;
            source_relays_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        };

        if source_relays.is_empty() {
            return Err(ConfigError::InvalidValue(
                "SOURCE_RELAYS".to_string(),
                "must contain at least one relay URL".to_string(),
            ));
        }

        let target_relays = if let Ok(target_relays_str) = env::var("TARGET_RELAYS") {
            target_relays_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<String>>()
        } else {
            vec![env::var("TARGET_RELAY").map_err(|_| {
                ConfigError::MissingVariable("TARGET_RELAY or TARGET_RELAYS".to_string())
            })?]
        };

        if target_relays.is_empty() {
            return Err(ConfigError::InvalidValue(
                "TARGET_RELAYS".to_string(),
                "must contain at least one relay URL".to_string(),
            ));
        }

        let job_interval_hours = env::var("JOB_INTERVAL_HOURS")
            .unwrap_or_else(|_| "1".to_string())
            .parse::<u64>()
            .map_err(|e: std::num::ParseIntError| {
                ConfigError::InvalidValue("JOB_INTERVAL_HOURS".to_string(), e.to_string())
            })?;

        let url_check_concurrency = env::var("URL_CHECK_CONCURRENCY")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<usize>()
            .map_err(|e: std::num::ParseIntError| {
                ConfigError::InvalidValue("URL_CHECK_CONCURRENCY".to_string(), e.to_string())
            })?;

        let url_check_timeout_secs = env::var("URL_CHECK_TIMEOUT_SECS")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<u64>()
            .map_err(|e: std::num::ParseIntError| {
                ConfigError::InvalidValue("URL_CHECK_TIMEOUT_SECS".to_string(), e.to_string())
            })?;

        let url_check_delay_ms = env::var("URL_CHECK_DELAY_MS")
            .unwrap_or_else(|_| "100".to_string())
            .parse::<u64>()
            .map_err(|e: std::num::ParseIntError| {
                ConfigError::InvalidValue("URL_CHECK_DELAY_MS".to_string(), e.to_string())
            })?;

        let backfill_batch_size = env::var("BACKFILL_BATCH_SIZE")
            .unwrap_or_else(|_| "500".to_string())
            .parse::<usize>()
            .map_err(|e: std::num::ParseIntError| {
                ConfigError::InvalidValue("BACKFILL_BATCH_SIZE".to_string(), e.to_string())
            })?;

        let backfill_max_events = env::var("BACKFILL_MAX_EVENTS")
            .unwrap_or_else(|_| "10000".to_string())
            .parse::<usize>()
            .map_err(|e: std::num::ParseIntError| {
                ConfigError::InvalidValue("BACKFILL_MAX_EVENTS".to_string(), e.to_string())
            })?;

        let relay_concurrency = env::var("RELAY_CONCURRENCY")
            .unwrap_or_else(|_| "50".to_string())
            .parse::<usize>()
            .map_err(|e: std::num::ParseIntError| {
                ConfigError::InvalidValue("RELAY_CONCURRENCY".to_string(), e.to_string())
            })?;

        let related_events_batch_size = env::var("RELATED_EVENTS_BATCH_SIZE")
            .unwrap_or_else(|_| "100".to_string())
            .parse::<usize>()
            .map_err(|e: std::num::ParseIntError| {
                ConfigError::InvalidValue("RELATED_EVENTS_BATCH_SIZE".to_string(), e.to_string())
            })?;

        Ok(Config {
            database_url,
            source_relays,
            target_relays,
            job_interval_hours,
            url_check_concurrency,
            url_check_timeout_secs,
            url_check_delay_ms,
            backfill_batch_size,
            backfill_max_events,
            relay_concurrency,
            related_events_batch_size,
        })
    }
}
