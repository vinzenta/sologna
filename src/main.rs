mod decoder;
mod sink;
mod ws;

use crate::sink::SinkOptions;
use serde::Deserialize;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Deserialize)]
struct Config {
    target_address: String,
    idl_file: Option<String>,
    commitment: Option<String>,
    #[serde(default)]
    run: RunConfig,
    #[serde(default)]
    sink: SinkConfig,
}
#[derive(Debug, Deserialize)]
struct RunConfig {
    #[serde(default = "default_log_level")]
    log_level: String,
    #[serde(default = "default_heartbeat")]
    heartbeat_interval_ms: u64,
    #[serde(default = "default_read_timeout")]
    read_timeout_ms: u64,
    #[serde(default = "default_backoff_base")]
    backoff_base_ms: u64,
    #[serde(default = "default_backoff_max")]
    backoff_max_ms: u64,
    #[serde(default = "default_backoff_jitter")]
    backoff_jitter: f64,
    #[serde(default = "default_decode_queue")]
    decode_queue_capacity: usize,
    #[serde(default = "default_decode_conc")]
    decode_concurrency: usize,
}

#[derive(Debug, Deserialize)]
struct SinkConfig {
    #[serde(default = "default_sink_type")]
    r#type: String,
    #[serde(default = "default_sink_addr")]
    addr: String,
    #[serde(default = "default_sink_queue")]
    queue_capacity: usize,
    #[serde(default = "default_sink_timeout")]
    write_timeout_ms: u64,
}

fn default_heartbeat() -> u64 {
    10_000
}
fn default_read_timeout() -> u64 {
    30_000
}
fn default_backoff_base() -> u64 {
    1_000
}
fn default_backoff_max() -> u64 {
    30_000
}
fn default_backoff_jitter() -> f64 {
    0.3
}
fn default_decode_queue() -> usize {
    256
}
fn default_decode_conc() -> usize {
    2
}
fn default_sink_type() -> String {
    "stdout".to_string()
}
fn default_sink_addr() -> String {
    #[cfg(windows)]
    {
        "\\\\.\\pipe\\sologna".to_string()
    }
    #[cfg(not(windows))]
    {
        "/tmp/sologna.sock".to_string()
    }
}
fn default_sink_queue() -> usize {
    1024
}
fn default_sink_timeout() -> u64 {
    1000
}
fn default_log_level() -> String {
    "info".to_string()
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            log_level: default_log_level(),
            heartbeat_interval_ms: default_heartbeat(),
            read_timeout_ms: default_read_timeout(),
            backoff_base_ms: default_backoff_base(),
            backoff_max_ms: default_backoff_max(),
            backoff_jitter: default_backoff_jitter(),
            decode_queue_capacity: default_decode_queue(),
            decode_concurrency: default_decode_conc(),
        }
    }
}

impl Default for SinkConfig {
    fn default() -> Self {
        Self {
            r#type: default_sink_type(),
            addr: default_sink_addr(),
            queue_capacity: default_sink_queue(),
            write_timeout_ms: default_sink_timeout(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load environment variables first (before tracing initialization)
    if let Ok(path) = dotenv::dotenv() {
        info!("📄 Loaded .env file from: {:?}", path);
    }

    // Initialize tracing (now RUST_LOG will be available)
    // Temporary placeholder; we'll re-init after reading config.toml to use configured log_level
    let _ = fmt()
        .with_env_filter(EnvFilter::new("info"))
        .compact()
        .try_init();

    // Load configuration from config.toml
    let config_content = match std::fs::read_to_string("config.toml") {
        Ok(content) => content,
        Err(e) => {
            error!("Failed to read config.toml: {}", e);
            error!("Please ensure config.toml exists in the project root");
            error!("Example config.toml content:");
            error!("  target_address = \"675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8\"");
            error!("  idl_file = \"idl/pumpamm.json\"  # optional");
            error!("  commitment = \"processed\"       # optional");
            std::process::exit(1);
        }
    };

    let config: Config = match toml::from_str(&config_content) {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to parse config.toml: {}", e);
            error!("Please check your config.toml format");
            std::process::exit(1);
        }
    };

    info!("🚀 Starting Sologna...");
    info!("📋 Loaded configuration from config.toml");
    info!(target_address = %config.target_address, "🎯 Monitoring logs for target address");
    if let Some(ref idl) = config.idl_file {
        info!(idl_file = %idl, "📄 Using IDL file for event decoding");
    } else {
        info!("📝 No IDL file specified - showing raw logs only");
    }
    // Determine commitment level (processed | confirmed | finalized)
    let commitment = match config.commitment.as_deref() {
        Some("processed") | None => "processed",
        Some("confirmed") => "confirmed",
        Some("finalized") => "finalized",
        Some(other) => {
            error!(commitment = %other, "Invalid commitment in config.toml; defaulting to 'processed'");
            "processed"
        }
    };
    // Re-initialize logging with configured log level
    let _ = fmt()
        .with_env_filter(EnvFilter::new(config.run.log_level.clone()))
        .compact()
        .try_init();
    info!(commitment = %commitment, log_level = %config.run.log_level, "📌 Using commitment level and log level");

    // Build SinkOptions from config only (env is for secrets only)
    let sink_opts = SinkOptions {
        kind: config.sink.r#type.clone(),
        addr: config.sink.addr.clone(),
        queue_capacity: config.sink.queue_capacity,
        write_timeout_ms: config.sink.write_timeout_ms,
        backoff_base_ms: config.run.backoff_base_ms,
        backoff_max_ms: config.run.backoff_max_ms,
        backoff_jitter: config.run.backoff_jitter,
    };

    // Run options from config
    let heartbeat_ms = config.run.heartbeat_interval_ms;
    let read_timeout_ms = config.run.read_timeout_ms;
    let backoff_base_ms = config.run.backoff_base_ms;
    let backoff_max_ms = config.run.backoff_max_ms;
    let backoff_jitter = config.run.backoff_jitter;
    let decode_queue_capacity = config.run.decode_queue_capacity;
    let decode_concurrency = config.run.decode_concurrency;

    // Pass options to ws
    let idl_path = config.idl_file.as_deref();
    ws::run_logs_subscription_with_opts(
        &config.target_address,
        idl_path,
        commitment,
        sink_opts,
        heartbeat_ms,
        read_timeout_ms,
        backoff_base_ms,
        backoff_max_ms,
        backoff_jitter,
        decode_queue_capacity,
        decode_concurrency,
    )
    .await
}
