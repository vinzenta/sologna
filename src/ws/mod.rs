use crate::decoder::IdlEventDecoder;
use crate::sink::{SinkOptions, spawn_sink_writer};
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use serde_json::json;
use std::env;
use std::sync::Arc;
use tokio::signal;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::{self, Duration, Instant};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn}; // for HELIUS_API_KEY secret only

mod decode_worker;
mod stack;

/// Subscribe to logs mentioning a target address and decode events using IDL
#[allow(clippy::too_many_arguments)]
pub async fn run_logs_subscription_with_opts(
    target_address: &str,
    idl_path: Option<&str>,
    commitment: &str,
    sink_opts: SinkOptions,
    heartbeat_ms: u64,
    read_timeout_ms_cfg: u64,
    backoff_base_ms: u64,
    backoff_max_ms: u64,
    backoff_jitter: f64,
    decode_queue_capacity: usize,
    decode_concurrency: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // Statistics for debugging
    let mut total_transactions = 0u64;
    let mut matching_transactions = 0u64;
    let mut non_matching_transactions = 0u64;
    // Load environment variables
    dotenv::dotenv().ok();

    let api_key = match env::var("HELIUS_API_KEY") {
        Ok(key) => {
            if key == "your_helius_api_key_here" || key.is_empty() {
                error!("HELIUS_API_KEY not set properly in .env file");
                error!("Please set your actual API key in the .env file:");
                error!("  HELIUS_API_KEY=your_actual_api_key_here");
                error!("Get your API key from: https://dashboard.helius.dev/");
                return Err("Please set your actual API key in the .env file".into());
            }
            key
        }
        Err(_) => {
            error!("HELIUS_API_KEY not found in environment");
            error!("Please make sure your .env file contains:");
            error!("  HELIUS_API_KEY=your_actual_api_key_here");
            error!("Get your API key from: https://dashboard.helius.dev/");
            return Err("HELIUS_API_KEY environment variable not set".into());
        }
    };

    let ws_url = format!("wss://mainnet.helius-rpc.com/?api-key={}", api_key);

    // Initialize decoder if IDL path is provided
    let decoder: Option<Arc<IdlEventDecoder>> = if let Some(idl_path) = idl_path {
        match IdlEventDecoder::from_idl_path(idl_path) {
            Ok(d) => {
                info!(idl_path = %idl_path, "📄 Loaded IDL file for event decoding");
                Some(Arc::new(d))
            }
            Err(e) => {
                warn!(idl_path = %idl_path, error = %e, "Failed to load IDL file, continuing without event decoding");
                None
            }
        }
    } else {
        info!("No IDL file provided - logs will not be decoded");
        None
    };

    info!("🚀 Starting Helius logs subscription...");
    info!(target_address = %target_address, "📍 Monitoring target address");
    info!("🌐 Preparing WebSocket connection to Helius API");

    let mut attempt: u32 = 0;

    // Spawn sink writer using provided options
    let sink_tx_opt: Option<mpsc::Sender<String>> = spawn_sink_writer(sink_opts);

    // Spawn a decoding worker/manager if decoder is available
    let mut decode_tx_opt: Option<mpsc::Sender<(String, Vec<String>)>> = None;
    if let Some(decoder_arc) = decoder.clone() {
        let tx = decode_worker::spawn_decode_worker(
            decoder_arc,
            decode_queue_capacity,
            decode_concurrency,
            sink_tx_opt.clone(),
            target_address.to_string(),
            commitment.to_string(),
        );
        decode_tx_opt = Some(tx);
    }

    // Heartbeat/read-timeout/backoff are provided via args now

    fn compute_backoff_ms(attempt: u32, base_ms: u64, max_ms: u64, jitter: f64) -> u64 {
        let exp = 1u64 << attempt.min(20);
        let raw = base_ms.saturating_mul(exp);
        let capped = raw.min(max_ms);
        if jitter > 0.0 {
            let mut rng = rand::thread_rng();
            let factor: f64 = rng.gen_range(1.0 - jitter..=1.0 + jitter);
            ((capped as f64) * factor).round() as u64
        } else {
            capped
        }
    }

    loop {
        info!("🔌 Connecting to Helius...");

        let (ws_stream, _response) = match tokio_tungstenite::connect_async(&ws_url).await {
            Ok(res) => res,
            Err(err) => {
                let backoff_ms =
                    compute_backoff_ms(attempt, backoff_base_ms, backoff_max_ms, backoff_jitter);
                warn!(error = %err, backoff_ms = %backoff_ms, "Connection failed, retrying...");
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                attempt = attempt.saturating_add(1);
                continue;
            }
        };

        attempt = 0;
        info!("✅ Connected successfully!");
        info!(target_address = %target_address, "📋 Subscribing to logs mentioning target address");

        let (mut write, mut read) = ws_stream.split();
        // Track subscription id so we can unsubscribe on shutdown
        let mut subscription_id: Option<u64> = None;

        let subscribe_request = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "logsSubscribe",
            "params": [
                { "mentions": [target_address] },
                { "commitment": commitment }
            ]
        });

        let subscription_message = subscribe_request.to_string();
        // Pretty print the subscription request for better readability
        if let Ok(pretty_subscription) =
            serde_json::from_str::<serde_json::Value>(&subscription_message)
        {
            info!(target_address = %target_address, subscription = %serde_json::to_string_pretty(&pretty_subscription).unwrap_or_else(|_| subscription_message.clone()), "📡 Sending subscription request");
        } else {
            info!(target_address = %target_address, subscription = %subscription_message, "📡 Sending subscription request");
        }
        debug!(
            "Subscription filter details: mentions=[{}], commitment={}",
            target_address, commitment
        );

        if let Err(err) = write.send(Message::Text(subscription_message.into())).await {
            error!(error = %err, "Failed to send subscription request");
            continue;
        }

        info!("📡 Subscription request sent. Listening for logs...");
        info!("💡 Press CTRL+C to gracefully shutdown");

        // Set up CTRL+C handling
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        tokio::spawn(async move {
            if let Ok(()) = signal::ctrl_c().await {
                let _ = shutdown_tx.send(());
            }
        });

        // Heartbeat interval (consume the immediate first tick to avoid instant ping)
        let effective_hb_ms = if heartbeat_ms == 0 {
            315_360_000_000
        } else {
            heartbeat_ms
        }; // ~10 years
        let mut heartbeat = time::interval(Duration::from_millis(effective_hb_ms));
        if heartbeat_ms == 0 {
            heartbeat.tick().await;
        }

        // Read inactivity timeout timer
        let effective_to_ms = if read_timeout_ms_cfg == 0 {
            315_360_000_000
        } else {
            read_timeout_ms_cfg
        };
        let inactivity = time::sleep(Duration::from_millis(effective_to_ms));
        tokio::pin!(inactivity);

        loop {
            tokio::select! {
                // Handle WebSocket messages
                next = read.next() => {
                    match next {
                        Some(Ok(Message::Text(text))) => {
                            // Reset inactivity timer on any received message
                            inactivity.as_mut().reset(Instant::now() + Duration::from_millis(effective_to_ms));
                            // Parse once
                            let parsed: serde_json::Value = match serde_json::from_str(&text) {
                                Ok(v) => v,
                                Err(_) => {
                                    debug!(message = %text, "📨 Raw WebSocket message");
                                    continue;
                                }
                            };
                            // Pretty log full JSON (trace only)
                            if tracing::enabled!(tracing::Level::TRACE) {
                                tracing::trace!(raw_json = %serde_json::to_string_pretty(&parsed).unwrap_or_else(|_| text.to_string()), "Full raw JSON response");
                            }

                            // Handle subscription response to our initial request (id == 1)
                            if parsed.get("id").and_then(|v| v.as_u64()) == Some(1) {
                                if let Some(result) = parsed.get("result") {
                                    if let Some(sub_id) = result.as_u64() {
                                        subscription_id = Some(sub_id);
                                        info!(subscription_id = %sub_id, "📬 Subscription response received");
                                    } else {
                                        info!(result = ?result, "📬 Subscription response received");
                                    }
                                } else if let Some(error) = parsed.get("error") {
                                    error!(error = ?error, "❌ Subscription request failed");
                                }
                                continue;
                            }

                            // Handle logsNotification
                            if parsed.get("method").and_then(|m| m.as_str()) == Some("logsNotification") {
                                let params = match parsed.get("params") { Some(p) => p, None => { debug!("logsNotification missing params"); continue; } };
                                let value = match params.get("result").and_then(|r| r.get("value")) { Some(v) => v, None => { debug!("logsNotification missing result.value"); continue; } };
                                let sig = match value.get("signature").and_then(|s| s.as_str()) { Some(s) => s.to_string(), None => { debug!("logsNotification missing signature"); continue; } };
                                let logs_arr = match value.get("logs").and_then(|l| l.as_array()) { Some(a) => a, None => { debug!("logsNotification missing logs array"); continue; } };
                                let logs_slices: Vec<&str> = logs_arr.iter().filter_map(|v| v.as_str()).collect();

                                total_transactions += 1;

                                // Check if logs actually mention the target address
                                let contains_target = logs_slices.iter().any(|log| log.contains(target_address));
                                if !contains_target {
                                    // Check for alternative patterns
                                    let contains_program = logs_slices.iter().any(|log|
                                        log.contains("Program") && (
                                            log.contains(&target_address[..8.min(target_address.len())]) ||  // First 8 chars
                                            log.contains(&target_address[target_address.len().saturating_sub(8)..]) || // Last 8 chars
                                            log.contains("invoke") ||
                                            log.contains("success") ||
                                            log.contains("consumed")
                                        )
                                    );

                                    if contains_program {
                                        debug!(
                                            signature = %sig,
                                            log_count = %logs_slices.len(),
                                            "📝 Transaction involves target program (indirect mention)"
                                        );
                                        matching_transactions += 1;
                                    } else {
                                        non_matching_transactions += 1;
                                        debug!(
                                            signature = %sig,
                                            target_address = %target_address,
                                            log_count = %logs_slices.len(),
                                            "Transaction had no target address mention"
                                        );
                                        tracing::trace!(logs = ?logs_slices, "Full log content for non-matching transaction");
                                        // Also show full response at trace
                                        tracing::trace!(full_response = %serde_json::to_string_pretty(&parsed).unwrap_or_else(|_| text.to_string()), "Complete JSON response for non-matching transaction");
                                    }
                                } else {
                                    matching_transactions += 1;
                                    debug!(signature = %sig, log_count = %logs_slices.len(), "New transaction logs received");
                                }

                                // Log statistics every 100 transactions
                                if total_transactions % 100 == 0 {
                                    let match_percentage = if total_transactions > 0 {
                                        (matching_transactions as f64 / total_transactions as f64) * 100.0
                                    } else {
                                        0.0
                                    };
                                    info!(
                                        total = %total_transactions,
                                        matching = %matching_transactions,
                                        non_matching = %non_matching_transactions,
                                        match_percentage = %format!("{:.1}%", match_percentage),
                                        "📊 Transaction statistics"
                                    );
                                }

                                // Extract and decode program data entries via guarded call-stack parser
                                if decode_tx_opt.is_some() {
                                    let extracted_data = stack::extract_program_data_for_target(&logs_slices, target_address);
                                    if !extracted_data.is_empty()
                                        && let Some(ref decode_tx) = decode_tx_opt
                                    {
                                            // Clone only here when enqueuing to worker (convert &str -> String)
                                            let extracted_owned: Vec<String> = extracted_data.iter().map(|s| (*s).to_string()).collect();
                                            // Try to send to decode worker; drop if channel is full to protect the reader
                                            match decode_tx.try_send((sig.clone(), extracted_owned)) {
                                                Ok(_) => { /* queued */ }
                                                Err(mpsc::error::TrySendError::Full(_)) => {
                                                    warn!(signature = %sig, "Decode queue full, dropping extracted data");
                                                }
                                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                                    warn!(signature = %sig, "Decode queue closed, skipping decoding");
                                                }
                                            }
                                    }
                                    // We no longer send raw program data to sink; decoded events are sent by the decode worker.
                                }

                                // Show raw logs at trace level only
                                if tracing::enabled!(tracing::Level::TRACE) {
                                    for (idx, log_line) in logs_slices.iter().enumerate() {
                                        tracing::trace!(index = %idx, log = %log_line, "Raw log line");
                                    }
                                }

                                continue;
                            }

                            // Any other JSON message (trace only)
                            if tracing::enabled!(tracing::Level::TRACE) {
                                tracing::trace!(json = %serde_json::to_string_pretty(&parsed).unwrap_or_else(|_| text.to_string()), "📨 JSON WebSocket message");
                            }
                        }
                        Some(Ok(Message::Binary(bin))) => {
                            inactivity.as_mut().reset(Instant::now() + Duration::from_millis(effective_to_ms));
                            debug!(size = %bin.len(), "📦 Binary message received");
                        }
                        Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {
                            inactivity.as_mut().reset(Instant::now() + Duration::from_millis(effective_to_ms));
                            // These are handled automatically by tungstenite
                            debug!("Ping/Pong message received");
                        }
                        Some(Ok(Message::Close(frame))) => {
                            info!(frame = ?frame, "🔌 WebSocket connection closed");
                            break;
                        }
                        Some(Ok(Message::Frame(_))) => {
                            // Frame messages are internal
                            debug!("WebSocket frame message");
                        }
                        Some(Err(err)) => {
                            error!(error = %err, "❌ WebSocket error occurred");
                            break;
                        }
                        None => {
                            info!("🔌 WebSocket stream ended");
                            break;
                        }
                    }
                }

                // Heartbeat tick: send a ping periodically
                _ = heartbeat.tick() => {
                    if heartbeat_ms > 0 {
                        if let Err(e) = write.send(Message::Ping(Vec::new().into())).await {
                            warn!("Heartbeat ping failed: {}", e);
                            break;
                        } else {
                            debug!("Heartbeat ping sent");
                        }
                    }
                }

                // Handle CTRL+C signal
                _ = &mut shutdown_rx => {
                    info!("🛑 Received CTRL+C, gracefully shutting down...");
                    // Attempt to unsubscribe before closing the socket
                    if let Some(sub_id) = subscription_id.take() {
                        let unsubscribe_request = json!({
                            "jsonrpc": "2.0",
                            "id": 2,
                            "method": "logsUnsubscribe",
                            "params": [sub_id]
                        });
                        let unsubscribe_msg = unsubscribe_request.to_string();
                        debug!(subscription_id = %sub_id, request = %unsubscribe_msg, "Sending logsUnsubscribe request");
                        if let Err(e) = write.send(Message::Text(unsubscribe_msg.into())).await {
                            warn!("Failed to send logsUnsubscribe: {}", e);
                        }
                    }
                    // Send close message
                    if let Err(e) = write.send(Message::Close(None)).await {
                        warn!("Failed to send close message: {}", e);
                    }
                    info!("👋 WebSocket connection closed gracefully");
                    return Ok(());
                }

                // Inactivity timeout fired: reconnect
                _ = &mut inactivity => {
                    warn!(timeout_ms = %effective_to_ms, "Read inactivity timeout; reconnecting");
                    break;
                }
            }
        }

        // Exponential backoff before reconnect
        attempt = attempt.saturating_add(1);
        let backoff_ms =
            compute_backoff_ms(attempt, backoff_base_ms, backoff_max_ms, backoff_jitter);
        info!(backoff_ms = %backoff_ms, attempt = %attempt, "🔄 Disconnected, reconnecting with exponential backoff");
        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
    }
}
