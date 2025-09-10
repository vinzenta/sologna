use crate::decoder::IdlEventDecoder;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, error, info, warn};

pub fn spawn_decode_worker(
    decoder: Arc<IdlEventDecoder>,
    capacity: usize,
    concurrency: usize,
    sink_tx: Option<mpsc::Sender<String>>,
    program_id: String,
    commitment: String,
) -> mpsc::Sender<(String, Vec<String>)> {
    let (tx, mut rx) = mpsc::channel::<(String, Vec<String>)>(capacity);
    let max_concurrency = concurrency.max(1);
    let semaphore = Arc::new(Semaphore::new(max_concurrency));
    tokio::spawn({
        let semaphore = semaphore.clone();
        async move {
            info!(workers = %max_concurrency, "🧵 Decode manager started with concurrency");
            while let Some((sig, extracted_data)) = rx.recv().await {
                if extracted_data.is_empty() {
                    continue;
                }
                let permit = semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("semaphore not to close");
                let decoder_arc = decoder.clone();
                let sink_tx_cloned = sink_tx.clone();
                let program_id_cloned = program_id.clone();
                let commitment_cloned = commitment.clone();
                tokio::spawn(async move {
                    // hold permit for the duration of this task
                    let _permit = permit;
                    info!(signature = %sig, data_entries = %extracted_data.len(), "🔍 Decoding program data entries...");
                    for (idx, b64) in extracted_data.iter().enumerate() {
                        match STANDARD.decode(b64) {
                            Ok(bytes) => match decoder_arc.decode(bytes.as_slice()) {
                                Ok((event_name, event_data)) => {
                                    info!(event_name = %event_name, signature = %sig, "✅ Decoded event");
                                    debug!(index = %idx, event_data = ?event_data, "Decoded event data");
                                    if let Some(ref sink) = sink_tx_cloned {
                                        let payload = json!({
                                            "signature": sig,
                                            "commitment": commitment_cloned,
                                            "program": program_id_cloned,
                                            "event_name": event_name,
                                            "event_data": event_data,
                                        })
                                        .to_string();
                                        if let Err(e) = sink.try_send(payload) {
                                            match e {
                                                mpsc::error::TrySendError::Full(_) => {
                                                    warn!("Sink queue full, dropping decoded event")
                                                }
                                                mpsc::error::TrySendError::Closed(_) => warn!(
                                                    "Sink queue closed, dropping decoded event"
                                                ),
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(index = %idx, signature = %sig, error = %e, "Failed to decode event");
                                    if bytes.len() >= 8 {
                                        debug!(discriminator = ?&bytes[..8], "Event discriminator bytes");
                                    }
                                }
                            },
                            Err(e) => {
                                warn!(index = %idx, signature = %sig, error = %e, "Failed to decode base64, trying base58 fallback");
                                if let Ok(bytes_bs58) = bs58::decode(b64).into_vec() {
                                    debug!("Attempting base58 decoding...");
                                    match decoder_arc.decode(bytes_bs58.as_slice()) {
                                        Ok((event_name, event_data)) => {
                                            info!(event_name = %event_name, signature = %sig, "✅ Decoded event (base58)");
                                            debug!(index = %idx, event_data = ?event_data, "Decoded event data");
                                            if let Some(ref sink) = sink_tx_cloned {
                                                let payload = json!({
                                                    "signature": sig,
                                                    "commitment": commitment_cloned,
                                                    "program": program_id_cloned,
                                                    "event_name": event_name,
                                                    "event_data": event_data,
                                                })
                                                .to_string();
                                                if let Err(e) = sink.try_send(payload) {
                                                    match e {
                                                        mpsc::error::TrySendError::Full(_) => {
                                                            warn!(
                                                                "Sink queue full, dropping decoded event"
                                                            )
                                                        }
                                                        mpsc::error::TrySendError::Closed(_) => {
                                                            warn!(
                                                                "Sink queue closed, dropping decoded event"
                                                            )
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(_) => {
                                            error!(index = %idx, signature = %sig, "Both base64 and base58 decoding failed");
                                        }
                                    }
                                }
                            }
                        }
                    }
                });
            }
            info!("🧵 Decode manager exiting (channel closed)");
        }
    });
    tx
}
