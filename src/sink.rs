use rand::Rng;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{self, timeout};
use tracing::{info, warn};

/// Spawn a background writer that streams newline-delimited messages to a local socket,
/// TCP, or stdout as configured.
#[derive(Clone, Debug)]
pub struct SinkOptions {
    pub kind: String,          // "stdout" | "tcp" | "local" | "none"
    pub addr: String,          // path/pipe name or host:port
    pub queue_capacity: usize, // sink queue capacity
    pub write_timeout_ms: u64, // write timeout
    pub backoff_base_ms: u64,
    pub backoff_max_ms: u64,
    pub backoff_jitter: f64,
}

pub fn spawn_sink_writer(opts: SinkOptions) -> Option<mpsc::Sender<String>> {
    if opts.kind.eq_ignore_ascii_case("none") {
        info!("Event sink disabled (kind=none)");
        return None;
    }

    let capacity = opts.queue_capacity;
    let write_timeout_ms = opts.write_timeout_ms;
    let backoff_base_ms = opts.backoff_base_ms;
    let backoff_max_ms = opts.backoff_max_ms;
    let backoff_jitter = opts.backoff_jitter;

    let (tx, mut rx) = mpsc::channel::<String>(capacity);

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

    match opts.kind.as_str() {
        kind if kind.eq_ignore_ascii_case("stdout") => {
            info!(capacity = %capacity, "Starting stdout event sink");
            tokio::spawn(async move {
                while let Some(line) = rx.recv().await {
                    println!("{}", line);
                }
                info!("Stdout event sink exiting (channel closed)");
            });
            Some(tx)
        }
        kind if kind.eq_ignore_ascii_case("tcp") => {
            let addr = opts.addr.clone();
            info!(addr = %addr, capacity = %capacity, timeout_ms = %write_timeout_ms, "Starting TCP event sink");
            tokio::spawn(async move {
                let mut attempt: u32 = 0;
                loop {
                    match TcpStream::connect(&addr).await {
                        Ok(mut stream) => {
                            if let Err(e) = stream.set_nodelay(true) {
                                warn!(error = %e, "Failed to set TCP_NODELAY");
                            }
                            info!(addr = %addr, "TCP sink connected");
                            attempt = 0;
                            while let Some(line) = rx.recv().await {
                                let data = format!("{}\n", line);
                                match timeout(
                                    Duration::from_millis(write_timeout_ms),
                                    stream.write_all(data.as_bytes()),
                                )
                                .await
                                {
                                    Ok(Ok(())) => {}
                                    Ok(Err(e)) => {
                                        warn!(error = %e, "TCP sink write failed; reconnecting");
                                        break;
                                    }
                                    Err(_) => {
                                        warn!(timeout_ms = %write_timeout_ms, "TCP sink write timed out; reconnecting");
                                        break;
                                    }
                                }
                            }
                            // fallthrough to reconnect
                        }
                        Err(e) => {
                            let backoff = compute_backoff_ms(
                                attempt,
                                backoff_base_ms,
                                backoff_max_ms,
                                backoff_jitter,
                            );
                            warn!(error = %e, backoff_ms = %backoff, "TCP sink connect failed; retrying");
                            time::sleep(Duration::from_millis(backoff)).await;
                            attempt = attempt.saturating_add(1);
                        }
                    }
                    if rx.is_closed() && rx.try_recv().is_err() {
                        info!("TCP event sink exiting (channel closed)");
                        break;
                    }
                }
            });
            Some(tx)
        }
        _ => {
            let addr = opts.addr.clone();
            info!(addr = %addr, capacity = %capacity, timeout_ms = %write_timeout_ms, "Starting LocalSocket event sink");
            tokio::spawn(async move {
                #[cfg(any(unix, windows))]
                {
                    use interprocess::local_socket::GenericFilePath;
                    use interprocess::local_socket::ToFsName as _;
                    use interprocess::local_socket::tokio::prelude::LocalSocketStream;
                    use interprocess::local_socket::traits::tokio::Stream as _;
                    let mut attempt: u32 = 0;
                    loop {
                        let name = match addr.as_str().to_fs_name::<GenericFilePath>() {
                            Ok(n) => n,
                            Err(e) => {
                                let backoff = compute_backoff_ms(
                                    attempt,
                                    backoff_base_ms,
                                    backoff_max_ms,
                                    backoff_jitter,
                                );
                                warn!(error = %e, addr = %addr, backoff_ms = %backoff, "Invalid local socket name; retrying");
                                time::sleep(Duration::from_millis(backoff)).await;
                                attempt = attempt.saturating_add(1);
                                continue;
                            }
                        };
                        match LocalSocketStream::connect(name).await {
                            Ok(mut stream) => {
                                info!(addr = %addr, "Local socket sink connected");
                                attempt = 0;
                                while let Some(line) = rx.recv().await {
                                    let data = format!("{}\n", line);
                                    match timeout(
                                        Duration::from_millis(write_timeout_ms),
                                        stream.write_all(data.as_bytes()),
                                    )
                                    .await
                                    {
                                        Ok(Ok(())) => {}
                                        Ok(Err(e)) => {
                                            warn!(error = %e, "Local socket write failed; reconnecting");
                                            break;
                                        }
                                        Err(_) => {
                                            warn!(timeout_ms = %write_timeout_ms, "Local socket write timed out; reconnecting");
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                let backoff = compute_backoff_ms(
                                    attempt,
                                    backoff_base_ms,
                                    backoff_max_ms,
                                    backoff_jitter,
                                );
                                warn!(addr = %addr, error = %e, backoff_ms = %backoff, "Local socket connect failed; retrying");
                                time::sleep(Duration::from_millis(backoff)).await;
                                attempt = attempt.saturating_add(1);
                            }
                        }
                        if rx.is_closed() && rx.try_recv().is_err() {
                            info!("Local socket event sink exiting (channel closed)");
                            break;
                        }
                    }
                }
                #[cfg(not(any(unix, windows)))]
                {
                    tracing::error!(
                        "Local sockets not supported on this platform; set SINK=tcp or SINK=stdout"
                    );
                    while let Some(line) = rx.recv().await {
                        println!("{}", line);
                    }
                }
            });
            Some(tx)
        }
    }
}

//
