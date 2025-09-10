use std::env;

use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Logging init
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).compact().init();

    // Resolve sink kind and address
    let sink_kind = env::var("SINK").unwrap_or_else(|_| "local".to_string());
    let addr = env::var("SINK_ADDR").unwrap_or_else(|_| default_addr());
    info!(%sink_kind, %addr, "Starting sink_reader");

    platform::run(&sink_kind, &addr).await
}
#[cfg(windows)]
fn default_addr() -> String {
    r"\\.\pipe\sologna".to_string()
}
#[cfg(unix)]
fn default_addr() -> String {
    "/tmp/sologna.sock".to_string()
}

#[cfg(windows)]
mod platform {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
    use tokio::net::{TcpListener, TcpStream};

    pub async fn run(sink_kind: &str, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        match sink_kind {
            k if k.eq_ignore_ascii_case("tcp") => run_tcp_server(addr).await?,
            k if k.eq_ignore_ascii_case("stdout") => {
                warn!("SINK=stdout produces no socket; set SINK=local or SINK=tcp");
            }
            _ => run_local_server(addr).await?,
        }
        Ok(())
    }

    async fn run_tcp_server(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(addr).await?;
        info!(%addr, "TCP server listening");
        loop {
            let (stream, peer) = listener.accept().await?;
            info!(?peer, "TCP client connected");
            tokio::spawn(async move {
                if let Err(e) = handle_tcp(stream).await {
                    warn!(error = %e, "TCP client handler ended with error");
                }
            });
        }
    }

    async fn handle_tcp(stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
        let reader = BufReader::new(stream);
        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await? {
            info!(json = %line, "📥 Event");
        }
        Ok(())
    }

    async fn run_local_server(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        info!(%addr, "Named pipe server listening");
        loop {
            let server = ServerOptions::new().create(addr)?;
            tokio::spawn(async move {
                if let Err(e) = handle_pipe(server).await {
                    warn!(error = %e, "Named pipe client handler ended with error");
                }
            });
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn handle_pipe(server: NamedPipeServer) -> Result<(), Box<dyn std::error::Error>> {
        server.connect().await?;
        let reader = BufReader::new(server);
        tokio::pin!(reader);
        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await? {
            info!(json = %line, "📥 Event");
        }
        Ok(())
    }
}

#[cfg(unix)]
mod platform {
    use super::*;
    use tokio::io::{AsyncBufReadExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};

    pub async fn run(sink_kind: &str, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        match sink_kind {
            k if k.eq_ignore_ascii_case("tcp") => run_tcp_server(addr).await?,
            k if k.eq_ignore_ascii_case("stdout") => {
                warn!("SINK=stdout produces no socket; set SINK=local or SINK=tcp");
            }
            _ => run_local_server(addr).await?,
        }
        Ok(())
    }

    async fn run_tcp_server(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(addr).await?;
        info!(%addr, "TCP server listening");
        loop {
            let (stream, peer) = listener.accept().await?;
            info!(?peer, "TCP client connected");
            tokio::spawn(async move {
                if let Err(e) = handle_tcp(stream).await {
                    warn!(error = %e, "TCP client handler ended with error");
                }
            });
        }
    }

    async fn handle_tcp(stream: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
        let reader = BufReader::new(stream);
        let mut lines = reader.lines();
        while let Some(line) = lines.next_line().await? {
            info!(json = %line, "📥 Event");
        }
        Ok(())
    }

    async fn run_local_server(addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        use tokio::net::UnixListener;
        if let Err(e) = std::fs::remove_file(addr)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::error!(%addr, error = %e, "Failed to remove existing socket path");
        }
        let listener = UnixListener::bind(addr)?;
        info!(%addr, "Unix socket server listening");
        loop {
            let (stream, _sockaddr) = listener.accept().await?;
            tokio::spawn(async move {
                let reader = BufReader::new(stream);
                tokio::pin!(reader);
                let mut lines = reader.lines();
                while let Some(line) = lines.next_line().await.unwrap_or(None) {
                    info!(json = %line, "📥 Event");
                }
            });
        }
    }
}
