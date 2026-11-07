use clap::Parser;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use veltrix_engine::Engine;
use veltrix_server::{Config, Server, bind};

#[derive(Debug, Parser)]
#[command(name = "veltrix-server", about = "Run the VeltrixDB server")]
struct Arguments {
    /// TCP address on which to listen.
    #[arg(long, default_value = "127.0.0.1:6379")]
    bind: SocketAddr,

    /// Active expiry cleanup interval in milliseconds.
    #[arg(long, default_value_t = 100)]
    cleanup_interval_ms: u64,

    /// Maximum number of simultaneous connections.
    #[arg(long, default_value_t = 10_000)]
    max_connections: usize,

    /// Atomic snapshot path. Omit to run without persistence.
    #[arg(long)]
    snapshot: Option<PathBuf>,

    /// Periodic snapshot interval in milliseconds.
    #[arg(long, default_value_t = 30_000)]
    snapshot_interval_ms: u64,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let arguments = Arguments::parse();
    if arguments.max_connections == 0
        || arguments.cleanup_interval_ms == 0
        || arguments.snapshot_interval_ms == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "interval and maximum connections must be greater than zero",
        ));
    }

    let config = Config {
        cleanup_interval: Duration::from_millis(arguments.cleanup_interval_ms),
        max_connections: arguments.max_connections,
        snapshot_path: arguments.snapshot.clone(),
        snapshot_interval: Duration::from_millis(arguments.snapshot_interval_ms),
        ..Config::default()
    };
    let engine = if let Some(path) = arguments.snapshot.filter(|path| path.exists()) {
        tokio::task::spawn_blocking(move || Engine::load_snapshot(&path))
            .await
            .map_err(|error| io::Error::other(format!("snapshot loader failed: {error}")))?
            .map_err(io::Error::other)?
    } else {
        Engine::new()
    };
    let (listener, local_address) = bind(arguments.bind).await?;
    println!("VeltrixDB listening on {local_address}");

    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal.cancel();
        }
    });
    Server::with_engine(engine, config)
        .serve(listener, shutdown)
        .await
}
