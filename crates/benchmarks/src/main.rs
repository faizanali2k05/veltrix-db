use clap::{Parser, ValueEnum};
use std::io;
use std::time::Instant;
use tokio::task::JoinSet;
use veltrix_client::Client;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Workload {
    Ping,
    ReadWrite,
}

#[derive(Debug, Parser)]
#[command(about = "Run identical RESP2 workloads against VeltrixDB or Redis")]
struct Arguments {
    #[arg(long, default_value = "127.0.0.1:6379")]
    addr: String,

    #[arg(long, value_enum, default_value_t = Workload::ReadWrite)]
    workload: Workload,

    #[arg(long, default_value_t = 100_000)]
    requests: usize,

    #[arg(long, default_value_t = 32)]
    clients: usize,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let arguments = Arguments::parse();
    if arguments.requests == 0 || arguments.clients == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "requests and clients must be greater than zero",
        ));
    }

    let started = Instant::now();
    let mut workers = JoinSet::new();
    for worker in 0..arguments.clients {
        let address = arguments.addr.clone();
        let base = arguments.requests / arguments.clients;
        let count = base + usize::from(worker < arguments.requests % arguments.clients);
        let workload = arguments.workload;
        workers.spawn(async move { run_worker(&address, workload, worker, count).await });
    }

    let mut operations = 0_usize;
    while let Some(result) = workers.join_next().await {
        operations += result.map_err(io::Error::other)??;
    }
    let elapsed = started.elapsed();
    #[allow(clippy::cast_precision_loss)]
    let throughput = operations as f64 / elapsed.as_secs_f64();
    println!("workload: {:?}", arguments.workload);
    println!("clients: {}", arguments.clients);
    println!("operations: {operations}");
    println!("elapsed_seconds: {:.6}", elapsed.as_secs_f64());
    println!("operations_per_second: {throughput:.2}");
    Ok(())
}

async fn run_worker(
    address: &str,
    workload: Workload,
    worker: usize,
    requests: usize,
) -> io::Result<usize> {
    let mut client = Client::connect(address).await?;
    match workload {
        Workload::Ping => {
            for _ in 0..requests {
                client.execute(["PING"]).await?;
            }
            Ok(requests)
        }
        Workload::ReadWrite => {
            let key = format!("veltrix-bench-{worker}");
            let value = "x".repeat(64);
            for _ in 0..requests {
                client.execute(["SET", &key, &value]).await?;
                client.execute(["GET", &key]).await?;
            }
            Ok(requests.saturating_mul(2))
        }
    }
}
