# Benchmarking Methodology and Results

These measurements are reproducible development-host observations, not claims
of production superiority. The benchmark client speaks the same RESP2 requests
to both servers and waits for every response.

## Environment

- Date: 7 November 2026
- Host: Lenovo ThinkPad E560
- CPU: Intel Core i7-6500U, 2 cores / 4 threads, up to 3.10 GHz
- Memory: 14 GiB
- OS: Linux x86_64, kernel `7.0.0-42-generic`
- Rust: `rustc 1.99.0 (b940084d7 2026-09-28)`
- VeltrixDB: release build from the current working tree
- Redis comparison: Ubuntu Redis Open Source `8.0.5`, temporarily extracted
  official package binaries
- Transport: TCP loopback (`127.0.0.1`)

## Server configuration

VeltrixDB:

```bash
cargo build --release --workspace
target/release/veltrix-server --bind 127.0.0.1:16379
```

Redis:

```bash
redis-server --port 16380 --bind 127.0.0.1 \
  --protected-mode no --save '' --appendonly no --daemonize no
```

Persistence was disabled on both. Both release servers remained running on the
same host; the non-target server was idle. No CPU pinning, frequency locking, or
host isolation was applied.

## Workloads

The included `veltrix-benchmarks` runner used 32 persistent client connections.
Each target received a warm-up, followed by five alternating VeltrixDB/Redis
samples.

```bash
# PING: 20,000 total request/response operations per sample
target/release/veltrix-benchmarks --addr HOST:PORT \
  --workload ping --clients 32 --requests 20000

# Read/write: 10,000 iterations, each one SET plus one GET = 20,000 operations
target/release/veltrix-benchmarks --addr HOST:PORT \
  --workload read-write --clients 32 --requests 10000
```

The read/write workload uses one 64-byte value and one key per client. The
runner reports aggregate completed operations divided by wall-clock duration.

## Raw results

All values are operations per second.

| Workload | Sample | VeltrixDB | Redis 8.0.5 |
| --- | ---: | ---: | ---: |
| PING | 1 | 61,212.24 | 75,355.05 |
| PING | 2 | 82,508.04 | 78,295.41 |
| PING | 3 | 82,475.03 | 74,253.40 |
| PING | 4 | 78,678.53 | 73,570.52 |
| PING | 5 | 85,885.47 | 82,900.08 |
| SET/GET | 1 | 37,140.37 | 34,724.06 |
| SET/GET | 2 | 50,134.12 | 42,106.89 |
| SET/GET | 3 | 43,956.81 | 54,645.15 |
| SET/GET | 4 | 58,979.72 | 41,701.96 |
| SET/GET | 5 | 71,526.77 | 80,048.69 |

| Workload | Target | Median | Nearest-rank p95 (n=5) |
| --- | --- | ---: | ---: |
| PING | VeltrixDB | 82,475.03 | 85,885.47 |
| PING | Redis 8.0.5 | 75,355.05 | 82,900.08 |
| SET/GET | VeltrixDB | 50,134.12 | 71,526.77 |
| SET/GET | Redis 8.0.5 | 42,106.89 | 80,048.69 |

## Interpretation and limits

Variance is substantial, especially in the SET/GET workload. Five samples on a
shared laptop are insufficient for broad conclusions. The runner does not yet
record latency histograms, coordinated-omission-aware percentiles, CPU time,
allocations, resident memory, or energy. VeltrixDB and Redis also have very
different feature sets. Re-run on an idle, pinned host with more samples before
using these values for engineering decisions.
