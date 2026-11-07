# Implementation Plan

## Phase 1 — Functional database

- [x] Cargo workspace and modular crate boundaries
- [x] Incremental, bounded RESP2 decoder and serializer
- [x] Tokio TCP server with concurrent connections and graceful shutdown
- [x] `PING`, `ECHO`, `GET`, `SET`, `DEL`, `EXISTS`, `TTL`, and `PTTL`
- [x] `SET EX` / `SET PX`, lazy expiry, and periodic cleanup
- [x] Reusable client library and CLI
- [x] Unit and real-socket integration tests
- [ ] Broader differential semantics testing against Redis

## Phase 2 — Data structures and memory policy (partial)

- [x] Add typed values for hashes, lists, sets, and sorted sets.
- [x] Implement and document a bounded command subset.
- [x] Add approximate per-key payload accounting through `MEMORY USAGE`.
- [ ] Add broad Redis differential tests.
- [ ] Track allocator-aware memory globally.
- [ ] Add configurable `noeviction`, LRU, and LFU policies.
- [ ] Measure lock contention before selecting sharding or a concurrent map.

## Phase 3 — Persistence (snapshot portion complete)

- [x] Define a versioned snapshot and atomic temp-file/rename workflow.
- [x] Recover expiry deadlines across restart.
- [x] Add persistence configuration, interrupted-temp-file, and restart tests.
- [x] Move filesystem and bulk serialization work off Tokio worker threads.
- [ ] Add append-only logging, checksums, partial-tail handling, and compaction.

## Phase 4 — Advanced systems

- Add Pub/Sub and optimistic transactions with explicit isolation semantics.
- Prototype primary/replica streaming with offsets and full resynchronization.
- Export latency, throughput, connection, memory, and persistence metrics.
- Run identical wire workloads against VeltrixDB and a pinned local Redis build;
  record CPU, RAM, OS, compiler, configuration, warm-up, sample count, and raw
  results. Do not present results when either server or methodology differs.
- Consider an optional monitoring dashboard after stable metrics exist.

Every phase ends with `cargo fmt`, warning-free Clippy, the full test suite, and
updated compatibility documentation. Feature claims are limited to passing,
published tests.

