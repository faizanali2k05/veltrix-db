# VeltrixDB Architecture

## Scope and compatibility

VeltrixDB is built from first principles rather than wrapping Redis. RESP2 and
the documented command semantics are the compatibility boundary. Unsupported
commands return an error. The supported boundary is intentionally limited and
is not production-ready.

## Workspace

| Crate | Responsibility |
| --- | --- |
| `veltrix-protocol` | Incremental RESP2 parsing and serialization |
| `veltrix-engine` | Storage, expiration, and shared-state synchronization |
| `veltrix-server` | Command dispatch, TCP lifecycle, and graceful shutdown |
| `veltrix-client` | Reusable async client and command-line interface |
| `veltrix-benchmarks` | Repeatable wire-level workload runner |

Dependencies point inward: server and client depend on protocol; server depends
on engine. The engine has no networking or Tokio dependency.

## Request path

```text
TCP bytes -> per-connection buffer -> RESP decoder -> command validation
          -> storage engine -> RESP value -> encoder -> TCP bytes
```

The decoder is incremental. Incomplete frames remain buffered until more TCP
bytes arrive; malformed frames are rejected with a protocol error. Frame size,
bulk-string size, nesting depth, and array length are bounded to prevent a peer
from creating unbounded parser work or memory use.

## Concurrency and shared state

Each accepted connection runs in its own Tokio task. All connections share one
`Engine`, backed by `Arc<RwLock<HashMap<Vec<u8>, Entry>>>`. Each entry contains
a typed string, hash, list, set, or sorted-set value. Engine operations are bounded,
synchronous critical sections, and the lock is never held across an `.await`.
This keeps the ownership model explicit and avoids blocking an executor with
I/O while locked. Phase 2 may shard the map after measurement.

Expiration uses both:

- lazy deletion whenever a key is accessed; and
- an asynchronous server maintenance task that periodically asks the engine to
  remove expired entries in bounded passes.

Deadlines use monotonic `Instant`, so wall-clock changes do not affect live
expiry. Snapshots store Unix-millisecond deadlines and reconstruct monotonic
deadlines during recovery.

## Lifecycle and shutdown

The accept loop, cleanup loop, and connection tasks share a cancellation token.
Shutdown stops accepting, signals all tasks, waits for active connections, and
then returns. Connections select cancellation alongside socket reads, so idle
clients do not prevent shutdown.

## Persistence boundary

Persistence is opt-in and snapshot-based. A save clones live entries under the
engine lock, releases the lock, serializes a versioned JSON document, writes a
same-directory temporary file, synchronizes it, atomically renames it, and then
synchronizes the parent directory. Encoding and filesystem work execute through
`spawn_blocking`; async networking workers do not perform filesystem I/O.

Periodic snapshots and a final controlled-shutdown snapshot are supported. An
incomplete temporary file cannot replace the last good snapshot. There is no
append-only log, so a hard crash can lose mutations since the last completed
snapshot. Corruption of the main snapshot is reported rather than ignored.

## Data model

The engine enforces one type per key and reports `WRONGTYPE` across mismatched
commands. Collection mutation preserves an existing key TTL, while `SET`
replaces both value and TTL. Empty hashes, lists, sets, and sorted sets are
removed. Sorted sets order by score and then member bytes; only finite scores
are accepted by the documented subset.

## Safety limits

Protocol limits and the maximum connection count are server concerns. Invalid
commands cannot mutate storage. Integer and expiry arithmetic is checked. The
code forbids unsafe Rust. There is no authentication or TLS, so the server must
remain on a trusted interface.

