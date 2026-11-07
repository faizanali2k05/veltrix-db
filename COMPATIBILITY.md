# Redis Compatibility

VeltrixDB implements a tested subset of Redis RESP2 behavior. It is not a Redis
fork and does not claim full Redis compatibility.

| Family | Supported commands and options | Important limits |
| --- | --- | --- |
| Connection | `PING [message]`, `ECHO message` | RESP2 array commands only; no inline protocol |
| Strings | `GET`, `SET`, `SET EX`, `SET PX` | No NX/XX/GET/KEEPTTL options |
| Keys | `DEL`, `EXISTS`, `TTL`, `PTTL` | No patterns, databases, rename, or explicit expire command |
| Hashes | `HSET`, `HGET`, `HDEL`, `HLEN` | No hash scans or increments |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE` | Pop count and blocking variants are not supported |
| Sets | `SADD`, `SREM`, `SISMEMBER`, `SMEMBERS`, `SCARD` | No algebra or scans; member order is deterministic, not promised compatible |
| Sorted sets | `ZADD`, `ZREM`, `ZSCORE`, `ZRANGE [WITHSCORES]`, `ZCARD` | Finite scores only; no ZADD options or score ranges |
| Introspection | `MEMORY USAGE key` | Approximate payload accounting, not Redis allocator accounting |

Missing commands return `ERR unknown command`. Type conflicts return Redis-style
`WRONGTYPE`. RESP3, authentication, TLS, Pub/Sub, transactions, scripting,
replication, clustering, eviction, multi-database selection, and append-only
logging are not implemented.

The project tests semantics at its supported boundary and verifies a real
Redis 8.0.5 `redis-cli` can connect. This does not imply drop-in compatibility
for Redis applications that use anything outside the table above.

