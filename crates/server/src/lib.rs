//! Tokio TCP server and command dispatch for `VeltrixDB`.

use bytes::BytesMut;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use veltrix_engine::{Engine, EngineError, TimeToLive};
use veltrix_protocol::{Decoder, Value};

/// Runtime configuration for a server instance.
#[derive(Clone, Debug)]
pub struct Config {
    /// Interval between active expiration passes.
    pub cleanup_interval: Duration,
    /// Maximum entries removed by each active expiration pass.
    pub cleanup_batch_size: usize,
    /// Maximum simultaneous client connections.
    pub max_connections: usize,
    /// Optional path for periodic and shutdown atomic snapshots.
    pub snapshot_path: Option<PathBuf>,
    /// Interval between snapshots when persistence is enabled.
    pub snapshot_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            cleanup_interval: Duration::from_millis(100),
            cleanup_batch_size: 1_000,
            max_connections: 10_000,
            snapshot_path: None,
            snapshot_interval: Duration::from_secs(30),
        }
    }
}

/// A `VeltrixDB` server sharing one storage engine across clients.
#[derive(Clone, Debug)]
pub struct Server {
    engine: Engine,
    config: Config,
}

impl Server {
    /// Create a server with an empty engine.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            engine: Engine::new(),
            config,
        }
    }

    /// Create a server around a supplied engine.
    #[must_use]
    pub const fn with_engine(engine: Engine, config: Config) -> Self {
        Self { engine, config }
    }

    /// Return a handle to the shared engine.
    #[must_use]
    pub fn engine(&self) -> Engine {
        self.engine.clone()
    }

    /// Serve a bound listener until `shutdown` is cancelled.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if accepting a connection fails or the maintenance
    /// task cannot be joined during shutdown.
    pub async fn serve(
        &self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> io::Result<()> {
        if self.config.cleanup_interval.is_zero()
            || self.config.snapshot_interval.is_zero()
            || self.config.max_connections == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cleanup interval and maximum connections must be greater than zero",
            ));
        }
        let permits = Arc::new(Semaphore::new(self.config.max_connections));
        let mut connections = JoinSet::new();
        let cleanup = tokio::spawn(cleanup_loop(
            self.engine.clone(),
            self.config.clone(),
            shutdown.clone(),
        ));
        let persistence = self.config.snapshot_path.clone().map(|path| {
            tokio::spawn(persistence_loop(
                self.engine.clone(),
                path,
                self.config.snapshot_interval,
                shutdown.clone(),
            ))
        });
        let mut accept_error = None;

        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                Some(result) = connections.join_next(), if !connections.is_empty() => {
                    if let Err(error) = result {
                        eprintln!("connection task failed: {error}");
                    }
                }
                accepted = listener.accept() => {
                    let (stream, _) = match accepted {
                        Ok(connection) => connection,
                        Err(error) => {
                            accept_error = Some(error);
                            break;
                        }
                    };
                    let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                        drop(stream);
                        continue;
                    };
                    let engine = self.engine.clone();
                    let connection_shutdown = shutdown.clone();
                    connections.spawn(async move {
                        let _permit = permit;
                        if let Err(error) = serve_connection(stream, engine, connection_shutdown).await {
                            eprintln!("connection I/O error: {error}");
                        }
                    });
                }
            }
        }

        shutdown.cancel();
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result {
                eprintln!("connection task failed during shutdown: {error}");
            }
        }
        cleanup
            .await
            .map_err(|error| io::Error::other(format!("cleanup task failed: {error}")))?;
        if let Some(persistence) = persistence {
            persistence
                .await
                .map_err(|error| io::Error::other(format!("persistence task failed: {error}")))??;
        }
        if let Some(error) = accept_error {
            Err(error)
        } else {
            Ok(())
        }
    }
}

async fn persistence_loop(
    engine: Engine,
    path: PathBuf,
    snapshot_interval: Duration,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let mut interval = tokio::time::interval(snapshot_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval.tick().await;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return persist(engine, path).await,
            _ = interval.tick() => persist(engine.clone(), path.clone()).await?,
        }
    }
}

async fn persist(engine: Engine, path: PathBuf) -> io::Result<()> {
    tokio::task::spawn_blocking(move || engine.save_snapshot(&path))
        .await
        .map_err(|error| io::Error::other(format!("snapshot worker failed: {error}")))?
        .map_err(io::Error::other)
}

async fn cleanup_loop(engine: Engine, config: Config, shutdown: CancellationToken) {
    let mut interval = tokio::time::interval(config.cleanup_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // The first Tokio interval tick is immediate; consume it before waiting.
    interval.tick().await;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = interval.tick() => {
                let _ = engine.cleanup_expired(config.cleanup_batch_size);
            }
        }
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    engine: Engine,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let decoder = Decoder::default();
    let mut buffer = BytesMut::with_capacity(8 * 1024);
    loop {
        while !buffer.is_empty() {
            match decoder.decode(&mut buffer) {
                Ok(Some(frame)) => {
                    let response = dispatch(&engine, frame);
                    stream.write_all(&response.to_bytes()).await?;
                }
                Ok(None) => break,
                Err(error) => {
                    let response = Value::Error(format!("ERR Protocol error: {error}"));
                    stream.write_all(&response.to_bytes()).await?;
                    return Ok(());
                }
            }
        }

        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            read = stream.read_buf(&mut buffer) => {
                if read? == 0 {
                    return Ok(());
                }
            }
        }
    }
}

fn dispatch(engine: &Engine, frame: Value) -> Value {
    let Value::Array(Some(values)) = frame else {
        return error("Protocol error: expected a non-null array");
    };
    let Some((first, rest)) = values.split_first() else {
        return error("empty command");
    };
    let Some(command) = value_bytes(first) else {
        return error("command name must be a string");
    };
    let mut args = Vec::with_capacity(rest.len());
    for value in rest {
        let Some(bytes) = value_bytes(value) else {
            return error("command arguments must be strings");
        };
        args.push(bytes);
    }

    if command.eq_ignore_ascii_case(b"PING") {
        command_ping(&args)
    } else if command.eq_ignore_ascii_case(b"ECHO") {
        command_echo(&args)
    } else if command.eq_ignore_ascii_case(b"GET") {
        command_get(engine, &args)
    } else if command.eq_ignore_ascii_case(b"SET") {
        command_set(engine, &args)
    } else if command.eq_ignore_ascii_case(b"DEL") {
        command_del(engine, &args)
    } else if command.eq_ignore_ascii_case(b"EXISTS") {
        command_exists(engine, &args)
    } else if command.eq_ignore_ascii_case(b"TTL") {
        command_ttl(engine, &args, false)
    } else if command.eq_ignore_ascii_case(b"PTTL") {
        command_ttl(engine, &args, true)
    } else if command.eq_ignore_ascii_case(b"HSET") {
        command_hset(engine, &args)
    } else if command.eq_ignore_ascii_case(b"HGET") {
        command_hget(engine, &args)
    } else if command.eq_ignore_ascii_case(b"HDEL") {
        command_hdel(engine, &args)
    } else if command.eq_ignore_ascii_case(b"HLEN") {
        command_hlen(engine, &args)
    } else if command.eq_ignore_ascii_case(b"LPUSH") {
        command_list_push(engine, &args, true)
    } else if command.eq_ignore_ascii_case(b"RPUSH") {
        command_list_push(engine, &args, false)
    } else if command.eq_ignore_ascii_case(b"LPOP") {
        command_list_pop(engine, &args, true)
    } else if command.eq_ignore_ascii_case(b"RPOP") {
        command_list_pop(engine, &args, false)
    } else if command.eq_ignore_ascii_case(b"LLEN") {
        command_llen(engine, &args)
    } else if command.eq_ignore_ascii_case(b"LRANGE") {
        command_lrange(engine, &args)
    } else if command.eq_ignore_ascii_case(b"SADD") {
        command_sadd(engine, &args)
    } else if command.eq_ignore_ascii_case(b"SREM") {
        command_srem(engine, &args)
    } else if command.eq_ignore_ascii_case(b"SISMEMBER") {
        command_sismember(engine, &args)
    } else if command.eq_ignore_ascii_case(b"SMEMBERS") {
        command_smembers(engine, &args)
    } else if command.eq_ignore_ascii_case(b"SCARD") {
        command_scard(engine, &args)
    } else if command.eq_ignore_ascii_case(b"ZADD") {
        command_zadd(engine, &args)
    } else if command.eq_ignore_ascii_case(b"ZREM") {
        command_zrem(engine, &args)
    } else if command.eq_ignore_ascii_case(b"ZSCORE") {
        command_zscore(engine, &args)
    } else if command.eq_ignore_ascii_case(b"ZRANGE") {
        command_zrange(engine, &args)
    } else if command.eq_ignore_ascii_case(b"ZCARD") {
        command_zcard(engine, &args)
    } else if command.eq_ignore_ascii_case(b"MEMORY") {
        command_memory(engine, &args)
    } else {
        error(&format!(
            "unknown command '{}'",
            String::from_utf8_lossy(command).escape_default()
        ))
    }
}

fn value_bytes(value: &Value) -> Option<&[u8]> {
    match value {
        Value::BulkString(Some(bytes)) => Some(bytes),
        Value::SimpleString(text) => Some(text.as_bytes()),
        _ => None,
    }
}

fn command_ping(args: &[&[u8]]) -> Value {
    match args {
        [] => Value::SimpleString("PONG".into()),
        [message] => Value::bulk(message.to_vec()),
        _ => arity("ping"),
    }
}

fn command_echo(args: &[&[u8]]) -> Value {
    match args {
        [message] => Value::bulk(message.to_vec()),
        _ => arity("echo"),
    }
}

fn command_get(engine: &Engine, args: &[&[u8]]) -> Value {
    match args {
        [key] => match engine.get(key) {
            Ok(value) => Value::BulkString(value),
            Err(error) => engine_error(error),
        },
        _ => arity("get"),
    }
}

fn command_set(engine: &Engine, args: &[&[u8]]) -> Value {
    let (key, value, ttl) = match args {
        [key, value] => (*key, *value, None),
        [key, value, option, amount]
            if option.eq_ignore_ascii_case(b"EX") || option.eq_ignore_ascii_case(b"PX") =>
        {
            let Some(ttl) = parse_expiry(option, amount) else {
                return error("invalid expire time in 'set' command");
            };
            (*key, *value, Some(ttl))
        }
        [_, _, _, _] => return syntax_error(),
        _ => return arity("set"),
    };
    engine.set(key.to_vec(), value.to_vec(), ttl);
    Value::SimpleString("OK".into())
}

fn parse_expiry(option: &[u8], amount: &[u8]) -> Option<Duration> {
    let amount = std::str::from_utf8(amount).ok()?.parse::<u64>().ok()?;
    if amount == 0 {
        return None;
    }
    let millis = if option.eq_ignore_ascii_case(b"EX") {
        amount.checked_mul(1_000)?
    } else {
        amount
    };
    let duration = Duration::from_millis(millis);
    Instant::now().checked_add(duration)?;
    Some(duration)
}

fn command_del(engine: &Engine, args: &[&[u8]]) -> Value {
    if args.is_empty() {
        arity("del")
    } else {
        integer_from_usize(engine.delete(args.iter().copied()))
    }
}

fn command_exists(engine: &Engine, args: &[&[u8]]) -> Value {
    if args.is_empty() {
        arity("exists")
    } else {
        integer_from_usize(engine.exists(args.iter().copied()))
    }
}

fn command_ttl(engine: &Engine, args: &[&[u8]], milliseconds: bool) -> Value {
    let [key] = args else {
        return arity(if milliseconds { "pttl" } else { "ttl" });
    };
    let value = match engine.ttl(key) {
        TimeToLive::Missing => -2,
        TimeToLive::Persistent => -1,
        TimeToLive::ExpiresIn(duration) if milliseconds => {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        }
        TimeToLive::ExpiresIn(duration) => {
            let rounded_up = duration.as_millis().div_ceil(1_000);
            i64::try_from(rounded_up).unwrap_or(i64::MAX)
        }
    };
    Value::Integer(value)
}

fn command_hset(engine: &Engine, args: &[&[u8]]) -> Value {
    if args.len() < 3 || args.len() % 2 == 0 {
        return arity("hset");
    }
    let fields: Vec<(Vec<u8>, Vec<u8>)> = args[1..]
        .chunks_exact(2)
        .map(|pair| (pair[0].to_vec(), pair[1].to_vec()))
        .collect();
    count_result(engine.hash_set(args[0], &fields))
}

fn command_hget(engine: &Engine, args: &[&[u8]]) -> Value {
    match args {
        [key, field] => match engine.hash_get(key, field) {
            Ok(value) => Value::BulkString(value),
            Err(error) => engine_error(error),
        },
        _ => arity("hget"),
    }
}

fn command_hdel(engine: &Engine, args: &[&[u8]]) -> Value {
    if args.len() < 2 {
        arity("hdel")
    } else {
        count_result(engine.hash_delete(args[0], args[1..].iter().copied()))
    }
}

fn command_hlen(engine: &Engine, args: &[&[u8]]) -> Value {
    match args {
        [key] => count_result(engine.hash_len(key)),
        _ => arity("hlen"),
    }
}

fn command_list_push(engine: &Engine, args: &[&[u8]], left: bool) -> Value {
    if args.len() < 2 {
        return arity(if left { "lpush" } else { "rpush" });
    }
    let values: Vec<Vec<u8>> = args[1..].iter().map(|value| value.to_vec()).collect();
    count_result(engine.list_push(args[0], &values, left))
}

fn command_list_pop(engine: &Engine, args: &[&[u8]], left: bool) -> Value {
    match args {
        [key] => match engine.list_pop(key, left) {
            Ok(value) => Value::BulkString(value),
            Err(error) => engine_error(error),
        },
        _ => arity(if left { "lpop" } else { "rpop" }),
    }
}

fn command_llen(engine: &Engine, args: &[&[u8]]) -> Value {
    match args {
        [key] => count_result(engine.list_len(key)),
        _ => arity("llen"),
    }
}

fn command_lrange(engine: &Engine, args: &[&[u8]]) -> Value {
    let [key, start, stop] = args else {
        return arity("lrange");
    };
    let (Some(start), Some(stop)) = (parse_i64(start), parse_i64(stop)) else {
        return error("value is not an integer or out of range");
    };
    match engine.list_range(key, start, stop) {
        Ok(values) => bulk_array(values),
        Err(error) => engine_error(error),
    }
}

fn command_sadd(engine: &Engine, args: &[&[u8]]) -> Value {
    if args.len() < 2 {
        return arity("sadd");
    }
    let members: Vec<Vec<u8>> = args[1..].iter().map(|value| value.to_vec()).collect();
    count_result(engine.set_add(args[0], &members))
}

fn command_srem(engine: &Engine, args: &[&[u8]]) -> Value {
    if args.len() < 2 {
        arity("srem")
    } else {
        count_result(engine.set_remove(args[0], args[1..].iter().copied()))
    }
}

fn command_sismember(engine: &Engine, args: &[&[u8]]) -> Value {
    match args {
        [key, member] => match engine.set_contains(key, member) {
            Ok(exists) => Value::Integer(i64::from(exists)),
            Err(error) => engine_error(error),
        },
        _ => arity("sismember"),
    }
}

fn command_smembers(engine: &Engine, args: &[&[u8]]) -> Value {
    match args {
        [key] => match engine.set_members(key) {
            Ok(values) => bulk_array(values),
            Err(error) => engine_error(error),
        },
        _ => arity("smembers"),
    }
}

fn command_scard(engine: &Engine, args: &[&[u8]]) -> Value {
    match args {
        [key] => count_result(engine.set_len(key)),
        _ => arity("scard"),
    }
}

fn command_zadd(engine: &Engine, args: &[&[u8]]) -> Value {
    if args.len() < 3 || args.len() % 2 == 0 {
        return arity("zadd");
    }
    let mut values = Vec::with_capacity((args.len() - 1) / 2);
    for pair in args[1..].chunks_exact(2) {
        let Ok(score) = std::str::from_utf8(pair[0]).unwrap_or("").parse::<f64>() else {
            return error("value is not a valid float");
        };
        values.push((score, pair[1].to_vec()));
    }
    count_result(engine.sorted_set_add(args[0], &values))
}

fn command_zrem(engine: &Engine, args: &[&[u8]]) -> Value {
    if args.len() < 2 {
        arity("zrem")
    } else {
        count_result(engine.sorted_set_remove(args[0], args[1..].iter().copied()))
    }
}

fn command_zscore(engine: &Engine, args: &[&[u8]]) -> Value {
    match args {
        [key, member] => match engine.sorted_set_score(key, member) {
            Ok(Some(score)) => Value::bulk(format_score(score).into_bytes()),
            Ok(None) => Value::BulkString(None),
            Err(error) => engine_error(error),
        },
        _ => arity("zscore"),
    }
}

fn command_zrange(engine: &Engine, args: &[&[u8]]) -> Value {
    let (key, start, stop, with_scores) = match args {
        [key, start, stop] => (*key, *start, *stop, false),
        [key, start, stop, option] if option.eq_ignore_ascii_case(b"WITHSCORES") => {
            (*key, *start, *stop, true)
        }
        [_, _, _, _] => return syntax_error(),
        _ => return arity("zrange"),
    };
    let (Some(start), Some(stop)) = (parse_i64(start), parse_i64(stop)) else {
        return error("value is not an integer or out of range");
    };
    match engine.sorted_set_range(key, start, stop) {
        Ok(values) => {
            let mut response = Vec::with_capacity(values.len() * if with_scores { 2 } else { 1 });
            for (member, score) in values {
                response.push(Value::bulk(member));
                if with_scores {
                    response.push(Value::bulk(format_score(score).into_bytes()));
                }
            }
            Value::Array(Some(response))
        }
        Err(error) => engine_error(error),
    }
}

fn command_zcard(engine: &Engine, args: &[&[u8]]) -> Value {
    match args {
        [key] => count_result(engine.sorted_set_len(key)),
        _ => arity("zcard"),
    }
}

fn command_memory(engine: &Engine, args: &[&[u8]]) -> Value {
    match args {
        [subcommand, key] if subcommand.eq_ignore_ascii_case(b"USAGE") => engine
            .memory_usage(key)
            .map_or(Value::BulkString(None), integer_from_usize),
        _ => error("unsupported MEMORY subcommand or wrong number of arguments"),
    }
}

fn parse_i64(value: &[u8]) -> Option<i64> {
    std::str::from_utf8(value).ok()?.parse().ok()
}

fn bulk_array(values: Vec<Vec<u8>>) -> Value {
    Value::Array(Some(values.into_iter().map(Value::bulk).collect()))
}

fn count_result(result: Result<usize, EngineError>) -> Value {
    match result {
        Ok(value) => integer_from_usize(value),
        Err(error) => engine_error(error),
    }
}

fn engine_error(engine_error: EngineError) -> Value {
    match engine_error {
        EngineError::WrongType => {
            Value::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())
        }
        EngineError::InvalidScore => error("resulting score is not a number (NaN)"),
    }
}

fn format_score(score: f64) -> String {
    score.to_string()
}

fn integer_from_usize(value: usize) -> Value {
    Value::Integer(i64::try_from(value).unwrap_or(i64::MAX))
}

fn arity(command: &str) -> Value {
    error(&format!(
        "wrong number of arguments for '{command}' command"
    ))
}

fn syntax_error() -> Value {
    error("syntax error")
}

fn error(message: &str) -> Value {
    Value::Error(format!("ERR {message}"))
}

/// Bind a socket address, returning a listener and its resolved local address.
///
/// # Errors
///
/// Returns an I/O error if the address cannot be bound or queried.
pub async fn bind(address: SocketAddr) -> io::Result<(TcpListener, SocketAddr)> {
    let listener = TcpListener::bind(address).await?;
    let local_address = listener.local_addr()?;
    Ok((listener, local_address))
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    fn command(parts: &[&[u8]]) -> Value {
        Value::Array(Some(
            parts
                .iter()
                .map(|part| Value::bulk(part.to_vec()))
                .collect(),
        ))
    }

    #[test]
    fn dispatches_core_commands_and_errors() {
        let engine = Engine::new();
        assert_eq!(
            dispatch(&engine, command(&[b"PING"])),
            Value::SimpleString("PONG".into())
        );
        assert_eq!(
            dispatch(&engine, command(&[b"SET", b"key", b"value"])),
            Value::SimpleString("OK".into())
        );
        assert_eq!(
            dispatch(&engine, command(&[b"GET", b"key"])),
            Value::bulk(b"value".to_vec())
        );
        assert!(matches!(
            dispatch(&engine, command(&[b"SET", b"key", b"value", b"NO", b"1"])),
            Value::Error(_)
        ));
    }
}
