use bytes::BytesMut;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use veltrix_client::Client;
use veltrix_engine::Engine;
use veltrix_protocol::{Decoder, Value};
use veltrix_server::{Config, Server};

struct TestServer {
    address: SocketAddr,
    shutdown: CancellationToken,
    task: JoinHandle<io::Result<()>>,
}

impl TestServer {
    async fn start() -> Self {
        Self::start_with(
            Engine::new(),
            Config {
                cleanup_interval: Duration::from_millis(5),
                ..Config::default()
            },
        )
        .await
    }

    async fn start_with(engine: Engine, config: Config) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let task = tokio::spawn(async move {
            Server::with_engine(engine, config)
                .serve(listener, server_shutdown)
                .await
        });
        Self {
            address,
            shutdown,
            task,
        }
    }

    async fn stop(self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(2), self.task)
            .await
            .expect("server did not shut down")
            .expect("server task panicked")
            .expect("server returned an error");
    }
}

async fn execute(client: &mut Client, args: &[&str]) -> Value {
    client
        .execute(args.iter().map(|arg| arg.as_bytes().to_vec()))
        .await
        .unwrap()
}

#[tokio::test]
async fn core_commands_work_over_real_tcp() {
    let server = TestServer::start().await;
    let mut client = Client::connect(server.address).await.unwrap();

    assert_eq!(
        execute(&mut client, &["PING"]).await,
        Value::SimpleString("PONG".into())
    );
    assert_eq!(
        execute(&mut client, &["ECHO", "hello"]).await,
        Value::bulk(b"hello".to_vec())
    );
    assert_eq!(
        execute(&mut client, &["GET", "missing"]).await,
        Value::BulkString(None)
    );
    assert_eq!(
        execute(&mut client, &["SET", "key", "value"]).await,
        Value::SimpleString("OK".into())
    );
    assert_eq!(
        execute(&mut client, &["GET", "key"]).await,
        Value::bulk(b"value".to_vec())
    );
    assert_eq!(
        execute(&mut client, &["EXISTS", "key", "key", "nope"]).await,
        Value::Integer(2)
    );
    assert_eq!(
        execute(&mut client, &["TTL", "key"]).await,
        Value::Integer(-1)
    );
    assert_eq!(
        execute(&mut client, &["DEL", "key", "key"]).await,
        Value::Integer(1)
    );
    assert_eq!(
        execute(&mut client, &["EXISTS", "key"]).await,
        Value::Integer(0)
    );

    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn collection_commands_and_wrong_types_work_over_tcp() {
    let server = TestServer::start().await;
    let mut client = Client::connect(server.address).await.unwrap();

    assert_eq!(
        execute(&mut client, &["HSET", "h", "a", "1", "b", "2"]).await,
        Value::Integer(2)
    );
    assert_eq!(
        execute(&mut client, &["HGET", "h", "a"]).await,
        Value::bulk(b"1".to_vec())
    );
    assert_eq!(
        execute(&mut client, &["HLEN", "h"]).await,
        Value::Integer(2)
    );

    assert_eq!(
        execute(&mut client, &["RPUSH", "l", "a", "b"]).await,
        Value::Integer(2)
    );
    assert_eq!(
        execute(&mut client, &["LPUSH", "l", "c"]).await,
        Value::Integer(3)
    );
    assert_eq!(
        execute(&mut client, &["LRANGE", "l", "0", "-1"]).await,
        Value::Array(Some(vec![
            Value::bulk(b"c".to_vec()),
            Value::bulk(b"a".to_vec()),
            Value::bulk(b"b".to_vec())
        ]))
    );

    assert_eq!(
        execute(&mut client, &["SADD", "s", "b", "a", "a"]).await,
        Value::Integer(2)
    );
    assert_eq!(
        execute(&mut client, &["SMEMBERS", "s"]).await,
        Value::Array(Some(vec![
            Value::bulk(b"a".to_vec()),
            Value::bulk(b"b".to_vec())
        ]))
    );

    assert_eq!(
        execute(&mut client, &["ZADD", "z", "2", "b", "1", "a"]).await,
        Value::Integer(2)
    );
    assert_eq!(
        execute(&mut client, &["ZRANGE", "z", "0", "-1", "WITHSCORES"]).await,
        Value::Array(Some(vec![
            Value::bulk(b"a".to_vec()),
            Value::bulk(b"1".to_vec()),
            Value::bulk(b"b".to_vec()),
            Value::bulk(b"2".to_vec())
        ]))
    );

    assert!(
        matches!(execute(&mut client, &["GET", "h"]).await, Value::Error(message) if message.starts_with("WRONGTYPE"))
    );
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn invalid_commands_do_not_break_the_connection() {
    let server = TestServer::start().await;
    let mut client = Client::connect(server.address).await.unwrap();
    assert!(matches!(
        execute(&mut client, &["NOTACOMMAND"]).await,
        Value::Error(_)
    ));
    assert!(matches!(
        execute(&mut client, &["SET", "key", "value", "PX", "0"]).await,
        Value::Error(_)
    ));
    assert!(matches!(
        client.execute([b"BAD\r\nCOMMAND".to_vec()]).await.unwrap(),
        Value::Error(_)
    ));
    assert_eq!(
        execute(&mut client, &["PING"]).await,
        Value::SimpleString("PONG".into())
    );
    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn expiration_works_in_milliseconds_and_seconds() {
    let server = TestServer::start().await;
    let mut client = Client::connect(server.address).await.unwrap();

    assert_eq!(
        execute(&mut client, &["SET", "px", "value", "PX", "40"]).await,
        Value::SimpleString("OK".into())
    );
    let Value::Integer(pttl) = execute(&mut client, &["PTTL", "px"]).await else {
        panic!("expected integer")
    };
    assert!((0..=40).contains(&pttl));
    tokio::time::sleep(Duration::from_millis(70)).await;
    assert_eq!(
        execute(&mut client, &["GET", "px"]).await,
        Value::BulkString(None)
    );
    assert_eq!(
        execute(&mut client, &["TTL", "px"]).await,
        Value::Integer(-2)
    );

    assert_eq!(
        execute(&mut client, &["SET", "ex", "value", "EX", "1"]).await,
        Value::SimpleString("OK".into())
    );
    let Value::Integer(ttl) = execute(&mut client, &["TTL", "ex"]).await else {
        panic!("expected integer")
    };
    assert!((0..=1).contains(&ttl));

    assert_eq!(
        execute(&mut client, &["SET", "ex", "persistent"]).await,
        Value::SimpleString("OK".into())
    );
    assert_eq!(
        execute(&mut client, &["TTL", "ex"]).await,
        Value::Integer(-1)
    );

    drop(client);
    server.stop().await;
}

#[tokio::test]
async fn snapshot_persistence_recovers_after_server_restart() {
    let directory =
        std::env::temp_dir().join(format!("veltrix-server-recovery-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let snapshot = directory.join("database.snapshot");
    let config = Config {
        cleanup_interval: Duration::from_millis(5),
        snapshot_path: Some(snapshot.clone()),
        snapshot_interval: Duration::from_secs(60),
        ..Config::default()
    };
    let server = TestServer::start_with(Engine::new(), config.clone()).await;
    let mut client = Client::connect(server.address).await.unwrap();
    assert_eq!(
        execute(&mut client, &["SET", "persisted", "yes"]).await,
        Value::SimpleString("OK".into())
    );
    assert_eq!(
        execute(&mut client, &["HSET", "hash", "field", "value"]).await,
        Value::Integer(1)
    );
    drop(client);
    server.stop().await;

    let recovered = Engine::load_snapshot(&snapshot).unwrap();
    let restarted = TestServer::start_with(recovered, config).await;
    let mut client = Client::connect(restarted.address).await.unwrap();
    assert_eq!(
        execute(&mut client, &["GET", "persisted"]).await,
        Value::bulk(b"yes".to_vec())
    );
    assert_eq!(
        execute(&mut client, &["HGET", "hash", "field"]).await,
        Value::bulk(b"value".to_vec())
    );
    drop(client);
    restarted.stop().await;
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn fragmented_and_pipelined_frames_are_decoded() {
    let server = TestServer::start().await;
    let mut stream = TcpStream::connect(server.address).await.unwrap();
    let request = Value::Array(Some(vec![
        Value::bulk(b"ECHO".to_vec()),
        Value::bulk(b"fragmented".to_vec()),
    ]))
    .to_bytes();
    for byte in request {
        stream.write_all(&[byte]).await.unwrap();
    }
    let ping = Value::Array(Some(vec![Value::bulk(b"PING".to_vec())])).to_bytes();
    stream.write_all(&ping).await.unwrap();

    let decoder = Decoder::default();
    let mut buffer = BytesMut::new();
    let mut responses = Vec::new();
    while responses.len() < 2 {
        stream.read_buf(&mut buffer).await.unwrap();
        while let Some(value) = decoder.decode(&mut buffer).unwrap() {
            responses.push(value);
        }
    }
    assert_eq!(
        responses,
        vec![
            Value::bulk(b"fragmented".to_vec()),
            Value::SimpleString("PONG".into())
        ]
    );

    drop(stream);
    server.stop().await;
}

#[tokio::test]
async fn malformed_input_gets_an_error_and_connection_close() {
    let server = TestServer::start().await;
    let mut stream = TcpStream::connect(server.address).await.unwrap();
    stream.write_all(b"$3\r\nabcXX").await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    assert!(response.starts_with(b"-ERR Protocol error:"));

    server.stop().await;
}

#[tokio::test]
async fn shutdown_does_not_wait_for_idle_clients() {
    let server = TestServer::start().await;
    let mut idle_client = TcpStream::connect(server.address).await.unwrap();
    server.stop().await;

    let mut byte = [0_u8; 1];
    let result = tokio::time::timeout(Duration::from_secs(1), idle_client.read(&mut byte))
        .await
        .expect("idle connection was not closed");
    assert!(
        matches!(result, Ok(0))
            || matches!(result, Err(ref error) if error.kind() == io::ErrorKind::ConnectionReset)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clients_do_not_lose_updates() {
    let server = TestServer::start().await;
    let mut tasks = Vec::new();
    for client_id in 0..32 {
        let address = server.address;
        tasks.push(tokio::spawn(async move {
            let mut client = Client::connect(address).await.unwrap();
            let key = format!("key-{client_id}");
            let value = format!("value-{client_id}");
            assert_eq!(
                client.execute(["SET", &key, &value]).await.unwrap(),
                Value::SimpleString("OK".into())
            );
            assert_eq!(
                client.execute(["GET", &key]).await.unwrap(),
                Value::bulk(value.into_bytes())
            );
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let mut client = Client::connect(server.address).await.unwrap();
    let keys: Vec<String> = (0..32).map(|id| format!("key-{id}")).collect();
    let mut command = vec![b"EXISTS".to_vec()];
    command.extend(keys.into_iter().map(String::into_bytes));
    assert_eq!(client.execute(command).await.unwrap(), Value::Integer(32));

    drop(client);
    server.stop().await;
}
