//! Reusable asynchronous RESP2 client for `VeltrixDB`.

use bytes::BytesMut;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};
use veltrix_protocol::{Decoder, Value};

/// A single-connection `VeltrixDB` client.
#[derive(Debug)]
pub struct Client {
    stream: TcpStream,
    decoder: Decoder,
    read_buffer: BytesMut,
}

impl Client {
    /// Connect to a server.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when address resolution or TCP connection fails.
    pub async fn connect(address: impl ToSocketAddrs) -> io::Result<Self> {
        Ok(Self {
            stream: TcpStream::connect(address).await?,
            decoder: Decoder::default(),
            read_buffer: BytesMut::with_capacity(8 * 1024),
        })
    }

    /// Execute one command made of binary-safe arguments.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the request cannot be written, the connection
    /// closes early, or the response is malformed.
    pub async fn execute<I, B>(&mut self, arguments: I) -> io::Result<Value>
    where
        I: IntoIterator<Item = B>,
        B: Into<Vec<u8>>,
    {
        let frame = Value::Array(Some(
            arguments
                .into_iter()
                .map(|argument| Value::bulk(argument.into()))
                .collect(),
        ));
        self.stream.write_all(&frame.to_bytes()).await?;
        self.read_response().await
    }

    async fn read_response(&mut self) -> io::Result<Value> {
        loop {
            match self.decoder.decode(&mut self.read_buffer) {
                Ok(Some(value)) => return Ok(value),
                Ok(None) => {}
                Err(error) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid server response: {error}"),
                    ));
                }
            }
            if self.stream.read_buf(&mut self.read_buffer).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "server closed before sending a complete response",
                ));
            }
        }
    }
}
