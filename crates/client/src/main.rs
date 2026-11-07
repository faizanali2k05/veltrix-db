use clap::Parser;
use std::io::{self, Write as _};
use tokio::io::{AsyncBufReadExt, BufReader};
use veltrix_client::Client;
use veltrix_protocol::Value;

#[derive(Debug, Parser)]
#[command(name = "veltrix-cli", about = "Connect to a VeltrixDB server")]
struct Arguments {
    /// Server address.
    #[arg(long, default_value = "127.0.0.1:6379")]
    addr: String,

    /// Command followed by arguments. Omit to enter interactive mode.
    #[arg(trailing_var_arg = true)]
    command: Vec<String>,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let arguments = Arguments::parse();
    let mut client = Client::connect(&arguments.addr).await?;
    if arguments.command.is_empty() {
        interactive(&mut client).await
    } else {
        let response = client.execute(arguments.command).await?;
        print_value(&response);
        Ok(())
    }
}

async fn interactive(client: &mut Client) -> io::Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    loop {
        print!("veltrix> ");
        io::stdout().flush()?;
        let Some(line) = lines.next_line().await? else {
            return Ok(());
        };
        let arguments: Vec<Vec<u8>> = line
            .split_whitespace()
            .map(|part| part.as_bytes().to_vec())
            .collect();
        if arguments.is_empty() {
            continue;
        }
        if arguments[0].eq_ignore_ascii_case(b"QUIT") {
            return Ok(());
        }
        print_value(&client.execute(arguments).await?);
    }
}

fn print_value(value: &Value) {
    match value {
        Value::SimpleString(text) => println!("{text}"),
        Value::Error(text) => eprintln!("(error) {text}"),
        Value::Integer(number) => println!("(integer) {number}"),
        Value::BulkString(Some(bytes)) => println!("{}", String::from_utf8_lossy(bytes)),
        Value::BulkString(None) | Value::Array(None) => println!("(nil)"),
        Value::Array(Some(values)) => {
            for (index, item) in values.iter().enumerate() {
                print!("{}) ", index + 1);
                print_value(item);
            }
        }
    }
}
