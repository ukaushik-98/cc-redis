use std::{
    collections::HashMap,
    hash::Hash,
    sync::{Arc, Mutex},
    time::{self, Duration, Instant},
};

use bytes::{Buf, BufMut};
use clap::Parser;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
};

enum RedisCommands {
    Echo(String),
    Ping(String),
    Set(String),
    Get(String),
}

#[derive(Debug)]
struct RedisEntry {
    value: String,
    stored: Instant,
    expirey: i32,
}

struct RedisDB {
    instance: Arc<Mutex<HashMap<String, RedisEntry>>>,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long, default_value_t = format!("6379"))]
    port: String,

    /// Number of times to greet
    #[arg(short, long, default_value_t = format!("master"))]
    replicaof: String,
}

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let args = Args::parse();

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await.unwrap();

    let db = RedisDB {
        instance: Arc::new(Mutex::new(HashMap::new())),
    };

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                println!("accepted new connection");

                let mut db_clone = RedisDB {
                    instance: db.instance.clone(),
                };

                tokio::spawn(async move {
                    loop {
                        let mut buf = Vec::new();
                        let mut buf_reader = BufReader::new(&mut stream);
                        let read_stream = buf_reader.read_buf(&mut buf).await.unwrap();

                        if read_stream == 0 {
                            println!("socket closed!");
                            break;
                        }

                        let command = match std::str::from_utf8(&buf) {
                            Ok(s) => s,
                            Err(_) => panic!("failed to parse input"),
                        };

                        let command: Vec<&str> = command.trim().split("\r\n").collect();

                        let response = parser(command, &mut db_clone);

                        let _ = stream.write(response.as_bytes()).await;
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn parser(command: Vec<&str>, db: &mut RedisDB) -> String {
    match command[2].to_ascii_lowercase().as_str() {
        "ping" => "+PONG\r\n".to_string(),
        "echo" => {
            format!("${}\r\n{}\r\n", command[4].len(), command[4])
        }
        "set" => {
            println!("command: {:?}", command);
            let px = if command.len() == 9 {
                command[8].parse().unwrap()
            } else {
                -1
            };

            let entry = RedisEntry {
                value: command[6].to_string(),
                stored: Instant::now(),
                expirey: px,
            };

            println!("REDIS ENTRY: {:?}", entry);

            db.instance
                .lock()
                .unwrap()
                .insert(command[4].to_string(), entry);

            "+OK\r\n".to_string()
        }
        "get" => {
            let mut db_lock = db.instance.lock().unwrap();
            let value: String;
            match db_lock.get(command[4]) {
                Some(val) => {
                    println!("REDIS ENTRY: {:?}", val);
                    let expirey: i32 = val.expirey.try_into().unwrap();
                    println!("EXPIREY: {}", expirey);
                    if expirey != -1
                        && Instant::now() - val.stored
                            >= Duration::from_millis(expirey.try_into().unwrap())
                    {
                        let _ = db_lock.remove(command[4]);
                        return "$-1\r\n".to_string();
                    }
                    value = val.value.clone()
                }
                None => return "$-1\r\n".to_string(),
            };
            format!("${}\r\n{}\r\n", value.len(), value)
        }
        _ => panic!("unrecognized command"),
    }
}
