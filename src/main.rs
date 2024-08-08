use std::{
    collections::HashMap, fmt::format, hash::Hash, io::Write, result, sync::{Arc, Mutex}, time::{self, Duration, Instant}
};

use bytes::{Buf, BufMut};
use clap::Parser;
use reqwest::Client;
use tokio::{
    fs::File, io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader}, net::{TcpListener, TcpStream}, stream
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
    status: Option<String>,
    replication_id: String,
    offset: String
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long, default_value_t = format!("6379"))]
    port: String,

    /// Number of times to greet
    #[arg(short, long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let args = Args::parse();

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).await.unwrap();

    let db = RedisDB {
        instance: Arc::new(Mutex::new(HashMap::new())),
        status: args.replicaof.clone(),
        replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
        offset: "0".to_string()
    };

    let res = match &args.replicaof {
        Some(val) => {
            let host = val.replace(" ", ":");
            println!("replica node - connecting to master {}", host);

            let mut buf = vec![];
            let mut socket = TcpStream::connect(&host).await.unwrap();
            println!("replica node - sending ping");
            let _ = socket.write_all(b"*1\r\n$4\r\nPING\r\n").await;
            let _ = socket.flush().await;

            let n = socket.read_buf(&mut buf).await.unwrap();
            let output = String::from_utf8_lossy(&buf);
            println!("response: {}", output);

            println!("replica node - sending listening port");
            buf.clear();
            let _ = socket.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n").await;
            let _ = socket.flush().await;
            let res = socket.read_buf(&mut buf).await.unwrap();
            let output1 = String::from_utf8_lossy(&buf);
            println!("response: {}", output1);

            println!("replica node - sending replica capabilities");
            buf.clear();
            let _ = socket.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n").await; 
            let res = socket.read_buf(&mut buf).await.unwrap();
            let output2 = String::from_utf8_lossy(&buf);
            println!("response: {}", output2);

            println!("replica node - sending replica capabilities");
            buf.clear();
            let _ = socket.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await; 
            let res = socket.read_buf(&mut buf).await.unwrap();
            let output2 = String::from_utf8_lossy(&buf);
            println!("response: {}", output2);

            // let mut file = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
            // println!("FILE: {}", file);
            // let _ = socket.write(format!("${}\r\n{}", file.len(), file.to_string()).as_bytes()).await; 
        },
        None => {
            // let host = "localhost:6380";
            // println!("replica node - connecting to master {}", host);

            // let mut buf = vec![];
            // let mut socket = TcpStream::connect(&host).await.unwrap();
            // println!("replica node - sending ping");
            // let _ = socket.write_all(b"*1\r\n$4\r\nPING\r\n").await;
            // let _ = socket.flush().await;

            // let n = socket.read_buf(&mut buf).await.unwrap();
            // let output = String::from_utf8_lossy(&buf);
            // println!("response: {}", output);

            // println!("replica node - sending listening port");
            // buf.clear();
            // let _ = socket.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n").await;
            // let _ = socket.flush().await;
            // let res = socket.read_buf(&mut buf).await.unwrap();
            // let output1 = String::from_utf8_lossy(&buf);
            // println!("response: {}", output1);

            // println!("replica node - sending replica capabilities");
            // buf.clear();
            // let _ = socket.write_all(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n").await; 
            // let res = socket.read_buf(&mut buf).await.unwrap();
            // let output2 = String::from_utf8_lossy(&buf);
            // println!("response: {}", output2);

            // println!("replica node - sending replica capabilities");
            // buf.clear();
            // let _ = socket.write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await; 
            // let res = socket.read_buf(&mut buf).await.unwrap();
            // let output2 = String::from_utf8_lossy(&buf);
            // println!("response: {}", output2);
            
            // let mut file = File::open("src/rdb.txt").await.unwrap();
            // let mut file_buffer = vec![];
            // let _ = file.read_to_end(&mut file_buffer).await;
            // println!("FILE: {}", String::from_utf8_lossy(&file_buffer));    
            // let _ = socket.write_all(&[format!("${}\r\n", file_buffer.len()).as_bytes(), &file_buffer].concat()).await;

            // let mut file = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
            // println!("FILE: {}", file);
            // let _ = socket.write(format!("${}\r\n{}", file.len(), file.to_string()).as_bytes()).await; 
        } 
    };

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                println!("accepted new connection");

                let mut db_clone = RedisDB {
                    instance: db.instance.clone(),
                    status: args.replicaof.clone(),
                    replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
                    offset: "0".to_string()
                };

                tokio::spawn(async move {
                    loop {
                        let mut buf = Vec::new();
                        let mut buf_reader = BufReader::new(&mut stream);
                        let read_stream = match buf_reader.read_buf(&mut buf).await {
                            Ok(val) => val,
                            Err(_) => 0,
                        };

                        if read_stream == 0 {
                            println!("socket closed!");
                            break;
                        }

                        let command = match std::str::from_utf8(&buf) {
                            Ok(s) => s,
                            Err(_) => panic!("failed to parse input"),
                        };

                        let command: Vec<&str> = command.trim().split("\r\n").collect();

                        println!("{:?}", command);

                        let response = parser(&command, &mut db_clone);

                        println!("{}", response);

                        let _ = stream.write(response.as_bytes()).await;

                        match command[2].to_ascii_lowercase().as_str() {
                            "psync" => {
                                let mut file = File::open("src/rdb.txt").await.unwrap();
                                let mut file_buffer = vec![];
                                let _ = file.read_to_end(&mut file_buffer).await;
                                let send_resp = &[format!("${}\r\n", file_buffer.len()).as_bytes(), &file_buffer].concat();
                                println!("FILE: {:?}", send_resp);
                                let _ = stream.write(format!("${}\r\n", file_buffer.len()).as_bytes()).await;
                            },
                            _ => {}
                        }
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn parser(command: &Vec<&str>, db: &mut RedisDB) -> String {
    match command[2].to_ascii_lowercase().as_str() {
        "ping" => "+PONG\r\n".to_string(),
        "echo" => {
            format!("${}\r\n{}\r\n", command[4].len(), command[4])
        }
        "set" => {
            println!("command: {:?}", command);
            let px = if command.len() == 11 {
                command[10].parse().unwrap()
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
        },
        "info" => {
            let role = match &db.status {
                Some(_) => {
                    "role:slave"
                },
                None => {
                    "role:master"
                },
            };
            let value = format!("role:{}:master_replid:{}:master_repl_offset:{}", role, db.replication_id, db.offset);
            format!("${}\r\n{}\r\n", value.len(), value)
        },
        "replconf" => {
            "+OK\r\n".to_string()
        },
        "psync" => {
            format!("+FULLRESYNC {} 0\r\n", db.replication_id)
        },
        _ => panic!("unrecognized command"),
    }
}
