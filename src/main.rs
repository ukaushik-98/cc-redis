use std::{
    clone, collections::HashMap, fmt::format, hash::Hash, io::Write, result, sync::{Arc, Mutex}, time::{self, Duration, Instant}
};

use bytes::{Buf, BufMut};
use clap::Parser;
use reqwest::Client;
use tokio::{
    fs::File, io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader}, net::{tcp::WriteHalf, TcpListener, TcpStream}, stream
};
use base64::prelude::*;

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
    offset: String,
    replica_streams: Arc<tokio::sync::Mutex<Vec<TcpStream>>>
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
        offset: "0".to_string(),
        replica_streams: Arc::new(tokio::sync::Mutex::new(Vec::new()))
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

            buf.clear();
            let res = socket.read_buf(&mut buf).await.unwrap();
            let output3 = String::from_utf8_lossy(&buf);
            println!("response: {}", output3);

            buf.clear();
            let res = socket.read_buf(&mut buf).await.unwrap();
            let output4 = String::from_utf8_lossy(&buf);
            println!("response: {}", output4);

            let mut db_clone = RedisDB {
                instance: db.instance.clone(),
                status: args.replicaof.clone(),
                replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
                offset: "0".to_string(),
                replica_streams: db.replica_streams.clone()
            };

            tokio::spawn(async move {
                loop {
                    // let arc_stream = arc_stream.clone();
                    // let mut inner_stream = arc_stream.lock().await;
                    let mut buf = Vec::new();
                    let mut buf_reader = BufReader::new(&mut socket);
                    let read_stream = match buf_reader.read_buf(&mut buf).await {
                        Ok(val) => val,
                        Err(_) => 0,
                    };
    
                    let command_str = match std::str::from_utf8(&buf) {
                        Ok(s) => s,
                        Err(_) => panic!("failed to parse input"),
                    };
    
                    if command_str == "" {
                        continue;
                    }
    
                    let command: Vec<&str> = command_str.trim().split("\r\n").collect();
                    println!("{:?} COMMAND: {:?}", Instant::now(), command);
    
                    let response = parser(&command, &mut db_clone);
    
                    println!("{}", response);
    
                    let _ = socket.write(response.as_bytes()).await;
                }
            });
             
        },
        None => {} 
    };

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                println!("accepted new connection");

                let mut db_clone = RedisDB {
                    instance: db.instance.clone(),
                    status: args.replicaof.clone(),
                    replication_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
                    offset: "0".to_string(),
                    replica_streams: db.replica_streams.clone()
                };
                tokio::spawn(async move {
                    // let arc_stream = Arc::new(tokio::sync::Mutex::new(&mut stream));
                    loop {
                        // let arc_stream = arc_stream.clone();
                        // let mut inner_stream = arc_stream.lock().await;
                        let mut buf = Vec::new();
                        let mut buf_reader = BufReader::new(&mut stream);
                        let read_stream = match buf_reader.read_buf(&mut buf).await {
                            Ok(val) => val,
                            Err(_) => 0,
                        };

                        let command_str = match std::str::from_utf8(&buf) {
                            Ok(s) => s,
                            Err(_) => panic!("failed to parse input"),
                        };

                        if command_str == "" {
                            continue;
                        }

                        let command: Vec<&str> = command_str.trim().split("\r\n").collect();
                        println!("{:?} COMMAND: {:?}", Instant::now(), command);

                        let response = parser(&command, &mut db_clone);

                        println!("{}", response);

                        let _ = stream.write(response.as_bytes()).await;

                        match command[2].to_ascii_lowercase().as_str() {
                            "psync" => {
                                let mut file = File::open("src/rdb.txt").await.unwrap();
                                let mut file_buffer = vec![];
                                let _ = file.read_to_end(&mut file_buffer).await;
                                let decoded_rdb = &BASE64_STANDARD.decode(&file_buffer).unwrap();
                                // let test = stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
                                // println!("WROTE PING: {:?}", test);
                                // stream.flush().await;
                                // let test = stream.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
                                // println!("WROTE PING: {:?}", test);
                                // stream.flush().await;
                                let psync_res_write = stream.write_all(&[format!("${}\r\n", decoded_rdb.len()).as_bytes(), &decoded_rdb].concat()).await.unwrap();
                                println!("WRITE SUCEEDED: {:?}", psync_res_write);
                                db_clone.replica_streams.lock().await.push(stream);
                                break;
                            },
                            "set" => {
                                let mut streams = db_clone.replica_streams.lock().await;
                                for stream in streams.iter_mut() {
                                    let res = stream.write(&buf).await;
                                    match res {
                                        Ok(_) => println!("Write succeeded"),
                                        Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                                            println!("Broken pipe, removing stream");
                                            // Code to remove the broken stream from your list of streams
                                        }
                                        Err(e) => println!("An unexpected error occurred: {}", e),
                                    }
                                }
                            }
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
    println!("command: {:?}", command);
    match command[2].to_ascii_lowercase().as_str() {
        "ping" => "+PONG".to_string(),
        "echo" => {
            format!("${}\r\n{}\r\n", command[4].len(), command[4])
        }
        "set" => {
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
