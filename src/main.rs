use bytes::{Buf, BufMut};
use tokio::{io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader}, net::TcpListener};

enum RedisCommands {
    Echo(String),
    Ping(String),
    Set(String),
    Get(String)
}

fn parser(command: Vec<&str>) -> String {
    match command[2].to_ascii_lowercase().as_str() {
        "ping" => {
            "+PONG\r\n".to_string()
        },
        "echo" => {
            format!("${}\r\n{}\r\n", command[4].len(), command[4])
        },
        _ => panic!("unrecognized command")
    }
}

#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move {
                    loop {
                        let mut buf = vec![];
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

                        let command: Vec<&str> = command.trim().split("\\r\\n").collect();

                        let response = parser(command);

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
