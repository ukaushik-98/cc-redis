use bytes::{Buf, BufMut};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpListener};


#[tokio::main]
async fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        match listener.accept().await {
            Ok((mut stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move {
                    let mut buf: Vec<u8> = vec![255];
                    loop {
                        let read_stream = stream.read(&mut buf).await.unwrap();
                        if read_stream == 0 {
                            println!("socket closed!");
                            break;
                        }
                        let _ = stream.write(b"+PONG\r\n").await;
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
