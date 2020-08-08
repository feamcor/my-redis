use tokio::net::{TcpListener, TcpStream};
use mini_redis::{Connection, Frame};

#[tokio::main]
async fn main() {
    let mut listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let _handle = tokio::spawn(async move {
            process(socket).await;
        });
    }
}

async fn process(socket: TcpStream) {
    use mini_redis::Command::{self, Get, Set};
    use std::collections::HashMap;

    let mut db = HashMap::new();
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(command) => {
                db.insert(command.key().to_string(), command.value().clone());
                Frame::Simple("OK".to_string())
            },
            Get(command) => {
                if let Some(value) = db.get(command.key()) {
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            },
            command => panic!("unimplemented {:?}", command),
        };
        connection.write_frame(&response).await.unwrap();
    }
}