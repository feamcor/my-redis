use tokio::net::{TcpListener, TcpStream};
use mini_redis::{Connection, Frame};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() {
    let mut listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("Listening...");

    let mut counter: u64 = 0;
    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        counter += 1;
        println!("#{}: Connection accepted.", counter);
        let db = db.clone();
        let _handle = tokio::spawn(async move {
            process(counter, socket, db).await;
        });
    }
}

async fn process(connection_id: u64, socket: TcpStream, db: Db) {
    use mini_redis::Command::{self, Get, Set};

    let mut connection = Connection::new(socket);
    println!("#{}: Connection processing...", connection_id);

    let mut counter: u64 = 0;
    while let Some(frame) = connection.read_frame().await.unwrap() {
        counter += 1;
        let response = match Command::from_frame(frame).unwrap() {
            Set(command) => {
                println!("#{}: #{}: {:?}", connection_id, counter, command);
                let mut db = db.lock().unwrap();
                db.insert(command.key().to_string(), command.value().clone());
                Frame::Simple("OK".to_string())
            },
            Get(command) => {
                println!("#{}: #{}: {:?}", connection_id, counter, command);
                let db = db.lock().unwrap();
                if let Some(value) = db.get(command.key()) {
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            },
            command => {
                panic!("unimplemented {:?}", command);
            },
        };
        connection.write_frame(&response).await.unwrap();
    }

    println!("#{}: Connection closed.", connection_id);
}