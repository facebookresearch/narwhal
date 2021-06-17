use super::*;
use futures::future::try_join_all;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::sleep;

pub fn listener(address: SocketAddr) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let mut transport = Framed::new(socket, LengthDelimitedCodec::new());
        match transport.next().await {
            Some(Ok(_)) => assert!(true),
            _ => assert!(false),
        }
    })
}

#[tokio::test]
async fn send() {
    // Make the network sender.
    let (tx, rx) = channel(1);
    let mut sender = NetSender::new(rx);
    tokio::spawn(async move {
        sender.run().await;
    });

    // Run a TCP server.
    let address = "127.0.0.1:5000".parse::<SocketAddr>().unwrap();
    let handle = listener(address);

    // Send a message.
    let message = NetMessage(Bytes::from("Ok"), vec![address]);
    let _ = tx.send(message).await;

    // Ensure the server received the message (ie. it did not panic).
    assert!(handle.await.is_ok());
}

#[tokio::test]
async fn broadcast() {
    // Make the network sender.
    let (tx, rx) = channel(1);
    let mut sender = NetSender::new(rx);
    tokio::spawn(async move {
        sender.run().await;
    });

    // Run 3 TCP servers.
    let (handles, addresses): (Vec<_>, Vec<_>) = (0..3)
        .map(|x| {
            let address = format!("127.0.0.1:{}", 5100 + x)
                .parse::<SocketAddr>()
                .unwrap();
            (listener(address), address)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .unzip();

    // Broadcast a message.
    let message = NetMessage(Bytes::from("Ok"), addresses);
    let _ = tx.send(message).await;

    // Ensure all servers received the broadcast.
    assert!(try_join_all(handles).await.is_ok());
}

#[tokio::test]
async fn receive() {
    // Make the network receiver.
    let address = "127.0.0.1:5200".parse::<SocketAddr>().unwrap();
    let (tx, mut rx): (Sender<String>, _) = channel(1);
    let receiver = NetReceiver::new(address.clone(), tx);
    tokio::spawn(async move {
        receiver.run().await;
    });
    sleep(Duration::from_millis(50)).await;

    // Send a message.
    let message = "Ok";
    let bytes = Bytes::from(bincode::serialize(message).unwrap());
    let stream = TcpStream::connect(address).await.unwrap();
    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
    transport.send(bytes.clone()).await.unwrap();

    // Ensure the message gets passed to the channel.
    match rx.recv().await {
        Some(value) => assert_eq!(value, message),
        _ => assert!(false),
    }
}
