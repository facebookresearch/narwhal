use super::*;
use crate::messages::messages_tests::*;
use crate::messages::SignedBlockHeader;
use crate::types::types_tests::*;
use bytes::Bytes;
use crypto::{generate_keypair, SecretKey};
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use rand::{rngs::StdRng, SeedableRng};
use rstest::*;
use std::collections::BTreeMap;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc::channel;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[rstest]
#[tokio::test]
async fn test_primary_net_receiver(
    signed_header: SignedBlockHeader,
    mut keys: Vec<(NodeID, SecretKey)>,
) {
    let port = 6500_u64;

    // 1. Make the receiver.
    let (id, _) = keys.pop().unwrap();
    let address = format!("127.0.0.1:{}", port).to_string();
    let (tx_deliver_channel, mut rx_deliver_channel) = channel(100);
    let mut primary_receiver = PrimaryNetReceiver::new(id, address.clone(), tx_deliver_channel);

    tokio::spawn(async move {
        if let Err(e) = primary_receiver.start_receiving().await {
            assert!(false, format!("Receiver error: {:?}", e))
        }
    });

    // 2. Send a message to the receiver.
    let message = PrimaryMessage::Header(signed_header.clone());
    let bytes: Vec<u8> = bincode::serialize(&message).unwrap();
    tokio::spawn(async move {
        if let Ok(stream) = TcpStream::connect(address).await {
            let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
            if let Err(e) = transport.send(Bytes::from(bytes)).await {
                assert!(false, format!("Error sending message: {:?}", e));
            };
        }
    });

    // 3. Drain the receiver channel and make sure the message is received.
    match rx_deliver_channel.recv().await.unwrap() {
        PrimaryMessage::Header(received) => assert_eq!(received, signed_header),
        _ => assert!(false),
    };
}

#[rstest]
#[tokio::test]
async fn test_primary_net_sender(signed_header: SignedBlockHeader, committee: Committee) {
    let addresses: BTreeMap<NodeID, String> = committee
        .authorities
        .iter()
        .map(|authority| {
            (
                authority.primary.name.clone(),
                format!("{}:{}", authority.primary.host, authority.primary.port),
            )
        })
        .collect();

    let ids: Vec<NodeID> = addresses.keys().map(|k| *k).clone().collect();

    let (mut tx_broadcast_channel, rx_broadcast_channel) = channel(100);
    // let (tx_loopback_channel, _) = channel(100);
    let mut sender = PrimaryNetSender::new(ids[0], committee, rx_broadcast_channel);

    tokio::spawn(async move {
        if let Err(e) = sender.start_sending().await {
            assert!(false, format!("Sender error: {:?}", e))
        }
    });

    let to_send = PrimaryMessage::Header(signed_header.clone());
    let _x = tokio::spawn(async move {
        if let Err(_e) = tx_broadcast_channel.send((1, to_send)).await {
            assert!(false);
        }
        tx_broadcast_channel
    });

    // 2. Check that a receiver got the message.
    let address_0 = addresses.get(&ids[0]).unwrap();
    let mut socket_0 = TcpListener::bind(&address_0).await.unwrap();
    tokio::spawn(async move {
        let _ = socket_0.accept().await;
    });

    let address_1 = addresses.get(&ids[1]).unwrap();
    let mut socket_1 = TcpListener::bind(&address_1).await.unwrap();
    tokio::spawn(async move {
        let _ = socket_1.accept().await;
    });

    let address_2 = addresses.get(&ids[2]).unwrap();
    let mut socket_2 = TcpListener::bind(&address_2).await.unwrap();
    tokio::spawn(async move {
        let _ = socket_2.accept().await;
    });

    let address_3 = addresses.get(&ids[3]).unwrap();
    let mut socket_3 = TcpListener::bind(&address_3).await.unwrap();
    let (stream, _) = socket_3.accept().await.unwrap();
    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
    let frame = transport.next().await;
    assert!(frame.is_some(), "Frame is empty.");
    let data = frame.unwrap();
    assert!(data.is_ok(), "Error receiving frame.");
    let message = data.unwrap();
    let deserialized = bincode::deserialize(&message[..]);
    assert!(deserialized.is_ok(), "Deserialization failed.");
    match deserialized.unwrap() {
        PrimaryMessage::Header(received) => assert_eq!(received, signed_header),
        _ => assert!(false),
    };
}

use crate::types::NodeID;
use tokio::sync::mpsc;

fn fixture_get_nodeid(x: u8) -> NodeID {
    let mut rng = StdRng::from_seed([x; 32]);
    generate_keypair(&mut rng).0
}

#[tokio::test]
async fn test_primary_net_message_drop() {
    let node: NodeID = fixture_get_nodeid(0);
    let data = Bytes::from("A custom message");

    /*
        Make message handles, check message not cancelled, and send a response back.
    */
    let (mut m_handle, m_response) = make_message(node.clone(), data.clone());

    let resp = Bytes::from("A response message");
    assert!(m_handle.is_cancelled() == false);

    m_handle.set_response(resp.clone());

    let (_nid, rmsg) = m_response.recv().await.unwrap();
    assert_eq!(resp, rmsg);

    /*
        Drop the message response handle, and check message is cancelled.
    */
    let mut m2 = {
        let (m2, _) = make_message(node.clone(), data.clone());
        m2
    };
    assert!(m2.is_cancelled() == true);

    // Drop manually
    let (mut m3, mr3) = make_message(node.clone(), data.clone());
    assert!(m3.is_cancelled() == false);
    mr3.cancel();
    assert!(m3.is_cancelled() == true);

    // Drop actual message
    let (m_handle4, m_response4) = make_message(node.clone(), data.clone());
    drop(m_handle4);

    match m_response4.recv().await {
        Err(DagError::MessageHandleDropped) => {}
        _ => panic!("Wrong error"),
    }
}

async fn echo_service(url: String) {
    // first run a test server that reflects messages.
    tokio::spawn(async move {
        let mut server = TcpListener::bind(url).await.unwrap();
        loop {
            let (stream, _) = server.accept().await.unwrap();
            let mut transport = Framed::new(stream, LengthDelimitedCodec::new());

            let msg = transport.next().await.unwrap().unwrap().freeze();
            println!("Received: {:?}", msg);
            assert!(msg == Bytes::from("OK_MESSAGE"));
            transport.send(msg).await.unwrap();
            // close channel with every message
        }
    });
    tokio::task::yield_now().await;
}

async fn echo_service_connection(url: String) {
    // first run a test server that reflects messages.
    tokio::spawn(async move {
        let mut server = TcpListener::bind(url).await.unwrap();
        loop {
            let (stream, _) = server.accept().await.unwrap();
            let mut transport = Framed::new(stream, LengthDelimitedCodec::new());

            while let Some(msg) = transport.next().await {
                let msg = msg.unwrap().freeze();
                println!("Received: {:?}", msg);
                assert!(msg == Bytes::from("OK_MESSAGE"));
                transport.send(msg).await.unwrap();
                // close channel with every message
            }
        }
    });
    tokio::task::yield_now().await;
}

#[tokio::test]
async fn test_primary_net_channel_ng() {
    let node: NodeID = fixture_get_nodeid(0);

    echo_service("127.0.0.1:8889".to_string()).await;

    let (mut tx, rx) = mpsc::channel(100);

    let mut pcs = PrimaryChannelState {
        host: "127.0.0.1:8889".to_string(),
        crecv: rx,
    };

    tokio::spawn(async move {
        let _ = pcs.run_watchdog_loop(None).await;
    });

    tokio::task::yield_now().await;

    // Now send some ok, old, and cancelled messages
    let (ok_handle, ok_response) = make_message(node.clone(), Bytes::from("OK_MESSAGE"));
    let (canc_handle, canc_response) = make_message(node.clone(), Bytes::from("CANCEL_MESSAGE"));
    canc_response.cancel();
    let (ok_handle2, ok_response2) = make_message(node.clone(), Bytes::from("OK_MESSAGE"));
    let (canc_handle2, canc_response2) = make_message(node.clone(), Bytes::from("CANCEL_MESSAGE"));
    canc_response2.cancel();

    tx.send(ok_handle).await.unwrap();
    tx.send(canc_handle).await.unwrap();
    tx.send(ok_handle2).await.unwrap();
    tx.send(canc_handle2).await.unwrap();

    tokio::task::yield_now().await;

    assert!(ok_response.recv().await.unwrap().1 == Bytes::from("OK_MESSAGE"));
    assert!(ok_response2.recv().await.unwrap().1 == Bytes::from("OK_MESSAGE"));
}

#[tokio::test]
async fn test_primary_net_channel_high_level_one() {
    let node: NodeID = fixture_get_nodeid(0);
    let node_err: NodeID = fixture_get_nodeid(1);
    let node100: NodeID = fixture_get_nodeid(100);

    // first run a test server that reflects messages.
    echo_service("127.0.0.1:8789".to_string()).await;

    let addresses: BTreeMap<NodeID, String> = vec![(node, "127.0.0.1:8789".to_string())]
        .iter()
        .cloned()
        .collect();
    let mut net = PrimaryNet::new(node100, addresses, None).await;
    tokio::task::yield_now().await;

    // Now send some ok, old, and cancelled messages
    let resp_ok = net
        .send_message(node.clone(), Bytes::from("OK_MESSAGE"))
        .await
        .unwrap();

    assert!(resp_ok.recv().await.unwrap().1 == Bytes::from("OK_MESSAGE"));

    let resp_err = net
        .send_message(node_err.clone(), Bytes::from("OK_MESSAGE"))
        .await;
    assert!(resp_err.is_err());
}

#[tokio::test]
async fn test_primary_net_channel_high_level_many() {
    let node: NodeID = fixture_get_nodeid(0);

    // first run a test server that reflects messages.
    echo_service_connection("127.0.0.1:8449".to_string()).await;

    let addresses: BTreeMap<NodeID, String> = vec![(node, "127.0.0.1:8449".to_string())]
        .iter()
        .cloned()
        .collect();

    let node100: NodeID = fixture_get_nodeid(100);
    let mut net = PrimaryNet::new(node100, addresses, None).await;
    tokio::task::yield_now().await;

    let mut resps = Vec::new();

    for _ in 0..10 {
        // Now send some ok, old, and cancelled messages
        let resp_ok = net
            .send_message(node.clone(), Bytes::from("OK_MESSAGE"))
            .await
            .unwrap();
        resps.push(resp_ok);
    }

    while let Some(resp_ok) = resps.pop() {
        assert!(resp_ok.recv().await.unwrap().1 == Bytes::from("OK_MESSAGE"));
    }
}

use futures::stream::futures_unordered::FuturesUnordered;

#[tokio::test]
async fn test_primary_net_channel_broadcast() {
    let node0: NodeID = fixture_get_nodeid(0);
    let node1: NodeID = fixture_get_nodeid(1);
    let node2: NodeID = fixture_get_nodeid(2);
    let node3: NodeID = fixture_get_nodeid(3);

    // first run a test server that reflects messages.
    echo_service("127.0.0.1:8201".to_string()).await;
    echo_service("127.0.0.1:8202".to_string()).await;
    echo_service("127.0.0.1:8203".to_string()).await;

    let addresses: BTreeMap<NodeID, String> = vec![
        (node0, "127.0.0.1:8200".to_string()),
        (node1, "127.0.0.1:8201".to_string()),
        (node2, "127.0.0.1:8202".to_string()),
        (node3, "127.0.0.1:8203".to_string()),
    ]
    .iter()
    .cloned()
    .collect();

    let mut net = PrimaryNet::new(node0, addresses, None).await;
    tokio::task::yield_now().await;

    // Now send some ok, old, and cancelled messages
    let resp_all = net.broadcast_message(Bytes::from("OK_MESSAGE")).await;

    // How to get 2f out of 3f responses in any order?
    let mut fut_stream = FuturesUnordered::from(
        resp_all
            .into_iter()
            .filter(|x| !x.is_err())
            .map(|x| x.unwrap().recv())
            .collect(),
    );

    let mut num = 0;
    while let Some(res) = fut_stream.next().await {
        let (nodex, _msg_resp) = res.unwrap();
        num += 1;
        println!("Got response from {:?} (num: {})", nodex, num);
        if num == 3 {
            break;
        }
    }
    drop(net);
    tokio::task::yield_now().await;
}
