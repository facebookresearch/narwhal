use crate::committee::*;
use crate::messages::messages_tests::*;
use crate::types::types_tests::*;

use crate::manage_primary::*;
use crate::manage_worker::*;
use crate::messages::WorkerChannelType;
use bytes::Bytes;
use futures::sink::SinkExt;
use rstest::*;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::stream::StreamExt;
use tokio::time::delay_for;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

#[rstest]
#[tokio::test]
#[ignore]
async fn test_full_primary_worker(committee: Committee, keys: Vec<(NodeID, SecretKey)>) {
    let total_transactions = 2000;
    let batch_size = 1000;
    full_primary_worker(committee, keys, total_transactions, batch_size).await;
}

pub async fn full_primary_worker(
    committee: Committee,
    mut keys: Vec<(NodeID, SecretKey)>,
    total_transactions: usize,
    batch_size: usize,
) {
    let mut workers = VecDeque::new();
    for auth in &committee.authorities {
        for (_, machine) in &auth.workers {
            let address = format!("{}:{}", machine.host, machine.port);
            workers.push_back((machine.name, address));
        }
    }
    let primaries: Vec<(NodeID, String)> = committee
        .authorities
        .iter()
        .map(|authority| {
            (
                authority.primary.name.clone(),
                format!("{}:{}", authority.primary.host, authority.primary.port),
            )
        })
        .collect();

    println!(
        "Nodes {:?} {:?} {:?} {:?}",
        primaries[0].0, primaries[1].0, primaries[2].0, primaries[3].0
    );

    let worker_receive_worker_port = 9080_u32; //worker receives messages here

    //Boot workers
    for (node_id, _) in primaries {
        if let Some(key) = keys.pop() {
            let worker_id = workers.pop_front().unwrap();
            println!("Node {:?} has worker {:?}", node_id, worker_id);
            let mut _worker =
                ManageWorker::new(worker_id.0, committee.clone(), batch_size, None).await;

            let mut primary = ManagePrimary::new(node_id, key.1, committee.clone());

            tokio::spawn(async move {
                primary.primary_core.start().await;
            });
        }
    }

    println!("All machines are on");
    //Step 2: Send a few transactions to worker of n0
    tokio::task::yield_now().await;

    // Connect to server using TCP
    let stream = TcpStream::connect(format!("127.0.0.1:{}", worker_receive_worker_port))
        .await
        .unwrap();

    let (read_sock, write_sock) = stream.into_split();
    let mut transport_write = FramedWrite::new(write_sock, LengthDelimitedCodec::new());
    let mut transport_read = FramedRead::new(read_sock, LengthDelimitedCodec::new());

    // Set the type of the channel
    let head = WorkerChannelType::Transaction;
    let header = Bytes::from(bincode::serialize(&head).expect("Error deserializing"));
    transport_write.send(header).await.expect("Error Sending");
    let _n = transport_read.next().await.expect("Error on test receive");

    tokio::spawn(async move {
        loop {
            let _ = transport_read.next().await;
        }
    });

    for t in 0..total_transactions {
        let txs = Bytes::from(t.to_string());

        // Write some data & wait for processing.
        let txs_copy = txs.clone();

        // println!("Before sending tx");
        if let Err(_e) = transport_write.send(txs_copy).await {
            break;
        }
    }

    //wait for a few seconds to run a full block and get a certificate
    delay_for(Duration::from_secs(3)).await;
}
