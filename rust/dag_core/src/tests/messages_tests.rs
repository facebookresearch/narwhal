use super::*;
use crate::net::{set_channel_type, worker_server_start};
use crate::receive_worker::ReceiveWorker;
use crate::send_worker::SendWorker;
use crate::store::Store;
use crate::types::types_tests::*;
use bytes::Bytes;
use futures::sink::SinkExt;
use rand::{rngs::StdRng, SeedableRng};
use rstest::*;
use std::collections::VecDeque;
use std::error::Error;
use tokio::net::TcpStream;
use tokio::sync::mpsc::channel;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[fixture]
pub fn committee(keys: Vec<(NodeID, SecretKey)>) -> Committee {
    let instance_id = 100;
    let mut rng = StdRng::from_seed([0; 32]);
    let authorities = keys
        .iter()
        .enumerate()
        .map(|(i, (id, _))| {
            let primary = Machine {
                name: *id,
                host: "127.0.0.1".to_string(),
                port: 8080 + i as u16,
            };
            let worker = Machine {
                name: get_keypair(&mut rng).0,
                host: "127.0.0.1".to_string(),
                port: 9080 + i as u16,
            };
            Authority {
                primary,
                workers: vec![(0, worker)],
                stake: 1,
            }
        })
        .collect();
    Committee {
        authorities,
        instance_id,
    }
}

#[fixture]
pub fn header(mut keys: Vec<(NodeID, SecretKey)>, committee: Committee) -> BlockHeader {
    let parents = Certificate::genesis(&committee)
        .iter()
        .map(|x| (x.primary_id, x.digest))
        .collect();
    let (node_id, _) = keys.pop().unwrap();
    BlockHeader {
        author: node_id,
        round: 1,
        parents,
        metadata: Metadata::default(),
        transactions_digest: WorkersDigests::new(),
        instance_id: committee.instance_id,
    }
}

#[fixture]
pub fn signed_header(mut keys: Vec<(NodeID, SecretKey)>, header: BlockHeader) -> SignedBlockHeader {
    let (_, secret_key) = keys.pop().unwrap();
    SignedBlockHeader::debug_new(header, &secret_key).unwrap()
}

#[fixture]
pub fn certificate(
    keys: Vec<(NodeID, SecretKey)>,
    committee: Committee,
    header: BlockHeader,
    signed_header: SignedBlockHeader,
) -> Certificate {
    let digest = signed_header.digest;
    let mut aggregator = SignatureAggregator::new();
    aggregator.init(header.clone(), digest, header.round, header.author);
    let mut result = None;
    for (id, secret) in keys.into_iter() {
        let digest = PartialCertificate::make_digest(digest, header.round, header.author);
        let signature = Signature::new(&digest, &secret);
        result = aggregator.append(id, signature, &committee).unwrap();
    }
    result.unwrap()
}

pub fn pool(input: u8) -> TransactionList {
    let mut pool = VecDeque::new();
    for i in 0..input as u8 {
        let mut transaction = Vec::new();
        transaction.push(i);
        pool.push_back(transaction);
    }
    pool
}

#[rstest]
fn test_block_check(signed_header: SignedBlockHeader, committee: Committee) {
    assert!(signed_header.check(&committee).is_ok());
}

#[rstest]
fn test_genesis(committee: Committee) {
    assert_eq!(
        Certificate::genesis(&committee).len(),
        committee.quorum_threshold()
    );
}

#[rstest]
fn test_parent_check_function(header: BlockHeader) {
    let parent_digest: Vec<Digest> = header
        .parents
        .iter()
        .map(|(_, digest)| digest.clone())
        .collect();

    assert!(header.has_parent(&parent_digest[0]));

    assert!(header.has_all_parents(&parent_digest));

    let some_parents = vec![parent_digest[0], parent_digest[1]];
    assert!(!header.has_all_parents(&some_parents));

    assert!(header.has_at_least_one_parent(&some_parents));
}

#[tokio::test]
async fn test_send_batch() {
    let mut rng = StdRng::from_seed([0; 32]);
    let (node_key, _) = get_keypair(&mut rng);
    let pool = pool(15);
    let mut send_pool = pool.clone();
    let ip = "127.0.0.1:8080".parse().unwrap();

    let (mut tx_send, tx_recv) = channel(100);
    for tx in pool {
        let _ = tx_send.send((ip, tx)).await;
    }

    let (send, mut recv) = channel(100);
    let _future_1 = tokio::spawn(async move {
        let mut send_worker = SendWorker::new(tx_recv, send, BATCH_SIZE);

        send_worker.start_sending(node_key).await
    });
    tokio::task::yield_now().await;
    'outer: while let Some(message) = recv.recv().await {
        match message.get_message() {
            WorkerMessage::Batch { transactions, .. } => {
                let mut transactions = transactions.clone();
                while let Some(send) = transactions.pop_front() {
                    if let Some(submitted) = send_pool.pop_front() {
                        assert_eq!(submitted, send);
                        if send_pool.len() == 0 {
                            break 'outer;
                        }
                    } else {
                        assert!(
                            false,
                            "My local pool is empty and there are transactions in the buffer"
                        )
                    }
                }
            }
            _ => assert!(false, "Unexpected other Worker Message"),
        }
    }
    assert!(send_pool.is_empty(), "My local pool still has transactions")
}

const BATCH_SIZE: usize = 15;

#[tokio::test]
async fn test_send_receive_batch() {
    let pool = pool(15);
    let mut rng = StdRng::from_seed([0; 32]);
    let (node_key, _) = get_keypair(&mut rng);

    let (send, recv) = channel(100);
    let (send_hash, mut recv_hash) = channel(100);

    let ip = "127.0.0.1:8080".parse().unwrap();

    let (mut tx_send, tx_recv) = channel(100);
    for tx in pool.clone() {
        let _ = tx_send.send((ip, tx)).await;
    }

    //launch one or more senders
    let _future_1 = tokio::spawn(async move {
        let mut send_worker = SendWorker::new(tx_recv, send, BATCH_SIZE);

        send_worker.start_sending(node_key).await
    });
    //launch one or more receivers
    let mut my_store = Store::new(".storage_receiver_storage".to_string()).await;

    let store2 = my_store.clone();
    let _future_2 = tokio::spawn(async move {
        let mut recv_worker = ReceiveWorker::new(0, recv, send_hash, store2);

        recv_worker.start_receiving().await.unwrap();
        return recv_worker.hasher_endpoint;
    });

    if let Some((_shard_id, recv_hash)) = recv_hash.recv().await {
        let data = my_store.read(recv_hash.0.to_vec()).await.unwrap().unwrap();
        let wm: WorkerMessage = bincode::deserialize(&data[..]).unwrap();

        if let WorkerMessage::Batch { transactions, .. } = wm {
            assert!(transactions.len() == 15);
        } else {
            panic!("Did not get the right structure.")
        }
    }
}

#[tokio::test]
async fn test_receive_network() -> Result<(), Box<dyn Error>> {
    let pool = pool(15);
    let mut rng = StdRng::from_seed([0; 32]);
    let (node_key, _) = get_keypair(&mut rng);
    let (send, recv) = channel(100);
    let (send2, _) = channel(100);
    let (send3, _) = channel(100);

    let _future_1 = tokio::spawn(async move {
        worker_server_start("127.0.0.1:8084".to_string(), send, send2, send3)
            .await
            .expect("Server failed to start.");
    });
    tokio::task::yield_now().await;

    // Send down the channel

    // Connect to server using TCP
    let stream = TcpStream::connect("127.0.0.1:8084").await?;

    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
    set_channel_type(&mut transport, WorkerChannelType::Worker).await;

    let mut buf = VecDeque::<Transaction>::with_capacity(1);

    for transaction in pool {
        buf.push_back(transaction);
    }

    let msg = WorkerMessage::Batch {
        node_id: node_key,
        special: 0,
        // Drain from the front of the pool either all transactions or the batch_size, whichever is less
        transactions: buf,
    };

    let data = Bytes::from(bincode::serialize(&msg)?);

    if let Err(e) = transport.send(data).await {
        assert!(false, "Sending TCP Message failed {:}", e);
    };
    tokio::task::yield_now().await;

    let (send_hash, mut recv_hash) = channel(100);

    //launch one or more receivers
    let mut my_store = Store::new(".receiver_storage".to_string()).await;

    let store2 = my_store.clone();
    let _future_2 = tokio::spawn(async move {
        let mut recv_worker = ReceiveWorker::new(0, recv, send_hash, store2);

        recv_worker.start_receiving().await.unwrap();
        return recv_worker.hasher_endpoint;
    });

    if let Some((_shard_id, recv_hash)) = recv_hash.recv().await {
        let data = my_store.read(recv_hash.0.to_vec()).await.unwrap().unwrap();
        let wm: WorkerMessage = bincode::deserialize(&data[..]).unwrap();

        if let WorkerMessage::Batch { transactions, .. } = wm {
            assert!(transactions.len() == 15);
        } else {
            panic!("Did not get the right structure.")
        }
    }
    Ok(())
}
