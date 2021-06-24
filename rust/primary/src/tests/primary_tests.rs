use super::*;
use crate::messages::messages_tests::*;
use crate::types::types_tests::*;
use crypto::SecretKey;
use rstest::*;
use tokio::sync::mpsc::channel;
use tokio::task;

#[rstest]
#[tokio::test]
async fn test_handle_blockheader_happy(
    mut keys: Vec<(NodeID, SecretKey)>,
    committee: Committee,
    mut header: BlockHeader,
) {
    let (tx_primary_sending, mut rx_primary_sending) = channel(100);
    let (tx_primary_receiving, rx_primary_receiving) = channel(100);
    let (tx_signature_channel, rx_signature_channel) = channel(100);
    let (id, secret_key) = keys.pop().unwrap();
    let mut store = Store::new(".storage_test_handle_blockheader_happy").unwrap();
    let signed_header = SignedBlockHeader::debug_new(header.clone(), &secret_key).unwrap();

    // 1. Run the signing service.
    let mut factory = SignatureFactory::new(secret_key, rx_signature_channel);
    tokio::spawn(async move {
        factory.start().await;
    });

    // 2. Make primary Core.
    let (tx_consensus_receiving, _rx_consensus_receiving) = channel(1000);
    let (_tx_consensus_sending, rx_consensus_sending) = channel(1000);
    let mut primary = PrimaryCore::new(
        id,
        committee,
        /* signature_channel */ tx_signature_channel,
        /* sending_endpoint */ tx_primary_sending,
        /* receiving_endpoint */ rx_primary_receiving,
        tx_primary_receiving.clone(),
        store.clone(),
        tx_consensus_receiving,
        rx_consensus_sending,
    );

    // Depend on the actual genesis certs
    header.parents = primary
        .processor
        .get_certificates(0)
        .unwrap()
        .iter()
        .map(|(digest, cert)| (cert.primary_id, digest.clone()))
        .collect();

    tokio::spawn(async move {
        primary.start().await;
    });

    for (other_primary_id, digest) in &header.parents {
        let digest_in_store = PartialCertificate::make_digest(&digest, 0, *other_primary_id);
        let _ = store.notify_read(digest_in_store.0.to_vec()).await;
    }

    // 3. Send a block header.
    let message = PrimaryMessage::Header(signed_header);
    tokio::spawn(async move {
        tx_primary_receiving.send(message).await.unwrap();
    });

    // 4. Drain channels.
    // We expect to receive a partial certificate of the block we sent in step 3.
    loop {
        match rx_primary_sending
            .recv()
            .await
            .expect("Primary channel is open (1)")
        {
            (_round, PrimaryMessage::PartialCert(_partial_certificate)) => {
                assert!(true);
                break;
            }
            (_, msg) => {
                println!("{:?}", msg);
                // assert!(false, "Expected a certificate.");
            }
        };
    }
}

#[rstest]
#[tokio::test]
async fn test_handle_blockheader_conflicting(
    mut keys: Vec<(NodeID, SecretKey)>,
    committee: Committee,
    header: BlockHeader,
) {
    // env_logger::init();

    let (tx_primary_sending, _xyz) = channel(100);
    let (tx_primary_receiving, rx_primary_receiving) = channel(100);
    let (id, secret_key) = keys.pop().unwrap();
    let (tx_signature_channel, rx_signature_channel) = channel(100);
    let mut store = Store::new(".storage_test_handle_blockheader_conflicting").unwrap();
    let signed_header = SignedBlockHeader::debug_new(header.clone(), &secret_key).unwrap();

    let mut conflicting_header = header.clone();
    conflicting_header.metadata = Metadata(Some([1; 32]));
    let conflicting_signed_header =
        SignedBlockHeader::debug_new(conflicting_header, &secret_key).unwrap();

    // 1. Run the signing service.
    let mut factory = SignatureFactory::new(secret_key, rx_signature_channel);
    tokio::spawn(async move {
        factory.start().await;
    });

    // 2. Make primary Core.
    let (tx_consensus_receiving, _rx_consensus_receiving) = channel(1000);
    let (_tx_consensus_sending, rx_consensus_sending) = channel(1000);
    let mut primary = PrimaryCore::new(
        id,
        committee,
        /* signature_channel */ tx_signature_channel,
        /* sending_endpoint */ tx_primary_sending,
        /* receiving_endpoint */ rx_primary_receiving,
        tx_primary_receiving.clone(),
        store.clone(),
        tx_consensus_receiving,
        rx_consensus_sending,
    );

    for (_, cert) in primary.processor.dag.get(&0).unwrap() {
        let digest_in_store = PartialCertificate::make_digest(&cert.digest, 0, cert.primary_id);
        let _ = store.notify_read(digest_in_store.0.to_vec()).await;
    }

    // 3. Send a block header.
    let x = primary.handle_blockheader(signed_header).await;

    // 4. Send a conflicting blockheader.
    let y = primary.handle_blockheader(conflicting_signed_header).await;

    let in_x = x.expect("No partial cert (1)");
    let in_y = y.expect("No partial cert (2)");
    println!("{:?} vs {:?}", in_x, in_y);

    assert!(in_x == in_y, "Did not receive the same response.");
}

#[rstest]
#[tokio::test]
async fn test_handle_blockheader_repeat(
    mut keys: Vec<(NodeID, SecretKey)>,
    committee: Committee,
    mut header: BlockHeader,
) {
    let (tx_primary_sending, _rx_primary_sending) = channel(100);
    let (tx_primary_receiving, rx_primary_receiving) = channel(100);
    let (id, secret_key) = keys.pop().unwrap();
    let (tx_signature_channel, rx_signature_channel) = channel(100);
    let store = Store::new(".storage_test_handle_blockheader_repeat").unwrap();
    let signed_header = SignedBlockHeader::debug_new(header.clone(), &secret_key)
        .expect("Inject a signed block failed.");

    // 1. Run the signing service.
    let mut factory = SignatureFactory::new(secret_key, rx_signature_channel);
    tokio::spawn(async move {
        factory.start().await;
    });

    // 2. Make primary Core.
    let (tx_consensus_receiving, _rx_consensus_receiving) = channel(1000);
    let (_tx_consensus_sending, rx_consensus_sending) = channel(1000);
    let mut primary = PrimaryCore::new(
        id,
        committee,
        /* signature_channel */ tx_signature_channel,
        /* sending_endpoint */ tx_primary_sending,
        /* receiving_endpoint */ rx_primary_receiving,
        tx_primary_receiving.clone(),
        store.clone(),
        tx_consensus_receiving,
        rx_consensus_sending,
    );

    task::yield_now().await;

    // 3. Send a block header.
    header.parents = primary
        .processor
        .get_certificates(0)
        .unwrap()
        .iter()
        .map(|(digest, cert)| (cert.primary_id, digest.clone()))
        .collect();

    for (other_primary_id, digest) in &header.parents {
        let digest_in_store = PartialCertificate::make_digest(&digest, 0, *other_primary_id);
        let _ = primary.store.notify_read(digest_in_store.0.to_vec()).await;
    }

    let partial_certificate_1 = primary
        .handle_blockheader(signed_header.clone())
        .await
        .expect("Unwrap signed header response (1.1)")
        .expect("Unwrap signed header response (1.2)");

    // 4. Send a again the same blockheader.
    let partial_certificate_2 = primary
        .handle_blockheader(signed_header)
        .await
        .expect("Unwrap signed header response (2.1)")
        .expect("Unwrap signed header response (2.2)");

    assert_eq!(
        partial_certificate_1, partial_certificate_2,
        "Expected equal certificates"
    );
}

#[rstest]
#[tokio::test]
async fn test_handle_blockheader_missing(
    mut keys: Vec<(NodeID, SecretKey)>,
    committee: Committee,
    mut header: BlockHeader,
) {
    let (tx_primary_sending, mut _rx_primary_sending) = channel(100);
    let (tx_primary_receiving, rx_primary_receiving) = channel(100);
    let (id, secret_key) = keys.pop().unwrap();
    let (tx_signature_channel, rx_signature_channel) = channel(100);
    let store = Store::new(".storage_test_handle_blockheader_missing").unwrap();

    header.transactions_digest.insert((Digest([1; 32]), 0));
    let signed_header = SignedBlockHeader::debug_new(header.clone(), &secret_key)
        .expect("Return a new signed header.");

    // 1. Run the signing service.
    let mut factory = SignatureFactory::new(secret_key, rx_signature_channel);
    tokio::spawn(async move {
        factory.start().await;
    });

    // 2. Make primary Core.
    let (tx_consensus_receiving, _rx_consensus_receiving) = channel(1000);
    let (_tx_consensus_sending, rx_consensus_sending) = channel(1000);
    let mut primary = PrimaryCore::new(
        id,
        committee,
        /* signature_channel */ tx_signature_channel,
        /* sending_endpoint */ tx_primary_sending,
        /* receiving_endpoint */ rx_primary_receiving,
        tx_primary_receiving.clone(),
        store,
        tx_consensus_receiving,
        rx_consensus_sending,
    );
    // 3. Send the blockheader.
    let res = primary
        .handle_blockheader(signed_header)
        .await
        .expect("There is a signed header.");
    assert!(res.is_none());
}

#[rstest]
#[tokio::test]
async fn test_handle_partial_certificate(
    mut keys: Vec<(NodeID, SecretKey)>,
    committee: Committee,
    header: BlockHeader,
) {
    let (tx_primary_sending, mut rx_primary_sending) = channel(100);
    let (tx_primary_receiving, rx_primary_receiving) = channel(100);
    let (primary_id, primary_secret_key) = keys.pop().unwrap();
    let (tx_signature_channel, rx_signature_channel) = channel(100);
    let store = Store::new(".storage_test_primary_core").unwrap();
    let signed_header = SignedBlockHeader::debug_new(header.clone(), &primary_secret_key).unwrap();

    // Run the signing service.
    let mut factory = SignatureFactory::new(primary_secret_key, rx_signature_channel);
    tokio::spawn(async move {
        factory.start().await;
    });

    // Make primary Core.
    let (tx_consensus_receiving, _rx_consensus_receiving) = channel(1000);
    let (_tx_consensus_sending, rx_consensus_sending) = channel(1000);
    println!("Make primary Core.");
    let mut primary = PrimaryCore::new(
        primary_id,
        committee,
        /* signature_channel */ tx_signature_channel,
        /* sending_endpoint */ tx_primary_sending,
        /* receiving_endpoint */ rx_primary_receiving,
        tx_primary_receiving.clone(),
        store,
        tx_consensus_receiving,
        rx_consensus_sending,
    );

    // Create a signed block header with the primary's signature.
    println!("Create a signed block header with the primary's signature.");
    let round = header.round;
    let sender = header.author;
    let digest = signed_header.digest.clone();

    // Init the primary aggregator to wait for partial certificates for this digest.
    primary
        .aggregator
        .init(header, digest.clone(), round, sender);

    tokio::spawn(async move {
        primary.start().await;
    });

    // Send partial certificates on the genesis block to the primary.
    // Note that the primary already initialized its aggregator with the genesis digest.
    println!("Send partial certificates on the genesis block to the primary.");
    for (id, secret_key) in keys {
        assert!(id != primary_id);
        let partial_certificate =
            PartialCertificate::debug_new(id, primary_id, digest.clone(), round, &secret_key);
        let message = PrimaryMessage::PartialCert(partial_certificate);
        let receiving = tx_primary_receiving.clone();
        tokio::spawn(async move {
            receiving.send(message).await.unwrap();
        });
    }

    // Drain channels.
    // We expect to receive (i) the first block emitted by the primary, and
    // (ii) a certificate for the block we initialized its aggregator with (the genesis block).

    println!("Drain channels (1).");
    match rx_primary_sending.recv().await.unwrap() {
        (_round, PrimaryMessage::Cert(_certificate)) => assert!(true),
        _ => assert!(false, "Expected a certificate!"),
    };
    println!("Drain channels (2).");
    match rx_primary_sending.recv().await.unwrap() {
        (_round, PrimaryMessage::Header(_signed_header)) => assert!(true),
        msg => assert!(false, "Expected a header! But got {:?}", msg),
    };
}

#[rstest]
#[tokio::test]
async fn test_primary_core_sync(
    mut keys: Vec<(NodeID, SecretKey)>,
    committee: Committee,
    mut header: BlockHeader,
) {
    let (tx_primary_sending, mut rx_primary_sending) = channel(100);
    let (tx_primary_receiving, rx_primary_receiving) = channel(100);
    let (id, secret_key) = keys.pop().unwrap();
    let (peer_id, peer_secret_key) = keys.pop().unwrap();
    let (tx_signature_channel, rx_signature_channel) = channel(100);
    let store = Store::new(".storage_test_primary_core_sync").unwrap();

    // 1. Run the signing service.
    let mut factory = SignatureFactory::new(secret_key, rx_signature_channel);
    tokio::spawn(async move {
        factory.start().await;
    });

    let (tx_consensus_receiving, _rx_consensus_receiving) = channel(1000);
    let (_tx_consensus_sending, rx_consensus_sending) = channel(1000);
    let mut primary = PrimaryCore::new(
        id,
        committee,
        /* signature_channel */ tx_signature_channel,
        /* sending_endpoint */ tx_primary_sending,
        /* receiving_endpoint */ rx_primary_receiving,
        tx_primary_receiving.clone(),
        store,
        tx_consensus_receiving,
        rx_consensus_sending,
    );

    //get their Digest
    let digest = Digest([1_u8; 32]);

    //get some more transactions
    //get their Digest
    let digest2 = Digest([2_u8; 32]);

    //build a block with the digest
    header.author = peer_id;
    header.transactions_digest.insert((digest.clone(), 0));
    header.transactions_digest.insert((digest2, 0));

    let signed_header = SignedBlockHeader::debug_new(header, &peer_secret_key).unwrap();

    // Send a new Digest and then the Signed Block, forget to send the second digest
    let message1 = PrimaryMessage::TxDigest(digest, peer_id, 0);
    let message2 = PrimaryMessage::Header(signed_header);

    tokio::spawn(async move {
        // println!("Sending header {:?}", message1);
        tx_primary_receiving.send(message1).await.unwrap();
        // println!("Sending header {:?}", message2);

        tx_primary_receiving.send(message2).await.unwrap();
    });

    // Start the Primary
    tokio::spawn(async move {
        primary.start().await;
    });

    // Check the sync requests of the primary

    loop {
        let (_round, msg) = rx_primary_sending.recv().await.unwrap();
        match msg {
            PrimaryMessage::SyncTxSend(info, id) => {
                println!("Need to sync at worker {:?}, from peer {:?} ", info, id);
                break;
            }
            _ => {
                println!("{:?}", msg);
                // assert!(false, "Failed to receive a sync message?");
            }
        };
    }
}
