use super::committee::*;
use super::error::*;
use super::messages::*;
use super::processor::*;
use super::store::Store;
use super::types::Metadata;
use super::types::*;
use crate::mempool::*;
use crate::primary_net::PrimaryNetSender;
use crate::sync_driver::SyncDriver;
use crate::synchronizer::*;
use futures::future::FutureExt;

use log::*;
use std::collections::{BTreeSet, HashMap};

use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Duration, Instant};

// #[cfg(test)]
// #[path = "tests/primary_tests.rs"]
// pub mod primary_tests;

const BLOCK_TIMER_RESOLUTION: u64 = 53; // 100; // ms

pub struct PrimaryCore {
    /// The name of this node.
    pub id: NodeID,
    /// Committee of this instance.
    pub committee: Committee,
    /// Current round number.
    pub round: RoundNumber,
    /// Processor with the logic to deliver messages.
    pub processor: Processor,
    // /// Holds the digests of all transactions batches.
    // pub batch_digests: HashMap<NodeID, WorkersDigests>,
    //  Holds digests to include in our block
    pub digests_to_include: WorkersDigests,
    /// Channel to request signatures.
    pub signature_channel: Sender<(Digest, oneshot::Sender<Signature>)>,
    /// Aggregate signatures into a certificate for our own blocks.
    pub aggregator: SignatureAggregator,
    /// Channel to send messages to remote machines and workers.
    pub sending_endpoint: Sender<(RoundNumber, PrimaryMessage)>,
    /// Channel to receive messages from remote machines and workers.
    pub receiving_endpoint: Receiver<PrimaryMessage>,
    /// Persistent storage.
    pub store: Store,
    /// Map remembering blockheaders we already signed (to avoid signing conflicting headers). Cleaned when self.round is advanced.
    pub received_headers: HashMap<(NodeID, RoundNumber), PartialCertificatedHeaders>,
    /// The channel to a task that waits for header
    pub header_waiter_channel: mpsc::UnboundedSender<(SignedBlockHeader, BlockHeader)>,
    // //  Channel to send messages to consensus
    // pub send_to_consensus: Sender<ConsensusMessage>,
    //  Channel to receive messages from consensus
    pub get_from_consensus: Receiver<ConsensusMessage>,
    //  Channel to send messages to synchronizer
    pub send_to_synchronizer: Sender<SyncMessage>,
    /// The last garbage collect round.
    pub last_garbage_collected_round: RoundNumber,
    /// Send messages to the sync driver that delays sync request by a few seconds.
    tx_missing: Sender<(Digest, PrimaryMessage)>,
    /// Receive commands from the sync driver so that they can be forwarded to the net.
    rx_sync_commands: Receiver<PrimaryMessage>,
}

impl PrimaryCore {
    /// Make a new core.
    pub fn new(
        id: NodeID,
        committee: Committee,
        signature_channel: Sender<(Digest, oneshot::Sender<Signature>)>,
        sending_endpoint: Sender<(RoundNumber, PrimaryMessage)>,
        receiving_endpoint: Receiver<PrimaryMessage>,
        loopback_receiving_endpoint: Sender<PrimaryMessage>,
        store: Store,
        // external_consensus: bool,
        get_from_hotstuff: Receiver<MempoolMessage>,
    ) -> Self {
        let inner_genesis = Certificate::genesis(&committee);

        let processor = Processor::new(committee.clone());

        // Sync driver. This struct receives sync requests from the primary,
        // delays them, and then forwards them to the network.
        let (tx_missing, rx_missing) = channel(1000);
        let (tx_commands, rx_commands) = channel(1000);
        SyncDriver::run(rx_missing, tx_commands.clone(), store.clone());

        // Initialize the task that waits for all header dependencies.
        let (tx, rx) = mpsc::unbounded_channel();
        let inner_store = store.clone();
        tokio::spawn(async move {
            header_waiter_process(inner_store, inner_genesis, rx, loopback_receiving_endpoint)
                .await;
        });

        let (tx_mempool_receiving, rx_mempool_receiving) = channel(1000);
        let (tx_mempool_sending, rx_mempool_sending) = channel(1000);
        let mut mempool = Mempool::new(
            tx_mempool_sending,
            rx_mempool_receiving,
            committee.clone(),
            id,
            // external_consensus,
            get_from_hotstuff,
        );
        tokio::spawn(async move {
            mempool.start().await;
        });

        let (tx_synchronizer_to_network, rx_synchronizer_to_network) = channel(1000);
        let mut primary_net_sender =
            PrimaryNetSender::new(id, committee.clone(), rx_synchronizer_to_network);

        tokio::spawn(async move {
            if let Err(e) = primary_net_sender.start_sending().await {
                error!("Receiver error: {:?}", e)
            }
        });

        let dag_synchronizer = DagSynchronizer::new(
            id,
            committee.clone(),
            store.clone(),
            tx_mempool_receiving,
            tx_synchronizer_to_network,
        );
        let (tx_synchronizer_receiving, rx_synchronizer_receiving) = channel(1000);
        tokio::spawn(async move {
            dag_synchronizer_process(rx_synchronizer_receiving, dag_synchronizer).await;
        });

        Self {
            id,
            committee,
            round: 0,
            processor,
            // batch_digests: HashMap::new(),
            digests_to_include: WorkersDigests::new(),
            signature_channel,
            aggregator: SignatureAggregator::new(),
            sending_endpoint,
            receiving_endpoint,
            store,
            received_headers: HashMap::new(),
            header_waiter_channel: tx,
            // send_to_consensus: tx_consensus_receiving,
            get_from_consensus: rx_mempool_sending,
            send_to_synchronizer: tx_synchronizer_receiving,
            last_garbage_collected_round: 0,
            tx_missing,
            rx_sync_commands: rx_commands,
        }
    }

    /// Each of our workers send transactions digests from which we build the block id.
    pub async fn handle_transactions_digest(
        &mut self,
        digest: Digest,
        worker_id: NodeID,
        shard_id: ShardID,
    ) -> Result<(), DagError> {
        // We convert the worker_id into a primary ID
        // let primary_id = self.committee.get_primary_for_worker(&worker_id)?;
        // We store by primary ID
        if let Ok(primary_id) = self.committee.get_primary_for_worker(&worker_id) {
            if primary_id == self.id {
                self.digests_to_include.insert((digest, shard_id));
            }
        } else {
            panic!(format!("{:?} worker has no primary", worker_id));
        };

        // Make a record
        let record = BlockPrimaryRecord::new(digest, worker_id, shard_id).to_bytes();

        // Here we persist the digest to be able to wait on the store using 'notify_read'.
        self.store
            .write(digest.0.to_vec(), record) //key and value are the same?
            .await
            .map_err(|e| DagError::StorageFailure {
                error: format!("{}", e),
            })?;

        Ok(())
    }

    /// Makes a new blockheader.
    pub async fn make_blockheader(
        &mut self,
        parents: BTreeSet<(NodeID, Digest)>,
        round: RoundNumber,
    ) -> Result<(), DagError> {
        // Get the transactions digest of for this round.
        // It is ok to send an empty block if there are not transactions digests.

        // Code to send 1/2 of empty blocks
        // let mut rng = rand::thread_rng();
        // let transactions_digest = if rng.gen::<u32>() % 2 == 0 { self.digests_to_include.clone() }
        // else { BTreeSet::new() };

        let transactions_digest = self.digests_to_include.clone();

        let number_of_digests = transactions_digest.len();
        debug!(
            "(Header {}) Number of digests: {}",
            round, number_of_digests
        );
        debug!("Digests: {:?}", transactions_digest);

        for x in &transactions_digest {
            info!(
                // This log is used to compute performance.
                "Making header with txs digest {:?} at make time {}",
                x.0,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            );
        }

        // Make blockheader.
        let blockheader = BlockHeader {
            author: self.id,
            round,
            metadata: Metadata::default(),
            parents,
            transactions_digest,
            instance_id: self.committee.instance_id,
        };
        let signed_blockheader =
            SignedBlockHeader::new(blockheader.clone(), &mut self.signature_channel).await?;
        let digest = signed_blockheader.digest;

        // Record that we are collecting signatures on this blockheader.
        self.aggregator.init(blockheader, digest, round, self.id);

        // Now process our very own blockheader, but without sending the message.
        match self.handle_blockheader(signed_blockheader.clone()).await {
            Ok(Some(partial)) => {
                if let Err(e) = self.handle_partial_certificate(partial).await {
                    error!("Error handling own cert: {:?}", e);
                }
            }
            Ok(None) => {
                error!("Could not get partial cert for own header?");
            }
            Err(e) => {
                error!("Error handling our own header {:?}", e);
            }
        };

        // And broadcast it to everyone else
        self.send_message(PrimaryMessage::Header(signed_blockheader))
            .await?;

        Ok(())
    }

    //This function assumes we have all the parents.
    pub fn check_parents_quorum(&self, header: &BlockHeader) -> bool {
        if header.parents.is_empty() {
            return false;
        }
        header
            .parents
            .iter()
            .map(|(primary_id, _digest)| self.committee.stake(&primary_id))
            .sum::<Stake>()
            >= self.committee.quorum_threshold()
    }

    pub fn check_parents_stored(&self, header: &BlockHeader) -> bool {
        let digest: BTreeSet<(NodeID, Digest)> = self
            .processor
            .get_certificates(header.round - 1)
            .cloned()
            .unwrap_or_else(HashMap::new)
            .iter()
            .map(|(digest, cert)| (cert.primary_id, *digest))
            .collect();

        digest.is_superset(&header.parents)
    }

    pub async fn write_header_to_store(
        &mut self,
        header: BlockHeader,
        digest: Digest,
    ) -> Result<(), DagError> {
        debug!(
            "Writing to store HeaderPrimaryRecord with digest: {:?}",
            digest
        );
        let record = HeaderPrimaryRecord::new(header, digest).to_bytes();
        self.store
            .write(digest.0.to_vec(), record)
            .await
            .map_err(|e| DagError::StorageFailure {
                error: format!("{}", e),
            })?;
        Ok(())
    }

    /// Handle an incoming blockheader.
    pub async fn handle_blockheader(
        &mut self,
        header: SignedBlockHeader,
    ) -> Result<Option<PartialCertificate>, DagError> {
        // 1. Deserialize the header and check it.
        debug!("Start process ...");

        let deserialized_header = header.check(&self.committee)?;

        let sender = deserialized_header.author;
        let round = deserialized_header.round;

        if round < self.last_garbage_collected_round {
            return Ok(None);
        }

        // 1.1 If this is a header for a block for which we already have a certificate stored,
        //     Then we just store it.
        if self
            .processor
            .contains_certificate_by_digest_round(&header.digest, round)
            && !self
                .processor
                .contains_header_by_digest_round(&header.digest, round)
        {
            self.write_header_to_store(deserialized_header.clone(), header.digest)
                .await?;

            // 5. Process the header. This "delivers" the header.
            self.processor
                .add_header(deserialized_header, header.digest);

            debug!("Bare store Header {:?} Round {:?}", header.digest, round);
            return Ok(None);
        }

        //2. Ignore headers from old rounds
        if round < self.round {
            debug!(
                "Received header from round {} but we are at round {}",
                round, self.round
            );
            return Ok(None);
        }

        // 3. If we already have a signed header stored for this (sender, round) re-transmit it.
        if let Some(already_received) = self.received_headers.get(&(sender, round)) {
            if already_received.header != header {
                warn!(
                    "Node {:?} sent conflicting headers for round {}.",
                    sender, round
                );
            }
            return Ok(already_received.partial_certificate.clone());
        }

        // 4. Verify that we have all transactions digests and parents certificates.
        let mut missing = false;

        for (digest, shard_id) in &deserialized_header.transactions_digest {
            if !self.store_has_digest(digest).await? {
                let worker_id = self.committee.get_worker_by_shard_id(&sender, *shard_id)?;

                let msg = PrimaryMessage::SyncTxSend(
                    SyncTxInfo {
                        digest: *digest,
                        shard_id: *shard_id,
                    },
                    worker_id,
                );
                //self.send_message(msg).await?;
                self.tx_missing
                    .send((*digest, msg))
                    .await
                    .expect("Failed to send message to sync driver");
                debug!("Missing TXBlock {:?}", digest);
                missing = true;
            };
        }

        for (other_primary_id, digest) in &deserialized_header.parents {
            // This is the key with which we store a cert in storage
            let digest_in_store =
                PartialCertificate::make_digest(*digest, round - 1, *other_primary_id);

            if !self
                .processor
                .contains_certificate_by_digest_round(&digest, round - 1)
            {
                let msg = PrimaryMessage::SyncCertRequest((*digest, round - 1), self.id, sender);
                //self.send_message(msg).await?;
                self.tx_missing
                    .send((digest_in_store, msg))
                    .await
                    .expect("Failed to send message to sync driver");
                debug!("Missing Parent {:?}", digest);
                missing = true;
            }
        }
        if missing {
            // Send to the waiter ...
            let _ = self
                .header_waiter_channel
                .send((header, deserialized_header));
            debug!("Missing dependencies.");
            return Ok(None);
        }

        // 5. Check parents total stake - we should have all parents ids by now.

        if !self.check_parents_quorum(&deserialized_header) {
            debug!("the digest of the WrongParents header: {:?}", header.digest);
            // Insert the header, BUT keep the response to None.
            // As a result any subsequent attempts to send a header will result in no action.
            self.received_headers
                .entry((sender, round))
                .or_insert_with(|| PartialCertificatedHeaders::new(header));

            dag_bail!(DagError::WrongParents);
        }

        // 6. All check passed: we can accept this blockheader.
        self.process_header(header, deserialized_header).await
    }

    pub async fn store_has_digest(&mut self, digest: &Digest) -> Result<bool, DagError> {
        Ok((self
            .store
            .read(digest.0.to_vec())
            .await
            .map_err(|e| DagError::ChannelError {
                error: format!("{}", e),
            })?)
        .is_some())
    }

    pub async fn send_message(&mut self, msg: PrimaryMessage) -> Result<(), DagError> {
        self.sending_endpoint
            .send((self.round, msg))
            .await
            .map_err(|e| DagError::ChannelError {
                error: format!("{}", e),
            })?;
        Ok(())
    }

    /// Process a correct blockheader (ie. that is already verified).
    async fn process_header(
        &mut self,
        header: SignedBlockHeader,
        deserialized_header: BlockHeader,
    ) -> Result<Option<PartialCertificate>, DagError> {
        let digest = header.digest;
        let sender = deserialized_header.author;
        let round = deserialized_header.round;

        dag_ensure!(
            !self.received_headers.contains_key(&(sender, round)),
            DagError::InternalError {
                error: "We should not have a header at this stage".to_string()
            }
        );
        let mut header_record = PartialCertificatedHeaders::new(header.clone());

        // 1. Counter-sign the blockheader (i.e make a partial certificate).
        let partial_certificate = PartialCertificate::new(
            self.id,
            sender,
            digest,
            round,
            self.signature_channel.clone(),
        )
        .await?;

        // 2. Store the blockheader.
        self.write_header_to_store(deserialized_header.clone(), header.digest)
            .await?;
        // ! This is the point of no return: we cannot fail after that.

        // 3. Remember that we signed this header.
        header_record.partial_certificate = Some(partial_certificate.clone());
        self.received_headers.insert((sender, round), header_record);

        // 5. Process the header. This "delivers" the header.
        self.processor.add_header(deserialized_header, digest);

        // 6. Return the partial certificate (ie. counter-signature).
        Ok(Some(partial_certificate))
    }

    /// Handles certificates.
    pub async fn handle_certificate(&mut self, certificate: Certificate) -> Result<(), DagError> {
        // info!("CERT: ({:?},{}) Deps: {:?} Txs: {:?}", certificate.header.author, certificate.header.round, &certificate.header.parents, &certificate.header.transactions_digest);

        // Ignore certificates that we already have.
        if self.processor.contains_certificate(&certificate) {
            return Ok(());
        }

        // Check the certificate.
        certificate.check(&self.committee)?;
        let digest = certificate.digest;
        let round = certificate.round;
        let header = certificate.header.clone();

        //TODO: No Longer important since we get certificates with headers

        // Add it to our persistent storage.
        //Note: it is important to distinguish from header digest. Otherwise, we might think that we have all parents certificates when actually we have only the headers
        let bytes: Vec<u8> = bincode::serialize(&certificate)?;
        let digest_to_store =
            PartialCertificate::make_digest(digest, round, certificate.primary_id);
        self.store
            .write(digest_to_store.0.to_vec(), bytes)
            .await
            .map_err(|e| DagError::StorageFailure {
                error: format!("{}", e),
            })?;

        // Try to remove from batch_digests all digests that have been certified.
        // Note: in the case of our own headers we always have them. Therefore when our own header gets a
        //       cert we are guaranteed to execute the positive branch and clean up the digest.
        if let Some(header) = self
            .processor
            .get_header(&certificate.digest, certificate.round)
        {
            let cert_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis();
            for x in &header.transactions_digest {
                let removed = self.digests_to_include.remove(x);
                if removed && header.author == self.id {
                    info!(
                         // This log is used to compute performance.
                        "Received our own certified digest {:?} at cert time {}",
                        x.0, cert_time
                    );
                }
            }
        } else {
            warn!("Have certificate but no header for {:?}", digest);
        }

        // Deliver the certificate.
        self.processor
            .add_header(header.clone(), certificate.digest);
        self.write_header_to_store(header, digest).await?;
        self.processor.add_certificate(certificate);

        // Check if this new certificate allows us to move to next round or
        if round >= self.round {
            self.try_move_new_round(round).await?;
        }

        Ok(())
    }

    /// Handle partial certificate (ie. echo-signatures on our own blocks).
    pub async fn handle_partial_certificate(
        &mut self,
        partial: PartialCertificate,
    ) -> Result<Option<Certificate>, DagError> {
        //   debug!("handling partial certificate {:?}", partial);
        if self.aggregator.partial.is_none()
            || self.aggregator.partial.as_mut().unwrap().round != partial.round
        {
            return Ok(None);
        }

        if partial.origin_node != self.id {
            error!("Received a partial ceritificate intended for another authority.");
            return Err(DagError::UnexpectedOrLatePartialCertificate);
        }

        if let Some(certificate) =
            self.aggregator
                .append(partial.author, partial.signature, &self.committee)?
        {
            self.aggregator.clear();

            // And handle our very own certificate -- but send nothing out.
            self.handle_certificate(certificate.clone()).await?;

            debug!("Certificate assembled for block {:?}", certificate.digest);
            return Ok(Some(certificate));
        }
        Ok(None)
    }

    /// Fetch a certificate to reply to sync requests.
    fn handle_sync_certificate(&self, digest: Digest, round: RoundNumber) -> Option<Certificate> {
        if let Some(data) = self.processor.get_certificates(round) {
            if let Some(certificate) = data.get(&digest) {
                return Some(certificate.clone());
            }
        }
        None
    }

    async fn handle_header_reply(
        &mut self,
        certificate: Certificate,
        header: BlockHeader,
    ) -> Result<(), DagError> {
        let cert_digest = certificate.digest;
        self.handle_certificate(certificate).await?;
        //certification id valid
        let digest = bincode::serialize(&header)?.digest();
        if digest == cert_digest {
            self.processor.add_header(header.clone(), digest);
            self.write_header_to_store(header, digest).await?;
        }
        Ok(())
    }

    async fn handle_header_request(
        &mut self,
        digest: Digest,
    ) -> Result<Option<(Certificate, BlockHeader)>, DagError> {
        if let Some(db_value) =
            self.store
                .read(digest.0.to_vec())
                .await
                .map_err(|e| DagError::StorageFailure {
                    error: format!("{}", e),
                })?
        {
            let record_header: HeaderPrimaryRecord = bincode::deserialize(&db_value[..])?;
            let header = record_header.header;
            let digest_cert_in_store =
                PartialCertificate::make_digest(digest, header.round, header.author);
            if let Some(db_value) = self
                .store
                .read(digest_cert_in_store.0.to_vec())
                .await
                .map_err(|e| DagError::StorageFailure {
                    error: format!("{}", e),
                })?
            {
                let certificate: Certificate = bincode::deserialize(&db_value[..])?;
                return Ok(Some((certificate, header)));
            }
        }
        Ok(None)
    }

    /// Try to move to a new round.
    async fn try_move_new_round(&mut self, round: RoundNumber) -> Result<(), DagError> {
        if let Some(data) = self.processor.get_certificates(round) {
            // TODO: [issue #101] Esnure the certificates are from different authorities.
            if data
                .iter()
                .map(|(_, certificate)| self.committee.stake(&certificate.primary_id))
                .sum::<Stake>()
                >= self.committee.quorum_threshold()
            {
                // let parents: BTreeSet<_> = data.iter().map(|(digest, _)| *digest).collect();
                // self.make_blockheader(parents.clone(), self.round).await?;
                let keys: Vec<(NodeID, u64)> = self
                    .received_headers
                    .keys()
                    .filter(|(_, r)| *r <= round)
                    .cloned()
                    .collect();
                for key in keys {
                    self.received_headers.remove(&key);
                }

                // Ask for sync for rounds round-1. We do not ask for the current round
                // to give blocks and certs nor received so far a chance to arrive naturally
                // before we 'pull' them.
                if self.round > 1 {
                    let data_prev_round = self.processor.get_certificates(round - 1).unwrap();
                    let digests_prev_round: Vec<Digest> =
                        data_prev_round.iter().map(|(digest, _)| *digest).collect();

                    let msg = SyncMessage::SyncUpToRound(
                        self.round - 1,
                        digests_prev_round,
                        self.last_garbage_collected_round,
                    );
                    self.send_to_synchronizer.send(msg).await.map_err(|e| {
                        DagError::ChannelError {
                            error: format!("{}", e),
                        }
                    })?;
                }
                self.round = round + 1;

                let round_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();

                info!("Moving to round {} at time {}", self.round, round_time);
            }
        }
        return Ok(());
    }

    // Main loop.
    pub async fn start(&mut self) {
        // Move to the first round (using the genesis).
        if let Err(e) = self.try_move_new_round(self.round).await {
            error!("Failed to move to the first round: {}", e);
        }

        let mut last_sent_round = 0;

        let timer = sleep(Duration::from_millis(BLOCK_TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // We trigger progress in the Primary server when:
                // 1. We receive a message from another peer primary.
                msg = self.receiving_endpoint.recv().fuse() => {
                    if let Some(msg) = msg {
                        if let Err(e) = self.handle_message(msg).await {
                            error!("{}", e);
                        }
                    }
                    else { break; }
                }

                // 2. We have just sealed some more state into the consensus and are trigerring cleanups.
                msg = self.get_from_consensus.recv().fuse() => {
                    if let Some(msg) = msg {
                        match msg {
                            ConsensusMessage::OrderedHeaders(committed_sequence, last_committed_round) => {

                                if last_committed_round >= GC_DEPTH{
                                let tx_digests_list = self.processor.clean_headers(committed_sequence, last_committed_round-GC_DEPTH, self.id); //should the process have id field?
                                for digests in tx_digests_list {
                                    self.digests_to_include.extend(digests);
                                }
                                self.last_garbage_collected_round = last_committed_round-GC_DEPTH;

                            }
                        }
                            _ => {
                                error!("Received unexpected message from consensus!");
                            }
                        }
                    }
                }

                // 3. The timer expires and we might, as a result want
                // to make a  new block.
                _ = &mut timer => {



            if self.round > last_sent_round {
                // Check if we should send the new block header

                    if self.round > last_sent_round {
                        // Check if we should send the new block header

                        if let Some(data) = self.processor.get_certificates(self.round - 1) {
                            // TODO: [issue #101] Esnure the certificates are from different authorities.
                            debug!("Sending for round {}", self.round);
                            let parents: BTreeSet<_> = data
                                .iter()
                                .map(|(digest, cert)| (cert.primary_id, *digest))
                                .collect();
                            last_sent_round = self.round;
                            if let Err(e) = self.make_blockheader(parents, self.round).await {
                                error!("{:?}", e);
                            }
                        }
                    }
                }
                 timer
                        .as_mut()
                        .reset(Instant::now() + Duration::from_millis(BLOCK_TIMER_RESOLUTION));
                },



                // Receive messages from the sync driver and forwards them to the network.
                message = self.rx_sync_commands.recv().fuse() => {
                    if let Some(message) = message {
                        self.send_message(message).await.expect("Failed to forward sync request to the network");
                    }
                }
            }
        }
    }

    pub async fn handle_message(&mut self, message: PrimaryMessage) -> Result<(), DagError> {
        match message {
            // Handle tx digests.
            PrimaryMessage::TxDigest(digest, node_id, shard_id) => {
                debug!(
                    "Received digest: {:?} from node {:?} (shard {:?})",
                    digest, node_id, shard_id
                );
                self.handle_transactions_digest(digest, node_id, shard_id)
                    .await?;
                Ok(())
            }

            // Handle headers.
            PrimaryMessage::Header(header) => {
                let data = self.handle_blockheader(header).await?;
                if let Some(partial_certificate) = data {
                    // Send back a partial certificate to he sender.
                    self.send_message(PrimaryMessage::PartialCert(partial_certificate))
                        .await?;
                };
                Ok(())
            }

            // Handle partial certificates.
            PrimaryMessage::PartialCert(partial_certificate) => {
                if let Some(certificate) =
                    self.handle_partial_certificate(partial_certificate).await?
                {
                    if certificate.round >= self.round - 1 {
                        // Broadcast our new certificate that might make it to consenus.
                        self.send_message(PrimaryMessage::Cert(certificate)).await?;
                    }
                };
                Ok(())
            }

            // Handle certificates.
            PrimaryMessage::Cert(certificate) => {
                self.handle_certificate(certificate).await?;
                Ok(())
            }
            // Below are sync messages.
            PrimaryMessage::SyncCertRequest((digest, round), requester, _) => {
                //Send the certificate
                let data = self.handle_sync_certificate(digest, round);
                if let Some(certificate) = data {
                    // Send back the certificate to he sender.
                    self.send_message(PrimaryMessage::SyncCertReply(certificate, requester))
                        .await?;
                }
                Ok(())
            }

            PrimaryMessage::SyncHeaderRequest(digest, requester, _) => {
                if let Some(data) = self.handle_header_request(digest).await? {
                    self.send_message(PrimaryMessage::SyncHeaderReply(data, requester))
                        .await?;
                }
                Ok(())
            }

            PrimaryMessage::SyncHeaderReply((certificate, header), _) => {
                self.handle_header_reply(certificate, header).await?;
                Ok(())
            }

            _ => {
                warn!("Received unknown message");
                Ok(())
            }
        }
    }
}
