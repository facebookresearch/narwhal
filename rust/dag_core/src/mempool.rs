use super::committee::*;
use super::types::*;
//use crate::consensus::Consensus;
use crate::error::DagError;
use crate::messages::{BlockHeader, GC_DEPTH};
use crate::types::MempoolMessage::*;
use bytes::Bytes;
use futures::future::FutureExt;
use futures::select;
use log::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct MempoolBackend {
    ///DAG in Memory
    pub headers_dag: HashMap<RoundNumber, BTreeMap<Digest, BlockHeader>>,
    ///Hashmap to lookup if a Blockheader is already committeed
    pub committed_blocks: HashMap<Digest, bool>,
    ///Send to DAG what to GC
    pub send_to_dag: Sender<ConsensusMessage>,
    ///Last synched  round
    pub head_of_sync: RoundNumber,
    /// FIFO of all committed certificates
    pub to_clean: VecDeque<Certificate>,
    /// Committee of this instance.
    pub committee: Committee,
    pub primary_id: NodeID,
    /// Last leader that had enough support
    pub last_committed_round: RoundNumber,
    /// Get Info from front-end
    pub from_front_end: Receiver<ConsensusMessage>,
}

impl MempoolBackend {
    pub fn new(
        send_to_dag: Sender<ConsensusMessage>,
        committee: Committee,
        primary_id: NodeID,
        from_front_end: Receiver<ConsensusMessage>,
    ) -> Self {
        Self {
            headers_dag: HashMap::new(),
            committed_blocks: HashMap::new(),
            send_to_dag,
            head_of_sync: 0,
            committee: committee.clone(),
            primary_id,
            to_clean: VecDeque::new(),
            last_committed_round: 0,
            from_front_end,
        }
    }

    pub async fn start(&mut self) {
        loop {
            let raw = self.from_front_end.recv().await;
            if let Some(msg) = raw {
                //let msg1 = msg.clone();
                match msg {
                    ConsensusMessage::Header(cert) => {
                        self.add_header(cert.header, cert.digest);
                    }
                    ConsensusMessage::Cleanup(cert) => {
                        self.to_clean.push_back(cert);
                        self.cleanup().await;
                    }
                    ConsensusMessage::SyncDone(round) => {
                        self.head_of_sync = round;
                        self.cleanup().await;
                    }
                    ConsensusMessage::OrderedHeaders(_, _) => {
                        //We are running async here
                        let _ = self.send_to_dag.send(msg).await;
                    }
                }
            }
        }
    }

    fn add_header(&mut self, header: BlockHeader, digest: Digest) {
        self.headers_dag
            .entry(header.round)
            .or_insert_with(BTreeMap::new)
            .insert(digest, header);
        self.committed_blocks.insert(digest, false);
    }

    pub fn get_header_digest_by_node_round(
        &self,
        node: NodeID,
        round: RoundNumber,
    ) -> Option<(&Digest, &BlockHeader)> {
        if let Some(headers) = self.headers_dag.get(&round) {
            for (digest, header) in headers {
                if header.author == node {
                    return Some((digest, header));
                }
            }
        }
        None
    }
    pub fn get_headers(&self, round: RoundNumber) -> Option<&BTreeMap<Digest, BlockHeader>> {
        self.headers_dag.get(&round)
    }

    fn is_committed(&self, digest: Digest) -> Result<bool, DagError> {
        Ok(*self
            .committed_blocks
            .get(&digest)
            .ok_or(DagError::NoBlockAvailable)?)
    }

    fn order_dag(&mut self, round: RoundNumber, head: NodeID) -> Vec<(BlockHeader, Digest)> {
        let mut ordered_blocks = VecDeque::new();
        {
            let sub_dag_to_commit = self.get_block_to_commit(head, round);

            for r in 1..round + 1 {
                if let Some(blocks) = sub_dag_to_commit.get(&r) {
                    let mut digests: Vec<Digest> =
                        blocks.iter().map(|(digest, _)| *digest).collect();
                    digests.sort();
                    for digest in digests {
                        let block = blocks.get(&digest).unwrap();
                        let clone = (*block).clone();
                        ordered_blocks.push_back((clone, digest));
                    }
                }
            }
        }
        self.commit_blocks(ordered_blocks.clone());
        return ordered_blocks.into_iter().collect();
    }

    //Create a HashMap of all blocks to commit per round
    fn get_block_to_commit(
        &self,
        head: NodeID,
        round: RoundNumber,
    ) -> HashMap<RoundNumber, BTreeMap<Digest, &BlockHeader>> {
        //HashMap of the blocks per Round and will be committed
        let mut to_commit = HashMap::new();
        //Helper FIFO Queue to process all blocks from this round until there are no more blocks to commit
        let mut to_process = VecDeque::new();
        let (head_digest, header) = self.get_header_digest_by_node_round(head, round).unwrap();
        //we slowly decrease rounds we process as blocks appear
        let mut current_round = header.round;
        //Here we keep the blocks that should be committed from round-1
        let mut to_commit_round = BTreeMap::new();

        if round == 1 {
            to_commit_round.insert(*head_digest, header);
            to_commit.insert(current_round, to_commit_round);
            return to_commit;
        }
        //The header of the round-1 that we are processing
        let mut parents_headers = self.get_headers(current_round - 1).unwrap();
        //Let's process the header
        to_process.push_back((head_digest, header));
        while let Some((head_digest, header)) = to_process.pop_front() {
            //Did we pop a block that is not a child of the current round?
            if current_round != header.round {
                to_commit.insert(current_round, to_commit_round.clone());
                if header.round > 1 {
                    to_commit_round = BTreeMap::new();
                    current_round = header.round;
                    parents_headers = self.get_headers(current_round - 1).unwrap();
                } else {
                    //we are at round 1 so we get all the blocks in the pipe and return
                    to_commit_round = BTreeMap::new();
                    current_round = header.round;
                }
            }
            to_commit_round.insert(*head_digest, header);

            let mut gc_round = 0;

            if current_round > GC_DEPTH && self.last_committed_round >= GC_DEPTH {
                gc_round = self.last_committed_round - GC_DEPTH + 1
            }
            if current_round > 1 && current_round > gc_round {
                for (_, digest) in &header.parents {
                    if let Err(_e) = self.is_committed(*digest) {
                        error!(
                            "Block {:?} is not synched for round {:?}",
                            digest,
                            header.round - 1
                        );
                    } else if !self.is_committed(*digest).unwrap()
                        && !to_process.iter().any(|(e, _)| e == &digest)
                    {
                        let parent_header = parents_headers.get(digest).unwrap();
                        to_process.push_back((digest, parent_header))
                    }
                }
            }
        }
        to_commit.insert(current_round, to_commit_round);
        to_commit
    }

    fn commit_blocks(&mut self, mut headers: VecDeque<(BlockHeader, Digest)>) {
        while let Some((block, digest)) = headers.pop_front() {
            if !self.committed_blocks.get(&digest).unwrap() {
                self.committed_blocks.insert(digest, true);
                let commit_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                info!("Commit block in round {}", block.round);
                for x in &block.transactions_digest {
                    info!(
                        // This log is used to compute performance.
                        "Commit digest {:?} at commit time {}",
                        x.0, commit_time
                    );
                }
            }
        }
    }

    async fn cleanup(&mut self) {
        if let Some(next_cert) = self.to_clean.pop_front() {
            if next_cert.round < self.head_of_sync
                && self
                    .get_header_digest_by_node_round(next_cert.primary_id, next_cert.round)
                    .is_some()
            {
                if !self.is_committed(next_cert.digest).unwrap() {
                    let sequence = self.order_dag(next_cert.round, next_cert.primary_id);
                    if self
                        .send_to_dag
                        .send(ConsensusMessage::OrderedHeaders(
                            sequence,
                            next_cert.round - 1,
                        ))
                        .await
                        .is_err()
                    {
                        error!("Failed to send ordered headers back to dag");
                    }

                    if self.last_committed_round > GC_DEPTH {
                        let gc_round = self.last_committed_round - GC_DEPTH;

                        let digests = self
                            .headers_dag
                            .iter()
                            .filter(|(k, _)| **k <= gc_round)
                            .map(|(_, v)| v.keys())
                            .flatten();

                        for digest in digests {
                            let _ = self.committed_blocks.remove(&digest);
                        }

                        self.headers_dag.retain(|&r, _| r >= gc_round);
                    }
                }
            } else {
                // synchronizer has not synched the round yet
                self.to_clean.push_front(next_cert);
            }
        }
    }
}

pub struct Mempool {
    ///Send HS the last block
    pub last_certified_certificate: Certificate,
    //   pub send_to_consensus: Sender<ConsensusMessage>,
    //  pub get_from_consensus: Receiver<ConsensusMessage>,
    //get headers from primary.
    pub get_from_dag: Receiver<ConsensusMessage>,
    /// Committee of this instance.
    pub committee: Committee,
    pub primary_id: NodeID,
    pub get_from_hotstuff: Receiver<MempoolMessage>,
    pub to_back_end: Sender<ConsensusMessage>,
}

impl Mempool {
    pub fn new(
        send_to_dag: Sender<ConsensusMessage>,
        get_from_dag: Receiver<ConsensusMessage>,
        committee: Committee,
        primary_id: NodeID,
        // _external_consensus: bool,
        get_from_hotstuff: Receiver<MempoolMessage>,
    ) -> Self {
        // let (tx_consensus_receiving, mut rx_consensus_receiving) = channel(1000);
        // let (_tx_consensus_sending, rx_consensus_sending) = channel(1000);
        let (to_back_end, from_front_end) = channel(1000);

        //If no external consensus is run the mempool runs its own (Async)
        // if !external_consensus {
        //     let mut consensus = Consensus::new(
        //         tx_consensus_sending,
        //         rx_consensus_receiving,
        //         committee.clone(),
        //     );
        //     tokio::spawn(async move {
        //         consensus.start().await;
        //     });
        //     tokio::spawn(async move {
        //         loop {
        //             from_front_end.recv().await;
        //         }
        //     });
        // } else {
        let mut backend =
            MempoolBackend::new(send_to_dag, committee.clone(), primary_id, from_front_end);
        // tokio::spawn(async move {
        //     loop {
        //         rx_consensus_receiving.recv().await;
        //     }
        // });
        tokio::spawn(async move {
            backend.start().await;
        });
        // }

        Self {
            last_certified_certificate: Certificate::genesis(&committee).pop().unwrap(),
            // send_to_consensus: tx_consensus_receiving,
            // get_from_consensus: rx_consensus_sending,
            get_from_dag,
            committee: committee.clone(),
            primary_id,
            get_from_hotstuff,
            to_back_end,
        }
    }

    pub async fn start(&mut self) {
        loop {
            select! {

                    raw= self.get_from_hotstuff.recv().fuse() =>{
                        if let Some(msg)=raw{
                           match msg{
                            //FE
                            GetPayload(channel) => {
                                let payload= self.get(1);
                            if let Err(_e) =   channel.send(payload){
                                error!("channel to hotstuff is closed ")
                                    };
                            }
                            //FE
                            VerifyPayload(payload, channel) => {
                                let reply= self.verify(&payload);
                            if let Err(_e) =   channel.send(reply){
                                error!("channel to hotstuff is closed ")
                            };                            }
                            CommitedCertificate(cert) =>{
                                if let Some(cert_serialized) = cert.into_iter().nth(0) {
                                    let committed_certificate: Certificate =
                                    bincode::deserialize(&cert_serialized[..]).expect("Error deserisliazing");
                                                                        info!("Time to commit from round {:?}", committed_certificate.round);

                                    let msg=ConsensusMessage::Cleanup(committed_certificate);
                                    let _ = self.to_back_end.send(msg).await;
                                    }


                            }
                          }
                        }
                    }

                    // raw= self.get_from_consensus.recv().fuse() =>{
                    //     if let Some(msg)=raw{
                    //         //BE
                    //         let _ = self.to_back_end.send(msg).await;
                    //     }
                    //     //TODO channel is closed

                    // }

                    raw= self.get_from_dag.recv().fuse() =>{

                            if let Some(msg)=raw{
                               // let msg1=msg.clone();
                                match msg{
                                    ConsensusMessage::Header(ref cert) => {
                                        self.update_cert(cert.clone());
                                        let _ = self.to_back_end.send(msg).await;

                                    }
                                    ConsensusMessage::SyncDone(_) => {
                                       let _ = self.to_back_end.send(msg).await;

                        }

                                _ => {}


                                }
                           // let _ = self.send_to_consensus.send(msg).await;
                        }
                        //TODO channel is closed
                    }



            }
        }
    }

    fn get(&mut self, _max: usize) -> Vec<Vec<u8>> {
        if self.last_certified_certificate.round == 0 {
            return Vec::default();
        }
        let proposal = Bytes::from(
            bincode::serialize(&self.last_certified_certificate).expect("Error serializing"),
        );
        let serialized = proposal.to_vec();
        let ret_val = vec![serialized];
        info!(
            "Now proposing block for round {:}",
            self.last_certified_certificate.round
        );
        return ret_val;
    }

    fn verify(&mut self, payload: &[Vec<u8>]) -> PayloadStatus {
        if let Some(cert_serialized) = payload.into_iter().nth(0) {
            let cert: Certificate =
                bincode::deserialize(&cert_serialized[..]).expect("Error deserisliazing");
            info!("Now verifying cert for round {:?}", cert.round);

            if let Err(_) = cert.check(&self.committee) {
                return PayloadStatus::Reject;
            } else {
                return PayloadStatus::Accept;
            }
        } else {
            info!("This block is empty {:?}", payload);
            return PayloadStatus::Accept;
        }
    }

    fn update_cert(&mut self, cert: Certificate) {
        // if cert.primary_id == self.primary_id {
        if cert.round > self.last_certified_certificate.round {
            info!("Now holding cert for round {:?}", cert.round);

            self.last_certified_certificate = cert;
        }
        //}
    }
}
