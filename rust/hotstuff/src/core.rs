use crate::aggregator::Aggregator;
use crate::config::Parameters;
use crate::error::{ConsensusError, ConsensusResult};
use crate::leader::LeaderElector;
use crate::messages::{Block, Timeout, Vote, QC, TC};
use crate::synchronizer::Synchronizer;
use crate::timer::Timer;
use async_recursion::async_recursion;
use bytes::Bytes;
use dag_core::committee::Committee;
use dag_core::types::*;
use log::{debug, error, info, warn};
use network::NetMessage;
use serde::{Deserialize, Serialize};
use std::cmp::max;

use dag_core::store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};
// #[cfg(test)]
// #[path = "tests/core_tests.rs"]
// pub mod core_tests;

pub type RoundNumber = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum CoreMessage {
    Propose(Block),
    Vote(Vote),
    Timeout(Timeout),
    TC(TC),
    LoopBack(Block),
    SyncRequest(Digest, NodeID),
}

pub struct Core {
    name: NodeID,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    signing_channel: Sender<(Digest, oneshot::Sender<Signature>)>,
    leader_elector: LeaderElector,
    send_to_mempool: Sender<MempoolMessage>,
    synchronizer: Synchronizer,
    core_channel: Receiver<CoreMessage>,
    network_channel: Sender<NetMessage>,
    commit_channel: Sender<Block>,
    round: RoundNumber,
    last_voted_round: RoundNumber,
    preferred_round: RoundNumber,
    high_qc: QC,
    timer: Timer<RoundNumber>,
    aggregator: Aggregator,
    last_certified_certificate: Certificate,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: NodeID,
        committee: Committee,
        parameters: Parameters,
        signing_channel: Sender<(Digest, oneshot::Sender<Signature>)>,
        store: Store,
        leader_elector: LeaderElector,
        send_to_mempool: Sender<MempoolMessage>,
        synchronizer: Synchronizer,
        core_channel: Receiver<CoreMessage>,
        network_channel: Sender<NetMessage>,
        commit_channel: Sender<Block>,
    ) -> Self {
        let aggregator = Aggregator::new(committee.clone());
        let last_certified_certificate = Certificate::genesis(&committee).pop().unwrap();
        Self {
            name,
            committee,
            parameters,
            signing_channel,
            store,
            leader_elector,
            send_to_mempool,
            synchronizer,
            network_channel,
            commit_channel,
            core_channel,
            round: 1,
            last_voted_round: 0,
            preferred_round: 0,
            high_qc: QC::genesis(),
            timer: Timer::new(),
            aggregator,
            last_certified_certificate,
        }
    }

    async fn store_block(&mut self, block: &Block) -> ConsensusResult<()> {
        let key = block.digest().0.to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store
            .write(key, value)
            .await
            .map_err(ConsensusError::from)
    }

    async fn schedule_timer(&mut self) {
        self.timer
            .schedule(self.parameters.timeout_delay, self.round)
            .await;
    }

    async fn transmit(&mut self, message: &CoreMessage, to: Option<NodeID>) -> ConsensusResult<()> {
        let addresses = if let Some(to) = to {
            debug!("Sending {:?} to {}", message, to);
            vec![self.committee.address(&to)?]
        } else {
            debug!("Broadcasting {:?}", message);
            self.committee.broadcast_addresses(&self.name)
        };
        let bytes = bincode::serialize(message).expect("Failed to serialize core message");
        let message = NetMessage(Bytes::from(bytes), addresses);
        if let Err(e) = self.network_channel.send(message).await {
            panic!("Failed to send block through network channel: {}", e);
        }
        Ok(())
    }

    // -- Start Safety Module --
    fn increase_last_voted_round(&mut self, target: RoundNumber) {
        self.last_voted_round = max(self.last_voted_round, target);
    }

    fn update_preferred_round(&mut self, target: RoundNumber) {
        self.preferred_round = max(self.preferred_round, target);
    }

    async fn make_vote(&mut self, block: &Block) -> Option<Vote> {
        // Check if we can vote for this block.
        let safety_rule_1 = block.round > self.last_voted_round;
        let safety_rule_2 = block.qc.round >= self.preferred_round;
        if !(safety_rule_1 && safety_rule_2) {
            return None;
        }

        // Ensure we won't vote for contradicting blocks.
        self.increase_last_voted_round(block.round);
        // TODO [issue #15]: Write to storage preferred_round and last_voted_round.
        Some(Vote::new(&block, self.name, &mut self.signing_channel).await)
    }
    // -- End Safety Module --

    // -- Start Pacemaker --
    fn update_high_qc(&mut self, qc: &QC) {
        if qc.round > self.high_qc.round {
            self.high_qc = qc.clone();
        }
    }

    async fn local_timeout_round(&mut self) -> ConsensusResult<()> {
        warn!("Timeout reached for round {}", self.round);
        self.increase_last_voted_round(self.round);
        let timeout = Timeout::new(
            self.high_qc.clone(),
            self.round,
            self.name,
            &mut self.signing_channel,
        )
        .await;
        debug!("Created {:?}", timeout);
        self.schedule_timer().await;
        let message = CoreMessage::Timeout(timeout.clone());
        self.transmit(&message, None).await?;
        self.handle_timeout(&timeout).await
    }

    #[async_recursion]
    async fn handle_vote(&mut self, vote: &Vote) -> ConsensusResult<()> {
        debug!("Processing {:?}", vote);
        if vote.round < self.round {
            return Ok(());
        }

        // Ensure the vote is well formed.
        vote.verify(&self.committee)?;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(qc) = self.aggregator.add_vote(vote.clone())? {
            debug!("Assembled {:?}", qc);

            // Process the QC.
            self.process_qc(&qc).await;

            // Make a new block if we are the next leader.
            if self.name == self.leader_elector.get_leader(self.round) {
                self.generate_proposal(None).await?;
            }
        }
        Ok(())
    }

    async fn handle_timeout(&mut self, timeout: &Timeout) -> ConsensusResult<()> {
        debug!("Processing {:?}", timeout);
        if timeout.round < self.round {
            return Ok(());
        }

        // Ensure the timeout is well formed.
        timeout.verify(&self.committee)?;

        // Process the QC embedded in the timeout.
        self.process_qc(&timeout.high_qc).await;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(tc) = self.aggregator.add_timeout(timeout.clone())? {
            debug!("Assembled {:?}", tc);

            // Try to advance the round.
            self.advance_round(tc.round).await;

            // Broadcast the TC.
            let message = CoreMessage::TC(tc.clone());
            self.transmit(&message, None).await?;

            // Make a new block if we are the next leader.
            if self.name == self.leader_elector.get_leader(self.round) {
                self.generate_proposal(Some(tc)).await?;
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn advance_round(&mut self, round: RoundNumber) {
        if round < self.round {
            return;
        }
        self.timer.cancel(self.round).await;
        self.round = round + 1;
        debug!("Moved to round {}", self.round);

        // Cleanup the vote aggregator.
        self.aggregator.cleanup(&self.round);

        // Schedule a new timer for this round.
        self.schedule_timer().await;
    }
    // -- End Pacemaker --

    #[async_recursion]
    async fn generate_proposal(&mut self, tc: Option<TC>) -> ConsensusResult<()> {
        // Make a new block.
        //  let mut payload = vec![];
        //let mut cert = self.last_certified_certificate.clone();
        // let mut first = true;
        //while cert.round == self.last_certified_certificate.round {
        // if !first {
        //     sleep(Duration::from_millis(10)).await;
        // }
        let (sender, receiver) = oneshot::channel();
        let msg = MempoolMessage::GetPayload(sender);
        self.send_to_mempool.send(msg).await.unwrap();

        let result = receiver.await.unwrap();
        if !result.is_empty() {
            let payload = result.clone();
            let cert_serialized = result.into_iter().nth(0).unwrap();
            let cert: Certificate =
                bincode::deserialize(&cert_serialized[..]).expect("Error deserisliazing");
            info!("Got Certificate {:?}", cert.round);
            //first = false;
            // }

            let block = Block::new(
                self.high_qc.clone(),
                tc,
                self.name,
                self.round,
                payload,
                &mut self.signing_channel,
            )
            .await;

            info!("Created Block {} with cert {:?}", block, cert.round);

            #[cfg(feature = "benchmark")]
            for x in &block.payload {
                info!("Created B{}({})", block.round, base64::encode(x));
            }

            info!("Created full{:?}", block);

            // Process our new block and broadcast it.
            let message = CoreMessage::Propose(block.clone());
            self.transmit(&message, None).await?;
            self.process_block(&block).await?;

            // Wait for the minimum block delay.
            sleep(Duration::from_millis(self.parameters.min_block_delay)).await;
            self.last_certified_certificate = cert.clone();
        } else {
            let block = Block::new(
                self.high_qc.clone(),
                tc,
                self.name,
                self.round,
                Vec::new(),
                &mut self.signing_channel,
            )
            .await;

            info!("Created Empty {:?}", block);

            // Process our new block and broadcast it.
            let message = CoreMessage::Propose(block.clone());
            self.transmit(&message, None).await?;
            self.process_block(&block).await?;

            // Wait for the minimum block delay.
            sleep(Duration::from_millis(self.parameters.min_block_delay)).await;
        }
        Ok(())
    }

    async fn process_qc(&mut self, qc: &QC) {
        self.advance_round(qc.round).await;
        self.update_high_qc(qc);
    }

    #[async_recursion]
    async fn process_block(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!("Processing {:?}", block);

        // Let's see if we have the last three ancestors of the block, that is:
        //      b0 <- |qc0; b1| <- |qc1; b2| <- |qc2; block|
        // If we don't, the synchronizer asks for them to other nodes. It will
        // then ensure we process all three ancestors in the correct order, and
        // finally make us resume processing this block.
        let (b0, b1, b2) = match self.synchronizer.get_ancestors(block).await? {
            Some(ancestors) => ancestors,
            None => {
                debug!("Processing of {} suspended: missing parent", block.digest());
                return Ok(());
            }
        };

        // Store the block only if we have already processed all its ancestors.
        self.store_block(block).await?;

        // Check if we can commit the head of the 2-chain.
        // Note that we commit blocks only if we have all its ancestors.
        let mut commit_rule = b0.round + 1 == b1.round;
        commit_rule &= b1.round + 1 == b2.round;
        commit_rule &= b2.round + 1 == block.round;
        if commit_rule {
            if !b0.payload.is_empty() {
                info!("Committed non empty {}", b0);

                #[cfg(feature = "benchmark")]
                for x in &b0.payload {
                    info!("Committed B{}({})", b0.round, base64::encode(x));
                }
                let cert_serialized = b0.payload.clone().into_iter().nth(0).unwrap();
                self.last_certified_certificate =
                    bincode::deserialize(&cert_serialized[..]).expect("Error deserisliazing");
                info!(
                    "Committed Cert of round {})",
                    self.last_certified_certificate.round
                );
                // Cleanup the mempool.
                let msg = MempoolMessage::CommitedCertificate(b0.payload.clone());

                self.send_to_mempool.send(msg).await.unwrap();
            }
            debug!("Committed {:?}", b0);
            if let Err(e) = self.commit_channel.send(b0.clone()).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }
        self.update_preferred_round(b1.round);

        // Ensure the block's round is as expected.
        // This check is important: it prevents bad leaders from producing blocks
        // far in the future that may cause overflow on the round number.
        if block.round != self.round {
            return Ok(());
        }

        // See if we can vote for this block.
        if let Some(vote) = self.make_vote(block).await {
            debug!("Created {:?}", vote);
            let next_leader = self.leader_elector.get_leader(self.round + 1);
            if next_leader == self.name {
                self.handle_vote(&vote).await?;
            } else {
                let message = CoreMessage::Vote(vote);
                self.transmit(&message, Some(next_leader)).await?;
            }
        }
        Ok(())
    }

    async fn handle_proposal(&mut self, block: &Block) -> ConsensusResult<()> {
        let digest = block.digest();

        // Ensure the block proposer is the right leader for the round.
        ensure!(
            block.author == self.leader_elector.get_leader(block.round),
            ConsensusError::WrongLeader {
                digest,
                leader: block.author,
                round: block.round
            }
        );

        // Check the block is correctly formed.
        block.verify(&self.committee)?;

        // Process the QC. This may allow us to advance round.
        self.process_qc(&block.qc).await;

        // Process the TC (if any). This may also allow us to advance round.
        if let Some(ref tc) = block.tc {
            self.advance_round(tc.round).await;
        }

        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and then make us resume processing this block.

        let (sender, receiver) = oneshot::channel();
        let msg = MempoolMessage::VerifyPayload(block.payload.clone(), sender);
        self.send_to_mempool.send(msg).await.unwrap();
        let reply = receiver.await.unwrap();

        match reply {
            PayloadStatus::Reject => {
                debug!("Failed to verify Payload {}", digest);
                return Ok(());
            }
            _ => self.process_block(block).await,
        }
    }

    async fn handle_sync_request(&mut self, digest: Digest, sender: NodeID) -> ConsensusResult<()> {
        if let Some(bytes) = self.store.read(digest.0.to_vec()).await? {
            let block = bincode::deserialize(&bytes)?;
            let message = CoreMessage::Propose(block);
            self.transmit(&message, Some(sender)).await?;
        }
        Ok(())
    }

    async fn handle_tc(&mut self, tc: TC) -> ConsensusResult<()> {
        self.advance_round(tc.round).await;
        if self.name == self.leader_elector.get_leader(self.round) {
            self.generate_proposal(Some(tc)).await?;
        }
        Ok(())
    }

    pub async fn run(&mut self) {
        // Upon booting, generate the very first block (if we are the leader).
        // Also, schedule a timer in case we don't hear from the leader.
        self.schedule_timer().await;
        if self.name == self.leader_elector.get_leader(self.round) {
            self.generate_proposal(None)
                .await
                .expect("Failed to send the first block");
        }

        // This is the main loop: it processes incoming blocks and votes,
        // and receive timeout notifications from our Timeout Manager.
        loop {
            let result = tokio::select! {
                Some(message) = self.core_channel.recv() => {
                    match message {
                        CoreMessage::Propose(block) => self.handle_proposal(&block).await,
                        CoreMessage::Vote(vote) => self.handle_vote(&vote).await,
                        CoreMessage::Timeout(timeout) => self.handle_timeout(&timeout).await,
                        CoreMessage::TC(tc) => self.handle_tc(tc).await,
                        CoreMessage::LoopBack(block) => self.process_block(&block).await,
                        CoreMessage::SyncRequest(digest, sender) => self.handle_sync_request(digest, sender).await
                    }
                },
                Some(_) = self.timer.notifier.recv() => self.local_timeout_round().await,
                else => break,
            };
            match result {
                Ok(()) => (),
                Err(ConsensusError::StoreError(e)) => error!("{}", e),
                Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                Err(e) => warn!("{}", e),
            }
        }
    }
}
