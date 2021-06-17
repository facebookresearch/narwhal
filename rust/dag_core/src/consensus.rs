use super::messages::*;
use super::types::*;
use crate::committee::Committee;
use crate::error::DagError;
use log::*;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{Receiver, Sender};
// #[cfg(test)]
// #[path = "tests/consensus_tests.rs"]
// pub mod consensus_tests;

pub struct Consensus {
    pub headers_dag: HashMap<RoundNumber, BTreeMap<Digest, BlockHeader>>,
    //Hashmap to lookup if a Blockheader is already committeed
    pub committed_blocks: HashMap<Digest, bool>,
    // pub header_ordered: VecDeque<(Digest, BlockHeader)>,
    //send information for garbage collection.
    pub send_to_dag: Sender<ConsensusMessage>,
    //get headers from primary.
    pub get_from_dag: Receiver<ConsensusMessage>,
    /// Committee of this instance.
    pub committee: Committee,
    /// Last Leader that had enough support
    pub last_committed_leader: (RoundNumber, NodeID),
}

impl Consensus {
    pub fn new(
        send_to_dag: Sender<ConsensusMessage>,
        get_from_dag: Receiver<ConsensusMessage>,
        committee: Committee,
    ) -> Self {
        let leader = committee.authorities[0].primary.name;

        Self {
            headers_dag: HashMap::new(),
            committed_blocks: HashMap::new(),
            //   header_ordered: VecDeque::new(),
            send_to_dag,
            get_from_dag,
            committee,
            last_committed_leader: (0, leader),
        }
    }

    pub async fn start(&mut self) {
        //to be continued...

        loop {
            if let Some(msg) = self.get_from_dag.recv().await {
                match msg {
                    ConsensusMessage::Header(cert) => {
                        self.add_header(cert.header, cert.digest);
                    }
                    ConsensusMessage::SyncDone(round) => {
                        info!("SyncDone for rounde {:?}", round);
                        if (round - 1) % 2 == 0 && round > 3 {
                            info!("running consensus for round {:?}", round - 2);
                            self.order_dag(round).await;
                        }
                    }
                    _ => {}
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

    pub fn get_headers(&self, round: RoundNumber) -> Option<&BTreeMap<Digest, BlockHeader>> {
        self.headers_dag.get(&round)
    }

    async fn order_dag(&mut self, round: RoundNumber) {
        if self.get_committed_leader(round).is_some() {
            let mut leaders_to_commit = self.get_leaders_to_commit(round - 2);
            while let Some((round_leader, round_number)) = leaders_to_commit.pop() {
                self.order_subdag(round_leader, round_number).await;
            }
        }
    }

    fn get_leaders_to_commit(&self, round: RoundNumber) -> Vec<(NodeID, RoundNumber)> {
        let mut leaders_to_commit = Vec::new();
        let mut round_proccessing = round;
        let mut leader_round = round;
        leaders_to_commit.push((self.get_round_leader(round), round));

        while round_proccessing > self.last_committed_leader.0 {
            if round_proccessing < leader_round
                && self.has_path_between_leaders(leader_round, round_proccessing)
            {
                leaders_to_commit
                    .push((self.get_round_leader(round_proccessing), round_proccessing));
                leader_round = round_proccessing;
            }
            if round_proccessing > 1 {
                round_proccessing -= 2;
            } else {
                round_proccessing = 0;
            }
            debug!("round processing is {:?}", round_proccessing);
        }
        leaders_to_commit
    }

    fn has_path_between_leaders(
        &self,
        leader_round: RoundNumber,
        candidate_round: RoundNumber,
    ) -> bool {
        debug!(
            "Checking path for {:?} and {:?}",
            leader_round, candidate_round
        );
        let causal_blocks =
            self.get_block_to_commit(self.get_round_leader(leader_round), leader_round);
        if let Some((candidate_block, _)) = self.get_header_digest_by_node_round(
            self.get_round_leader(candidate_round),
            candidate_round,
        ) {
            if let Some(causal_blocks_of_round) = causal_blocks.get(&candidate_round) {
                return causal_blocks_of_round.contains_key(candidate_block);
            } else {
                debug!("no causal blocks for round {:?}", candidate_round);
            }
        }

        false
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
        debug!(
            "I have deleted until round {:?}, but I am accessing round {:?}",
            self.last_committed_leader.0,
            current_round - 1
        );
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
            if current_round > GC_DEPTH && self.last_committed_leader.0 >= GC_DEPTH {
                gc_round = self.last_committed_leader.0 - GC_DEPTH + 1
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

    fn is_committed(&self, digest: Digest) -> Result<bool, DagError> {
        Ok(*self
            .committed_blocks
            .get(&digest)
            .ok_or(DagError::NoBlockAvailable)?)
    }

    async fn order_subdag(&mut self, leader: NodeID, round: RoundNumber) {
        let mut ordered_blocks = VecDeque::new();

        {
            let sub_dag_to_commit = self.get_block_to_commit(leader, round);

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
        self.last_committed_leader = (round, leader);
        let sequence = ordered_blocks.into_iter().collect();

        self.garbage_collect(sequence, round).await;
    }

    async fn garbage_collect(
        &mut self,
        committed_sequence: Vec<(BlockHeader, Digest)>,
        last_committed_round: RoundNumber,
    ) {
        if self
            .send_to_dag
            .send(ConsensusMessage::OrderedHeaders(
                committed_sequence,
                last_committed_round,
            ))
            .await
            .is_err()
        {
            error!("Failed to send ordered headers back to dag");
        }

        if last_committed_round > GC_DEPTH {
            let gc_round = last_committed_round - GC_DEPTH;

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

    fn commit_blocks(&mut self, mut headers: VecDeque<(BlockHeader, Digest)>) {
        while let Some((block, digest)) = headers.pop_front() {
            if !self.committed_blocks.get(&digest).unwrap() {
                self.committed_blocks.insert(digest, true);
                // self.header_ordered.push_back((digest, block.clone()));
                debug!("Commit Block {:?}", digest);
                let commit_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
                // info!("Commit block in round {}", block.round);
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

    //returns the deterministically selected leader of the round, TODO: Parse coin from DAG and give as parameter
    pub fn get_round_leader(&self, round: RoundNumber) -> NodeID {
        //        self.committee.authorities[make_u64_digest(round) % self.committee.authorities.len()]
        self.committee.authorities[(round / 2) as usize % self.committee.authorities.len()]
            .primary
            .name
    }

    pub fn get_committed_leader(&self, round: RoundNumber) -> Option<NodeID> {
        let leader = self.get_round_leader(round - 2);
        let support = self.compute_weak_support(round - 1, leader);
        if support > (self.committee.quorum_threshold() / 2) {
            return Some(leader);
        }
        info!(
            "Leader {:?} did not have enough support for round {:?} ",
            leader,
            round - 1
        );
        None
    }

    pub fn compute_support(&self, round: RoundNumber, leader: NodeID) -> Stake {
        //first check that you see the leaders block
        if let Some((leader_digest, _)) = self.get_header_digest_by_node_round(leader, round - 4) {
            //Then get all its direct ancestors
            if let Some(headers) = self.get_headers(round - 3) {
                let supporting_digests: Vec<Digest> = headers
                    .iter()
                    .filter(|(_, header)| header.has_parent(leader_digest))
                    .map(|(digest, _)| digest)
                    .cloned()
                    .collect();

                // Then all the grandkids
                if let Some(headers) = self.get_headers(round - 2) {
                    //for each header we check if it has any parent in supporting_headers
                    let support_supporters_digest: Vec<Digest> = headers
                        .iter()
                        .filter(|(_, header)| header.has_at_least_one_parent(&supporting_digests))
                        .map(|(digest, _)| digest)
                        .cloned()
                        .collect();
                    // Once more for common_core
                    if let Some(headers) = self.get_headers(round - 1) {
                        //for each header we check if it has any parent in supporting_headers
                        let support_stake: Stake = headers
                            .iter()
                            .filter(|(_, header)| {
                                header.has_at_least_one_parent(&support_supporters_digest)
                            })
                            .map(|(_, header)| self.committee.stake(&header.author))
                            .sum();
                        //this is the support
                        return support_stake;
                    }
                }
            }
        }
        0
    }

    pub fn compute_weak_support(&self, round: RoundNumber, leader: NodeID) -> Stake {
        //first check that you see the leaders block
        if let Some((leader_digest, _)) = self.get_header_digest_by_node_round(leader, round - 1) {
            //Then get all its direct ancestors
            if let Some(headers) = self.get_headers(round) {
                let support_stake: Stake = headers
                    .iter()
                    .filter(|(_, header)| header.has_parent(leader_digest))
                    .map(|(_, header)| self.committee.stake(&header.author))
                    .sum();
                //this is the support
                return support_stake;
            }
        }
        0
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
}
