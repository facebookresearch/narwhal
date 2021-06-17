use super::messages::*;
use super::types::*;
use crate::committee::Committee;
use log::*;
use std::cmp::max;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::collections::{BTreeMap, BTreeSet};
use tokio::sync::mpsc::{Receiver, Sender};

// #[cfg(test)]
// #[path = "tests/consensus_tests.rs"]
// pub mod consensus_tests;

pub struct Consensus {
    //bool = true if the header is already ordered.
    pub headers_dag: HashMap<RoundNumber, BTreeMap<Digest, (BlockHeader, bool)>>,
    pub header_ordered: VecDeque<(BlockHeader, Digest)>,
    //we have all causal history of this round (including).
    pub wave_sync: WaveNumber,
    //all causal history of the leader of this wave is committed.
    pub wave_committed: WaveNumber,
    //send information for garbage collection.
    pub send_to_dag: Sender<ConsensusMessage>,
    //get headers from primary.
    pub get_from_dag: Receiver<ConsensusMessage>,
    /// Committee of this instance.
    pub committee: Committee,
    //A map of rounds and "randomly" chosen leaders. Note that we might not have some in our DAG.
    pub uncommitted_wave_leaders: HashMap<WaveNumber, NodeID>,
    // Sequence of the leaders that all agree on. Later can be used to order or the DAG by one-by-one applying some pre-defined deterministic rule on each leader's causal history.
    pub committed_wave_leaders: VecDeque<(WaveNumber, NodeID)>,
    //no committed leaders from this round on.
    pub next_uncommitted_wave: WaveNumber,
}

impl Consensus {
    pub fn new(
        send_to_dag: Sender<ConsensusMessage>,
        get_from_dag: Receiver<ConsensusMessage>,
        committee: Committee,
    ) -> Self {
        Self {
            headers_dag: HashMap::new(),
            wave_sync: 0,
            wave_committed: 0,
            header_ordered: VecDeque::new(),
            send_to_dag,
            get_from_dag,
            committee,
            uncommitted_wave_leaders: HashMap::new(),
            committed_wave_leaders: VecDeque::new(),
            next_uncommitted_wave: 1,
        }
    }

    pub async fn start(&mut self) {
        //to be continued...
        loop {
            if let Some(msg) = self.get_from_dag.recv().await {
                match msg {
                    ConsensusMessage::Header(header, digest) => {
                        self.add_header(header, digest);
                    }
                    ConsensusMessage::SyncDone(round) => {
                        let wave = round / 4;
                        if wave > self.wave_sync {
                            self.order_dag(wave).await;
                            self.wave_sync = wave;
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
            .insert(digest, (header, false));
    }

    pub fn get_headers(
        &self,
        round: RoundNumber,
    ) -> Option<&BTreeMap<Digest, (BlockHeader, bool)>> {
        self.headers_dag.get(&round)
    }

    fn lucky_wave(&self, wave: WaveNumber, leader: NodeID) -> bool {
        let support = self.compute_support(wave, leader);
        support >= self.committee.quorum_threshold()
    }

    pub fn get_wave_leader(&self, wave: WaveNumber) -> NodeID {
        self.committee.authorities[wave as usize % self.committee.authorities.len()]
            .primary
            .name
    }

    fn get_all_children_of_digests(
        &self,
        digests: Vec<Digest>,
        round: RoundNumber,
    ) -> Option<Vec<Digest>> {
        return if let Some(headers) = self.get_headers(round) {
            let children: Vec<Digest> = headers
                .iter()
                .filter(|(_, (header, _))| header.has_at_least_one_parent(&digests))
                .map(|(digest, _)| digest)
                .cloned()
                .collect();
            Some(children)
        } else {
            None
        };
    }

    fn get_all_descendants_of_digests(
        &self,
        digests: Vec<Digest>,
        start_round: RoundNumber,
        end_round: RoundNumber,
    ) -> Option<Vec<Digest>> {
        let mut tmp_digests = digests;
        for r in start_round..end_round + 1 {
            match self.get_all_children_of_digests(tmp_digests.clone(), r) {
                Some(tmp) => {
                    tmp_digests = tmp;
                }
                _ => {
                    return None;
                }
            }
        }
        return Some(tmp_digests);
    }

    fn is_path_between_wave_leaders(&self, high_wave: WaveNumber, low_wave: WaveNumber) -> bool {
        //we should have the leaders, so I guess panic is ok if not?
        let low_leader = self.uncommitted_wave_leaders.get(&low_wave).unwrap();
        let high_leader = self.uncommitted_wave_leaders.get(&high_wave).unwrap();
        let low_wave_first_round = low_wave * 4 - 3;
        let high_wave_first_round = high_wave * 4 - 3;

        if let Some((low_leader_digest, _)) =
            self.get_header_digest_by_node_round(*low_leader, low_wave_first_round)
        {
            let mut digests = Vec::new();
            digests.push(*low_leader_digest);

            if let Some(descendants) = self.get_all_descendants_of_digests(
                digests.clone(),
                low_wave_first_round + 1,
                high_wave_first_round,
            ) {
                if let Some((high_leader_digest, _)) =
                    self.get_header_digest_by_node_round(*high_leader, high_wave_first_round)
                {
                    return descendants.contains(&high_leader_digest);
                }
            }
        }
        return false;
    }

    fn commit_next_wave(&mut self, wave: WaveNumber) -> Vec<(BlockHeader, Digest)> {
        let mut wave_to_commit = wave;
        let mut w = wave - 1;
        while w > self.next_uncommitted_wave {
            if self.is_path_between_wave_leaders(wave_to_commit, w) {
                wave_to_commit = w;
            }
            w = w - 1;
        }
        let leader_to_commit = self.uncommitted_wave_leaders.get(&wave_to_commit).unwrap();
        self.committed_wave_leaders
            .push_back((wave_to_commit, *leader_to_commit));
        let headers_to_commit = self.get_leader_causal_history(wave_to_commit, *leader_to_commit);
        self.next_uncommitted_wave = wave_to_commit + 1;

        let mut headers_for_primary = Vec::new();
        for headers in headers_to_commit {
            for header in headers {
                info!("Commit Block {:?}", header.clone());
                self.header_ordered.push_front(header.clone());
                headers_for_primary.push(header);
            }
        }
        headers_for_primary
    }

    async fn order_dag(&mut self, wave: WaveNumber) {
        let wave_leader = self.get_wave_leader(wave);
        for wave in self.wave_sync + 1..wave + 1 {
            self.uncommitted_wave_leaders
                .insert(wave, self.get_wave_leader(wave));
        }
        if self.lucky_wave(wave, wave_leader) {
            let mut headers_for_primary = Vec::new();
            while self.next_uncommitted_wave < wave {
                headers_for_primary.extend(self.commit_next_wave(wave));
            }
            self.wave_committed = wave;
            if self
                .send_to_dag
                .send(ConsensusMessage::OrderedHeaders(
                    headers_for_primary,
                    wave * 4 - 3,
                ))
                .await
                .is_err()
            {
                //log an error
            }
        }
    }

    pub fn get_uncommitted_parents_headers(
        &self,
        headers: VecDeque<(BlockHeader, Digest)>,
        round: RoundNumber,
    ) -> VecDeque<(BlockHeader, Digest)> {
        let mut parents_digests = BTreeSet::new();
        for (header, _) in headers {
            for (_, digest) in header.parents {
                parents_digests.insert(digest);
            }
        }
        let round_headers = self.get_headers(round).unwrap();
        let mut parents_uncommitted_digests: Vec<Digest> = round_headers
            .iter()
            .filter(|(digest, _)| (parents_digests.contains(digest)))
            .filter(|(_, (_, ordered))| (!*ordered))
            .map(|(digest, _)| (*digest))
            .collect();
        parents_uncommitted_digests.sort();
        let mut parents_uncommitted_headers = VecDeque::new();
        for digest in parents_uncommitted_digests {
            let (header, _) = round_headers.get(&digest).unwrap();
            parents_uncommitted_headers.push_back((header.clone(), digest));
        }
        return parents_uncommitted_headers;
    }

    pub fn get_leader_causal_history(
        &self,
        wave: WaveNumber,
        leader: NodeID,
    ) -> VecDeque<VecDeque<(BlockHeader, Digest)>> {
        let leader_round = wave * 4 - 3;
        let last_ordered_round = max(0, (self.next_uncommitted_wave as i32 - 1) * 4 - 3) as u64;
        let mut headers_to_commit = VecDeque::new();

        let (digest, header) = self
            .get_header_digest_by_node_round(leader, leader_round)
            .unwrap(); //We must have it
        let mut headers = VecDeque::new();
        headers.push_back((header.clone(), *digest));
        headers_to_commit.push_front(headers.clone());

        let mut r = leader_round;
        while r > last_ordered_round + 1 {
            let tmp = self.get_uncommitted_parents_headers(headers, r);
            headers_to_commit.push_front(tmp.clone());
            headers = tmp;
            r -= 1;
        }
        headers_to_commit
    }

    pub fn compute_support(&self, wave: WaveNumber, leader: NodeID) -> Stake {
        //first check that you see the leaders block
        let wave_last_round = wave * 4;
        if let Some((leader_digest, _)) =
            self.get_header_digest_by_node_round(leader, wave_last_round - 3)
        {
            let mut digests = Vec::new();
            digests.push(*leader_digest);
            if let Some(descendants) = self.get_all_descendants_of_digests(
                digests.clone(),
                wave_last_round - 2,
                wave_last_round,
            ) {
                return descendants
                    .iter()
                    .map(|digest| {
                        let (header, _) = self
                            .get_headers(wave_last_round)
                            .unwrap()
                            .get(&digest)
                            .unwrap();
                        self.committee.stake(&header.author)
                    })
                    .sum();
            }
        }
        0
    }

    //returns the deterministically selected leader of the round, TODO: Parse coin from DAG and give as parameter
    pub fn get_round_leader(&self, round: RoundNumber) -> NodeID {
        //        self.committee.authorities[make_u64_digest(round) % self.committee.authorities.len()]
        self.committee.authorities[(round % 4) as usize % self.committee.authorities.len()]
            .primary
            .name
    }

    pub fn get_header_digest_by_node_round(
        &self,
        node: NodeID,
        round: RoundNumber,
    ) -> Option<(&Digest, &BlockHeader)> {
        if let Some(headers) = self.headers_dag.get(&round) {
            for (digest, (header, _)) in headers {
                if header.author == node {
                    return Some((digest, header));
                }
            }
        }
        None
    }
}
