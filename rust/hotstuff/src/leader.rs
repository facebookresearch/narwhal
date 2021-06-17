use crate::core::RoundNumber;
use dag_core::committee::Committee;
use dag_core::types::*;

pub type LeaderElector = RRLeaderElector;

pub struct RRLeaderElector {
    committee: Committee,
}

impl RRLeaderElector {
    pub fn new(committee: Committee) -> Self {
        Self { committee }
    }

    pub fn get_leader(&self, round: RoundNumber) -> NodeID {
        let mut keys: Vec<_> = self
            .committee
            .authorities
            .iter()
            .map(|x| x.primary.name)
            .collect();
        keys.sort();
        keys[round as usize % self.committee.size()]
    }
}
