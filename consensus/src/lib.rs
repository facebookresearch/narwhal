// Copyright(C) Facebook, Inc. and its affiliates.
use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, Round};
use std::cmp::max;
use std::collections::{BTreeSet, HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The representation of the DAG in memory.
type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

/// The state that needs to be persisted for crash-recovery.
struct State {
    /// The last committed round.
    last_committed_round: Round,
    /// The last committed leader round.
    last_leader_committed_round: Round,

    // Keeps the last committed round for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    last_committed: HashMap<PublicKey, Round>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag,
    // Keeps track of which parties are in steady state waves
    ss_validator_sets: HashMap<u64, BTreeSet<PublicKey>>,

    // Keeps track of which parties are in fallback waves
    fb_validator_sets: HashMap<u64, BTreeSet<PublicKey>>,
}

impl State {
    fn new(genesis: Vec<Certificate>) -> Self {
        let genesis = genesis
            .into_iter()
            .map(|x| (x.origin(), (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        let mut ss_sets = HashMap::new();
        let ss_validators = genesis.iter().map(|(x, (_, _))| *x).collect();
        ss_sets.insert(0, ss_validators);

        Self {
            last_committed_round: 0,
            last_leader_committed_round: 0,
            last_committed: genesis.iter().map(|(x, (_, y))| (*x, y.round())).collect(),
            dag: [(0, genesis)].iter().cloned().collect(),
            ss_validator_sets: ss_sets,
            fb_validator_sets: HashMap::new(),
        }
    }

    /// Update and clean up internal state base on committed certificates.
    fn update(&mut self, certificate: &Certificate, gc_depth: Round) {
        self.last_committed
            .entry(certificate.origin())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());

        let last_committed_round = *self.last_committed.values().max().unwrap();
        self.last_committed_round = last_committed_round;

        for (name, round) in &self.last_committed {
            self.dag.retain(|r, authorities| {
                authorities.retain(|n, _| n != name || r >= round);
                !authorities.is_empty() && r + gc_depth >= last_committed_round
            });
        }
    }
}

pub struct Consensus {
    /// The committee information.
    committee: Committee,
    /// The depth of the garbage collector.
    gc_depth: Round,

    /// Receives new certificates from the primary. The primary should send us new certificates only
    /// if it already sent us its whole history.
    rx_primary: Receiver<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    tx_primary: Sender<Certificate>,
    /// Outputs the sequence of ordered certificates to the application layer.
    tx_output: Sender<Certificate>,

    /// The genesis certificates.
    genesis: Vec<Certificate>,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        gc_depth: Round,
        rx_primary: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
    ) {
        tokio::spawn(async move {
            Self {
                committee: committee.clone(),
                gc_depth,
                rx_primary,
                tx_primary,
                tx_output,
                genesis: Certificate::genesis(&committee),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // The consensus state (everything else is immutable).
        let mut state = State::new(self.genesis.clone());

        // Listen to incoming certificates.
        while let Some(certificate) = self.rx_primary.recv().await {
            debug!("Processing {:?}", certificate);
            let round = certificate.round();

            // Add the new certificate to the local storage.
            state.dag.entry(round).or_insert_with(HashMap::new).insert(
                certificate.origin(),
                (certificate.digest(), certificate.clone()),
            );

            // Update the mode of the validator and see if a leader could be committed
            let leader = match self.try_ordering(&certificate, &mut state) {
                Some(x) => x.clone(),
                None => continue,
            };

            // Elect leaders every even round
            if round % 2 != 0 || round < 2 {
                continue;
            }

            // Look at the previous round for votes
            let r = round - 1;

            // Get the certificate's digest of the leader. If we already ordered this leader, there is nothing to do.
            let leader_round = r - 1;

            // If we already committed the leader then we are done
            if leader_round <= state.last_leader_committed_round {
                continue;
            }

            // Get an ordered list of past leaders that are linked to the current leader.
            debug!("Leader {:?} has enough support", leader);
            let mut sequence = Vec::new();
            state.last_leader_committed_round = leader_round;
            for leader in self.order_wave_leaders(&leader, &mut state).iter().rev() {
                // Starting from the oldest leader, flatten the sub-dag referenced by the leader.
                for x in self.order_dag(leader, &state) {
                    // Update and clean up internal state.
                    state.update(&x, self.gc_depth);

                    // Add the certificate to the sequence.
                    sequence.push(x);
                }
            }

            // Log the latest committed round of every authority (for debug).
            if log_enabled!(log::Level::Debug) {
                for (name, round) in &state.last_committed {
                    debug!("Latest commit of {}: Round {}", name, round);
                }
            }

            // Output the sequence in the right order.
            for certificate in sequence {
                #[cfg(not(feature = "benchmark"))]
                info!("Committed {}", certificate.header);

                #[cfg(feature = "benchmark")]
                for digest in certificate.header.payload.keys() {
                    // NOTE: This log entry is used to compute performance.
                    info!("Committed {} -> {:?}", certificate.header, digest);
                }

                self.tx_primary
                    .send(certificate.clone())
                    .await
                    .expect("Failed to send certificate to primary");

                if let Err(e) = self.tx_output.send(certificate).await {
                    warn!("Failed to output certificate: {}", e);
                }
            }
        }
    }

    fn try_ordering(&self, certificate: &Certificate, state: &mut State) -> Option<Certificate> {
        let wave = certificate.round() / 4;
        let first_steady_state_leader_round = 4 * wave;

        // First round in a wave so determine the vote type
        if certificate.round() % 4 == 0 {
            return self.determine_validator_vote_type(certificate, wave, state);
        } else if certificate.round() > 2 && certificate.round() % 4 == 2 {
            return self.try_steady_commit(
                certificate,
                wave,
                first_steady_state_leader_round,
                state,
            );
        } else {
            return None;
        }
    }

    // Updates the mode of a certificate if necessary
    fn determine_validator_vote_type(
        &self,
        certificate: &Certificate,
        wave: u64,
        state: &mut State,
    ) -> Option<Certificate> {
        // Waves and rounds are zero-indexed
        if state
            .ss_validator_sets
            .entry(wave)
            .or_insert(BTreeSet::new())
            .contains(&certificate.origin())
        {
            return None;
        }

        if state
            .fb_validator_sets
            .entry(wave)
            .or_insert(BTreeSet::new())
            .contains(&certificate.origin())
        {
            return None;
        }

        let fb_leader_round = 4 * (wave - 1);
        let ss_leader_round = fb_leader_round + 2;

        let ss_leader = self.try_steady_commit(&certificate, wave - 1, ss_leader_round, state);
        let fb_leader = self.try_fallback_commit(&certificate, wave - 1, fb_leader_round, state);

        if ss_leader.is_some() {
            state
                .ss_validator_sets
                .entry(wave)
                .or_insert(BTreeSet::new())
                .insert(certificate.origin());
            return ss_leader;
        }

        if fb_leader.is_some() {
            state
                .ss_validator_sets
                .entry(wave)
                .or_insert(BTreeSet::new())
                .insert(certificate.origin());
            return fb_leader;
        }

        state
            .fb_validator_sets
            .entry(wave)
            .or_insert(BTreeSet::new())
            .insert(certificate.origin());

        return None;
    }

    // Checks whether there is a steady state wave commit
    fn try_steady_commit(
        &self,
        certificate: &Certificate,
        wave: u64,
        ss_leader_round: u64,
        state: &mut State,
    ) -> Option<Certificate> {
        let dag = &state.dag;

        let (_, leader) = match self.leader(ss_leader_round, &state.dag) {
            Some(x) => x,
            None => return None,
        };

        let ss_sets = state
            .ss_validator_sets
            .entry(wave)
            .or_insert(BTreeSet::new());

        // Find the potential votes of certificates in steady state mode
        let stake: Stake = state
            .dag
            .get(&(certificate.round() - 1))
            .expect("Should have previous round certificates")
            .values()
            .filter(|(d, x)| certificate.header.parents.contains(d) && self.linked(x, leader, dag))
            .filter(|(_, x)| ss_sets.contains(&x.origin()))
            .map(|(_, x)| self.committee.stake(&x.origin()))
            .sum();

        // Commit if there is at least 2f+1 steady state votes
        if stake >= self.committee.quorum_threshold() {
            return Some(leader.clone());
        }
        return None;
    }

    // Checks whether there is a fallback wave commit
    fn try_fallback_commit(
        &self,
        certificate: &Certificate,
        wave: u64,
        fb_leader_round: u64,
        state: &mut State,
    ) -> Option<Certificate> {
        let dag = &state.dag;

        let (_, leader) = match self.fb_leader(fb_leader_round, &state.dag) {
            Some(x) => x,
            None => return None,
        };

        let fb_sets = state
            .fb_validator_sets
            .entry(wave)
            .or_insert(BTreeSet::new());

        // Find the potential votes of certificates in fallback mode
        let stake: Stake = state
            .dag
            .get(&(certificate.round() - 1))
            .expect("Should have previous round certificates")
            .values()
            .filter(|(d, x)| certificate.header.parents.contains(d) && self.linked(x, leader, dag))
            .filter(|(_, x)| fb_sets.contains(&x.origin()))
            .map(|(_, x)| self.committee.stake(&x.origin()))
            .sum();

        // Commit if there is at least 2f+1 fallback votes
        if stake >= self.committee.quorum_threshold() {
            return Some(leader.clone());
        }
        return None;
    }

    /// Returns the certificate (and the certificate's digest) originated by the leader of the
    /// specified round (if any).
    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        // TODO: We should elect the leader of round r-2 using the common coin revealed at round r.
        // At this stage, we are guaranteed to have 2f+1 certificates from round r (which is enough to
        // compute the coin). We currently just use round-robin.
        #[cfg(test)]
        let seed = 0;
        #[cfg(not(test))]
        let seed = round;

        // Elect the leader.
        let leader = self.committee.leader(seed as usize);

        // Return its certificate and the certificate's digest.
        dag.get(&round).map(|x| x.get(&leader)).flatten()
    }

    /// Returns the certificate (and the certificate's digest) originated by the fallback leader of the
    /// specified round (if any).
    fn fb_leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        // TODO: We should elect the leader of round r-2 using the common coin revealed at round r.
        // At this stage, we are guaranteed to have 2f+1 certificates from round r (which is enough to
        // compute the coin). We currently just use round-robin.
        #[cfg(test)]
        let seed = 1;
        #[cfg(not(test))]
        let seed = round + 1;

        // Elect the leader.
        let leader = self.committee.leader(seed as usize);

        // Return its certificate and the certificate's digest.
        dag.get(&round).map(|x| x.get(&leader)).flatten()
    }

    /// Order the past leaders that we didn't already commit.
    fn order_wave_leaders(&self, leader: &Certificate, state: &mut State) -> Vec<Certificate> {
        let mut to_commit = vec![leader.clone()];
        let mut leader = leader;
        let dag = &state.dag;
        let mut r = leader.round() - 2;

        while r > state.last_committed_round {
            let wave = r / 4;

            let ss_sets = state
                .ss_validator_sets
                .entry(wave)
                .or_insert(BTreeSet::new());

            let fb_sets = state
                .fb_validator_sets
                .entry(wave)
                .or_insert(BTreeSet::new());

            let ss_stake: Stake;
            let fb_stake: Stake;

            if r % 4 == 2 {
                let second_ss_leader_round = 4 * wave + 2;
                let second_ss_leader = self.leader(second_ss_leader_round, &state.dag);

                let fb_leader_round = 4 * wave;
                let fb_leader = self.fb_leader(fb_leader_round, &state.dag);

                ss_stake = match second_ss_leader {
                    Some((_, l)) => state
                        .dag
                        .get(&(r + 1))
                        .expect("Should have previous round certificates")
                        .values()
                        .filter(|(d, x)| {
                            leader.header.parents.contains(d) && self.linked(x, l, dag)
                        })
                        .filter(|(_, x)| ss_sets.contains(&x.origin()))
                        .map(|(_, x)| self.committee.stake(&x.origin()))
                        .sum(),
                    None => 0,
                };

                fb_stake = match fb_leader {
                    Some((_, l)) => state
                        .dag
                        .get(&(r + 1))
                        .expect("Should have previous round certificates")
                        .values()
                        .filter(|(d, x)| {
                            leader.header.parents.contains(d) && self.linked(x, l, dag)
                        })
                        .filter(|(_, x)| fb_sets.contains(&x.origin()))
                        .map(|(_, x)| self.committee.stake(&x.origin()))
                        .sum(),
                    None => 0,
                };

                // If f+1 steady state votes and at most f fallback votes then steady state commit
                if ss_stake >= self.committee.validity_threshold()
                    && fb_stake < self.committee.validity_threshold()
                {
                    match second_ss_leader {
                        Some((_, l)) => {
                            to_commit.push(l.clone());
                            leader = l;
                        }
                        None => {}
                    };
                }

                // If f+1 fallback votes and at most f steady state votes then fallback commit
                if ss_stake < self.committee.validity_threshold()
                    && fb_stake >= self.committee.validity_threshold()
                {
                    match fb_leader {
                        Some((_, l)) => {
                            to_commit.push(l.clone());
                            leader = l;
                        }
                        None => {}
                    };
                }
            } else {
                let first_ss_leader_round = 4 * wave;
                let first_ss_leader = self.leader(first_ss_leader_round, &state.dag);

                ss_stake = match first_ss_leader {
                    Some((_, l)) => state
                        .dag
                        .get(&(r + 1))
                        .expect("Should have previous round certificates")
                        .values()
                        .filter(|(d, x)| {
                            leader.header.parents.contains(d) && self.linked(x, l, dag)
                        })
                        .filter(|(_, x)| ss_sets.contains(&x.origin()))
                        .map(|(_, x)| self.committee.stake(&x.origin()))
                        .sum(),
                    None => 0,
                };

                fb_stake = 0;

                // If f+1 steady state votes and at most f fallback votes then steady state commit
                if ss_stake >= self.committee.validity_threshold()
                    && fb_stake < self.committee.validity_threshold()
                {
                    match first_ss_leader {
                        Some((_, l)) => {
                            to_commit.push(l.clone());
                            leader = l;
                        }
                        None => {}
                    };
                }
            }

            r -= 2;
        }

        to_commit
    }

    /// Checks if there is a path between two leaders.
    fn linked(&self, leader: &Certificate, prev_leader: &Certificate, dag: &Dag) -> bool {
        let mut parents = vec![leader];
        for r in (prev_leader.round()..leader.round()).rev() {
            parents = dag
                .get(&(r))
                .expect("We should have the whole history by now")
                .values()
                .filter(|(digest, _)| parents.iter().any(|x| x.header.parents.contains(digest)))
                .map(|(_, certificate)| certificate)
                .collect();
        }
        parents.contains(&prev_leader)
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    fn order_dag(&self, leader: &Certificate, state: &State) -> Vec<Certificate> {
        debug!("Processing sub-dag of {:?}", leader);
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![leader];
        while let Some(x) = buffer.pop() {
            debug!("Sequencing {:?}", x);
            ordered.push(x.clone());
            for parent in &x.header.parents {
                let (digest, certificate) = match state
                    .dag
                    .get(&(x.round() - 1))
                    .map(|x| x.values().find(|(x, _)| x == parent))
                    .flatten()
                {
                    Some(x) => x,
                    None => continue, // We already ordered or GC up to here.
                };

                // We skip the certificate if we (1) already processed it or (2) we reached a round that we already
                // committed for this authority.
                let mut skip = already_ordered.contains(&digest);
                skip |= state
                    .last_committed
                    .get(&certificate.origin())
                    .map_or_else(|| false, |r| r == &certificate.round());
                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                }
            }
        }

        // Ensure we do not commit garbage collected certificates.
        ordered.retain(|x| x.round() + self.gc_depth >= state.last_committed_round);

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        ordered.sort_by_key(|x| x.round());
        ordered
    }
}
