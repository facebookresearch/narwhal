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
        ss_sets.insert(1, ss_validators);

        Self {
            last_committed_round: 0,
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
            let leader = match self.update_validator_mode(&certificate, &mut state) {
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
            if leader_round <= state.last_committed_round {
                continue;
            }

            // Get an ordered list of past leaders that are linked to the current leader.
            debug!("Leader {:?} has enough support", leader);
            let mut sequence = Vec::new();
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

    // Updates the mode of a certificate if necessary
    fn update_validator_mode(
        &self,
        certificate: &Certificate,
        state: &mut State,
    ) -> Option<Certificate> {
        // Waves and rounds are zero-indexed
        let ss_wave = certificate.round() / 2;
        let fb_wave = certificate.round() / 4;

        // Starts from ss_wave = 1
        if ss_wave == 0 {
            return None;
        }

        // If the mode of a certificate is already determined to be steady state then return
        if state
            .ss_validator_sets
            .entry(ss_wave)
            .or_insert(BTreeSet::new())
            .contains(&certificate.origin())
        {
            return None;
        }

        // If the mode of the certificate is already determined to be fallback then return
        if state
            .fb_validator_sets
            .entry(fb_wave)
            .or_insert(BTreeSet::new())
            .contains(&certificate.origin())
        {
            return None;
        }

        // If certificate was in the previous steady state wave and there was a commit, then it is
        // in the steady state
        let ss_leader = self.try_steady_commit(&certificate, ss_wave - 1, state);
        if state
            .ss_validator_sets
            .entry(ss_wave - 1)
            .or_insert(BTreeSet::new())
            .contains(&certificate.origin())
            && ss_leader.is_some()
        {
            state
                .ss_validator_sets
                .entry(ss_wave)
                .or_insert(BTreeSet::new())
                .insert(certificate.origin());
            return ss_leader;
        }

        // If the certificate was in the previous fallback wave and there was a commit, then it is
        // in the current steady state wave
        let fb_leader = self.try_fallback_commit(&certificate, fb_wave - 1, state);

        if state
            .fb_validator_sets
            .entry(fb_wave - 1)
            .or_insert(BTreeSet::new())
            .contains(&certificate.origin())
            && fb_leader.is_some()
        {
            state
                .ss_validator_sets
                .entry(ss_wave)
                .or_insert(BTreeSet::new())
                .insert(certificate.origin());
            return fb_leader;
        }

        // Otherwise the certificate is in fallback mode
        state
            .fb_validator_sets
            .entry(fb_wave)
            .or_insert(BTreeSet::new())
            .insert(certificate.origin());

        return None;
    }

    // Checks whether there is a steady state wave commit
    fn try_steady_commit(
        &self,
        certificate: &Certificate,
        ss_wave: u64,
        state: &mut State,
    ) -> Option<Certificate> {
        let dag = &state.dag;

        // Get the steady state leader of the current ss_wave
        let ss_leader_round = 2 * ss_wave;

        let (_, leader) = match self.leader(ss_leader_round, &state.dag) {
            Some(x) => x,
            None => return None,
        };

        let ss_sets = state
            .ss_validator_sets
            .entry(ss_wave)
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
        fb_wave: u64,
        state: &mut State,
    ) -> Option<Certificate> {
        let dag = &state.dag;

        // Get the current fallback leader of the current fb_wave
        let fb_leader_round = 4 * fb_wave;

        let (_, leader) = match self.fb_leader(fb_leader_round, &state.dag) {
            Some(x) => x,
            None => return None,
        };

        let fb_sets = state
            .fb_validator_sets
            .entry(fb_wave)
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

        for r in (state.last_committed_round + 2..leader.round() - 1)
            .rev()
            .step_by(2)
        {
            // Get the current steady state wave number
            let wave = r / 2;

            let ss_sets = state
                .ss_validator_sets
                .entry(wave)
                .or_insert(BTreeSet::new());

            // Get the certificate proposed by the previous leader.
            let (_, prev_ss_leader) = match self.leader(r, &state.dag) {
                Some(x) => x,
                None => continue,
            };

            let ss_stake: Stake = state
                .dag
                .get(&(2 * wave + 1))
                .expect("Should have previous round certificates")
                .values()
                .filter(|(d, x)| {
                    leader.header.parents.contains(d) && self.linked(x, prev_ss_leader, dag)
                })
                .filter(|(_, x)| ss_sets.contains(&x.origin()))
                .map(|(_, x)| self.committee.stake(&x.origin()))
                .sum();

            let fb_stake: Stake;
            let fb_sets = state
                .fb_validator_sets
                .entry(wave / 2)
                .or_insert(BTreeSet::new());

            // If an odd steady state wave then check the fallback votes
            if wave % 2 != 0 {
                // Get the certificate proposed by the previous leader.
                let (_, prev_fb_leader) = match self.fb_leader(r - 2, &state.dag) {
                    Some(x) => x,
                    None => continue,
                };

                fb_stake = state
                    .dag
                    .get(&(2 * wave + 1))
                    .expect("Should have previous round certificates")
                    .values()
                    .filter(|(d, x)| {
                        leader.header.parents.contains(d) && self.linked(x, prev_fb_leader, dag)
                    })
                    .filter(|(_, x)| fb_sets.contains(&x.origin()))
                    .map(|(_, x)| self.committee.stake(&x.origin()))
                    .sum();

                // If f+1 fallback votes and at most f steady state votes then fallback commit
                if ss_stake < self.committee.validity_threshold()
                    && fb_stake >= self.committee.validity_threshold()
                {
                    to_commit.push(prev_fb_leader.clone());
                    leader = prev_fb_leader;
                }
            } else {
                fb_stake = 0;
            }

            // If f+1 steady state votes and at most f fallback votes then steady state commit
            if ss_stake >= self.committee.validity_threshold()
                && fb_stake < self.committee.validity_threshold()
            {
                to_commit.push(prev_ss_leader.clone());
                leader = prev_ss_leader;
            }
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
