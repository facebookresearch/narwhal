use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use primary::{Certificate, Round};
use std::cmp::max;
use std::collections::{BTreeSet, HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};

type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

/*
struct State {
    last_committed: HashMap<PublicKey, Round>,
    last_committed_leader: Certificate,
    dag: Dag,
}

impl State {
    fn new(genesis: &[Certificate]) -> Self {
        let genesis = genesis
            .iter()
            .cloned()
            .map(|x| (x.origin, (x.digest(), x)))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed: HashMap::new(),
            last_committed_leader: Certificate::default(),
            dag: [(0, genesis)].iter().cloned().collect(),
        }
    }
}
*/

pub struct Consensus {
    committee: Committee,
    gc_depth: Round,

    rx_waiter: Receiver<Certificate>,
    tx_primary: Sender<Round>,

    last_committed: HashMap<PublicKey, Round>,
    last_committed_leader: Certificate,
    dag: Dag,
    genesis: Vec<Certificate>,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        gc_depth: Round,
        rx_waiter: Receiver<Certificate>,
        tx_primary: Sender<Round>,
    ) {
        tokio::spawn(async move {
            let genesis = Certificate::genesis(&committee)
                .into_iter()
                .map(|x| (x.origin, (x.digest(), x)))
                .collect::<HashMap<_, _>>();

            Self {
                committee,
                gc_depth,
                rx_waiter,
                tx_primary,
                last_committed: HashMap::new(),
                last_committed_leader: Certificate::default(),
                dag: [(0, genesis.clone())].iter().cloned().collect(),
                genesis: genesis.into_iter().map(|(_, (_, x))| x).collect(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        //let mut state = State::new(&self.genesis);

        while let Some(certificate) = self.rx_waiter.recv().await {
            let round = certificate.round;

            // Add the new certificate to the local storage.
            self.dag
                .entry(round)
                .or_insert_with(HashMap::new)
                .insert(certificate.origin, (certificate.digest(), certificate));

            // Try to order the dag to commit. Start from the highest round for which we have at least
            // 2f+1 certificates. This is because we need them to reveal the common coin.
            let r = round - 1;

            // We only elect leaders for even round numbers.
            if r % 2 != 0 || r < 2 {
                continue;
            }

            // Get the certificate's digest of the leader of round r-2.
            let leader_round = r - 2;
            let (leader_digest, certificate) = match self.leader(leader_round, &self.dag) {
                Some(x) => x,
                None => continue,
            };

            // Check if the leader has f+1 support from its children (ie. round r-1).
            let stake: Stake = self
                .dag
                .get(&(r - 1))
                .expect("We should have the whole history by now")
                .values()
                .filter(|(_, x)| x.header.parents.contains(&leader_digest))
                .map(|(_, x)| self.committee.stake(&x.origin))
                .sum();

            // If it is the case, we can commit the leader. But first, we need to recursively go back to
            // the last committed leader, and commit all preceding leaders in the right order. Committing
            // a leader block means committing all its dependencies.
            if stake >= self.committee.validity_threshold() {
                // Get an ordered list of past leaders that are linked to the current leader.
                let mut sequence = Vec::new();
                while let Some(leader_certificate) = self.order_leaders(certificate).pop() {
                    // Starting from the oldest leader, flatten the sub-dag referenced by the leader.
                    let mut ordered = self.order_dag(leader_certificate, &self.last_committed);
                    while let Some(x) = ordered.pop() {
                        self.last_committed
                            .entry(x.origin.clone())
                            .and_modify(|r| *r = max(*r, x.round))
                            .or_insert_with(|| x.round);

                        sequence.push(x);
                    }
                }

                // Update internal state.
                self.last_committed_leader = certificate.clone();

                let round = self.last_committed_leader.round;
                if round >= self.gc_depth {
                    let gc_round = round - self.gc_depth;
                    self.dag.retain(|r, _| r >= &gc_round);
                }
            }
        }
    }

    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        // TODO: We should elect the leader of round r-2 using the common coin revealed at round r.
        // At this stage, we are guaranteed to have 2f+1 certificates from round r (which is enough to
        // compute the coin). We currently just use round-robin.
        let coin = round;

        // Elect the leader.
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        let leader = keys[coin as usize % self.committee.size()];

        // Return the certificate (and the certificate's digest) originated by the leader on the
        // specified round (if any).
        dag.get(&round).map(|x| x.get(&leader)).flatten()
    }

    /// Order the past leaders that are linked to the current leader.
    fn order_leaders<'a>(&'a self, certificate: &'a Certificate) -> Vec<&'a Certificate> {
        let mut leader_certificate = certificate;
        let mut to_commit = vec![leader_certificate];
        while let Some(previous_leader_certificate) = self.linked_leader(leader_certificate) {
            // Compute the stop condition. We stop ordering leaders when we reached either (1) the last
            // committed leader, or (2) the genesis, or (3) we past the GC depth.
            let mut stop = &self.last_committed_leader == previous_leader_certificate;
            stop |= self.genesis.contains(previous_leader_certificate);
            let round = self.last_committed_leader.round;
            if round >= self.gc_depth {
                stop |= previous_leader_certificate.round < round - self.gc_depth;
            }
            if stop {
                break;
            }

            leader_certificate = previous_leader_certificate;
            to_commit.push(previous_leader_certificate);
        }

        to_commit
    }

    /// Returns the certificate originated by the previous leader, if it is linked to the input leader.
    fn linked_leader(&self, leader_certificate: &Certificate) -> Option<&Certificate> {
        let r = leader_certificate.round;

        // Get the certificate proposed by the pervious leader.
        let previous_leader_round = r - 2;
        let (previous_leader_digest, previous_leader_certificate) =
            match self.leader(previous_leader_round, &self.dag) {
                Some(x) => x,
                None => return None,
            };

        // Gather the children of the previous leader.
        let children: BTreeSet<_> = self
            .dag
            .get(&(r - 1))
            .expect("We should have the whole history by now")
            .values()
            .filter(|(_, x)| x.header.parents.contains(&previous_leader_digest))
            .map(|(digest, _)| digest.clone())
            .collect();

        // Return the previous leader's certificate if there is a path between the two leaders.
        match children.is_disjoint(&leader_certificate.header.parents) {
            true => None,
            false => Some(&previous_leader_certificate),
        }
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    fn order_dag(
        &self,
        certificate: &Certificate,
        last_committed: &HashMap<PublicKey, Round>,
    ) -> Vec<Certificate> {
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![certificate];
        while let Some(x) = buffer.pop() {
            ordered.push(x.clone());
            for parent in &x.header.parents {
                let (digest, certificate) = self
                    .dag
                    .get(&(x.header.round - 1))
                    .expect("We should have the whole history by now")
                    .values()
                    .find(|(x, _)| x == parent)
                    .expect("We should have the whole history by now");

                // We stop ordering when we either reach the genesis, or a round that we already committed
                // for this authority (to avoid committing twice the same blocks).
                let mut skip = self.genesis.contains(certificate);
                skip |= already_ordered.contains(&digest);
                if let Some(round) = last_committed.get(&certificate.origin) {
                    skip |= &certificate.round == round;
                }

                if !skip {
                    buffer.push(certificate);

                    already_ordered.insert(digest);
                }
            }
        }

        ordered
    }
}
