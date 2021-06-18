use config::{Committee, Stake};
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use log::{info, warn};
use primary::{Certificate, Round};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The representation of the DAG in memory.
type Dag = HashMap<Round, HashMap<PublicKey, (Digest, Certificate)>>;

pub struct Consensus {
    /// The committee information.
    committee: Committee,

    /// Receives new certificates from the primary. The primary should send us new certificates only
    /// if it already sent us its whole history.
    rx_waiter: Receiver<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    tx_primary: Sender<Certificate>,
    /// Outputs the sequence of ordered certificates to the application layer.
    tx_output: Sender<Certificate>,

    /// The genesis certificates.
    genesis: Vec<Certificate>,
    /// Keeps the last committed round for each authority. This map is used to clean up the dag and
    /// ensure we don't commit twice the same certificate.
    //last_committed: HashMap<PublicKey, Round>,
    /// The representation of the DAG in memory.
    dag: Dag,
}

impl Consensus {
    pub fn spawn(
        committee: Committee,
        rx_waiter: Receiver<Certificate>,
        tx_primary: Sender<Certificate>,
        tx_output: Sender<Certificate>,
    ) {
        tokio::spawn(async move {
            let genesis = Certificate::genesis(&committee)
                .into_iter()
                .map(|x| (x.origin, (x.digest(), x)))
                .collect::<HashMap<_, _>>();

            Self {
                committee,
                rx_waiter,
                tx_primary,
                tx_output,
                genesis: genesis.iter().map(|(_, (_, x))| x.clone()).collect(),
                //last_committed: HashMap::new(),
                dag: [(0, genesis)].iter().cloned().collect(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // Keeps the last committed round for each authority. This map is used to clean up the dag and
        // ensure we don't commit twice the same certificate.
        let mut last_committed = HashMap::new();

        while let Some(certificate) = self.rx_waiter.recv().await {
            println!("RECEIVED {:?}", certificate);
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
            if r % 2 != 0 || r < 4 {
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
                let ordered_leaders = self.order_leaders(certificate, &last_committed);
                for leader_certificate in ordered_leaders.iter().rev() {
                    // Starting from the oldest leader, flatten the sub-dag referenced by the leader.
                    let ordered = self.order_dag(leader_certificate, &last_committed);
                    for x in ordered.iter().rev() {
                        // Update internal state.
                        last_committed
                            .entry(x.origin.clone())
                            .and_modify(|r| *r = max(*r, x.round))
                            .or_insert_with(|| x.round);

                        for (name, round) in &last_committed {
                            println!("NEW STATE: {}: {}", name, round);
                        }
                        println!();

                        // Add the certificate to the sequence.
                        sequence.push(x.clone());
                    }
                }

                // Clean up the dag.
                for (name, round) in &last_committed {
                    self.dag.retain(|r, authorities| {
                        authorities.retain(|n, _| n != name || r >= round);
                        !authorities.is_empty()
                    });
                }

                // Output the sequence in the right order.
                for certificate in sequence {
                    println!("COMMITTED: {:?}", certificate);
                    info!("Committed B{}", certificate.round);

                    #[cfg(feature = "benchmark")]
                    for (digest, _) in &certificate.header.payload {
                        // NOTE: This log entry is used to compute performance.
                        info!("Committed B{}({:?})", certificate.round, digest);
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
    }

    /// Returns the certificate (and the certificate's digest) originated by the leader of the
    /// specified round (if any).
    fn leader<'a>(&self, round: Round, dag: &'a Dag) -> Option<&'a (Digest, Certificate)> {
        // TODO: We should elect the leader of round r-2 using the common coin revealed at round r.
        // At this stage, we are guaranteed to have 2f+1 certificates from round r (which is enough to
        // compute the coin). We currently just use round-robin.
        let coin = round;

        // Elect the leader.
        let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
        keys.sort();
        let leader = keys[coin as usize % self.committee.size()];

        // Return its certificate and the certificate's digest.
        dag.get(&round).map(|x| x.get(&leader)).flatten()
    }

    /// Order the past leaders that are linked to the current leader.
    fn order_leaders<'a>(
        &'a self,
        certificate: &'a Certificate,
        last_committed: &HashMap<PublicKey, Round>,
    ) -> Vec<&'a Certificate> {
        let mut leader_certificate = certificate;

        // If we already ordered the last leader, there is nothing to do.
        if last_committed
            .get(&leader_certificate.origin)
            .map_or_else(|| false, |r| r == &leader_certificate.round)
        {
            return Vec::new();
        }

        // If we didn't, we look for all past leaders that we didn't order yet.
        let mut to_commit = vec![leader_certificate];
        while let Some(previous_leader_certificate) = self.linked_leader(leader_certificate) {
            // Compute the stop condition. We stop ordering leaders when we reached either the genesis,
            // or (2) the last committed leader.
            println!(
                "{:?} AND {:?} ARE LINKED",
                leader_certificate, previous_leader_certificate
            );
            let mut stop = self.genesis.contains(previous_leader_certificate);
            stop |= last_committed
                .get(&previous_leader_certificate.origin)
                .map_or_else(|| false, |r| r == &previous_leader_certificate.round);

            if stop {
                break;
            }

            leader_certificate = previous_leader_certificate;
            to_commit.push(previous_leader_certificate);
        }
        println!("ORDER LEADERS DONE: {:?}", to_commit);
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

        // Gather the children of the previous leader. Return the previous leader's certificate if there is
        // a path between the two leaders.
        self.dag
            .get(&(r - 1))
            .expect("We should have the whole history by now")
            .values()
            .find(|(digest, certificate)| {
                certificate.header.parents.contains(&previous_leader_digest)
                    && leader_certificate.header.parents.contains(digest)
            })
            .map(|_| previous_leader_certificate)
    }

    /// Flatten the dag referenced by the input certificate. This is a classic depth-first search (pre-order):
    /// https://en.wikipedia.org/wiki/Tree_traversal#Pre-order
    fn order_dag(
        &self,
        certificate: &Certificate,
        last_committed: &HashMap<PublicKey, Round>,
    ) -> Vec<Certificate> {
        println!("ORDERING: {:?}", certificate);
        let mut ordered = Vec::new();
        let mut already_ordered = HashSet::new();

        let mut buffer = vec![certificate];
        while let Some(x) = buffer.pop() {
            println!("ADDING TO STACK: {:?}", x);
            ordered.push(x.clone());
            for parent in &x.header.parents {
                let (digest, certificate) = match self
                    .dag
                    .get(&(x.round - 1))
                    .expect("We should have the whole history by now")
                    .values()
                    .find(|(x, _)| x == parent)
                {
                    Some(x) => x,
                    None => continue, // We already ordered this parent (as well as his ancestors).
                };

                // We skip the certificate if we (1) already processed it, (2) we reached the genesis, or (3) we
                // reached a round that we already committed for this authority.
                let mut skip = already_ordered.contains(&digest);
                skip |= self.genesis.contains(certificate);
                skip |= last_committed
                    .get(&certificate.origin)
                    .map_or_else(|| false, |r| r == &certificate.round);

                if !skip {
                    buffer.push(certificate);
                    already_ordered.insert(digest);
                }
            }
        }

        // Ordering the output by round is not really necessary but it makes the commit sequence prettier.
        println!("ORDER SUB DAG DONE");
        ordered.sort_by(|x, y| y.round.cmp(&x.round));
        ordered
    }
}
