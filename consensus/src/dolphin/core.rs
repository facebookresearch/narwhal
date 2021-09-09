// Copyright(C) Facebook, Inc. and its affiliates.
use crate::dolphin::committer::Committer;
use crate::dolphin::virtual_state::VirtualState;
use crate::state::State;
use config::{Committee, Stake};
use crypto::Hash as _;
use log::{debug, info, log_enabled, warn};
use primary::{Certificate, Metadata, Round};
use std::collections::BTreeSet;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

pub struct Dolphin {
    /// The committee information.
    committee: Committee,
    /// The leader timeout value.
    timeout: u64,
    /// The garbage collection depth.
    gc_depth: Round,

    /// Receives new certificates from the primary. The primary should send us new certificates only
    /// if it already sent us its whole history.
    rx_certificate: Receiver<Certificate>,
    /// Outputs the sequence of ordered certificates to the primary (for cleanup and feedback).
    tx_commit: Sender<Certificate>,
    /// Sends the virtual parents to the primary's proposer.
    tx_parents: Sender<Metadata>,
    /// Outputs the sequence of ordered certificates to the application layer.
    tx_output: Sender<Certificate>,

    /// The genesis certificates.
    genesis: Vec<Certificate>,
    /// The virtual dag round to share with the primary.
    virtual_round: Round,
    /// Implements the commit logic and returns an ordered list of certificates.
    committer: Committer,
}

impl Dolphin {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        committee: Committee,
        timeout: u64,
        gc_depth: Round,
        rx_certificate: Receiver<Certificate>,
        tx_commit: Sender<Certificate>,
        tx_parents: Sender<Metadata>,
        tx_output: Sender<Certificate>,
    ) {
        tokio::spawn(async move {
            Self {
                committee: committee.clone(),
                timeout,
                gc_depth,
                rx_certificate,
                tx_commit,
                tx_parents,
                tx_output,
                genesis: Certificate::genesis(&committee),
                virtual_round: 0,
                committer: Committer::new(committee, gc_depth),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        // The consensus state (everything else is immutable).
        let mut state = State::new(self.gc_depth, self.genesis.clone());
        let mut virtual_state = VirtualState::new(self.committee.clone(), self.genesis.clone());

        // The timer keeping track of the leader timeout.
        let timer = sleep(Duration::from_millis(self.timeout));
        tokio::pin!(timer);

        let mut quorum = Some(self.genesis.iter().map(|x| (x.digest(), 0)).collect());
        let mut advance_early = true;
        loop {
            if (timer.is_elapsed() || advance_early) && quorum.is_some() {
                if !advance_early {
                    warn!(
                        "Timing out for round {}, moving to the next round",
                        self.virtual_round
                    );
                }

                // Advance to the next round.
                self.virtual_round += 1;
                debug!("Virtual dag moved to round {}", self.virtual_round);

                // Send the virtual parents to the primary's proposer.
                self.tx_parents
                    .send(Metadata::new(self.virtual_round, quorum.unwrap()))
                    .await
                    .expect("Failed to send virtual parents to primary");

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.timeout);
                timer.as_mut().reset(deadline);

                // Reset the quorum.
                quorum = None;
            }

            tokio::select! {
                Some(certificate) = self.rx_certificate.recv() => {
                    debug!("Processing {:?}", certificate);
                    let virtual_round = certificate.virtual_round();

                    // Add the new certificate to the local storage.
                    state.add(certificate.clone());

                    // Try adding the certificate to the virtual dag.
                    if !virtual_state.try_add(&certificate) {
                        continue;
                    }
                    debug!("Adding virtual {:?}", certificate);

                    // Try to commit.
                    let sequence = self.committer.try_commit(&certificate, &mut state, &mut virtual_state);

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

                        self.tx_commit
                            .send(certificate.clone())
                            .await
                            .expect("Failed to send committed certificate to primary");

                        if let Err(e) = self.tx_output.send(certificate).await {
                            warn!("Failed to output certificate: {}", e);
                        }
                    }

                    // If the certificate is not from our virtual round, it cannot help us advance round.
                    if self.virtual_round != virtual_round {
                        continue;
                    }
                    debug!("Trying to advance round");

                    // Try to advance to the next (virtual) round.
                    let (parents, authors): (BTreeSet<_>, Vec<_>) = virtual_state
                        .dag
                        .get(&virtual_round)
                        .expect("We just added a certificate with this round")
                        .values()
                        .map(|(digest, x)| ((digest.clone(), x.virtual_round()), x.origin()))
                        .collect::<Vec<_>>()
                        .iter()
                        .cloned()
                        .unzip();

                    //if authors.iter().any(|x| x == &self.name) {
                    quorum = (authors
                        .iter()
                        .map(|x| self.committee.stake(x))
                        .sum::<Stake>() >= self.committee.quorum_threshold())
                        .then(|| parents);
                    debug!("Got quorum for round {}: {}", self.virtual_round, quorum.is_some());

                    advance_early = match virtual_round % 2 {
                        0 => self.enough_votes(virtual_round, &virtual_state) || !advance_early,
                        _ => virtual_state.steady_leader((virtual_round+1)/2).is_some(),
                    };
                    debug!("Can early advance for round {}: {}", self.virtual_round, advance_early);
                    //}
                },
                () = &mut timer => {
                    // Nothing to do.
                }
            }
        }
    }

    /// Check if we gathered a quorum of votes for the leader.
    fn enough_votes(&mut self, virtual_round: Round, virtual_state: &VirtualState) -> bool {
        let wave = (virtual_round + 1) / 2;
        virtual_state.steady_leader(wave - 1).map_or_else(
            || false,
            |(leader_digest, _)| {
                // Either we got 2f+1 votes for the leader.
                virtual_state
                    .dag
                    .get(&virtual_round)
                    .expect("We just added a certificate with this round")
                    .values()
                    .filter(|(_, x)| x.virtual_parents().contains(&leader_digest))
                    .map(|(_, x)| self.committee.stake(&x.origin()))
                    .sum::<Stake>()
                    >= self.committee.quorum_threshold()

                // Or we go f+1 votes that are not for the leader.
                    || virtual_state
                        .dag
                        .get(&virtual_round)
                        .expect("We just added a certificate with this round")
                        .values()
                        .filter(|(_, x)| !x.virtual_parents().contains(&leader_digest))
                        .map(|(_, x)| self.committee.stake(&x.origin()))
                        .sum::<Stake>()
                        >= self.committee.validity_threshold()
            },
        )
    }
}
