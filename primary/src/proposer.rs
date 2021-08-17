// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header};
use crate::primary::Round;
use config::{Committee, WorkerId};
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use std::convert::TryFrom;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,

    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<(Vec<Certificate>, Round)>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Digest, WorkerId)>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,

    /// The current physical round of the dag.
    round: Round,
    // The current virtual round of the dag.
    virtual_round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Certificate>,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(Digest, WorkerId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,
    // Data used for calculating who the leader is
    committee: Committee,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        header_size: usize,
        max_header_delay: u64,
        rx_core: Receiver<(Vec<Certificate>, Round)>,
        rx_workers: Receiver<(Digest, WorkerId)>,
        tx_core: Sender<Header>,
    ) {
        let genesis = Certificate::genesis(&committee)
            .iter()
            .map(|x| x.clone())
            .collect();

        tokio::spawn(async move {
            Self {
                name,
                signature_service,
                header_size,
                max_header_delay,
                rx_core,
                rx_workers,
                tx_core,
                round: 1,
                virtual_round: 1,
                last_parents: genesis,
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
                committee: committee.clone(),
            }
            .run()
            .await;
        });
    }

    async fn make_header(&mut self) {
        // Make a new header.
        let header = Header::new(
            self.name,
            self.round,
            0,
            self.digests.drain(..).collect(),
            self.last_parents.drain(..).map(|x| x.digest()).collect(),
            &mut self.signature_service,
        )
        .await;
        debug!("Created {:?}", header);

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    async fn create_header(&mut self) -> Header {
        // Make a new header.
        let header = Header::new(
            self.name,
            self.round,
            0,
            self.digests.drain(..).collect(),
            self.last_parents.drain(..).map(|x| x.digest()).collect(),
            &mut self.signature_service,
        )
        .await;
        debug!("Created {:?}", header);

        header
    }


    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.round);

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        let dag_timer = sleep(Duration::from_millis(self.max_header_delay));
        let mut advance_virtual_round = false;
        tokio::pin!(timer);
        tokio::pin!(dag_timer);

        loop {
            // Check if we can propose a new header. We propose a new header when one of the following
            // conditions is met:
            // 1. We have a quorum of certificates from the previous round and the specified maximum
            // inter-header delay has passed.
            // 2. We have a quorum of certificates from the previous round and one includes the leader block.
            let enough_parents = !self.last_parents.is_empty();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();

            // change core to send parents one by one instead of 2f+1 (same logic for advance round condition)
            // make aggregator on the proposer
            if (timer_expired || enough_digests) && enough_parents {
                let mut header = self.create_header().await;
                // Make a new header.
                //self.make_header().await;
                self.payload_size = 0;

                if advance_virtual_round {
                    header.virtual_round = self.virtual_round;
                    advance_virtual_round = false;
                }

                self.tx_core
                    .send(header)
                    .await
                    .expect("Failed to send header");

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);

            }

            tokio::select! {
                // don't need the round
                Some((parents, round)) = self.rx_core.recv() => {
                    if round < self.round {
                        continue;
                    }

                    // Advance to the next round.
                    self.round = round + 1;
                    debug!("Dag moved to round {}", self.round);

                    // Signal that we have enough parent certificates to propose a new header.
                    self.last_parents = parents;


                    let leader = self.committee.leader(self.virtual_round);
                    // For each parent we check if there is a vote for the leader
                    let leader_votes = self.last_parents
                                            .clone()
                                            .into_iter()
                                            .map(|x| x.votes).filter(|x| x.into_iter().filter(|(pk, _)| pk.eq(&leader)).count() > 0);

                    // Check if the timer has expired
                    let dag_timer_expired = dag_timer.is_elapsed();
                    // The condition for advancing an odd round is at least one parent has a vote for the leader
                    if self.virtual_round % 2 == 1 {
                        // If there are 0 leader votes then don't advance the round and the timer has not expired
                        if leader_votes.clone().count() == 0 && !dag_timer_expired {
                            continue;
                        }
                    }

                    // The condition for advancing an even round are 2f+1 votes to the steady state leader
                    if self.virtual_round % 2 == 0 {
                        // If less than 2f+1 votes then don't advance the round and check for additional timeout
                        if leader_votes.clone().count() < usize::try_from(self.committee.clone().quorum_threshold()).unwrap() && !dag_timer_expired {
                            continue;
                        }
                    }

                    // Advance to the next virtual round.
                    self.virtual_round += 1;
                    debug!("Dag moved to virtual round {}", self.virtual_round);
                    advance_virtual_round = true;


                    // Signal that we have enough parent certificates to propose a new header.
                    //self.last_parents = parents;
                }
                Some((digest, worker_id)) = self.rx_workers.recv() => {
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                }
                () = &mut timer => {
                    // Nothing to do.
                }
            }
        }
    }
}
