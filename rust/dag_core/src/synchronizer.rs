use super::committee::*;
use super::error::*;
use super::messages::*;
use super::store::Store;
use super::types::*;
use crate::store::DBValue;
use bincode::Error;
use futures::future::{Fuse, FutureExt};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::{future::FusedFuture, pin_mut};
use futures::{select, StreamExt};
use log::*;
use rand::seq::SliceRandom;
use std::cmp::min;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};

use std::cmp::max;

/// A service that keeps watching for the dependencies of blocks.
pub async fn header_waiter_process(
    mut store: Store,          // A copy of the inner store
    genesis: Vec<Certificate>, // The genesis set of certs
    mut rx: mpsc::UnboundedReceiver<(SignedBlockHeader, BlockHeader)>, // A channel to receive Headers
    loopback_commands: Sender<PrimaryMessage>,
) {
    // Register the genesis certs ...
    for cert in &genesis {
        // let bytes: Vec<u8> = bincode::serialize(&cert)?;
        let digest_in_store = PartialCertificate::make_digest(cert.digest, 0, cert.primary_id);
        let _ = store
            .write(digest_in_store.0.to_vec(), digest_in_store.0.to_vec()) //digest_in_store.0.to_vec() --> bytes ?
            .await;
    }

    // Create an unordered set of futures
    let mut waiting_headers = FuturesUnordered::new();
    // Now process headers and also headers with satisfied dependencies
    loop {
        select! {
            signer_header_result = waiting_headers.select_next_some() => {
                // Once we have all header dependencies, we try to re-inject this signed header
                if let Ok(signed_header) = signer_header_result {
                    if let Err(e) = loopback_commands.send(PrimaryMessage::Header(signed_header)).await {
                        error!("Error sending loopback command: {}", e);
                    }
                }
                else {
                    error!("Error in waiter.");
                }
            }
            msg = rx.recv().fuse() => {
                if let Some((signed_header, header)) = msg {
                    let i2_store = store.clone();
                    let fut = header_waiter(i2_store, signed_header, header);
                    waiting_headers.push(fut);
                }
                else {
                    // Channel is closed, so we exit.
                    // Our primary is gone!
                    warn!("Exiting digest waiter process.");
                    break;
                }
            }
        }
    }
}

/// The future that waits for a set of headers and digest associated with a header.
pub async fn header_waiter(
    mut store: Store,                 // A copy of the store
    signed_header: SignedBlockHeader, // The signed header structure
    header: BlockHeader,              // The header for which we wait for dependencies.
) -> Result<SignedBlockHeader, DagError> {
    debug!(
        "[DEP] ASK H {:?} D {}",
        (header.round, header.author),
        header.transactions_digest.len()
    );
    //Note: we now store different digests for header and certificates to avoid false positive.
    for (other_primary_id, digest) in &header.parents {
        let digest_in_store =
            PartialCertificate::make_digest(*digest, header.round - 1, *other_primary_id);
        if store.notify_read(digest_in_store.0.to_vec()).await.is_err() {
            return Err(DagError::StorageFailure {
                error: "Error in reading store from 'header_waiter'.".to_string(),
            });
        }
    }
    for (digest, _) in &header.transactions_digest {
        if store.notify_read(digest.0.to_vec()).await.is_err() {
            return Err(DagError::StorageFailure {
                error: "Error in reading store from 'header_waiter'.".to_string(),
            });
        }
    }
    debug!(
        "[DEP] GOT H {:?} D {}",
        (header.round, header.author),
        header.transactions_digest.len()
    );
    Ok(signed_header)
}

pub async fn dag_synchronizer_process(
    mut get_from_dag: Receiver<SyncMessage>,
    dag_synchronizer: DagSynchronizer,
) {
    let mut digests_to_sync: Vec<Digest> = Vec::new();
    let mut round_to_sync: RoundNumber = 0;
    let mut last_synced_round: RoundNumber = 0;
    let mut rollback_stop_round: RoundNumber = 1;
    let mut sq: SyncNumber = 0;

    let rollback_fut = Fuse::terminated();
    pin_mut!(rollback_fut);
    loop {
        select! {

            msg = get_from_dag.recv().fuse() => {
                if let Some(SyncMessage::SyncUpToRound(round, digests, last_gc_round)) = msg {
                    if round > round_to_sync {
                        debug!("DAG sync: received request to sync digests: {:?} up to round {}", digests, round);
                        round_to_sync = round;
                        digests_to_sync = digests;
                        rollback_stop_round = max(last_gc_round+1, 1);
                        if rollback_fut.is_terminated(){
                            last_synced_round = round_to_sync;
                            rollback_fut.set(rollback_headers(dag_synchronizer.clone(), digests_to_sync.clone(), round_to_sync, rollback_stop_round, sq).fuse());
                            debug!("DAG sync: go.");
                        }
                        else {
                            debug!("DAG sync: drop.");
                        }
                    }
                } else{
                    warn!("Exiting DagSynchronizer::start().");
                    break;
                }

            }

            res = rollback_fut => {
                if let Err(e) = res{
                  error!("rollback_headers returns error: {:?}", e);
                }
                else{
                 sq += 1;
                 if round_to_sync > last_synced_round{
                     last_synced_round = round_to_sync;
                     // rollback_stop_round = max(last_synced_round, 1);
                     rollback_fut.set(rollback_headers(dag_synchronizer.clone(), digests_to_sync.clone(), round_to_sync, rollback_stop_round, sq).fuse());
                  }
                }

            }
        }
    }
}

pub async fn handle_header_digest(
    mut dag_synchronizer: DagSynchronizer,
    digest: Digest,
    rollback_stop_round: RoundNumber,
    sq: SyncNumber,
) -> Result<Option<Vec<Digest>>, DagError> {
    //TODO: issue: should we try the processor first? we need concurrent access to it...

    if let Ok(dbvalue) = dag_synchronizer.store.read(digest.0.to_vec()).await {
        match dbvalue {
            None => {
                debug!("invoking send_sync_header_requests: {:?}", digest);
                dag_synchronizer
                    .send_sync_header_requests(digest, sq)
                    .await?;

                // Exponential backoff delay
                let mut delay = 50;
                loop {
                    select! {
                        ret =  dag_synchronizer.store.notify_read(digest.0.to_vec()).fuse() =>{
                            if let Ok(record_header) = ret {
                               return Ok(dag_synchronizer.handle_record_header(record_header, rollback_stop_round).await?);
                            } else {
                                //handle the error.
                            }
                        }

                        _ = sleep(Duration::from_millis(200)).fuse() => {
                            debug!("Trigger Sync on {:?}", digest);
                            dag_synchronizer.send_sync_header_requests(digest, sq).await?;
                            delay *= 4;
                        }
                    }
                }
            }

            // HERE
            Some(record_header) => {
                let result: Result<HeaderPrimaryRecord, Error> =
                    bincode::deserialize(&record_header);
                if let Err(e) = result {
                    panic!("Reading digest {:?} from store gives us a struct that we cannot deserialize: {}", digest, e);
                }
                return Ok(dag_synchronizer
                    .handle_record_header(record_header, rollback_stop_round)
                    .await?);
            }
        }
    } else {
        //handle the error.
    }

    Ok(None)
}

//sync all digests' causal history and pass to consensus
pub async fn rollback_headers(
    mut dag_synchronizer: DagSynchronizer,
    digests: Vec<Digest>,
    round: RoundNumber,
    rollback_stop_round: RoundNumber,
    sq: SyncNumber,
) -> Result<(), DagError> {
    let mut digests_in_process = FuturesUnordered::new();

    for digest in digests {
        let fut = handle_header_digest(dag_synchronizer.clone(), digest, rollback_stop_round, sq);
        digests_in_process.push(fut);
    }

    while !digests_in_process.is_empty() {
        // let option = digests_in_process.select_next_some().await?;
        let option = match digests_in_process.select_next_some().await {
            Ok(option) => option,
            Err(e) => panic!("Panix {}", e),
        };
        if let Some(parents_digests) = option {
            for digest in parents_digests {
                let fut =
                    handle_header_digest(dag_synchronizer.clone(), digest, rollback_stop_round, sq);
                digests_in_process.push(fut);
            }
        }
    }

    let msg = ConsensusMessage::SyncDone(round);
    dag_synchronizer.send_to_consensus(msg).await?;
    info!("DAG sync: sent to consensus SyncDone for round  {}", round);
    Ok(())
}

#[derive(Clone)]
pub struct DagSynchronizer {
    pub id: NodeID,
    pub send_to_consensus_channel: Sender<ConsensusMessage>,
    pub send_to_network: Sender<(RoundNumber, PrimaryMessage)>,
    pub store: Store,
    pub committee: Committee,
}

impl DagSynchronizer {
    pub fn new(
        id: NodeID,
        committee: Committee,
        store: Store,
        send_to_consensus_channel: Sender<ConsensusMessage>,
        send_to_network: Sender<(RoundNumber, PrimaryMessage)>,
    ) -> Self {
        DagSynchronizer {
            id,
            send_to_consensus_channel,
            send_to_network,
            store,
            committee,
        }
    }

    pub async fn send_sync_header_requests(
        &mut self,
        digest: Digest,
        sq: SyncNumber,
    ) -> Result<(), DagError> {
        //Pick random 3 validators. Hopefully one will have the header. We can implement different strategies.
        let authorities = self.committee.authorities.choose_multiple(
            &mut rand::thread_rng(),
            min(self.committee.quorum_threshold() / 2, 3),
        ); // self.committee.quorum_threshold());

        debug!("asking for header with digest: {:?}", digest);

        for source in authorities {
            if source.primary.name == self.id {
                continue;
            }

            let msg = PrimaryMessage::SyncHeaderRequest(digest, self.id, source.primary.name);

            if let Err(e) = self.send_to_network.send((sq, msg)).await {
                panic!("error: {}", e);
            }
        }
        Ok(())
    }

    pub async fn handle_record_header(
        &mut self,
        record_header: DBValue,
        rollback_stop_round: RoundNumber,
    ) -> Result<Option<Vec<Digest>>, DagError> {
        // let mut record_header: HeaderPrimaryRecord = bincode::deserialize(&record_header[..])?;
        let mut record_header: HeaderPrimaryRecord = bincode::deserialize(&record_header)
            .expect("Deserialization of primary record failure");

        if !record_header.passed_to_consensus {
            let digest_in_store = PartialCertificate::make_digest(
                record_header.digest,
                record_header.header.round,
                record_header.header.author,
            );
            let cert_raw = self
                .store
                .notify_read(digest_in_store.0.to_vec())
                .await
                .unwrap();
            let cert: Certificate =
                bincode::deserialize(&cert_raw).expect("Deserialization of certificate failure");

            let msg = ConsensusMessage::Header(cert);
            self.send_to_consensus(msg)
                .await
                .expect("Fail to send to consensus channel");

            record_header.passed_to_consensus = true;

            self.store
                .write(record_header.digest.0.to_vec(), record_header.to_bytes())
                .await
                .map_err(|e| DagError::StorageFailure {
                    error: format!("{}", e),
                })?;

            return if record_header.header.round == rollback_stop_round {
                //no need need to sync parents because we gc them and consensus does not going to order them.
                Ok(None)
            } else {
                let digests: Vec<Digest> = record_header
                    .header
                    .parents
                    .iter()
                    .clone()
                    .map(|(_, digest)| *digest)
                    .collect();

                Ok(Some(digests))
            };
        }
        Ok(None)
    }

    pub async fn send_to_consensus(&mut self, msg: ConsensusMessage) -> Result<(), DagError> {
        self.send_to_consensus_channel
            .send(msg)
            .await
            .map_err(|e| DagError::ChannelError {
                error: format!("{}", e),
            })?;
        Ok(())
    }
}
