// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::Certificate;
use crate::primary::Round;
use crypto::Digest;
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::error;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

/// Waits to receive all the ancestors of a certificate before looping it back to the `Core`
/// for further processing.
pub struct CertificateWaiter {
    /// The persistent storage.
    store: Store,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Round,
    /// Receives sync commands from the `Synchronizer`.
    rx_synchronizer: Receiver<Certificate>,
    /// Loops back to the core certificates for which we got all parents.
    tx_core: Sender<Certificate>,
    /// List of digests (certificates) that are waiting to be processed. Their processing will
    /// resume when we get all their dependencies.
    pending: HashMap<Digest, (Round, Sender<()>)>,
    /// The last garbage collected round.
    gc_round: Round,
}

impl CertificateWaiter {
    pub fn spawn(
        store: Store,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        rx_synchronizer: Receiver<Certificate>,
        tx_core: Sender<Certificate>,
    ) {
        tokio::spawn(async move {
            Self {
                store,
                consensus_round,
                gc_depth,
                rx_synchronizer,
                tx_core,
                pending: HashMap::new(),
                gc_round: 0,
            }
            .run()
            .await
        });
    }

    /// Helper function. It waits for particular data to become available in the storage and then
    /// delivers the specified header.
    async fn waiter(
        mut missing: Vec<(Vec<u8>, Store)>,
        deliver: Certificate,
        mut handler: Receiver<()>,
    ) -> DagResult<Certificate> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| deliver).map_err(DagError::from)
            }
            _ = handler.recv() => Ok(deliver),
        }
    }

    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        loop {
            tokio::select! {
                Some(certificate) = self.rx_synchronizer.recv() => {
                    let header_id = certificate.header.id.clone();

                    // Ensure we process only once this certificate.
                    if self.pending.contains_key(&header_id) {
                        continue;
                    }

                    // Add the certificate to the waiter pool. The waiter will return it to us when
                    // all its parents are in the store.
                    let mut wait_for: Vec<_> = certificate
                        .header
                        .parents
                        .iter()
                        .cloned()
                        .map(|x| (x.to_vec(), self.store.clone()))
                        .collect();
                    if let Some(metadata) = certificate.header.metadata.as_ref() {

                        // TODO: This is a problem. If we wait for a certificate, then advance our gc_round,
                        // it may happen that this certificate never returns if one of the weak links becomes
                        // older than GC. As a hack, we disable the round certificate check in the core and
                        // accept outdated certificates.

                        wait_for.extend(
                            metadata
                                .virtual_parents
                                .iter()
                                .filter(|(_, r)| r > &self.gc_round)
                                .map(|(x, _)| (x.to_vec(), self.store.clone()))
                                .collect::<Vec<_>>()
                        )
                    }
                    let (tx_cancel, rx_cancel) = channel(1);
                    self.pending.insert(header_id, (certificate.round(), tx_cancel));
                    let fut = Self::waiter(wait_for, certificate, rx_cancel);
                    waiting.push(fut);
                }
                Some(result) = waiting.next() => match result {
                    Ok(certificate) => {
                        self.tx_core.send(certificate).await.expect("Failed to send certificate");
                    },
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },
            }

            // Cleanup internal state. Deliver the certificates waiting on garbage collected ancestors.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                self.gc_round = round - self.gc_depth;
                for (r, handler) in self.pending.values() {
                    if *r <= self.gc_round + 1 {
                        let _ = handler.send(()).await;
                    }
                }
                let gc_round = self.gc_round;
                self.pending.retain(|_, (r, _)| r > &mut (gc_round + 1));
            }
        }
    }
}
