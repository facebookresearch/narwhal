use crate::consensus::{Round, CHANNEL_CAPACITY};
use tokio::sync::mpsc::{Receiver, Sender, channel};
use store::Store;
use primary::Certificate;
use futures::future::try_join_all;
use futures::stream::FuturesOrdered;
use futures::stream::StreamExt as _;
use log::error;
use crate::error::{ConsensusResult, ConsensusError};

pub struct Committer {
    gc_depth: Round,
    rx_deliver: Receiver<Certificate>,
}

impl Committer {
    pub fn spawn(
        store: Store,
        gc_depth: Round,
        rx_commit: Receiver<Certificate>,
    ) -> Self {
        let (tx_deliver, rx_deliver) = channel(CHANNEL_CAPACITY);

        tokio::spawn(async move {
            CertificateWaiter::spawn(store, rx_commit, tx_deliver);
        });

        Self {
            gc_depth,
            rx_deliver
        }
    }

    async fn run(&mut self){
        loop {
            tokio::select! {
                Some(certificate) = self.rx_deliver.recv() => {
                    // TODO: ORDER SUB DAG
                }
            }
        }
    }
}

/// Waits to receive all the ancestors of a certificate before sending it through the output
/// channel. The outputs are in the same order as the input (FIFO).
pub struct CertificateWaiter {
    /// The persistent storage.
    store: Store,
    /// Receives input certificates.
    rx_input: Receiver<Certificate>,
    /// Outputs the certificates once we have all its parents.
    tx_output: Sender<Certificate>,
}

impl CertificateWaiter {
    pub fn spawn(
        store: Store,
        rx_input: Receiver<Certificate>,
        tx_output: Sender<Certificate>,
    ) {
        tokio::spawn(async move {
            Self {
                store,
                rx_input,
                tx_output,
            }
            .run()
            .await
        });
    }

    /// Helper function. It waits for particular data to become available in the storage
    /// and then delivers the specified header.
    async fn waiter(
        mut missing: Vec<(Vec<u8>, Store)>,
        deliver: Certificate,
    ) -> ConsensusResult<Certificate> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();

        try_join_all(waiting)
            .await
            .map(|_| deliver)
            .map_err(ConsensusError::from)
    }

    async fn run(&mut self) {
        let mut waiting = FuturesOrdered::new();

        loop {
            tokio::select! {
                Some(certificate) = self.rx_input.recv() => {
                    // Add the certificate to the waiter pool. The waiter will return it to us
                    // when all its parents are in the store.
                    let wait_for = certificate
                        .header
                        .parents
                        .iter()
                        .cloned()
                        .map(|x| (x.to_vec(), self.store.clone()))
                        .collect();
                    let fut = Self::waiter(wait_for, certificate);
                    waiting.push(fut);
                }
                Some(result) = waiting.next() => match result {
                    Ok(certificate) => {
                        self.tx_output.send(certificate).await.expect("Failed to send certificate");
                    },
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },
            }
        }
    }
}