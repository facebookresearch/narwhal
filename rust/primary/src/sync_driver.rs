use crate::types::PrimaryMessage;
use crypto::Digest;
use futures::future::FutureExt;
use futures::select;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::*;
use std::collections::HashMap;
use std::collections::VecDeque;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};

const DELAY: u64 = 25;
const DELAY_EXPONENT: u64 = 2;

pub struct SyncDriver;

impl SyncDriver {
    pub fn run(
        mut missing: Receiver<(Digest, PrimaryMessage)>,
        commands: Sender<PrimaryMessage>,
        store: Store,
    ) {
        tokio::spawn(async move {
            let mut pending = FuturesUnordered::new();
            let mut timings: HashMap<Digest, (u64, VecDeque<(Digest, PrimaryMessage)>)> =
                HashMap::new();

            loop {
                select! {
                    some_message = missing.recv().fuse() => {
                        if let Some((digest, message)) = some_message {

                            if timings.contains_key(&digest) {
                                // We are already looking for this digest, so put it in the queue
                                let (_delay, queue) = timings.get_mut(&digest).unwrap();
                                queue.push_back((digest, message));
                            }
                            else {
                                // This is the first time we look for this
                                timings.insert(digest.clone(), (DELAY, VecDeque::new()));
                                pending.push(Self::waiter(DELAY, DELAY, (digest, message), store.clone(), commands.clone()));
                            }
                        }
                        else {
                            return;
                        }
                    },
                    (digest, _request, found) = pending.select_next_some() => {

                        if found {
                            // Clean up the structure.
                            timings.remove(&digest);
                        }
                        else {
                            // if there is another request pick it from the queue and send it.
                            if let Some((delay, queue)) = timings.get_mut(&digest) {
                                *delay *= DELAY_EXPONENT;
                                if *delay < 1000*60*60 {
                                    // queue.push_back((digest, request));
                                    if let Some((early_digest, early_request)) = queue.pop_front(){
                                        pending.push(Self::waiter(0, *delay, (early_digest, early_request), store.clone(), commands.clone()));
                                    }
                                }
                            }
                        }

                    }
                }
            }
        });
    }

    async fn waiter(
        initial_delay: u64,
        delay: u64,
        deliver: (Digest, PrimaryMessage),
        mut store: Store,
        commands: Sender<PrimaryMessage>,
    ) -> (Digest, PrimaryMessage, bool) {
        let (digest, request) = deliver;

        // Initial delay
        if initial_delay > 0 {
            sleep(Duration::from_millis(initial_delay)).await;
        }
        // Check before sending
        match store.read(digest.0.to_vec()).await {
            Ok(Some(_)) => return (digest, request, true),
            Ok(None) => (),
            Err(e) => panic!("Failed to read digest from store: {}", e),
        };

        // Log a message
        let v = match request {
            PrimaryMessage::SyncHeaderRequest(_, _, _) => "C",
            PrimaryMessage::SyncTxSend(_, _) => "T",
            _ => "O",
        };
        info!("Sync Waiter for {:?} delay {} {}", digest, delay, v);

        // Send Sync command
        commands
            .send(request.clone())
            .await
            .expect("Failed to send request");
        // Wait for response
        sleep(Duration::from_millis(delay)).await;
        // Check if response
        match store.read(digest.0.to_vec()).await {
            Ok(Some(_)) => (digest, request, true),
            Ok(None) => (digest, request, false),
            Err(e) => panic!("Failed to read digest from store: {}", e),
        }
    }
}
