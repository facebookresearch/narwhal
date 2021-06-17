use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use std::collections::HashMap;
use std::hash::Hash;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration};

// #[cfg(test)]
// #[path = "tests/timer_tests.rs"]
// pub mod timer_tests;

type TimerDuration = u64;

enum Command<Id> {
    Schedule(TimerDuration, Id),
    Cancel(Id),
}

pub struct Timer<Id> {
    inner: Sender<Command<Id>>,
    pub notifier: Receiver<Id>,
}

impl<Id: 'static + Hash + Eq + Clone + Send + Sync> Timer<Id> {
    pub fn new() -> Self {
        let (tx_notifier, rx_notifier) = channel(100);
        let (tx_inner, mut rx_inner): (Sender<Command<Id>>, _) = channel(100);
        tokio::spawn(async move {
            let mut waiting = FuturesUnordered::new();
            let mut pending = HashMap::new();
            loop {
                tokio::select! {
                    Some(message) = rx_inner.recv() => {
                        match message {
                            Command::Schedule(delay, id) => {
                                let (tx_handler, rx_handler) = channel(1);
                                if let Some(tx) = pending.insert(id.clone(), tx_handler) {
                                    let _ = tx.send(()).await;
                                }
                                let fut = Self::waiter(delay, id, rx_handler);
                                waiting.push(fut);
                            },
                            Command::Cancel(id) => {
                                if let Some(tx) = pending.remove(&id) {
                                    let _ = tx.send(()).await;
                                }
                            }
                        }
                    },
                    Some(message) = waiting.next() => {
                        if let Some(id) = message {
                            let _ = pending.remove(&id);
                            if let Err(e) = tx_notifier.send(id).await {
                                panic!("Failed to send timer notification: {}", e);
                            }
                        }
                    },
                    else => break,
                }
            }
        });
        Self {
            inner: tx_inner,
            notifier: rx_notifier,
        }
    }

    async fn waiter(delay: TimerDuration, id: Id, mut handler: Receiver<()>) -> Option<Id> {
        tokio::select! {
            () = sleep(Duration::from_millis(delay)) => Some(id),
            _ = handler.recv() => None,
        }
    }

    pub async fn schedule(&mut self, delay: TimerDuration, id: Id) {
        let message = Command::Schedule(delay, id);
        if let Err(e) = self.inner.send(message).await {
            panic!("Failed to schedule timer: {}", e);
        }
    }

    pub async fn cancel(&mut self, id: Id) {
        let message = Command::Cancel(id);
        if let Err(e) = self.inner.send(message).await {
            panic!("Failed to cancel timer: {}", e);
        }
    }
}
