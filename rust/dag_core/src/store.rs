use log::*;
use rand::{thread_rng, Rng};
use rocksdb::Options;
use rocksdb::DB;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use tokio::sync::oneshot;
use tokio::task;

// #[cfg(test)]
// #[path = "tests/store_tests.rs"]
// mod storage_tests;

#[derive(Debug)]
pub struct StoreError {
    msg: String,
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(StoreError: {})", self.msg)
    }
}

// Type to use when returning results from store operations
type StoreResult<T> = std::result::Result<T, StoreError>;

pub fn make_error<T>(msg: &str) -> StoreResult<T> {
    StoreResult::Err(StoreError {
        msg: msg.to_string(),
    })
}

pub type DBKey = Vec<u8>;
pub type DBValue = Vec<u8>;

#[derive(Clone)]
pub struct Store {
    command_input_endpoint: std::sync::mpsc::SyncSender<StoreCommand>,
}

pub enum StoreCommand {
    Read(DBKey, oneshot::Sender<StoreResult<Option<DBValue>>>),
    Write(DBKey, DBValue, oneshot::Sender<StoreResult<()>>),
    Delete(DBKey, oneshot::Sender<StoreResult<()>>),
    NotifyRead(DBKey, oneshot::Sender<StoreResult<DBValue>>),
    Close(oneshot::Sender<StoreResult<()>>),
}

impl Store {
    pub async fn new(path: String) -> Store {
        // Initialize an async channel, that never blocks the sender.
        let (tx, rx) = std::sync::mpsc::sync_channel(10_000);

        // The database connection.
        let _res = task::spawn_blocking(move || {
            // do some compute-heavy work or call synchronous code

            let db = DB::open_default(path).unwrap();
            let mut opts = Options::default();
            let mut rng = thread_rng();
            let extra: usize = rng.gen_range(0, 64);
            opts.set_write_buffer_size((64 + extra) * 1024 * 1024);

            let mut pending_notifications: HashMap<
                Vec<u8>,
                VecDeque<oneshot::Sender<StoreResult<DBValue>>>,
            > = HashMap::new();

            while let Ok(cmd) = rx.recv() {
                match cmd {
                    StoreCommand::Write(key, value, resp) => {
                        match db.put(&key, &value) {
                            Err(_e) => {
                                let _ = resp.send(make_error("Error on write."));
                            }
                            _ => {
                                let _ = resp.send(Ok(()));

                                // Now send all notifications.
                                if let Some(mut nofification_channels) =
                                    pending_notifications.remove(&key)
                                {
                                    while let Some(chan) = nofification_channels.pop_front() {
                                        let _ = chan.send(Ok(value.clone()));
                                    }
                                }
                            }
                        };
                    }
                    StoreCommand::Delete(key, resp) => match db.delete(key) {
                        Ok(()) => {
                            let _ = resp.send(Ok(()));
                        }
                        Err(e) => {
                            error!("operational problem encountered: {}", e);
                            let _ = resp.send(make_error("Failed to execute delete."));
                        }
                    },
                    StoreCommand::Read(key, resp) => {
                        if resp.is_closed() {
                            continue;
                        }
                        match db.get(key) {
                            Ok(r) => {
                                let _ = resp.send(Ok(r));
                            }
                            Err(e) => {
                                error!("operational problem encountered: {}", e);
                                let _ = resp.send(make_error("Failed to execute read."));
                            }
                        }
                    }
                    StoreCommand::NotifyRead(key, resp) => {
                        if resp.is_closed() {
                            continue;
                        }
                        match db.get(&key) {
                            Ok(Some(r)) => {
                                let _ = resp.send(Ok(r));
                            }
                            Ok(None) => {
                                let notify_record = pending_notifications
                                    .entry(key)
                                    .or_insert_with(VecDeque::new);
                                notify_record.push_back(resp);
                            }
                            Err(e) => {
                                error!("operational problem encountered: {}", e);
                                let _ = resp.send(make_error("Failed to execute notify read."));
                            }
                        }
                    }

                    StoreCommand::Close(resp) => {
                        let _ = resp.send(Ok(()));
                        break;
                    }
                };
            }
        });

        Store {
            command_input_endpoint: tx,
        }
    }

    pub async fn read(&mut self, key: DBKey) -> StoreResult<Option<DBKey>> {
        let (sender, receiver) = oneshot::channel();

        self.command_input_endpoint
            .send(StoreCommand::Read(key, sender))
            .or_else(|_e| make_error("Failed to send read."))?;
        receiver
            .await
            .or_else(|_e| make_error("Failed to receive read."))?
    }

    pub async fn notify_read(&mut self, key: DBKey) -> StoreResult<DBKey> {
        let (sender, receiver) = oneshot::channel();

        self.command_input_endpoint
            .send(StoreCommand::NotifyRead(key, sender))
            .or_else(|_e| make_error("Failed to send read."))?;
        receiver
            .await
            .or_else(|_e| make_error("Failed to receive read."))?
    }

    pub async fn delete(&mut self, key: DBKey) -> StoreResult<()> {
        let (sender, receiver) = oneshot::channel();

        self.command_input_endpoint
            .send(StoreCommand::Delete(key, sender))
            .or_else(|_e| make_error("Failed to send delete."))?;
        receiver
            .await
            .or_else(|_e| make_error("Failed to receive delete."))?
    }

    pub async fn write(&mut self, key: DBKey, value: DBValue) -> StoreResult<()> {
        let (sender, _receiver) = oneshot::channel();

        self.command_input_endpoint
            .send(StoreCommand::Write(key, value, sender))
            .or_else(|_e| make_error("Failed to send write."))

        // _receiver.await.or_else(|_e| make_error("Failed to receive read."))?
    }
}
