#![recursion_limit = "1024"]

#[macro_use]
extern crate failure;
extern crate base64;
extern crate ed25519_dalek;
extern crate rstest;

#[macro_use]
mod error;
mod aggregators;
mod certificate_waiter;
mod core;
mod header_waiter;
mod helper;
mod messages;
mod payload_receiver;
mod primary;
mod proposer;
mod synchronizer;

pub mod committee;
pub mod manage_primary;
pub mod old_messages;
pub mod old_primary;
pub mod old_synchronizer;
pub mod primary_net;
pub mod processor;
mod sync_driver;
pub mod sync_server;
pub mod types;

pub use crate::messages::{Certificate, Header};
pub use crate::primary::{Primary, PrimaryWorkerMessage, Round, WorkerPrimaryMessage};
