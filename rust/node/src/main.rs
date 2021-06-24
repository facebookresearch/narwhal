use anyhow::{Context, Result};
use clap::{crate_name, crate_version, App, AppSettings, SubCommand};
use config::Export as _;
use config::Import as _;
use config::{Committee, KeyPair, Parameters, WorkerId};
use consensus::Consensus;
use crypto::PublicKey;
use env_logger::Env;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::channel;
use tokio::time::{sleep, Duration};
use worker::Worker;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("A research implementation of Narwhal and Tusk.")
        .args_from_usage("-v... 'Sets the level of verbosity'")
        .subcommand(
            SubCommand::with_name("generate_keys")
                .about("Print a fresh key pair to file")
                .args_from_usage("--filename=<FILE> 'The file where to print the new key pair'"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Run a node")
                .args_from_usage("--keys=<FILE> 'The file containing the node keys'")
                .args_from_usage("--committee=<FILE> 'The file containing committee information'")
                .args_from_usage("--parameters=[FILE] 'The file containing the node parameters'")
                .args_from_usage("--store=<PATH> 'The path where to create the data store'")
                .subcommand(SubCommand::with_name("primary").about("Run a single primary"))
                .subcommand(
                    SubCommand::with_name("worker")
                        .about("Run a single worker")
                        .args_from_usage("--id=<INT> 'The worker id'"),
                )
                .setting(AppSettings::SubcommandRequiredElseHelp),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    match matches.subcommand() {
        ("generate_keys", Some(subm)) => {
            let filename = subm.value_of("filename").unwrap();
            KeyPair::new()
                .export(filename)
                .context("Failed to generate key pair")?;
        }
        ("run", Some(subm)) => {
            let key_file = subm.value_of("keys").unwrap();
            let committee_file = subm.value_of("committee").unwrap();
            let parameters_file = subm.value_of("parameters");
            let store_path = subm.value_of("store").unwrap();

            // Read the committee and node's keypair from file.
            let keypair = KeyPair::import(key_file).context("Failed to load the node's keypair")?;
            let committee = Committee::import(committee_file)
                .context("Failed to load the committee information")?;

            // Load default parameters if none are specified.
            let parameters = match parameters_file {
                Some(filename) => {
                    Parameters::import(filename).context("Failed to load the node's parameters")?
                }
                None => Parameters::default(),
            };

            // Make the data store.
            let store = Store::new(store_path).context("Failed to create a store")?;

            // Check whether to run a primary, a worker, or an entire authority.
            match subm.subcommand() {
                ("primary", _) => run_primary(keypair, committee, parameters, store)
                    .context("Failed to spawn primary")?,
                ("worker", Some(subsubm)) => {
                    let id = subsubm
                        .value_of("id")
                        .unwrap()
                        .parse::<WorkerId>()
                        .context("The worker id must be a positive integer")?;
                    Worker::spawn(keypair.name, id, committee, parameters, store);
                }
                _ => unreachable!(),
            }

            // TODO: prevent from terminating in a better way....
            loop {
                sleep(Duration::from_millis(60_000)).await;
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

/// Spawn a single primary.
fn run_primary(
    keypair: KeyPair,
    committee: Committee,
    _parameters: Parameters,
    _store: Store,
) -> Result<()> {
    let name = keypair.name;
    let secret_key = keypair.secret;

    // Adapt the committee to the old primary.
    // TODO: Adapt to cleaned up primary.
    let old_committee = primary::committee::Committee {
        authorities: committee
            .authorities
            .into_iter()
            .map(|(name, authority)| {
                let primary = primary::committee::Machine {
                    name,
                    host: authority.primary.worker_to_primary.ip().to_string(),
                    port: authority.primary.worker_to_primary.port(),
                };
                let workers: Vec<(_, _)> = authority
                    .workers
                    .into_iter()
                    .map(|(shard_id, addresses)| {
                        let mut id = [0u8; 32];
                        for (i, byte) in shard_id.to_le_bytes().iter().enumerate() {
                            id[i] = *byte;
                        }
                        let machine = primary::committee::Machine {
                            name: PublicKey(
                                name.0
                                    .iter()
                                    .zip(id.iter())
                                    .map(|(x, y)| x ^ y)
                                    .collect::<Vec<_>>()
                                    .try_into()
                                    .unwrap(),
                            ),
                            host: addresses.primary_to_worker.ip().to_string(),
                            port: addresses.primary_to_worker.port(),
                        };
                        (shard_id, machine)
                    })
                    .collect();
                let stake = authority.stake as usize;
                primary::committee::Authority {
                    primary,
                    workers,
                    stake,
                }
            })
            .collect(),
        instance_id: 100,
    };

    // Spawn the consensus core.
    let (tx_consensus_receiving, rx_consensus_receiving) = channel(1000);
    let (tx_consensus_sending, rx_consensus_sending) = channel(1000);
    let mut consensus = Consensus::new(
        tx_consensus_sending,
        rx_consensus_receiving,
        old_committee.clone(),
    );
    tokio::spawn(async move {
        consensus.start().await;
    });

    // Spawn the primary core.
    let mut primary = primary::manage_primary::ManagePrimary::new(
        name,
        secret_key,
        old_committee,
        tx_consensus_receiving,
        rx_consensus_sending,
    );
    tokio::spawn(async move {
        primary.primary_core.start().await;
    });

    Ok(())
}
