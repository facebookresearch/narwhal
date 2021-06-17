mod config;
extern crate env_logger;
use bytes::BufMut;
use bytes::{Bytes, BytesMut};
use clap::{App, SubCommand};
use config::*;
use dag_core::committee::*;
use dag_core::manage_primary::ManagePrimary;
use dag_core::manage_worker::*;

use dag_core::messages::WorkerChannelType;
use dag_core::store::Store;
use dag_core::types::*;
use futures::future::join_all;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use hotstuff::Consensus;
use hotstuff::Export;
use hotstuff::Parameters;
use log::*;
use rand::Rng;
use rand::{rngs::StdRng, SeedableRng};
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::runtime::Builder;
use tokio::sync::mpsc::channel;
use tokio::time::sleep;
use tokio::time::{interval, Instant};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = App::new("Mempool research")
        .version("0.0.1")
        .about("A faster mempool.")
        .args_from_usage("-v 'Activate verbose logging'")
        .subcommand(SubCommand::with_name("benchmark")
            .about("Runs a benchmark")
            .subcommand(SubCommand::with_name("node")
                .args_from_usage("
                    --committee=<PATH> 'Sets the file describing the public configurations of all nodes'
                    --batch=<INT> 'The transactions batch size'
                    --node=<PATH> 'Sets the file describing the private configurations of the node'
                    --all 'Whether to run the primary and all workers or only one machine'
                    --parameters=[FILE] 'The file containing the node parameters'")
            )
            .subcommand(SubCommand::with_name("client")
                .args_from_usage("
                    --address=<STR> 'The network address of the node where to send txs'
                    --size=<INT> 'The size of each transaction in bytes'
                    --rate=<INT> 'rate (rxs/s) at which to send the transactions'
                    --others=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'
                ")
            )
        )
        .subcommand(SubCommand::with_name("generate")
            .about("Generate a committee configuration file and the nodes configuration files.")
            .args_from_usage("
                --nodes=<INT> 'The number of nodes'
                --workers=<INT> 'The number of workers per node'
            ")
        )
        .get_matches();

    let log_level = match matches.is_present("v") {
        true => "debug",
        false => "info",
    };
    env_logger::from_env(env_logger::Env::default().default_filter_or(log_level))
        .format_timestamp_millis()
        .init();

    match matches.subcommand() {
        ("benchmark", Some(subm)) => match subm.subcommand() {
            ("node", Some(subsubm)) => {
                let committee_config_path = subsubm.value_of("committee").unwrap();
                let node_config_path = subsubm.value_of("node").unwrap();
                let parameters_file = subsubm.value_of("parameters");

                let batch_size = subsubm
                    .value_of("batch")
                    .unwrap()
                    .parse::<usize>()
                    .expect("Batch size should be an integer");
                let all = subsubm.is_present("all");
                run_benchmark_node(
                    committee_config_path,
                    node_config_path,
                    batch_size,
                    all,
                    parameters_file,
                )?;
            }
            ("client", Some(subsubm)) => {
                let rate = subsubm
                    .value_of("rate")
                    .unwrap()
                    .parse::<u64>()
                    .expect("Rate (Txs/s) at which to send the transactions");
                let transactions_size = subsubm
                    .value_of("size")
                    .unwrap()
                    .parse::<usize>()
                    .expect("Transactions size should be an integer");
                let address = subsubm.value_of("address").unwrap();
                let others = subsubm
                    .values_of("others")
                    .unwrap_or_default()
                    .into_iter()
                    .map(|x| x.parse::<SocketAddr>())
                    .collect::<Result<Vec<_>, _>>()
                    .expect("Invalid socket address format");
                run_benchmark_client(transactions_size, address, rate, others)?;
            }
            _ => {
                error!("Invalid benchmark sub command");
            }
        },
        ("generate", Some(subm)) => {
            let nodes = subm
                .value_of("nodes")
                .unwrap()
                .parse::<usize>()
                .expect("Number of nodes should be an integer");
            let workers = subm
                .value_of("workers")
                .unwrap()
                .parse::<usize>()
                .expect("Number of workers should be an integer");
            generate_configs(nodes, workers)?;
        }
        _ => {
            error!("Unknown command");
        }
    }
    Ok(())
}

/// Print config files assuming localhost for all network addresses.
fn generate_configs(nodes: usize, workers: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = StdRng::from_seed([0; 32]);
    let primary_keys: Vec<_> = (0..nodes).map(|_| get_keypair(&mut rng)).collect();
    let workers_keys: Vec<_> = (0..nodes * workers)
        .map(|_| get_keypair(&mut rng))
        .collect();

    // Make the committee.
    let authorities = primary_keys
        .iter()
        .enumerate()
        .map(|(i, (id, _))| {
            let port = 8080 + 100 * i as u16;
            let primary = Machine {
                name: *id,
                host: "127.0.0.1".to_string(),
                port,
            };
            let shards_workers: Vec<(_, _)> = (0..workers)
                .map(|x| {
                    let worker = Machine {
                        name: workers_keys[i * workers + x].0,
                        host: "127.0.0.1".to_string(),
                        port: port + (x + 1) as u16,
                    };
                    (x as u32, worker)
                })
                .collect();
            Authority {
                primary,
                workers: shards_workers,
                stake: 1,
            }
        })
        .collect();
    let committee = Committee {
        authorities,
        instance_id: 100,
    };
    committee.write("local_committee_config.json")?;

    // Make the node's config.
    for (i, key) in primary_keys.iter().chain(workers_keys.iter()).enumerate() {
        let (id, secret_key) = key;
        let node_config = NodeConfig {
            id: id.encode_base64(),
            secret_key: secret_key.encode_base64(),
        };
        let filename = format!("node_config_{:?}.json", i).to_string();
        node_config.write(&filename)?;
    }
    Ok(())
}

fn run_benchmark_node(
    committee_config_path: &str,
    node_config_path: &str,
    batch_size: usize,
    all: bool,
    parameters: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rt = Builder::new_multi_thread()
        .worker_threads(16)
        .thread_name("bench-node")
        .thread_stack_size(3 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();

    // Load default parameters if none are specified.
    let parameters = match parameters {
        Some(filename) => Parameters::read(filename)?,
        None => Parameters::default(),
    };

    let committee = Committee::read(committee_config_path)?;
    // TODO: Sanitize committee. There should the same number of workers for all
    // nodes, at least one worker per node, at least one node, and each (address, port)
    // can only be used once.
    let node_config = NodeConfig::read(node_config_path)?;
    let node_id = NodeID::from_base64(&node_config.id)?;
    let secret_key = SecretKey::from_base64(&node_config.secret_key)?;
    let authority = committee.find_authority(&node_id)?;
    info!("Committee size: {}", committee.size());
    info!("Number of workers per node: {}", authority.workers.len());
    info!("Node Id: {:?}", node_id);
    info!("Txs batch size set to {}", batch_size);

    rt.block_on(async {
        // Run the primary and all workers.
        let (tx_mempool, rx_mempool) = channel(1000);
        let (tx, mut rx) = channel(1000);

        tokio::spawn(async move {
            loop {
                rx.recv().await;
            }
        });

        if all {
            // Share one store!
            let store = Store::new(
                format!(".storage_integrated_{:?}_shared", authority.primary.name).to_string(),
            )
            .await;
            for (_, machine) in authority.workers {
                let worker_id = machine.name;
                let _ = ManageWorker::new(
                    worker_id,
                    committee.clone(),
                    batch_size,
                    Some(store.clone()),
                )
                .await;
            }
            let primary_id = authority.primary.name;
            let mut primary = ManagePrimary::new(
                primary_id, secret_key, committee, // true,
                rx_mempool,
            );
            Consensus::run(
                primary.primary_core.id.clone(),
                primary.primary_core.committee.clone(),
                parameters,
                primary.primary_core.signature_channel.clone(),
                primary.primary_core.store.clone(),
                tx_mempool,
                tx,
            )
            .await
            .unwrap();

            primary.primary_core.start().await;

            // Run a primary.
        } else if committee.is_primary(&node_id).unwrap() {
            let mut primary = ManagePrimary::new(
                node_id, secret_key, committee, //true,
                rx_mempool,
            );
            Consensus::run(
                primary.primary_core.id.clone(),
                primary.primary_core.committee.clone(),
                parameters,
                primary.primary_core.signature_channel.clone(),
                primary.primary_core.store.clone(),
                tx_mempool,
                tx,
            )
            .await
            .unwrap();
            primary.primary_core.start().await;

            // Run a worker.
        } else {
            let _ = ManageWorker::new(node_id, committee.clone(), batch_size, None).await;
            loop {
                sleep(Duration::from_millis(500)).await;
            }
        }
    });
    Ok(())
}

fn run_benchmark_client(
    transactions_size: usize,
    address: &str,
    rate: u64,
    others: Vec<SocketAddr>,
) -> Result<(), Box<dyn std::error::Error>> {
    let rt = Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("bench-client")
        .thread_stack_size(3 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        // Wait for all other clients to be ready.
        wait(others).await;

        // Connect to our worker.
        let stream = TcpStream::connect(address)
            .await
            .expect("Failed to connect to worker");
        let (read_sock, write_sock) = stream.into_split();
        let mut transport_write = FramedWrite::new(write_sock, LengthDelimitedCodec::new());
        let mut transport_read = FramedRead::new(read_sock, LengthDelimitedCodec::new());

        // Set the type of the channel.
        let head = WorkerChannelType::Transaction;
        let header = Bytes::from(bincode::serialize(&head).expect("Error deserializing"));
        transport_write.send(header).await.expect("Error Sending");
        let _n = transport_read.next().await.expect("Error on test receive");

        // Drain the ACKs from the network.
        tokio::spawn(async move { while transport_read.next().await.is_some() {} });

        // Send all transactions.
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 1000 / PRECISION;

        info!("Sending transactions to node {:?}", address);
        info!("Transactions size {} bytes", transactions_size);
        info!("Transactions rate {} tx/s", rate);

        let burst = rate / PRECISION;
        let mut counter = 0;
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);
        info!(
            "Start sending at {:?} ms",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        let mut txs = BytesMut::with_capacity(transactions_size);
        let mut rng = rand::thread_rng();
        let mut tag = rng.gen::<u64>();
        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            // Send burst.

            for x in 0..burst {
                if x == counter % burst {
                    // Send special transaction
                    txs.reserve(transactions_size);
                    txs.put_u64(0);
                    txs.put_u64(counter);
                    txs.resize(transactions_size, 5u8);
                    let frozen_tx = txs.split().freeze();
                    if let Err(e) = transport_write.send(frozen_tx).await {
                        warn!("Failed to send transaction: {}", e);
                        break 'main;
                    }
                    info!("Sending special transaction {}", counter);
                } else {
                    txs.reserve(transactions_size);
                    txs.put_u64(1);
                    txs.put_u64(tag);
                    tag += 1;
                    txs.put_u64(x as u64);
                    txs.resize(transactions_size, 0);

                    let frozen_tx = txs.split().freeze();
                    if let Err(e) = transport_write.send(frozen_tx).await {
                        warn!("Failed to send transaction: {}", e);
                        break 'main;
                    }
                }
            }
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                warn!("Transaction rate too high for this client");
            }
            counter += 1;
        }
    });
    Ok(())
}

async fn wait(nodes: Vec<SocketAddr>) {
    info!("Waiting for all nodes to be online...");
    join_all(nodes.iter().cloned().map(|address| {
        tokio::spawn(async move {
            while TcpStream::connect(address).await.is_err() {
                sleep(Duration::from_millis(10)).await;
            }
        })
    }))
    .await;
}
