use super::error::*;
use super::types::*;
use rand::{rngs::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Write};
use std::net::SocketAddr;

#[derive(Clone, Serialize, Deserialize)]
pub struct Machine {
    pub name: NodeID,
    pub host: String,
    pub port: u16,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Authority {
    pub primary: Machine,
    pub workers: Vec<(ShardID, Machine)>,
    pub stake: Stake,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Committee {
    pub authorities: Vec<Authority>,
    pub instance_id: SequenceNumber,
}

impl Committee {
    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    pub fn stake(&self, primary_id: &NodeID) -> Stake {
        for auth in &self.authorities {
            if *primary_id == auth.primary.name {
                return auth.stake;
            }
        }
        0
    }

    pub fn get_worker_by_shard_id(
        &self,
        primary_id: &NodeID,
        shard_id: ShardID,
    ) -> Result<NodeID, DagError> {
        for auth in &self.authorities {
            if *primary_id == auth.primary.name {
                for (sid, work) in &auth.workers {
                    if *sid == shard_id {
                        return Ok(work.name);
                    }
                }
            }
        }
        Err(DagError::UnknownNode)
    }

    pub fn stake_worker(&self, worker_id: &NodeID) -> Stake {
        for auth in &self.authorities {
            for (_, work) in &auth.workers {
                if *worker_id == work.name {
                    return auth.stake;
                }
            }
        }
        0
    }

    pub fn get_primary_for_worker(&self, worker_id: &NodeID) -> Result<NodeID, DagError> {
        for auth in &self.authorities {
            for (_, work) in &auth.workers {
                if *worker_id == work.name {
                    return Ok(auth.primary.name);
                }
            }
        }
        Err(DagError::UnknownNode)
    }

    fn total_votes(&self) -> Stake {
        self.authorities.iter().map(|x| x.stake).sum()
    }

    pub fn quorum_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        2 * self.total_votes() / 3 + 1
    }

    pub fn validity_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        (self.total_votes() + 2) / 3
    }

    pub fn get_url(&self, node: &NodeID) -> Result<String, DagError> {
        for auth in &self.authorities {
            if *node == auth.primary.name {
                return Ok(format!("{}:{}", auth.primary.host, auth.primary.port));
            }
            for (_, machine) in &auth.workers {
                if *node == machine.name {
                    return Ok(format!("{}:{}", machine.host, machine.port));
                }
            }
        }
        Err(DagError::UnknownNode)
    }
    //This function is only used for Hotstuff
    //so we need to avoid port conflicts with the DAG
    pub fn address(&self, node: &NodeID) -> Result<SocketAddr, DagError> {
        self.get_url(node).map(|x| {
            let mut address: SocketAddr = x.parse().unwrap();
            address.set_port(address.port() + 1000);
            address
        })
    }

    /// Check whether an id is a worker or primary id.
    pub fn is_primary(&self, node: &NodeID) -> Result<bool, DagError> {
        for auth in &self.authorities {
            if *node == auth.primary.name {
                return Ok(true);
            }
            for (_, machine) in &auth.workers {
                if *node == machine.name {
                    return Ok(false);
                }
            }
        }
        Err(DagError::UnknownNode)
    }

    /// Find the authority associated with an id.
    pub fn find_authority(&self, name: &NodeID) -> Result<Authority, DagError> {
        self.authorities
            .iter()
            .find(|auth| {
                auth.workers
                    .iter()
                    .any(|(_, machine)| machine.name == *name)
                    || auth.primary.name == *name
            })
            .cloned()
            .ok_or(DagError::UnknownNode)
    }

    /// Create a new committee by reading it from file.
    pub fn read(path: &str) -> Result<Self, std::io::Error> {
        let data = fs::read(path)?;
        Ok(serde_json::from_slice(data.as_slice())?)
    }

    /// Write the committee to a json file.
    pub fn write(&self, path: &str) -> Result<(), std::io::Error> {
        let file = OpenOptions::new().create(true).write(true).open(path)?;
        let mut writer = BufWriter::new(file);
        let data = serde_json::to_string_pretty(self).unwrap();
        writer.write_all(data.as_ref())?;
        writer.write_all(b"\n")?;
        Ok(())
    }

    /// Create a test committee with equal stake and 1 worker per primary.
    pub fn debug_new(nodes: Vec<NodeID>, base_port: u16) -> Self {
        let instance_id = 100;
        let mut rng = StdRng::from_seed([0; 32]);
        let authorities = nodes
            .iter()
            .enumerate()
            .map(|(i, node)| {
                let primary = Machine {
                    name: *node,
                    host: "127.0.0.1".to_string(),
                    port: base_port + i as u16,
                };
                let worker = Machine {
                    name: get_keypair(&mut rng).0,
                    host: "127.0.0.1".to_string(),
                    port: base_port + (i + nodes.len()) as u16,
                };
                Authority {
                    primary,
                    workers: vec![(0, worker)],
                    stake: 1,
                }
            })
            .collect();
        Self {
            authorities,
            instance_id,
        }
    }

    pub fn get_workers_addresses(&self) -> Vec<String> {
        self.authorities
            .iter()
            .map(|auth| {
                auth.workers
                    .iter()
                    .map(|(_, machine)| format!("{}:{}", machine.host, machine.port))
                    .collect::<Vec<_>>()
            })
            .flatten()
            .collect()
    }

    pub fn broadcast_addresses(&self, myself: &NodeID) -> Vec<SocketAddr> {
        self.authorities
            .iter()
            .filter(|x| x.primary.name != *myself)
            .map(|auth| {
                let machine = &auth.primary;
                format!("{}:{}", machine.host, machine.port + 1000)
                    .parse()
                    .unwrap()
            })
            .collect()
    }
}
