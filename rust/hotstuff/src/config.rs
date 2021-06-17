use crate::ConsensusError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::BufWriter;
use std::io::Write as _;

// pub type Stake = u32;
// pub type EpochNumber = u128;

#[derive(Serialize, Deserialize)]
pub struct Parameters {
    pub timeout_delay: u64,
    pub sync_retry_delay: u64,
    pub max_payload_size: usize,
    pub min_block_delay: u64,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            timeout_delay: 5000,
            sync_retry_delay: 10_000,
            max_payload_size: 500,
            min_block_delay: 100,
        }
    }
}

pub trait Export: Serialize + DeserializeOwned {
    fn read(path: &str) -> Result<Self, ConsensusError> {
        let reader = || -> Result<Self, std::io::Error> {
            let data = fs::read(path)?;
            Ok(serde_json::from_slice(data.as_slice())?)
        };
        reader().map_err(|e| ConsensusError::ReadError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }

    fn write(&self, path: &str) -> Result<(), ConsensusError> {
        let writer = || -> Result<(), std::io::Error> {
            let file = OpenOptions::new().create(true).write(true).open(path)?;
            let mut writer = BufWriter::new(file);
            let data = serde_json::to_string_pretty(self).unwrap();
            writer.write_all(data.as_ref())?;
            writer.write_all(b"\n")?;
            Ok(())
        };
        writer().map_err(|e| ConsensusError::WriteError {
            file: path.to_string(),
            message: e.to_string(),
        })
    }
}

impl Export for Parameters {}

// #[derive(Clone, Serialize, Deserialize)]
// pub struct Authority {
//     pub name: PublicKey,
//     pub stake: Stake,
//     pub address: SocketAddr,
// }

// #[derive(Clone, Serialize, Deserialize)]
// pub struct Committee {
//     pub authorities: HashMap<PublicKey, Authority>,
//     pub epoch: EpochNumber,
// }

// impl Committee {
//     pub fn new(info: Vec<(PublicKey, Stake, SocketAddr)>, epoch: EpochNumber) -> Self {
//         Self {
//             authorities: info
//                 .into_iter()
//                 .map(|(name, stake, address)| {
//                     let authority = Authority {
//                         name,
//                         stake,
//                         address,
//                     };
//                     (name, authority)
//                 })
//                 .collect(),
//             epoch,
//         }
//     }

//     pub fn size(&self) -> usize {
//         self.authorities.len()
//     }

//     pub fn stake(&self, name: &PublicKey) -> Stake {
//         self.authorities.get(&name).map_or_else(|| 0, |x| x.stake)
//     }

//     pub fn quorum_threshold(&self) -> Stake {
//         // If N = 3f + 1 + k (0 <= k < 3)
//         // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
//         let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
//         2 * total_votes / 3 + 1
//     }

//     pub fn address(&self, name: &PublicKey) -> ConsensusResult<SocketAddr> {
//         self.authorities
//             .get(name)
//             .map(|x| x.address)
//             .ok_or_else(|| ConsensusError::NotInCommittee(*name))
//     }

//     pub fn broadcast_addresses(&self, myself: &PublicKey) -> Vec<SocketAddr> {
//         self.authorities
//             .values()
//             .filter(|x| x.name != *myself)
//             .map(|x| x.address)
//             .collect()
//     }
// }
