// Copyright (c) Facebook, Inc. and its affiliates.
use crypto::{generate_keypair, PublicKey, SecretKey};
use dag_core::committee::{Authority, Committee, Machine};
use rand::{rngs::StdRng, SeedableRng};

// Fixture
pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

// Fixture
pub fn committee() -> Committee {
    let mut rng = StdRng::from_seed([1; 32]);
    let authorities = keys()
        .iter()
        .enumerate()
        .map(|(i, (id, _))| {
            let primary = Machine {
                name: *id,
                host: "127.0.0.1".to_string(),
                port: 8080 + i as u16,
            };
            let worker = Machine {
                name: generate_keypair(&mut rng).0,
                host: "127.0.0.1".to_string(),
                port: 9080 + i as u16,
            };
            Authority {
                primary,
                workers: vec![(0, worker)],
                stake: 1,
            }
        })
        .collect();
    Committee {
        authorities,
        instance_id: 100,
    }
}
