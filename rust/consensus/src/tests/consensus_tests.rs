use super::*;
use config::{Authority, PrimaryAddresses};
use crypto::{generate_keypair, SecretKey};
use primary::Header;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::collections::{BTreeSet, VecDeque};
use tokio::sync::mpsc::channel;

// Fixture
fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

// Fixture
pub fn mock_committee() -> Committee {
    Committee {
        authorities: keys()
            .iter()
            .map(|(id, _)| {
                (
                    *id,
                    Authority {
                        stake: 1,
                        primary: PrimaryAddresses {
                            primary_to_primary: "0.0.0.0:0".parse().unwrap(),
                            worker_to_primary: "0.0.0.0:0".parse().unwrap(),
                        },
                        workers: HashMap::default(),
                    },
                )
            })
            .collect(),
    }
}

// Fixture
fn mock_certificate(
    origin: PublicKey,
    round: Round,
    parents: BTreeSet<Digest>,
) -> (Digest, Certificate) {
    let certificate = Certificate {
        header: Header {
            parents,
            ..Header::default()
        },
        origin,
        round,
        ..Certificate::default()
    };
    (certificate.digest(), certificate)
}

/*
fn certificates(start: Round, stop: Round) -> Vec<Certificate> {
    let mut certificates = Vec::new();
    let mut parents = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    for round in start..stop {
        let mut next_parents = BTreeSet::new();
        for (name, _) in &keys {
            let (digest, certificate) = mock_certificate(name.clone(), round, parents.clone());
            certificates.push(certificate);
            next_parents.insert(digest);
        }
        parents = next_parents;
    }
    certificates
}
*/

#[tokio::test]
async fn commit_one_leader() {
    let mut keys = keys();
    let _ = keys.pop();

    let mut certificates = VecDeque::new();
    let mut parents = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    for round in 1..=5 {
        let mut next_parents = BTreeSet::new();
        for (name, _) in &keys {
            let (digest, certificate) = mock_certificate(name.clone(), round, parents.clone());
            certificates.push_back(certificate);
            next_parents.insert(digest);
        }
        parents = next_parents;
    }

    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(mock_committee(), rx_waiter, tx_primary, tx_output);
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed the 12 first certificates to the consensus (they don't commit anything).
    for _ in 1..=12 {
        let certificate = certificates.pop_front().unwrap();
        tx_waiter.send(certificate).await.unwrap();
    }

    // The 13th certificate is the first with round 5. It should thus commits the leader of round 2.
    let certificate = certificates.pop_front().unwrap();
    tx_waiter.send(certificate).await.unwrap();

    // Ensure the first 3 ordered certificates are from round 1 (they are the parents of the committed
    // leader); then the leader's certificate should be committed.
    for _ in 1..=3 {
        let certificate = rx_output.recv().await.unwrap();
        assert_eq!(certificate.round, 1);
    }
    let certificate = rx_output.recv().await.unwrap();
    assert_eq!(certificate.round, 2);
}

#[tokio::test]
async fn commit_three_leaders() {
    let mut keys = keys();
    let _ = keys.pop();

    let mut certificates = VecDeque::new();
    let mut parents = Certificate::genesis(&mock_committee())
        .iter()
        .map(|x| x.digest())
        .collect::<BTreeSet<_>>();

    for round in 1..=9 {
        let mut next_parents = BTreeSet::new();
        for (name, _) in &keys {
            let (digest, certificate) = mock_certificate(name.clone(), round, parents.clone());
            certificates.push_back(certificate);
            next_parents.insert(digest);
        }
        parents = next_parents;
    }

    let (tx_waiter, rx_waiter) = channel(1);
    let (tx_primary, mut rx_primary) = channel(1);
    let (tx_output, mut rx_output) = channel(1);
    Consensus::spawn(mock_committee(), rx_waiter, tx_primary, tx_output);
    tokio::spawn(async move { while rx_primary.recv().await.is_some() {} });

    // Feed all certificates to the consensus.
    tokio::spawn(async move {
        while let Some(certificate) = certificates.pop_front() {
            tx_waiter.send(certificate).await.unwrap();
        }
    });

    //
    for r in 1..=15 {
        let certificate = rx_output.recv().await.unwrap();
        let expected = ((r - 1) / keys.len() as u64) + 1;
        assert_eq!(certificate.round, expected);
    }

    let certificate = rx_output.recv().await.unwrap();
    assert_eq!(certificate.round, 6);
}
