use super::*;
use crate::types::types_tests::*;
use crypto::generate_keypair;
use rand::{rngs::StdRng, SeedableRng};
use rstest::*;

#[fixture]
pub fn committee(keys: Vec<(NodeID, SecretKey)>) -> Committee {
    let instance_id = 100;
    let mut rng = StdRng::from_seed([0; 32]);
    let authorities = keys
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
        instance_id,
    }
}

#[fixture]
pub fn header(mut keys: Vec<(NodeID, SecretKey)>, committee: Committee) -> BlockHeader {
    let parents = Certificate::genesis(&committee)
        .iter()
        .map(|x| (x.primary_id, x.digest.clone()))
        .collect();
    let (node_id, _) = keys.pop().unwrap();
    BlockHeader {
        author: node_id,
        round: 1,
        parents,
        metadata: Metadata::default(),
        transactions_digest: WorkersDigests::new(),
        instance_id: committee.instance_id,
    }
}

#[fixture]
pub fn signed_header(mut keys: Vec<(NodeID, SecretKey)>, header: BlockHeader) -> SignedBlockHeader {
    let (_, secret_key) = keys.pop().unwrap();
    SignedBlockHeader::debug_new(header, &secret_key).unwrap()
}

#[fixture]
pub fn certificate(
    keys: Vec<(NodeID, SecretKey)>,
    committee: Committee,
    header: BlockHeader,
    signed_header: SignedBlockHeader,
) -> Certificate {
    let digest = signed_header.digest;
    let mut aggregator = SignatureAggregator::new();
    aggregator.init(header.clone(), digest.clone(), header.round, header.author);
    let mut result = None;
    for (id, secret) in keys.into_iter() {
        let digest = PartialCertificate::make_digest(&digest, header.round, header.author);
        let signature = Signature::new(&digest, &secret);
        result = aggregator.append(id, signature, &committee).unwrap();
    }
    result.unwrap()
}

#[rstest]
fn test_block_check(signed_header: SignedBlockHeader, committee: Committee) {
    assert!(signed_header.check(&committee).is_ok());
}

#[rstest]
fn test_genesis(committee: Committee) {
    assert_eq!(
        Certificate::genesis(&committee).len(),
        committee.quorum_threshold()
    );
}

#[rstest]
fn test_parent_check_function(header: BlockHeader) {
    let parent_digest: Vec<Digest> = header
        .parents
        .iter()
        .map(|(_, digest)| digest.clone())
        .collect();

    assert!(header.has_parent(&parent_digest[0]));

    assert!(header.has_all_parents(&parent_digest));

    let some_parents = vec![parent_digest[0].clone(), parent_digest[1].clone()];
    assert!(!header.has_all_parents(&some_parents));

    assert!(header.has_at_least_one_parent(&some_parents));
}
