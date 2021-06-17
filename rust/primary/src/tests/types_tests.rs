use super::*;
use crate::messages::messages_tests::*;
use crypto::generate_keypair;
use rand::{rngs::StdRng, SeedableRng};
use rstest::*;

#[fixture]
pub fn keys() -> Vec<(NodeID, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

#[rstest]
fn test_certificate(
    keys: Vec<(NodeID, SecretKey)>,
    committee: Committee,
    header: BlockHeader,
    signed_header: SignedBlockHeader,
) {
    let digest = signed_header.digest;
    let mut aggregator = SignatureAggregator::new();
    let original_id = keys[0].0;
    aggregator.init(header, digest.clone(), 1, original_id);

    let mut result = None;
    for (id, secret) in keys.into_iter() {
        let digest = PartialCertificate::make_digest(&digest, 1, original_id);
        let signature = Signature::new(&digest, &secret);
        result = aggregator.append(id, signature, &committee).unwrap();
    }
    assert!(result.is_some());
    assert!(result.unwrap().check(&committee).is_ok());
}
