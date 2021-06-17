use super::*;
use crate::messages::messages_tests::*;
use ed25519_dalek::Digest as DalekDigest;
use rand::{rngs::StdRng, SeedableRng};
use rstest::*;
use tokio::sync::mpsc::channel;
use tokio::sync::oneshot;

#[fixture]
pub fn keys() -> Vec<(NodeID, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| get_keypair(&mut rng)).collect()
}

impl Digestible for [u8; 5] {
    fn digest(self: &[u8; 5]) -> Digest {
        let mut h = dalek::Sha512::new();
        let mut hash = [0u8; 64];
        let mut digest = [0u8; 32];
        h.update(&self);
        hash.copy_from_slice(h.finalize().as_slice());
        digest.copy_from_slice(&hash[..32]);
        Digest(digest)
    }
}

#[rstest]
fn test_signature(keys: Vec<(NodeID, SecretKey)>) {
    let (id1, sec1) = &keys[0];
    let (id2, _sec2) = &keys[1];

    let message = b"hello";
    let bad_message = b"hellx";
    let s = Signature::new(&message.digest(), &sec1);
    assert!(s.check(&message.digest(), *id1).is_ok());
    assert!(s.check(&message.digest(), *id2).is_err());
    assert!(s.check(&bad_message.digest(), *id1).is_err());
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
    aggregator.init(header, digest, 1, original_id);

    let mut result = None;
    for (id, secret) in keys.into_iter() {
        let digest = PartialCertificate::make_digest(digest, 1, original_id);
        let signature = Signature::new(&digest, &secret);
        result = aggregator.append(id, signature, &committee).unwrap();
    }
    assert!(result.is_some());
    assert!(result.unwrap().check(&committee).is_ok());
}

#[rstest]
fn test_partial_certificate(keys: Vec<(NodeID, SecretKey)>) {
    let digest = b"hello".digest();
    let (id, sec) = &keys[0];
    let partial = PartialCertificate::debug_new(*id, *id, digest, 0, &sec);
    assert!(partial.debug_check().is_ok());
}

#[rstest]
#[tokio::test]
async fn test_signature_factory(mut keys: Vec<(NodeID, SecretKey)>) {
    let digest = b"hello".digest();
    let (pub_key, secret_key) = keys.pop().unwrap();
    let (mut tx, rx) = channel(100);
    let (sender, receiver) = oneshot::channel();

    let mut factory = SignatureFactory::new(secret_key, rx);
    tokio::spawn(async move {
        factory.start().await;
    });

    tx.send((digest, sender)).await.unwrap();
    let signature = receiver.await;
    assert!(signature.is_ok());
    assert!(signature.unwrap().check(&digest, pub_key).is_ok());
}
