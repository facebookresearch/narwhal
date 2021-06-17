use super::*;
use std::collections::VecDeque;

use crate::committee::Committee;
use crate::messages::messages_tests::*;
use crate::types::types_tests::*;

use std::collections::BTreeSet;

use rstest::*;

#[fixture]
pub fn processor(committee: Committee) -> Processor {
    Processor::new(committee)
}

#[fixture]
pub fn many_signed_headers(
    keys: Vec<(NodeID, SecretKey)>,
    committee: Committee,
) -> Vec<SignedBlockHeader> {
    let parents: BTreeSet<_> = Certificate::genesis(&committee)
        .iter()
        .map(|x| (x.primary_id, x.digest))
        .collect();

    keys.iter()
        .take(committee.quorum_threshold())
        .map(|(id, secret)| {
            let blockheader = BlockHeader {
                author: *id,
                round: 1,
                parents: parents.clone(),
                metadata: Metadata::default(),
                transactions_digest: WorkersDigests::new(),
                instance_id: committee.instance_id,
            };
            SignedBlockHeader::debug_new(blockheader, &secret).unwrap()
        })
        .collect()
}

#[fixture]
pub fn many_signed_headers_rounds(
    keys: Vec<(NodeID, SecretKey)>,
    committee: Committee,
) -> VecDeque<Vec<(Certificate, BlockHeader)>> {
    //the rest are crashed
    let threshold_keys: Vec<(NodeID, SecretKey)> = keys
        .into_iter()
        .take(committee.quorum_threshold())
        .collect();

    let mut all_header_certificates: VecDeque<Vec<(Certificate, BlockHeader)>> = VecDeque::new();
    //first parents are genesis
    let mut parents: BTreeSet<_> = Certificate::genesis(&committee)
        .iter()
        .map(|x| (x.primary_id, x.digest))
        .collect();
    for i in 1..41 {
        //make a block per author in the non-crashed group
        let round_headers_certificates: Vec<(Certificate, BlockHeader)> = threshold_keys
            .iter()
            .map(|(id, _)| {
                let blockheader = BlockHeader {
                    author: *id,
                    round: i,
                    parents: parents.clone(),
                    metadata: Metadata::default(),
                    transactions_digest: WorkersDigests::new(),
                    instance_id: committee.instance_id,
                };

                return (
                    Certificate::debug_new(&blockheader, &threshold_keys),
                    blockheader,
                );
            })
            .collect();

        parents = round_headers_certificates
            .iter()
            .map(|(certificate, header)| (header.author, certificate.digest))
            .collect();

        all_header_certificates.push_back(round_headers_certificates);
    }

    return all_header_certificates;
}

#[rstest]
fn test_get_missing_header_certificate(
    committee: Committee,
    mut many_signed_headers_rounds: VecDeque<Vec<(Certificate, BlockHeader)>>,
) {
    //Populate the dag but skip a round of blocks and certificates, then check if the processor returns the same set

    let mut processor = Processor::new(committee);
    let mut i = 1;
    let mut missing_header_digests = Vec::new();
    while let Some(round_headers) = many_signed_headers_rounds.pop_front() {
        if i == 1 {
            //drop the block header
            round_headers
                .iter()
                .map(|(certificate, _header)| {
                    processor.add_certificate(certificate.clone());
                    missing_header_digests.push(certificate.digest);
                })
                .for_each(drop);
        } else {
            round_headers
                .iter()
                .map(|(certificate, header)| {
                    processor.add_certificate(certificate.clone());
                    processor.add_header(header.clone(), certificate.digest)
                })
                .for_each(drop);
        }
        i += 1;
    }

    let processor_missing = processor.get_missing_header_certificate(i - 1, 1);

    for cert in &processor_missing {
        assert!(missing_header_digests.contains(&cert.digest))
    }
}

#[rstest]
fn test_add_block(mut processor: Processor, header: BlockHeader, certificate: Certificate) {
    let digest = certificate.digest;
    let round = header.round;
    processor.add_header(header, digest);
    processor.add_certificate(certificate);
    assert!(processor.dag.contains_key(&round));
    assert!(processor.headers.contains_key(&round));
}

#[rstest]
fn test_add_pending_block(
    keys: Vec<(NodeID, SecretKey)>,
    mut processor: Processor,
    header: BlockHeader,
    many_signed_headers: Vec<SignedBlockHeader>,
) {
    let (id, secret) = &keys[0];
    let parents = many_signed_headers
        .iter()
        .map(|x| (id.clone(), x.digest))
        .collect();
    let round = header.round + 1;
    let blockheader = BlockHeader {
        author: *id,
        round,
        parents,
        metadata: Metadata::default(),
        transactions_digest: WorkersDigests::new(),
        instance_id: header.instance_id,
    };
    let header = SignedBlockHeader::debug_new(blockheader.clone(), &secret).unwrap();
    let digest = header.digest;
    processor.add_header(blockheader, digest);
}
