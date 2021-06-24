use super::messages::*;
use super::types::*;
use crate::committee::Committee;
use crypto::Digest;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

pub struct Processor {
    /// Holds all headers.
    pub headers: HashMap<RoundNumber, HashMap<Digest, BlockHeader>>,
    /// Holds all certificates forming the dag.
    pub dag: HashMap<RoundNumber, HashMap<Digest, Certificate>>,
    /// Committee of this instance.
    pub committee: Committee,
    // Last Leader that had enough support
}

impl Processor {
    /// Make a new processor.
    pub fn new(committee: Committee) -> Self {
        let genesis = Certificate::genesis(&committee);

        let dag: HashMap<_, _> = [(
            0,
            genesis.into_iter().map(|x| (x.digest.clone(), x)).collect(),
        )]
        .iter()
        .cloned()
        .collect();

        Self {
            headers: HashMap::new(),
            dag,
            committee,
        }
    }

    /// Add a correct block header to the storage.
    pub fn add_header(&mut self, header: BlockHeader, digest: Digest) {
        self.headers
            .entry(header.round)
            .or_insert_with(HashMap::new)
            .insert(digest, header);
    }

    pub fn contains_header_by_digest_round(&self, digest: &Digest, round: RoundNumber) -> bool {
        if let Some(headers) = self.headers.get(&round) {
            return headers.contains_key(&digest);
        }
        false
    }

    /// Add a correct certificate to the storage.
    pub fn add_certificate(&mut self, certificate: Certificate) {
        self.dag
            .entry(certificate.round)
            .or_insert_with(HashMap::new)
            .insert(certificate.digest.clone(), certificate);
    }

    pub fn get_certificates(
        &self,
        round: RoundNumber,
    ) -> Option<&HashMap<Digest, Certificate, RandomState>> {
        self.dag.get(&round)
    }

    pub fn get_headers(
        &self,
        round: RoundNumber,
    ) -> Option<&HashMap<Digest, BlockHeader, RandomState>> {
        self.headers.get(&round)
    }

    pub fn contains_certificate(&self, certificate: &Certificate) -> bool {
        let round = certificate.round;
        if let Some(certificates) = self.dag.get(&round) {
            return certificates.contains_key(&certificate.digest);
        }
        false
    }

    pub fn contains_certificate_by_digest_round(
        &self,
        digest: &Digest,
        round: RoundNumber,
    ) -> bool {
        if let Some(certificates) = self.dag.get(&round) {
            return certificates.contains_key(&digest);
        }
        false
    }

    pub fn get_header(&self, digest: &Digest, round: RoundNumber) -> Option<&BlockHeader> {
        if let Some(headers) = self.headers.get(&round) {
            return headers.get(digest);
        };
        None
    }

    pub fn clean_headers(
        &mut self,
        committed_sequence: Vec<(BlockHeader, Digest)>,
        last_committed_round: RoundNumber,
        my_id: NodeID,
    ) -> Vec<WorkersDigests> {
        for (header, digest) in committed_sequence {
            self.cleanup_single(header.round, &digest);
        }
        self.cleanup_and_reuse_digests(last_committed_round, my_id)
    }

    /// Remove a specific header.
    pub fn cleanup_single(&mut self, round: RoundNumber, digest: &Digest) {
        if let Some(headers) = self.headers.get_mut(&round) {
            let _ = headers.remove(digest);
        }
    }

    /// Remove all headers older than a specific round and return the txs digests authored by the specofied node id.
    pub fn cleanup_and_reuse_digests(
        &mut self,
        round: RoundNumber,
        id: NodeID,
    ) -> Vec<WorkersDigests> {
        let digests = self
            .headers
            .iter()
            .filter(|(k, _)| **k <= round)
            .map(|(_, v)| {
                v.values()
                    .filter(|x| x.author == id)
                    .map(|x| x.transactions_digest.clone())
                    .collect::<Vec<_>>()
            })
            .flatten()
            .collect();
        self.headers.retain(|&r, _| r > round);
        digests
    }
}
