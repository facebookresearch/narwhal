use super::error::*;
use super::messages::*;
use super::types::*;
use crypto::Digest;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};

/// A task that hanldes the requests to Sync Header/Certs
pub fn run_sync_header_server(
    mut store: Store,
    mut rx: Receiver<(RoundNumber, Digest, NodeID, NodeID)>,
    sending_endpoint: Sender<(RoundNumber, PrimaryMessage)>,
) {
    tokio::spawn(async move {
        while let Some((round, digest, requester, _)) = rx.recv().await {
            if let Ok(Some(data)) = handle_header_request(&mut store, digest).await {
                let msg = PrimaryMessage::SyncHeaderReply(data, requester);

                let _ =
                    sending_endpoint
                        .send((round, msg))
                        .await
                        .map_err(|e| DagError::ChannelError {
                            error: format!("{}", e),
                        });
            }
        }
    });
}

/// A future that reads the Cert and Heder from the DB and returns them.
async fn handle_header_request(
    store: &mut Store,
    digest: Digest,
) -> Result<Option<(Certificate, BlockHeader)>, DagError> {
    if let Some(db_value) =
        store
            .read(digest.0.to_vec())
            .await
            .map_err(|e| DagError::StorageFailure {
                error: format!("{}", e),
            })?
    {
        let record_header: HeaderPrimaryRecord = bincode::deserialize(&db_value[..])?;
        let header = record_header.header;
        let digest_cert_in_store =
            PartialCertificate::make_digest(&digest, header.round, header.author);
        if let Some(db_value) = store
            .read(digest_cert_in_store.0.to_vec())
            .await
            .map_err(|e| DagError::StorageFailure {
                error: format!("{}", e),
            })?
        {
            let certificate: Certificate = bincode::deserialize(&db_value[..])?;
            return Ok(Some((certificate, header)));
        }
    }
    Ok(None)
}
