//! Lamport mutex with zenoh network as transport layer.

use crate::common::*;
use crate::lamport_mutex::{LamportMutex, LamportMutexGuard};
use crate::network::{LamportMutexNetwork, ZenohNetwork};

pub struct ZenohLamportMutex {
    lamport_mutex: LamportMutex<ZenohNetwork>,
    zenoh_network: Arc<ZenohNetwork>,
}

impl ZenohLamportMutex {
    pub async fn new(id: PeerId, num_of_peers: u64) -> ZenohLamportMutex {
        let workspace_prefix: Arc<zenoh::Path> = Arc::new(zenoh::Path::try_from("/lamport_mutex/").unwrap());

        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let tx_api = Arc::new(tx_api);

        let zenoh_network = Arc::new(ZenohNetwork::new(id, workspace_prefix.clone(), tx_api.clone()).await);
        let lamport_mutex = LamportMutex::new(id, num_of_peers, zenoh_network.clone(), tx_api.clone(), rx_api);

        Self { lamport_mutex, zenoh_network }
    }

    pub async fn lock(&self) -> LamportMutexGuard<'_, ZenohNetwork> {
        self.lamport_mutex.lock().await
    }

    pub async fn shutdown_all(&self) {
        self.zenoh_network.shutdown().await;
        self.lamport_mutex.shutdown().await;
    }

    pub fn shutdown(&self) {
        futures::executor::block_on(self.shutdown_all());
    }
}
