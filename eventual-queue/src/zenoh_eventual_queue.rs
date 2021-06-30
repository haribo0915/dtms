use crate::common::*;
use crate::eventual_queue::EventualQueue;
use crate::network::{EQNetwork, ZenohNetwork};

pub struct ZenohEventualQueue {
    eventual_queue: EventualQueue<ZenohNetwork>,
    zenoh_network: Arc<ZenohNetwork>,
}

impl ZenohEventualQueue {
    pub async fn new(id: PeerId, num_of_peers: u64) -> Self {
        let workspace_prefix: Arc<zenoh::Path> = Arc::new(zenoh::Path::try_from("/eventual_queue/").unwrap());

        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let tx_api = Arc::new(tx_api);

        let zenoh_network = Arc::new(ZenohNetwork::new(id, workspace_prefix.clone(), tx_api.clone()).await);
        let eventual_queue = EventualQueue::new(id, num_of_peers, zenoh_network.clone(), tx_api.clone(), rx_api);

        Self { eventual_queue, zenoh_network }
    }

    pub async fn enqueue(&self, item: u64) {
        self.eventual_queue.enqueue(item).await
    }

    pub async fn dequeue(&self) -> Option<u64> {
        self.eventual_queue.dequeue().await
    }

    pub async fn shutdown_all(&self) {
        self.zenoh_network.shutdown().await;
        self.eventual_queue.shutdown().await;
    }

    pub fn shutdown(&self) {
        futures::executor::block_on(self.shutdown_all());
    }
}
