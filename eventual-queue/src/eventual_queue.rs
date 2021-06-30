//! Eventual queue.

use crate::common::*;
use crate::core::EQCore;
use crate::error::EQError;
use crate::network::EQNetwork;
use std::fmt;

pub struct EventualQueue<N: EQNetwork> {
    id: PeerId,
    tx_api: Arc<mpsc::UnboundedSender<EQMsg>>,
    rx_dequeued_item: watch::Receiver<Option<u64>>,
    _eq_join_handle: JoinHandle<Result<(), EQError>>,
    marker_n: std::marker::PhantomData<N>,
}

impl<N: EQNetwork> EventualQueue<N> {
    pub fn new(
        id: PeerId, num_of_peers: u64, network: Arc<N>, tx_api: Arc<mpsc::UnboundedSender<EQMsg>>, rx_api: mpsc::UnboundedReceiver<EQMsg>,
    ) -> EventualQueue<N> {
        let (rx_dequeued_item, _eq_join_handle) = EQCore::spawn(id, num_of_peers, network, rx_api);

        Self {
            id,
            tx_api,
            rx_dequeued_item,
            _eq_join_handle,
            marker_n: std::marker::PhantomData,
        }
    }

    #[tracing::instrument(level="trace", skip(self), fields(id=self.id))]
    pub async fn enqueue(&self, item: u64) {
        let _ = self.tx_api.send(EQMsg::Enqueue { item });
    }

    pub async fn dequeue(&self) -> Option<u64> {
        let _ = self.tx_api.send(EQMsg::Dequeue);
        let _ = self.rx_dequeued_item.clone().changed().await;

        *self.rx_dequeued_item.borrow()
    }

    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();

        let _ = self.tx_api.send(EQMsg::Shutdown { tx });
        let _ = rx.await;
    }
}

pub enum EQMsg {
    Enqueue {
        item: u64,
    },
    Dequeue,
    EnqueueRequest {
        id: PeerId,
        index: ItemSerialNumber,
        item: u64,
    },
    DequeueRequest {
        id: PeerId,
        index: ItemSerialNumber,
        timestamp: zenoh::Timestamp,
    },
    DequeueReply {
        id: PeerId,
    },
    PopRequest {
        id: PeerId,
        index: ItemSerialNumber,
    },
    PopReply {
        id: PeerId,
    },
    Shutdown {
        tx: oneshot::Sender<()>,
    },
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ItemSerialNumber(pub PeerId, pub u64);

impl fmt::Display for ItemSerialNumber {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {})", self.0, self.1)
    }
}
