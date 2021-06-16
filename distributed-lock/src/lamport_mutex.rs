//! Lamport mutex.

use crate::common::*;
use crate::core::LamportMutexCore;
use crate::error::LamportMutexError;
use crate::network::LamportMutexNetwork;

pub struct LamportMutex<N: LamportMutexNetwork> {
    id: PeerId,
    tx_api: Arc<mpsc::UnboundedSender<LamportMutexMsg>>,
    rx_lock: watch::Receiver<()>,
    _lamport_mutex_join_handle: JoinHandle<Result<(), LamportMutexError>>,
    marker_n: std::marker::PhantomData<N>,
}

impl<N: LamportMutexNetwork> LamportMutex<N> {
    pub fn new(
        id: PeerId, num_of_peers: u64, network: Arc<N>, tx_api: Arc<mpsc::UnboundedSender<LamportMutexMsg>>,
        rx_api: mpsc::UnboundedReceiver<LamportMutexMsg>,
    ) -> LamportMutex<N> {
        let (rx_lock, _lamport_mutex_join_handle) = LamportMutexCore::spawn(id, num_of_peers, network, rx_api);

        Self {
            id,
            tx_api,
            rx_lock,
            _lamport_mutex_join_handle,
            marker_n: std::marker::PhantomData,
        }
    }

    #[tracing::instrument(level="trace", skip(self), fields(id=self.id))]
    pub async fn lock(&self) -> LamportMutexGuard<'_, N> {
        let _ = self.tx_api.send(LamportMutexMsg::Lock);
        let _ = self.rx_lock.clone().changed().await;

        LamportMutexGuard { lamport_mutex: self }
    }

    pub async fn unlock(&self) {
        let (tx, rx) = oneshot::channel();

        let _ = self.tx_api.send(LamportMutexMsg::Release { tx });
        let _ = rx.await;
    }

    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();

        let _ = self.tx_api.send(LamportMutexMsg::Shutdown { tx });
        let _ = rx.await;
    }
}

pub struct LamportMutexGuard<'a, N: LamportMutexNetwork> {
    lamport_mutex: &'a LamportMutex<N>,
}

impl<'a, N: LamportMutexNetwork> Drop for LamportMutexGuard<'a, N> {
    fn drop(&mut self) {
        futures::executor::block_on(self.lamport_mutex.unlock());
    }
}

pub enum LamportMutexMsg {
    Request { id: PeerId, timestamp: zenoh::Timestamp },
    Reply { id: PeerId, timestamp: zenoh::Timestamp },
    Lock,
    Release { tx: oneshot::Sender<Result<(), LamportMutexError>> },
    Shutdown { tx: oneshot::Sender<()> },
}
