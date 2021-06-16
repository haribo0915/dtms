//! The lamport mutex network interface.

use crate::common::*;
use async_trait::async_trait;
use futures::StreamExt;

#[async_trait]
pub trait LamportMutexNetwork: Send + Sync + 'static {
    async fn broadcast(&self) -> Result<()>;

    async fn reply(&self, target: PeerId, reply_msg: LamportMutexMsg) -> Result<()>;

    async fn shutdown(&self);
}

pub struct ZenohNetwork {
    id: PeerId,
    zenoh: Arc<Zenoh>,
    workspace_prefix: Arc<zenoh::Path>,
    tx_shutdown_network_core: mpsc::Sender<()>,
    rx_is_network_core_shutdown: watch::Receiver<()>,
    _zenoh_network_core_handle: JoinHandle<Result<()>>,
}

impl ZenohNetwork {
    pub async fn new(id: PeerId, workspace_prefix: Arc<zenoh::Path>, tx_api: Arc<mpsc::UnboundedSender<LamportMutexMsg>>) -> Self {
        let mut config = zenoh::ConfigProperties::default();
        config.insert(zenoh::net::config::ZN_ADD_TIMESTAMP_KEY, "true".to_string());
        config.insert(zenoh::net::config::ZN_PEER_KEY, "tcp/localhost:7447".to_string());

        let zenoh = Arc::new(Zenoh::new(config.clone()).await.unwrap());

        // Transmit the shutdown message to network core.
        let (tx_shutdown_network_core, rx_shutdown_network_core) = mpsc::channel(1);

        // Notify network once the core was shutdown successfully.
        let (tx_is_network_core_shutdown, rx_is_network_core_shutdown) = watch::channel(());

        let _zenoh_network_core_handle = ZenohNetworkCore::spawn(
            id,
            workspace_prefix.clone(),
            tx_api.clone(),
            rx_shutdown_network_core,
            tx_is_network_core_shutdown,
        )
        .await;

        Self {
            id,
            zenoh,
            workspace_prefix,
            tx_shutdown_network_core,
            rx_is_network_core_shutdown,
            _zenoh_network_core_handle,
        }
    }
}

#[async_trait]
impl LamportMutexNetwork for ZenohNetwork {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn broadcast(&self) -> Result<()> {
        // Create zenoh workspace with prefix.
        let zenoh = self.zenoh.clone();
        let key: zenoh::Path = format!("{}", self.workspace_prefix).try_into().unwrap();
        let workspace = zenoh.workspace(Some(key)).await?;

        // Put a request to the key: /{workspace_prefix}/request/{from}.
        let key: zenoh::Path = format!("request/{}", self.id).try_into().unwrap();
        tracing::trace!("peer_{} broadcast lock request", self.id);
        workspace.put(&key, "lock_request".into()).await?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, target, reply_msg))]
    async fn reply(&self, target: PeerId, reply_msg: LamportMutexMsg) -> Result<()> {
        // Put a reply to the key: /{workspace_prefix}/reply/{to}/{from},
        if let LamportMutexMsg::Reply { .. } = reply_msg {
            // Create zenoh workspace with prefix.
            let zenoh = self.zenoh.clone();
            let key: zenoh::Path = format!("{}", self.workspace_prefix).try_into().unwrap();
            let workspace = zenoh.workspace(Some(key)).await?;

            let key: zenoh::Path = format!("reply/{}/{}", target, self.id).try_into().unwrap();
            let _ = workspace.put(&key, "lock_reply".into()).await?;
            tracing::trace!("peer_{} put lock reply to peer_{}", self.id, target);

            Ok(())
        } else {
            Err(anyhow!("invalid reply message"))
        }
    }

    async fn shutdown(&self) {
        let _ = self.tx_shutdown_network_core.send(()).await;
        let _ = self.rx_is_network_core_shutdown.clone().changed().await;
    }
}

pub struct ZenohNetworkCore {
    id: PeerId,
    zenoh: Zenoh,
    workspace_prefix: Arc<zenoh::Path>,
    tx_api: Arc<mpsc::UnboundedSender<LamportMutexMsg>>,
    rx_shutdown_network_core: mpsc::Receiver<()>,
    tx_is_network_core_shutdown: watch::Sender<()>,
}

impl ZenohNetworkCore {
    pub async fn spawn(
        id: PeerId, workspace_prefix: Arc<zenoh::Path>, tx_api: Arc<mpsc::UnboundedSender<LamportMutexMsg>>,
        rx_shutdown_network_core: mpsc::Receiver<()>, tx_is_network_core_shutdown: watch::Sender<()>,
    ) -> JoinHandle<Result<()>> {
        let mut config = zenoh::ConfigProperties::default();
        config.insert(zenoh::net::config::ZN_ADD_TIMESTAMP_KEY, "true".to_string());
        config.insert(zenoh::net::config::ZN_PEER_KEY, "tcp/localhost:7447".to_string());

        let zenoh = Zenoh::new(config.clone()).await.unwrap();

        let this = Self {
            id,
            zenoh,
            workspace_prefix,
            tx_api,
            rx_shutdown_network_core,
            tx_is_network_core_shutdown,
        };

        tokio::spawn(this.main())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(id=self.id))]
    pub async fn main(mut self) -> Result<()> {
        // Create zenoh workspace with prefix.
        let key: zenoh::Path = format!("{}", self.workspace_prefix).try_into().unwrap();
        let workspace = self.zenoh.workspace(Some(key)).await?;

        // Subscribe to the key: /{workspace_prefix}/request/*.
        let key: zenoh::Selector = format!("request/*").try_into().unwrap();
        let mut lock_request_stream = workspace.subscribe(&key).await.unwrap();

        // Subscribe to the key: /{workspace_prefix}/reply/{to}/*.
        let key: zenoh::Selector = format!("reply/{}/*", self.id).try_into().unwrap();
        let mut lock_reply_stream = workspace.subscribe(&key).await.unwrap();

        loop {
            tokio::select! {
                Some(change) = lock_request_stream.next() => {
                    tracing::trace!("request change path: {}, timestamp: {}", change.path, change.timestamp.clone());
                    let peer_id: PeerId = change.path.last_segment().parse().unwrap();
                    let timestamp: Timestamp = change.timestamp;

                    let _ = self.tx_api.send(LamportMutexMsg::Request {id: peer_id, timestamp});
                }
                Some(change) = lock_reply_stream.next() => {
                    let peer_id: PeerId = change.path.last_segment().parse().unwrap();
                    let timestamp: Timestamp = change.timestamp;

                    let _ = self.tx_api.send(LamportMutexMsg::Reply {id: peer_id, timestamp});
                }
                Some(_) = self.rx_shutdown_network_core.recv() => {
                    tracing::trace!("zenoh network core was shutdown");
                    break;
                }
                else => {}
            }
        }

        lock_request_stream.close().await.unwrap();
        tracing::trace!("lock request stream was closed");

        lock_reply_stream.close().await.unwrap();
        tracing::trace!("lock reply stream was closed");

        self.zenoh.close().await.unwrap();
        tracing::trace!("zenoh client was closed");

        let _ = self.tx_is_network_core_shutdown.send(());

        Ok(())
    }
}
