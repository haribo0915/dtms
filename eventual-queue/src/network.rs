//! The eventual queue network interface.

use crate::common::*;
use crate::eventual_queue::ItemSerialNumber;
use async_trait::async_trait;
use futures::StreamExt;
use std::fmt;
use zenoh::Value;

#[async_trait]
pub trait EQNetwork: Send + Sync + 'static {
    async fn broadcast_enqueue_request(&self, enqueue_req: EQMsg) -> Result<()>;

    async fn broadcast_dequeue_request(&self, index: ItemSerialNumber) -> Result<()>;

    async fn broadcast_pop_request(&self, pop_req: EQMsg) -> Result<()>;

    async fn reply_dequeue(&self, target: PeerId) -> Result<()>;

    async fn reply_pop(&self, target: PeerId) -> Result<()>;

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
    pub async fn new(id: PeerId, workspace_prefix: Arc<zenoh::Path>, tx_api: Arc<mpsc::UnboundedSender<EQMsg>>) -> Self {
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
impl EQNetwork for ZenohNetwork {
    async fn broadcast_enqueue_request(&self, enqueue_req: EQMsg) -> Result<()> {
        if let EQMsg::EnqueueRequest { id: _, index, item } = enqueue_req {
            let zenoh = self.zenoh.clone();
            let key: zenoh::Path = format!("{}", self.workspace_prefix).try_into().unwrap();
            let workspace = zenoh.workspace(Some(key)).await?;

            let key: zenoh::Path = format!("enqueue/request/{}", self.id).try_into().unwrap();
            let enqueue_msg: EnqueueMsg = EnqueueMsg { index, item };
            // tracing::trace!("peer_{} broadcast enqueue request", self.id);
            workspace
                .put(&key, Value::Json(serde_json::to_string(&enqueue_msg).unwrap().into()))
                .await?;
        }

        Ok(())
    }

    async fn broadcast_dequeue_request(&self, index: ItemSerialNumber) -> Result<()> {
        let zenoh = self.zenoh.clone();
        let key: zenoh::Path = format!("{}", self.workspace_prefix).try_into().unwrap();
        let workspace = zenoh.workspace(Some(key)).await?;

        let key: zenoh::Path = format!("dequeue/request/{}", self.id).try_into().unwrap();
        // tracing::trace!("peer_{} broadcast dequeue request", self.id);
        workspace
            .put(&key, Value::Json(serde_json::to_string(&index).unwrap().into()))
            .await?;

        Ok(())
    }

    async fn broadcast_pop_request(&self, pop_req: EQMsg) -> Result<()> {
        if let EQMsg::PopRequest { id: _, index } = pop_req {
            let zenoh = self.zenoh.clone();
            let key: zenoh::Path = format!("{}", self.workspace_prefix).try_into().unwrap();
            let workspace = zenoh.workspace(Some(key)).await?;

            let key: zenoh::Path = format!("pop/request/{}", self.id).try_into().unwrap();
            // tracing::trace!("peer_{} broadcast pop request", self.id);
            workspace
                .put(&key, Value::Json(serde_json::to_string(&index).unwrap().into()))
                .await?;
        }

        Ok(())
    }

    async fn reply_dequeue(&self, target: PeerId) -> Result<()> {
        let zenoh = self.zenoh.clone();
        let key: zenoh::Path = format!("{}", self.workspace_prefix).try_into().unwrap();
        let workspace = zenoh.workspace(Some(key)).await?;

        let key: zenoh::Path = format!("dequeue/reply/{}/{}", target, self.id).try_into().unwrap();
        let _ = workspace.put(&key, "dequeue_reply".into()).await?;
        // tracing::trace!("peer_{} put dequeue reply to peer_{}", self.id, target);

        Ok(())
    }

    async fn reply_pop(&self, target: PeerId) -> Result<()> {
        let zenoh = self.zenoh.clone();
        let key: zenoh::Path = format!("{}", self.workspace_prefix).try_into().unwrap();
        let workspace = zenoh.workspace(Some(key)).await?;

        let key: zenoh::Path = format!("pop/reply/{}/{}", target, self.id).try_into().unwrap();
        let _ = workspace.put(&key, "pop_reply".into()).await?;
        // tracing::trace!("peer_{} put pop reply to peer_{}", self.id, target);

        Ok(())
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
    tx_api: Arc<mpsc::UnboundedSender<EQMsg>>,
    rx_shutdown_network_core: mpsc::Receiver<()>,
    tx_is_network_core_shutdown: watch::Sender<()>,
}

impl ZenohNetworkCore {
    pub async fn spawn(
        id: PeerId, workspace_prefix: Arc<zenoh::Path>, tx_api: Arc<mpsc::UnboundedSender<EQMsg>>, rx_shutdown_network_core: mpsc::Receiver<()>,
        tx_is_network_core_shutdown: watch::Sender<()>,
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
        let key: zenoh::Path = format!("{}", self.workspace_prefix).try_into().unwrap();
        let workspace = self.zenoh.workspace(Some(key)).await?;

        let key: zenoh::Selector = format!("enqueue/request/*").try_into().unwrap();
        let mut enqueue_request_stream = workspace.subscribe(&key).await.unwrap();

        let key: zenoh::Selector = format!("dequeue/request/*").try_into().unwrap();
        let mut dequeue_request_stream = workspace.subscribe(&key).await.unwrap();

        let key: zenoh::Selector = format!("pop/request/*").try_into().unwrap();
        let mut pop_request_stream = workspace.subscribe(&key).await.unwrap();

        let key: zenoh::Selector = format!("dequeue/reply/{}/*", self.id).try_into().unwrap();
        let mut dequeue_reply_stream = workspace.subscribe(&key).await.unwrap();

        let key: zenoh::Selector = format!("pop/reply/{}/*", self.id).try_into().unwrap();
        let mut pop_reply_stream = workspace.subscribe(&key).await.unwrap();

        loop {
            tokio::select! {
                Some(change) = enqueue_request_stream.next() => {
                    if let Some(Value::Json(json_string)) = change.value {
                        let peer_id: PeerId = change.path.last_segment().parse().unwrap();
                        // Deserialize the request.
                        let enqueue_msg: EnqueueMsg = serde_json::from_str(&json_string).unwrap();

                        // tracing::trace!("enqueue request change path: {}, {}", change.path, enqueue_msg);

                        let _ = self.tx_api.send(EQMsg::EnqueueRequest {id: peer_id, index: enqueue_msg.index, item: enqueue_msg.item});
                    }
                }
                Some(change) = dequeue_request_stream.next() => {
                    // tracing::trace!("dequeue request change path: {}, timestamp: {}", change.path, change.timestamp.clone());
                    if let Some(Value::Json(json_string)) = change.value {
                        let peer_id: PeerId = change.path.last_segment().parse().unwrap();
                        let timestamp: Timestamp = change.timestamp;
                        // Deserialize the request.
                        let index: ItemSerialNumber = serde_json::from_str(&json_string).unwrap();

                        let _ = self.tx_api.send(EQMsg::DequeueRequest {id: peer_id, index, timestamp});
                    }
                }
                Some(change) = pop_request_stream.next() => {
                    // tracing::trace!("pop request change path: {}", change.path);
                    if let Some(Value::Json(json_string)) = change.value {
                        let peer_id: PeerId = change.path.last_segment().parse().unwrap();
                        // Deserialize the request.
                        let index: ItemSerialNumber = serde_json::from_str(&json_string).unwrap();

                        let _ = self.tx_api.send(EQMsg::PopRequest {id: peer_id, index});
                    }
                }
                Some(change) = dequeue_reply_stream.next() => {
                    let peer_id: PeerId = change.path.last_segment().parse().unwrap();
                    let _ = self.tx_api.send(EQMsg::DequeueReply {id: peer_id});
                }
                Some(change) = pop_reply_stream.next() => {
                    let peer_id: PeerId = change.path.last_segment().parse().unwrap();
                    let _ = self.tx_api.send(EQMsg::PopReply {id: peer_id});
                }
                Some(_) = self.rx_shutdown_network_core.recv() => {
                    tracing::trace!("zenoh network core was shutdown");
                    break;
                }
                else => {}
            }
        }

        enqueue_request_stream.close().await.unwrap();
        tracing::trace!("enqueue request stream was closed");

        dequeue_request_stream.close().await.unwrap();
        tracing::trace!("dequeue request stream was closed");

        pop_request_stream.close().await.unwrap();
        tracing::trace!("pop request stream was closed");

        dequeue_reply_stream.close().await.unwrap();
        tracing::trace!("dequeue reply stream was closed");

        pop_reply_stream.close().await.unwrap();
        tracing::trace!("pop reply stream was closed");

        self.zenoh.close().await.unwrap();
        tracing::trace!("zenoh client was closed");

        let _ = self.tx_is_network_core_shutdown.send(());

        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct EnqueueMsg {
    index: ItemSerialNumber,
    item: u64,
}

impl fmt::Display for EnqueueMsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "index: {}, item:{}", self.index, self.item)
    }
}
