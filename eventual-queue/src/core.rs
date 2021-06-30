//! The core logic of lamport mutex based on Ricart–Agrawala algorithm.

#![allow(dead_code)]

use crate::common::*;
use crate::error::{EQError, EQResult};
use crate::eventual_queue::ItemSerialNumber;
use crate::network::EQNetwork;
use async_trait::async_trait;
use indexmap::IndexMap;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::HashSet;
use tokio::sync::watch::Receiver;

pub(crate) struct EQCore<N: EQNetwork> {
    id: PeerId,
    num_of_peers: u64,
    num_of_dequeue_replies_granted: u64,
    dequeuing_item_index: ItemSerialNumber,
    dequeue_request_timestamp: Option<Timestamp>,
    enqueue_item_counter: u64,
    map: IndexMap<ItemSerialNumber, u64>,
    deferred_pop_items: HashSet<ItemSerialNumber>,
    state_type: StateType,
    network: Arc<N>,
    rx_api: mpsc::UnboundedReceiver<EQMsg>,
    tx_dequeued_item: watch::Sender<Option<u64>>,
}

impl<N: EQNetwork> EQCore<N> {
    pub fn spawn(
        id: PeerId, num_of_peers: u64, network: Arc<N>, rx_api: mpsc::UnboundedReceiver<EQMsg>,
    ) -> (Receiver<Option<u64>>, JoinHandle<EQResult<()>>) {
        let (tx_dequeued_item, rx_dequeued_item) = watch::channel(None);

        let this: EQCore<N> = Self {
            id,
            num_of_peers,
            num_of_dequeue_replies_granted: 0,
            dequeuing_item_index: ItemSerialNumber(0, 0),
            dequeue_request_timestamp: None,
            enqueue_item_counter: 0,
            map: IndexMap::new(),
            deferred_pop_items: HashSet::new(),
            state_type: StateType::Idle,
            network,
            rx_api,
            tx_dequeued_item,
        };

        (rx_dequeued_item, tokio::spawn(this.main()))
    }

    #[tracing::instrument(level="trace", skip(self), fields(id=self.id))]
    pub async fn main(mut self) -> EQResult<()> {
        tracing::trace!("initializing");
        loop {
            match &self.state_type {
                StateType::Idle => IdleState::new(&mut self).run().await?,
                StateType::PreRequesting => PreRequestingState::new(&mut self).run().await?,
                StateType::Requesting => RequestingState::new(&mut self).run().await?,
                StateType::Pop => PopState::new(&mut self).run().await?,
                StateType::Shutdown => {
                    tracing::trace!("eventual queue core was shutdown");
                    return Ok(());
                }
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn set_current_state(&mut self, target_state: StateType) {
        self.state_type = target_state;
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn enqueue(&mut self, item: u64) -> EQResult<()> {
        let index = ItemSerialNumber(self.id, self.enqueue_item_counter);
        self.enqueue_item_counter += 1;
        self.map.insert(index, item);

        let _ = self
            .network
            .broadcast_enqueue_request(EQMsg::EnqueueRequest { id: self.id, index: index.clone(), item })
            .await;
        tracing::trace!("finish broadcasting enqueue request message");

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, _sender, index, item))]
    pub fn handle_enqueue_request(&mut self, _sender: PeerId, index: ItemSerialNumber, item: u64) -> EQResult<()> {
        if self.deferred_pop_items.contains(&index) {
            self.deferred_pop_items.remove(&index);
        } else {
            self.map.insert(index, item);
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, sender, index))]
    pub async fn handle_pop_request(&mut self, sender: PeerId, index: ItemSerialNumber) -> EQResult<()> {
        if self.map.contains_key(&index) {
            self.map.remove(&index);
        } else {
            self.deferred_pop_items.insert(index);
        }

        self.network
            .reply_pop(sender)
            .await
            .map_err(|_| EQError::EQNetwork(anyhow!("peer_{} failed to reply pop request to peer_{}", self.id, sender)))
    }

    #[tracing::instrument(level = "trace", skip(self, sender))]
    pub async fn handle_dequeue_reply(&mut self, sender: PeerId) -> EQResult<()> {
        self.num_of_dequeue_replies_granted += 1;
        tracing::trace!(
            "got dequeue reply from peer_{}, current number of dequeue replies granted: {}",
            sender,
            self.num_of_dequeue_replies_granted
        );

        if self.num_of_dequeue_replies_granted == self.num_of_peers - 1 {
            self.set_current_state(StateType::Pop);
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn retry_to_dequeue(&mut self) {
        tracing::trace!("return to PRE-REQUESTING state and retry to dequeue another item");
        // Initialize
        self.num_of_dequeue_replies_granted = 0u64;
        self.dequeue_request_timestamp = None;
        self.set_current_state(StateType::PreRequesting);
    }

    #[tracing::instrument(level = "trace", skip(self, err_msg))]
    pub fn reject_to_handle_reply(&mut self, err_msg: String) -> EQResult<()> {
        Err(EQError::NotAllowedToReply(anyhow!(err_msg)))
    }

    #[tracing::instrument(level = "trace", skip(self, self_timestamp, id, timestamp))]
    pub async fn compare_and_reply(&self, self_timestamp: Timestamp, id: PeerId, timestamp: Timestamp) -> EQResult<()> {
        return if timestamp < self_timestamp {
            let _ = self
                .network
                .reply_dequeue(id)
                .await
                .map_err(|_| EQError::EQNetwork(anyhow!("peer_{} failed to reply to peer_{}", self.id, id)));
            Ok(())
        } else {
            Err(EQError::NotAllowedToReply(anyhow!(
                "not allowed to reply to the request with lower priority"
            )))
        };
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// All possible states of a lamport mutex peer.
#[derive(Clone, Copy, Debug)]
pub enum StateType {
    Idle,
    PreRequesting,
    Requesting,
    Pop,
    Shutdown,
}

impl StateType {
    /// Check if currently in idle state.
    pub fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }

    /// Check if currently in pre-requesting state.
    pub fn is_pre_requesting(&self) -> bool {
        matches!(self, Self::PreRequesting)
    }

    /// Check if currently in requesting state.
    pub fn is_requesting(&self) -> bool {
        matches!(self, Self::Requesting)
    }

    /// Check if currently in critical-section state.
    pub fn is_pop(&self) -> bool {
        matches!(self, Self::Pop)
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait State {
    async fn handle_enqueue_request(&mut self, sender: PeerId, index: ItemSerialNumber, item: u64) -> EQResult<()>;

    async fn handle_dequeue_request(&mut self, sender: PeerId, index: ItemSerialNumber, timestamp: zenoh::Timestamp) -> EQResult<()>;

    async fn handle_dequeue_reply(&mut self, sender: PeerId) -> EQResult<()>;

    async fn handle_pop_request(&mut self, sender: PeerId, index: ItemSerialNumber) -> EQResult<()>;

    async fn handle_pop_reply(&mut self, sender: PeerId) -> EQResult<()>;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Volatile state specific to a eventual queue peer in idle state.
struct IdleState<'a, N: EQNetwork> {
    pub(super) core: &'a mut EQCore<N>,
}

impl<'a, N: EQNetwork> IdleState<'a, N> {
    pub(self) fn new(core: &'a mut EQCore<N>) -> Self {
        Self { core }
    }

    #[tracing::instrument(level="trace", skip(self), fields(id=self.core.id, state="idle"))]
    pub(self) async fn run(mut self) -> EQResult<()> {
        tracing::info!("running in IDLE state");

        loop {
            if !self.core.state_type.is_idle() {
                return Ok(());
            }

            if let Some(msg) = self.core.rx_api.recv().await {
                match msg {
                    EQMsg::Enqueue { item } => {
                        let _ = self.core.enqueue(item).await;
                    }
                    EQMsg::Dequeue => {
                        tracing::trace!("transfer to PRE-REQUESTING state");
                        self.core.set_current_state(StateType::PreRequesting);
                    }
                    EQMsg::EnqueueRequest { id, index, item } => {
                        let _ = self.handle_enqueue_request(id, index, item).await;
                    }
                    EQMsg::DequeueRequest { id, index, timestamp } => {
                        let _ = self.handle_dequeue_request(id, index, timestamp).await;
                    }
                    EQMsg::DequeueReply { id } => {
                        let _ = self.handle_dequeue_reply(id).await;
                    }
                    EQMsg::PopRequest { id, index } => {
                        let _ = self.handle_pop_request(id, index).await;
                    }
                    EQMsg::PopReply { id } => {
                        let _ = self.handle_pop_reply(id).await;
                    }
                    EQMsg::Shutdown { tx } => {
                        self.core.set_current_state(StateType::Shutdown);
                        let _ = tx.send(());
                    }
                }
            } else {
                tracing::error!("error occurred when receiving message");
                self.core.set_current_state(StateType::Shutdown);
            }
        }
    }
}

#[async_trait]
impl<'a, N: EQNetwork> State for IdleState<'a, N> {
    async fn handle_enqueue_request(&mut self, sender: PeerId, index: ItemSerialNumber, item: u64) -> EQResult<()> {
        self.core.handle_enqueue_request(sender, index, item)
    }

    #[tracing::instrument(level = "trace", skip(self, sender, _index, _timestamp))]
    async fn handle_dequeue_request(&mut self, sender: PeerId, _index: ItemSerialNumber, _timestamp: zenoh::Timestamp) -> EQResult<()> {
        self.core
            .network
            .reply_dequeue(sender)
            .await
            .map_err(|_| EQError::EQNetwork(anyhow!("peer_{} failed to reply dequeue request to peer_{}", self.core.id, sender)))
    }

    #[tracing::instrument(level = "trace", skip(self, _sender))]
    async fn handle_dequeue_reply(&mut self, _sender: PeerId) -> EQResult<()> {
        self.core
            .reject_to_handle_reply("the dequeue reply action is not allowed in IDLE state".to_string())
    }

    #[tracing::instrument(level = "trace", skip(self, sender, index))]
    async fn handle_pop_request(&mut self, sender: PeerId, index: ItemSerialNumber) -> EQResult<()> {
        self.core.handle_pop_request(sender, index).await
    }

    async fn handle_pop_reply(&mut self, _sender: PeerId) -> EQResult<()> {
        self.core
            .reject_to_handle_reply("the pop reply action is not allowed in IDLE state".to_string())
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Volatile state specific to a eventual queue peer in pre-requesting state.
struct PreRequestingState<'a, N: EQNetwork> {
    pub(super) core: &'a mut EQCore<N>,
    pub(super) pending_dequeue_requests: Vec<EQMsg>,
}

impl<'a, N: EQNetwork> PreRequestingState<'a, N> {
    pub(self) fn new(core: &'a mut EQCore<N>) -> Self {
        Self { core, pending_dequeue_requests: vec![] }
    }

    #[tracing::instrument(level="trace", skip(self), fields(id=self.core.id, state="pre-requesting"))]
    pub(self) async fn run(mut self) -> EQResult<()> {
        tracing::info!("running in PRE-REQUESTING state");

        if self.core.map.is_empty() {
            tracing::trace!("current queue is empty, return nothing");
            let _ = self.core.tx_dequeued_item.send(None);
            tracing::trace!("transfer to IDLE state");
            self.core.set_current_state(StateType::Idle);
        } else {
            let mut rng: StdRng = SeedableRng::from_entropy();
            let random_idx: usize = rng.gen_range(0..self.core.map.len());
            self.core.dequeuing_item_index = *self.core.map.get_index(random_idx).unwrap().0;
            tracing::trace!("try to dequeue item with index: {}", self.core.dequeuing_item_index);

            let _ = self
                .core
                .network
                .broadcast_dequeue_request(self.core.dequeuing_item_index)
                .await;
        }

        loop {
            if !self.core.state_type.is_pre_requesting() {
                return Ok(());
            }

            if let Some(msg) = self.core.rx_api.recv().await {
                match msg {
                    EQMsg::Enqueue { item } => {
                        let _ = self.core.enqueue(item).await;
                    }
                    EQMsg::EnqueueRequest { id, index, item } => {
                        let _ = self.handle_enqueue_request(id, index, item).await;
                    }
                    EQMsg::DequeueRequest { id, index, timestamp } => {
                        if let Err(EQError::ConflictIndex(e)) = self.handle_dequeue_request(id, index, timestamp).await {
                            tracing::error!("{}", e);
                            self.core.retry_to_dequeue();
                            return Ok(());
                        }
                    }
                    EQMsg::DequeueReply { id } => {
                        let _ = self.handle_dequeue_reply(id).await;
                    }
                    EQMsg::PopRequest { id, index } => {
                        if let Err(EQError::DoubleDequeue(e)) = self.handle_pop_request(id, index).await {
                            tracing::error!("{}", e);
                            self.core.retry_to_dequeue();
                            return Ok(());
                        }
                    }
                    EQMsg::PopReply { id } => {
                        let _ = self.handle_pop_reply(id).await;
                    }
                    EQMsg::Shutdown { tx } => {
                        self.core.set_current_state(StateType::Shutdown);
                        let _ = tx.send(());
                    }
                    _ => {}
                }
            } else {
                tracing::error!("error occurred when receiving message");
                self.core.set_current_state(StateType::Shutdown);
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub(self) async fn flush_pending_dequeue_requests(&mut self) -> EQResult<()> {
        let mut has_lower_priority = false;

        for request in &self.pending_dequeue_requests {
            let request_timestamp_ = self.core.dequeue_request_timestamp.clone();
            match request {
                EQMsg::DequeueRequest { id, index: _, timestamp } => {
                    // If this peer got its own request timestamp, start comparing it with the pending
                    // requests to determine the priority.
                    if let Some(request_timestamp) = request_timestamp_ {
                        if let Ok(_) = self
                            .core
                            .compare_and_reply(request_timestamp.clone(), *id, timestamp.clone())
                            .await
                        {
                            tracing::trace!("peer_{} has lower priority compared with peer_{}", self.core.id, *id);
                            has_lower_priority = true;
                        }
                    }
                    // Else, this peer hasn't got its own timestamp, nor had it got all replies yet.
                    else {
                        return Err(EQError::InvalidOperation(anyhow!("not allowed to flush pending requests")));
                    }
                }
                _ => {}
            }
        }
        return if has_lower_priority {
            Err(EQError::ConflictIndex(anyhow!("conflict index with low priority")))
        } else {
            Ok(())
        };
    }
}

#[async_trait]
impl<'a, N: EQNetwork> State for PreRequestingState<'a, N> {
    async fn handle_enqueue_request(&mut self, sender: PeerId, index: ItemSerialNumber, item: u64) -> EQResult<()> {
        self.core.handle_enqueue_request(sender, index, item)
    }

    #[tracing::instrument(level = "trace", skip(self, sender, timestamp))]
    async fn handle_dequeue_request(&mut self, sender: PeerId, index: ItemSerialNumber, timestamp: zenoh::Timestamp) -> EQResult<()> {
        if sender == self.core.id {
            // Record its own request timestamp.
            tracing::trace!("got own request timestamp: {}", timestamp);
            self.core.dequeue_request_timestamp = Some(timestamp);

            // Process other requests after receiving its own request timestamp.
            tracing::trace!("start flushing pending requests with length: {}", self.pending_dequeue_requests.len());
            return match self.flush_pending_dequeue_requests().await {
                Ok(_) => {
                    tracing::trace!("transfer to REQUESTING state");
                    self.core.set_current_state(StateType::Requesting);
                    Ok(())
                }
                Err(_) => Err(EQError::ConflictIndex(anyhow!("conflict index with low priority"))),
            };
        }

        if self.core.dequeuing_item_index == index {
            // Hold other requests temporarily until this peer received its own request timestamp.
            tracing::trace!("push peer_{}'s request to pending requests", sender);
            self.pending_dequeue_requests
                .push(EQMsg::DequeueRequest { id: sender, index, timestamp });
        } else {
            let _ = self
                .core
                .network
                .reply_dequeue(sender)
                .await
                .map_err(|_| EQError::EQNetwork(anyhow!("peer_{} failed to reply dequeue request to peer_{}", self.core.id, sender)));
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, sender))]
    async fn handle_dequeue_reply(&mut self, sender: PeerId) -> EQResult<()> {
        self.core.handle_dequeue_reply(sender).await
    }

    #[tracing::instrument(level = "trace", skip(self, sender, index))]
    async fn handle_pop_request(&mut self, sender: PeerId, index: ItemSerialNumber) -> EQResult<()> {
        let _ = self.core.handle_pop_request(sender, index).await;

        return if self.core.dequeuing_item_index == index {
            Err(EQError::DoubleDequeue(anyhow!("abort the current dequeuing request")))
        } else {
            Ok(())
        };
    }

    async fn handle_pop_reply(&mut self, _sender: PeerId) -> EQResult<()> {
        self.core
            .reject_to_handle_reply("the pop reply action is not allowed in PRE-REQUESTING state".to_string())
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Volatile state specific to a eventual queue peer in requesting state.
struct RequestingState<'a, N: EQNetwork> {
    pub(super) core: &'a mut EQCore<N>,
}

impl<'a, N: EQNetwork> RequestingState<'a, N> {
    pub(self) fn new(core: &'a mut EQCore<N>) -> Self {
        Self { core }
    }

    #[tracing::instrument(level="trace", skip(self), fields(id=self.core.id, state="requesting"))]
    pub(self) async fn run(mut self) -> EQResult<()> {
        tracing::info!("running in REQUESTING state");

        loop {
            if !self.core.state_type.is_requesting() {
                return Ok(());
            }

            if let Some(msg) = self.core.rx_api.recv().await {
                match msg {
                    EQMsg::Enqueue { item } => {
                        let _ = self.core.enqueue(item).await;
                    }
                    EQMsg::EnqueueRequest { id, index, item } => {
                        let _ = self.handle_enqueue_request(id, index, item).await;
                    }
                    EQMsg::DequeueRequest { id, index, timestamp } => {
                        if let Err(EQError::ConflictIndex(e)) = self.handle_dequeue_request(id, index, timestamp).await {
                            tracing::error!("{}", e);
                            self.core.retry_to_dequeue();
                            return Ok(());
                        }
                    }
                    EQMsg::DequeueReply { id } => {
                        let _ = self.handle_dequeue_reply(id).await;
                    }
                    EQMsg::PopRequest { id, index } => {
                        if let Err(EQError::DoubleDequeue(e)) = self.handle_pop_request(id, index).await {
                            tracing::error!("{}", e);
                            self.core.retry_to_dequeue();
                            return Ok(());
                        }
                    }
                    EQMsg::PopReply { id } => {
                        let _ = self.handle_pop_reply(id).await;
                    }
                    EQMsg::Shutdown { tx } => {
                        self.core.set_current_state(StateType::Shutdown);
                        let _ = tx.send(());
                    }
                    _ => {}
                }
            } else {
                tracing::error!("error occurred when receiving message");
                self.core.set_current_state(StateType::Shutdown);
            }
        }
    }
}

#[async_trait]
impl<'a, N: EQNetwork> State for RequestingState<'a, N> {
    async fn handle_enqueue_request(&mut self, sender: PeerId, index: ItemSerialNumber, item: u64) -> EQResult<()> {
        self.core.handle_enqueue_request(sender, index, item)
    }

    #[tracing::instrument(level = "trace", skip(self, sender, index, timestamp))]
    async fn handle_dequeue_request(&mut self, sender: PeerId, index: ItemSerialNumber, timestamp: zenoh::Timestamp) -> EQResult<()> {
        if self.core.dequeuing_item_index == index {
            if let Ok(_) = self
                .core
                .compare_and_reply(self.core.dequeue_request_timestamp.clone().unwrap(), sender, timestamp.clone())
                .await
            {
                return Err(EQError::ConflictIndex(anyhow!("conflict index with low priority")));
            }
        } else {
            let _ = self
                .core
                .network
                .reply_dequeue(sender)
                .await
                .map_err(|_| EQError::EQNetwork(anyhow!("peer_{} failed to reply dequeue request to peer_{}", self.core.id, sender)));
        }

        Ok(())
    }

    async fn handle_dequeue_reply(&mut self, sender: PeerId) -> EQResult<()> {
        self.core.handle_dequeue_reply(sender).await
    }

    async fn handle_pop_request(&mut self, sender: PeerId, index: ItemSerialNumber) -> EQResult<()> {
        let _ = self.core.handle_pop_request(sender, index).await;

        return if self.core.dequeuing_item_index == index {
            Err(EQError::DoubleDequeue(anyhow!("abort the current dequeuing request")))
        } else {
            Ok(())
        };
    }

    async fn handle_pop_reply(&mut self, _sender: PeerId) -> EQResult<()> {
        self.core
            .reject_to_handle_reply("the pop reply action is not allowed in REQUESTING state".to_string())
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Volatile state specific to a eventual queue peer in critical-section state.
struct PopState<'a, N: EQNetwork> {
    pub(super) core: &'a mut EQCore<N>,
    pub(super) num_of_pop_replies_granted: u64,
    pub(super) dequeuing_item: Option<u64>,
}

impl<'a, N: EQNetwork> PopState<'a, N> {
    pub(self) fn new(core: &'a mut EQCore<N>) -> Self {
        let dequeuing_item = *core.map.get(&core.dequeuing_item_index).unwrap();

        Self {
            core,
            num_of_pop_replies_granted: 0,
            dequeuing_item: Some(dequeuing_item),
        }
    }

    #[tracing::instrument(level="trace", skip(self), fields(id=self.core.id, state="pop"))]
    pub(self) async fn run(mut self) -> EQResult<()> {
        tracing::info!("running in POP state");

        let _ = self
            .core
            .network
            .broadcast_pop_request(EQMsg::PopRequest {
                id: self.core.id,
                index: self.core.dequeuing_item_index,
            })
            .await;

        loop {
            if !self.core.state_type.is_pop() {
                return Ok(());
            }

            if let Some(msg) = self.core.rx_api.recv().await {
                match msg {
                    EQMsg::Enqueue { item } => {
                        let _ = self.core.enqueue(item).await;
                    }
                    EQMsg::EnqueueRequest { id, index, item } => {
                        let _ = self.handle_enqueue_request(id, index, item).await;
                    }
                    EQMsg::DequeueRequest { id, index, timestamp } => {
                        let _ = self.handle_dequeue_request(id, index, timestamp).await;
                    }
                    EQMsg::DequeueReply { id } => {
                        let _ = self.handle_dequeue_reply(id).await;
                    }
                    EQMsg::PopRequest { id, index } => {
                        let _ = self.handle_pop_request(id, index).await;
                    }
                    EQMsg::PopReply { id } => {
                        let _ = self.handle_pop_reply(id).await;
                    }
                    EQMsg::Shutdown { tx } => {
                        self.core.set_current_state(StateType::Shutdown);
                        let _ = tx.send(());
                    }
                    _ => {}
                }
            } else {
                tracing::error!("error occurred when receiving message");
                self.core.set_current_state(StateType::Shutdown);
            }
        }
    }
}

#[async_trait]
impl<'a, N: EQNetwork> State for PopState<'a, N> {
    async fn handle_enqueue_request(&mut self, sender: PeerId, index: ItemSerialNumber, item: u64) -> EQResult<()> {
        self.core.handle_enqueue_request(sender, index, item)
    }

    #[tracing::instrument(level = "trace", skip(self, sender, index, _timestamp))]
    async fn handle_dequeue_request(&mut self, sender: PeerId, index: ItemSerialNumber, _timestamp: zenoh::Timestamp) -> EQResult<()> {
        if self.core.dequeuing_item_index != index {
            let _ = self
                .core
                .network
                .reply_dequeue(sender)
                .await
                .map_err(|_| EQError::EQNetwork(anyhow!("peer_{} failed to reply dequeue request to peer_{}", self.core.id, sender)));
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, _sender))]
    async fn handle_dequeue_reply(&mut self, _sender: PeerId) -> EQResult<()> {
        self.core
            .reject_to_handle_reply("the dequeue reply action is not allowed in POP state".to_string())
    }

    #[tracing::instrument(level = "trace", skip(self, sender, index))]
    async fn handle_pop_request(&mut self, sender: PeerId, index: ItemSerialNumber) -> EQResult<()> {
        return if self.core.id == sender && self.core.dequeuing_item_index == index {
            self.core.map.remove(&index);
            Ok(())
        } else {
            self.core.handle_pop_request(sender, index).await
        };
    }

    async fn handle_pop_reply(&mut self, sender: PeerId) -> EQResult<()> {
        self.num_of_pop_replies_granted += 1;
        tracing::trace!(
            "got pop reply from peer_{}, current number of pop replies granted: {}",
            sender,
            self.num_of_pop_replies_granted
        );

        if self.num_of_pop_replies_granted == self.core.num_of_peers - 1 {
            let _ = self.core.tx_dequeued_item.send(self.dequeuing_item);

            // Initialize
            self.core.num_of_dequeue_replies_granted = 0u64;
            self.core.dequeue_request_timestamp = None;
            tracing::trace!("transfer to IDLE state");
            self.core.set_current_state(StateType::Idle);
        }

        Ok(())
    }
}
