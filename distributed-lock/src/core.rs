//! The core logic of lamport mutex based on Ricart–Agrawala algorithm.

#![allow(dead_code)]

use crate::common::*;
use crate::error::{LamportMutexError, LamportMutexResult};
use crate::network::LamportMutexNetwork;
use async_trait::async_trait;
use tokio::sync::watch::Receiver;

pub(crate) struct LamportMutexCore<N: LamportMutexNetwork> {
    id: PeerId,
    num_of_peers: u64,
    num_of_replies_granted: u64,
    request_timestamp: Option<Timestamp>,
    deferred_replies: Vec<LamportMutexMsg>,
    state_type: StateType,
    network: Arc<N>,
    rx_api: mpsc::UnboundedReceiver<LamportMutexMsg>,
    tx_lock: watch::Sender<()>,
}

impl<N: LamportMutexNetwork> LamportMutexCore<N> {
    pub fn spawn(
        id: PeerId, num_of_peers: u64, network: Arc<N>, rx_api: mpsc::UnboundedReceiver<LamportMutexMsg>,
    ) -> (Receiver<()>, JoinHandle<LamportMutexResult<()>>) {
        let (tx_lock, rx_lock) = watch::channel(());

        let this: LamportMutexCore<N> = Self {
            id,
            num_of_peers,
            num_of_replies_granted: 0,
            request_timestamp: None,
            deferred_replies: vec![],
            state_type: StateType::Idle,
            network,
            rx_api,
            tx_lock,
        };

        (rx_lock, tokio::spawn(this.main()))
    }

    #[tracing::instrument(level="trace", skip(self), fields(id=self.id))]
    pub async fn main(mut self) -> LamportMutexResult<()> {
        tracing::trace!("initializing");
        loop {
            match &self.state_type {
                StateType::Idle => IdleState::new(&mut self).run().await?,
                StateType::PreRequesting => PreRequestingState::new(&mut self).run().await?,
                StateType::Requesting => RequestingState::new(&mut self).run().await?,
                StateType::CriticalSection => CriticalSectionState::new(&mut self).run().await?,
                StateType::Shutdown => {
                    tracing::trace!("lamport mutex core was shutdown");
                    return Ok(());
                }
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn set_current_state(&mut self, target_state: StateType) {
        self.state_type = target_state;
    }

    #[tracing::instrument(level = "trace", skip(self, self_timestamp, id, timestamp))]
    pub async fn compare_and_reply(&self, self_timestamp: Timestamp, id: PeerId, timestamp: Timestamp) -> LamportMutexResult<()> {
        tracing::trace!("try to compare timestamp: {} with peer_{}: {}", self_timestamp, id, timestamp);
        if timestamp < self_timestamp {
            // The timestamp wrapped in the reply is irrelevant to the algorithm.
            self.network
                .reply(id, LamportMutexMsg::Reply { id: self.id, timestamp: self_timestamp })
                .await
                .map_err(|_| LamportMutexError::LamportMutexNetwork(anyhow!("peer_{} failed to reply to peer_{}", self.id, id)))
        } else {
            Err(LamportMutexError::NotAllowedToReply(anyhow!(
                "not allowed to reply to the request with lower priority"
            )))
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// All possible states of a lamport mutex peer.
#[derive(Clone, Copy, Debug)]
pub enum StateType {
    Idle,
    PreRequesting,
    Requesting,
    CriticalSection,
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
    pub fn is_critical_section(&self) -> bool {
        matches!(self, Self::CriticalSection)
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait State {
    async fn handle_lock_request(&mut self, sender: PeerId, timestamp: zenoh::Timestamp) -> LamportMutexResult<()>;

    async fn handle_lock_reply(&mut self, sender: PeerId, timestamp: zenoh::Timestamp) -> LamportMutexResult<()>;

    async fn handle_lock_release(&mut self) -> LamportMutexResult<()>;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Volatile state specific to a lamport mutex peer in idle state.
struct IdleState<'a, N: LamportMutexNetwork> {
    pub(super) core: &'a mut LamportMutexCore<N>,
}

impl<'a, N: LamportMutexNetwork> IdleState<'a, N> {
    pub(self) fn new(core: &'a mut LamportMutexCore<N>) -> Self {
        Self { core }
    }

    #[tracing::instrument(level="trace", skip(self), fields(id=self.core.id, state="idle"))]
    pub(self) async fn run(mut self) -> LamportMutexResult<()> {
        tracing::trace!("running in IDLE state");
        loop {
            if !self.core.state_type.is_idle() {
                return Ok(());
            }

            if let Some(msg) = self.core.rx_api.recv().await {
                match msg {
                    LamportMutexMsg::Request { id, timestamp } => {
                        let _ = self.handle_lock_request(id, timestamp).await;
                    }
                    LamportMutexMsg::Reply { id, timestamp } => {
                        let _ = self.handle_lock_reply(id, timestamp).await;
                    }
                    LamportMutexMsg::Lock => {
                        tracing::trace!("transfer to PRE-REQUESTING state");
                        self.core.set_current_state(StateType::PreRequesting);
                    }
                    LamportMutexMsg::Release { .. } => {
                        let _ = self.handle_lock_release().await;
                    }
                    LamportMutexMsg::Shutdown { tx } => {
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
impl<'a, N: LamportMutexNetwork> State for IdleState<'a, N> {
    #[tracing::instrument(level = "trace", skip(self, sender, timestamp))]
    async fn handle_lock_request(&mut self, sender: PeerId, timestamp: zenoh::Timestamp) -> LamportMutexResult<()> {
        // The timestamp wrapped in the reply is irrelevant to the algorithm.
        self.core
            .network
            .reply(sender, LamportMutexMsg::Reply { id: self.core.id, timestamp })
            .await
            .map_err(|_| LamportMutexError::LamportMutexNetwork(anyhow!("peer_{} failed to reply to peer_{}", self.core.id, sender)))
    }

    #[tracing::instrument(level = "trace", skip(self, _sender, _timestamp))]
    async fn handle_lock_reply(&mut self, _sender: PeerId, _timestamp: zenoh::Timestamp) -> LamportMutexResult<()> {
        let err_msg = "the request action is not allowed in IDLE state";
        tracing::error!("{}", err_msg);
        Err(LamportMutexError::NotAllowedToReply(anyhow!(err_msg)))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_lock_release(&mut self) -> LamportMutexResult<()> {
        let err_msg = "the release action is not allowed in IDLE state";
        tracing::error!("{}", err_msg);
        Err(LamportMutexError::NotAllowedToRelease(anyhow!(err_msg)))
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Volatile state specific to a lamport mutex peer in pre-requesting state.
struct PreRequestingState<'a, N: LamportMutexNetwork> {
    pub(super) core: &'a mut LamportMutexCore<N>,
    pub(super) pending_requests: Vec<LamportMutexMsg>,
}

impl<'a, N: LamportMutexNetwork> PreRequestingState<'a, N> {
    pub(self) fn new(core: &'a mut LamportMutexCore<N>) -> Self {
        Self { core, pending_requests: vec![] }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub(self) async fn run(mut self) -> LamportMutexResult<()> {
        tracing::trace!("running in PRE-REQUESTING state");
        let _ = self.core.network.broadcast().await;
        tracing::trace!("finish broadcasting request message");

        loop {
            if !self.core.state_type.is_pre_requesting() {
                return Ok(());
            }

            if let Some(msg) = self.core.rx_api.recv().await {
                match msg {
                    LamportMutexMsg::Request { id, timestamp } => {
                        let _ = self.handle_lock_request(id, timestamp).await;
                    }
                    LamportMutexMsg::Reply { id, timestamp } => {
                        let _ = self.handle_lock_reply(id, timestamp).await;
                    }
                    LamportMutexMsg::Release { .. } => {
                        let _ = self.handle_lock_release().await;
                    }
                    LamportMutexMsg::Shutdown { tx } => {
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

    #[tracing::instrument(level = "trace", skip(self, has_got_all_replies))]
    pub(self) async fn flush_pending_requests(&mut self, has_got_all_replies: bool) -> LamportMutexResult<()> {
        for request in &self.pending_requests {
            let request_timestamp_ = self.core.request_timestamp.clone();
            match request {
                LamportMutexMsg::Request { id, timestamp } => {
                    // If this peer got its own request timestamp, start comparing it with the pending
                    // requests to determine the priority.
                    if let Some(request_timestamp) = request_timestamp_ {
                        match self
                            .core
                            .compare_and_reply(request_timestamp.clone(), *id, timestamp.clone())
                            .await
                        {
                            Err(LamportMutexError::NotAllowedToReply(_)) => {
                                self.core
                                    .deferred_replies
                                    .push(LamportMutexMsg::Request { id: *id, timestamp: timestamp.clone() });

                                tracing::trace!("deferred peer_{}'s request", *id);
                            }
                            _ => {}
                        }
                    }
                    // Else if this peer had the greatest priority and was ready to enter critical section,
                    // then just defer all the replies.
                    else if has_got_all_replies {
                        self.core
                            .deferred_replies
                            .push(LamportMutexMsg::Request { id: *id, timestamp: timestamp.clone() });

                        tracing::trace!("deferred peer_{}'s request", *id);
                    }
                    // Else, this peer hasn't got its own timestamp, nor had it got all replies yet.
                    else {
                        return Err(LamportMutexError::InvalidOperation(anyhow!("not allowed to flush pending requests")));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<'a, N: LamportMutexNetwork> State for PreRequestingState<'a, N> {
    #[tracing::instrument(level = "trace", skip(self, sender, timestamp))]
    async fn handle_lock_request(&mut self, sender: PeerId, timestamp: zenoh::Timestamp) -> LamportMutexResult<()> {
        if sender == self.core.id {
            // Record its own request timestamp.
            tracing::trace!("got own request timestamp: {}", timestamp);
            self.core.request_timestamp = Some(timestamp);

            // Process other requests after receiving its own request timestamp.
            tracing::trace!("start flushing pending requests with queue length: {}", self.pending_requests.len());
            self.flush_pending_requests(false).await.unwrap();

            tracing::trace!("transfer to REQUESTING state");
            self.core.set_current_state(StateType::Requesting);

            return Ok(());
        }
        // Hold other requests temporarily until this peer received its own request timestamp.
        tracing::trace!("push peer_{}'s request to pending queue", sender);
        self.pending_requests
            .push(LamportMutexMsg::Request { id: sender, timestamp });

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, sender, _timestamp))]
    async fn handle_lock_reply(&mut self, sender: PeerId, _timestamp: zenoh::Timestamp) -> LamportMutexResult<()> {
        self.core.num_of_replies_granted += 1;
        tracing::trace!(
            "got reply from peer_{}, current number of replies granted: {}",
            sender,
            self.core.num_of_replies_granted
        );

        if self.core.num_of_replies_granted == self.core.num_of_peers - 1 {
            tracing::trace!(
                "got all replies, start flushing pending requests with queue length: {}",
                self.pending_requests.len()
            );
            self.flush_pending_requests(true).await.unwrap();
            tracing::trace!("transfer to CRITICAL-SECTION state");
            self.core.set_current_state(StateType::CriticalSection);
            // Notify the lock success.
            self.core.tx_lock.send(()).unwrap();
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_lock_release(&mut self) -> LamportMutexResult<()> {
        let err_msg = "the release action is not allowed in PRE-REQUESTING state";
        tracing::error!("{}", err_msg);
        Err(LamportMutexError::NotAllowedToRelease(anyhow!(err_msg)))
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Volatile state specific to a lamport mutex peer in requesting state.
struct RequestingState<'a, N: LamportMutexNetwork> {
    pub(super) core: &'a mut LamportMutexCore<N>,
}

impl<'a, N: LamportMutexNetwork> RequestingState<'a, N> {
    pub(self) fn new(core: &'a mut LamportMutexCore<N>) -> Self {
        Self { core }
    }

    #[tracing::instrument(level="trace", skip(self), fields(id=self.core.id, state="requesting"))]
    pub(self) async fn run(mut self) -> LamportMutexResult<()> {
        tracing::trace!("running in REQUESTING state");
        loop {
            if !self.core.state_type.is_requesting() {
                return Ok(());
            }

            if let Some(msg) = self.core.rx_api.recv().await {
                match msg {
                    LamportMutexMsg::Request { id, timestamp } => {
                        let _ = self.handle_lock_request(id, timestamp).await;
                    }
                    LamportMutexMsg::Reply { id, timestamp } => {
                        let _ = self.handle_lock_reply(id, timestamp).await;
                    }
                    LamportMutexMsg::Release { .. } => {
                        let _ = self.handle_lock_release().await;
                    }
                    LamportMutexMsg::Shutdown { tx } => {
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
impl<'a, N: LamportMutexNetwork> State for RequestingState<'a, N> {
    #[tracing::instrument(level = "trace", skip(self, sender, timestamp))]
    async fn handle_lock_request(&mut self, sender: PeerId, timestamp: zenoh::Timestamp) -> LamportMutexResult<()> {
        match self
            .core
            .compare_and_reply(self.core.request_timestamp.clone().unwrap(), sender, timestamp.clone())
            .await
        {
            Err(LamportMutexError::NotAllowedToReply(_)) => {
                self.core
                    .deferred_replies
                    .push(LamportMutexMsg::Request { id: sender, timestamp });

                tracing::trace!("deferred peer_{}'s request", sender);
            }
            _ => {}
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, sender, _timestamp))]
    async fn handle_lock_reply(&mut self, sender: PeerId, _timestamp: zenoh::Timestamp) -> LamportMutexResult<()> {
        self.core.num_of_replies_granted += 1;
        tracing::trace!(
            "got reply from peer_{}, current number of replies granted: {}",
            sender,
            self.core.num_of_replies_granted
        );

        if self.core.num_of_replies_granted == self.core.num_of_peers - 1 {
            tracing::trace!("transfer to CRITICAL-SECTION state");
            self.core.set_current_state(StateType::CriticalSection);
            // Notify the lock success.
            self.core.tx_lock.send(()).unwrap();
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_lock_release(&mut self) -> LamportMutexResult<()> {
        let err_msg = "the release action is not allowed in REQUESTING state";
        tracing::error!("{}", err_msg);
        Err(LamportMutexError::NotAllowedToRelease(anyhow!(err_msg)))
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////

/// Volatile state specific to a lamport mutex peer in critical-section state.
struct CriticalSectionState<'a, N: LamportMutexNetwork> {
    pub(super) core: &'a mut LamportMutexCore<N>,
}

impl<'a, N: LamportMutexNetwork> CriticalSectionState<'a, N> {
    pub(self) fn new(core: &'a mut LamportMutexCore<N>) -> Self {
        Self { core }
    }

    #[tracing::instrument(level="trace", skip(self), fields(id=self.core.id, state="critical-section"))]
    pub(self) async fn run(mut self) -> LamportMutexResult<()> {
        tracing::trace!("running in CRITICAL-SECTION state");
        loop {
            if !self.core.state_type.is_critical_section() {
                return Ok(());
            }

            if let Some(msg) = self.core.rx_api.recv().await {
                match msg {
                    LamportMutexMsg::Request { id, timestamp } => {
                        let _ = self.handle_lock_request(id, timestamp).await;
                    }
                    LamportMutexMsg::Reply { id, timestamp } => {
                        let _ = self.handle_lock_reply(id, timestamp).await;
                    }
                    LamportMutexMsg::Release { tx } => {
                        let _ = tx.send(self.handle_lock_release().await);
                    }
                    LamportMutexMsg::Shutdown { tx } => {
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
    pub(self) async fn flush_deferred_replies(&mut self) -> LamportMutexResult<()> {
        for request in &self.core.deferred_replies {
            if let LamportMutexMsg::Request { id, timestamp } = request {
                // The timestamp wrapped in the reply is irrelevant to the algorithm.
                let _ = self
                    .core
                    .network
                    .reply(*id, LamportMutexMsg::Reply { id: self.core.id, timestamp: timestamp.clone() })
                    .await
                    .map_err(|_| LamportMutexError::LamportMutexNetwork(anyhow!("peer_{} failed to reply to peer_{}", self.core.id, *id)));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl<'a, N: LamportMutexNetwork> State for CriticalSectionState<'a, N> {
    #[tracing::instrument(level = "trace", skip(self, sender, timestamp))]
    async fn handle_lock_request(&mut self, sender: PeerId, timestamp: zenoh::Timestamp) -> LamportMutexResult<()> {
        self.core
            .deferred_replies
            .push(LamportMutexMsg::Request { id: sender, timestamp });

        tracing::trace!("deferred peer_{}'s request", sender);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, _sender, _timestamp))]
    async fn handle_lock_reply(&mut self, _sender: PeerId, _timestamp: zenoh::Timestamp) -> LamportMutexResult<()> {
        let err_msg = "the reply action is not allowed in CRITICAL-SECTION state";
        tracing::error!("{}", err_msg);
        Err(LamportMutexError::NotAllowedToReply(anyhow!(err_msg)))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn handle_lock_release(&mut self) -> LamportMutexResult<()> {
        tracing::trace!(
            "quit critical section, start flushing deferred replies with length: {}",
            self.core.deferred_replies.len()
        );
        self.flush_deferred_replies().await.unwrap();

        // Initialize
        self.core.num_of_replies_granted = 0u64;
        self.core.request_timestamp = None;
        self.core.deferred_replies.clear();
        tracing::trace!("transfer to IDLE state");
        self.core.set_current_state(StateType::Idle);

        Ok(())
    }
}
