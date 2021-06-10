use crate::common::*;
use crate::error::CRDTResult;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

pub struct PNCounter {
    id: PeerId,
    num_of_peers: usize,
    zenoh: Arc<Zenoh>,
    workspace_prefix: Arc<zenoh::Path>,
    state: Arc<Mutex<State>>,
}

impl PNCounter {
    pub async fn new(id: PeerId, num_of_peers: usize, zenoh: Arc<Zenoh>) -> Self {
        let workspace_prefix = Arc::new(zenoh::Path::try_from("/pn_counter/").unwrap());
        let state = Arc::new(Mutex::new(State::new(num_of_peers)));
        let _ =
            EventListener::spawn(id, zenoh.clone(), workspace_prefix.clone(), state.clone()).await;

        PNCounter {
            id,
            num_of_peers,
            zenoh,
            workspace_prefix,
            state,
        }
    }

    #[tracing::instrument(level = "trace", skip(self, count), fields(id=self.id))]
    pub async fn increase(&self, count: usize) {
        self.state.lock().await.cnt_of_increments[self.id] += count;
        tracing::info!("peer_{} increased counter by {}", self.id, count);
    }

    #[tracing::instrument(level = "trace", skip(self, count), fields(id=self.id))]
    pub async fn decrease(&self, count: usize) {
        self.state.lock().await.cnt_of_decrements[self.id] += count;
        tracing::info!("peer_{} decreased counter by {}", self.id, count);
    }

    pub async fn value(&self) -> isize {
        let state = self.state.lock().await;
        let sum_of_increments: usize = state.cnt_of_increments.iter().sum();
        let sum_of_decrements: usize = state.cnt_of_decrements.iter().sum();

        (sum_of_increments as isize) - (sum_of_decrements as isize)
    }

    #[tracing::instrument(level = "trace", skip(self), fields(id=self.id))]
    pub async fn broadcast_state_merge_event(&self) -> CRDTResult<()> {
        let zenoh: Arc<Zenoh> = self.zenoh.clone();
        let key: zenoh::Path = format!("{}", self.workspace_prefix).try_into().unwrap();
        let workspace = zenoh.workspace(Some(key)).await.unwrap();

        let key: zenoh::Path = format!("merge/request/{}", self.id).try_into().unwrap();
        let state = self.state.lock().await;
        workspace
            .put(
                &key,
                Value::Json(serde_json::to_string(&*state).unwrap().into()),
            )
            .await
            .unwrap();

        Ok(())
    }
}

struct EventListener {
    id: PeerId,
    zenoh: Arc<Zenoh>,
    workspace_prefix: Arc<zenoh::Path>,
    state: Arc<Mutex<State>>,
}

impl EventListener {
    pub async fn spawn(
        id: PeerId,
        zenoh: Arc<Zenoh>,
        workspace_prefix: Arc<zenoh::Path>,
        state: Arc<Mutex<State>>,
    ) -> JoinHandle<Result<()>> {
        let this = Self {
            id,
            zenoh,
            workspace_prefix,
            state,
        };

        tokio::spawn(this.main())
    }

    #[tracing::instrument(level = "trace", skip(self), fields(id=self.id))]
    pub async fn main(self) -> Result<()> {
        let key: zenoh::Path = format!("{}", self.workspace_prefix).try_into().unwrap();
        let workspace = self.zenoh.workspace(Some(key)).await?;

        let key: zenoh::Selector = format!("merge/request/*").try_into().unwrap();
        let mut state_stream = workspace.subscribe(&key).await.unwrap();

        loop {
            if let Some(change) = state_stream.next().await {
                let peer_id: PeerId = change.path.last_segment().parse().unwrap();

                if peer_id == self.id {
                    continue;
                }

                tracing::trace!(
                    "peer_{} received merge event from peer_{}",
                    self.id,
                    peer_id
                );

                if let Some(Value::Json(other)) = change.value {
                    let other: State = serde_json::from_str(&other).unwrap();
                    self.state.lock().await.update(&other);
                }
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    cnt_of_increments: Vec<usize>,
    cnt_of_decrements: Vec<usize>,
}

impl State {
    pub fn new(size: usize) -> Self {
        Self {
            cnt_of_increments: vec![0; size],
            cnt_of_decrements: vec![0; size],
        }
    }

    pub fn merge(&self, other: &Self) -> Self {
        let cnt_of_increments: Vec<usize> = self
            .cnt_of_increments
            .iter()
            .zip(other.cnt_of_increments.iter())
            .map(|(&lhs, &rhs)| lhs.max(rhs))
            .collect();

        let cnt_of_decrements: Vec<usize> = self
            .cnt_of_decrements
            .iter()
            .zip(other.cnt_of_decrements.iter())
            .map(|(&lhs, &rhs)| lhs.max(rhs))
            .collect();

        Self {
            cnt_of_increments,
            cnt_of_decrements,
        }
    }

    #[tracing::instrument(level = "trace", skip(self, other))]
    pub fn update(&mut self, other: &Self) {
        *self = self.merge(other);
        tracing::trace!("updated state: {:?}", self);
    }
}
