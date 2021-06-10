use crate::common::*;
use crate::error::CRDTResult;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::cmp::max;
use std::collections::HashMap;
use tokio::sync::Mutex;

pub struct BCounter {
    id: PeerId,
    num_of_peers: usize,
    zenoh: Arc<Zenoh>,
    workspace_prefix: Arc<zenoh::Path>,
    state: Arc<Mutex<State>>,
}

impl BCounter {
    pub async fn new(id: PeerId, num_of_peers: usize, zenoh: Arc<Zenoh>) -> Self {
        let workspace_prefix = Arc::new(zenoh::Path::try_from("/bounded_counter/").unwrap());
        let state = Arc::new(Mutex::new(State::new(id)));
        let _ =
            EventListener::spawn(id, zenoh.clone(), workspace_prefix.clone(), state.clone()).await;

        BCounter {
            id,
            num_of_peers,
            zenoh,
            workspace_prefix,
            state,
        }
    }

    #[tracing::instrument(level = "trace", skip(self, count), fields(id=self.id))]
    pub async fn increase(&self, count: usize) {
        *self.state
            .lock()
            .await
            .peers_to_transferred_quota_map
            .entry((self.id, self.id))
            .or_insert(0) += count;

        tracing::info!("peer_{} increased counter by {}", self.id, count);
    }

    #[tracing::instrument(level = "trace", skip(self, count), fields(id=self.id))]
    pub async fn decrease(&self, count: usize) {
        if self.has_sufficient_quota(count).await {
            *self.state
                .lock()
                .await
                .peer_id_to_consumed_quota_map
                .entry(self.id)
                .or_insert(0) += count;

            tracing::info!("peer_{} decreased counter by {}", self.id, count);
        } else {
            tracing::error!("peer_{} cannot decrease counter by {}", self.id, count);
        }
    }

    pub async fn value(&self) -> isize {
        let state = self.state.lock().await;
        let sum_of_increments: usize = state
            .peers_to_transferred_quota_map
            .iter()
            .filter(|(&(sender, receiver), _transferred_quota)| sender == receiver)
            .map(|(_, &transferred_quota)| transferred_quota)
            .sum();
        let sum_of_decrements: usize = state.peer_id_to_consumed_quota_map.values().sum();

        (sum_of_increments as isize) - (sum_of_decrements as isize)
    }

    #[tracing::instrument(level = "trace", skip(self, peer_id, count), fields(id=self.id))]
    pub async fn transfer_quota_to(&self, peer_id: usize, count: usize) {
        if self.has_sufficient_quota(count).await {
            *self.state
                .lock()
                .await
                .peers_to_transferred_quota_map
                .entry((self.id, peer_id))
                .or_insert(0) += count;

            tracing::info!(
                "peer_{} transferred {} quota to peer_{}",
                self.id,
                count,
                peer_id
            );
        } else {
            tracing::error!(
                "peer_{} cannot transferred quota {} to peer_{}",
                self.id,
                count,
                peer_id
            );
        }
    }

    #[tracing::instrument(level = "trace", skip(self, decrements), fields(id=self.id))]
    pub async fn has_sufficient_quota(&self, decrements: usize) -> bool {
        let state = self.state.lock().await;
        let sum_of_given_quota: usize = state
            .peers_to_transferred_quota_map
            .iter()
            .filter(|(&(_sender, receiver), _transferred_quota)| receiver == self.id)
            .map(|(_, &transferred_quota)| transferred_quota)
            .sum();
        let sum_of_transferred_quota: usize = state
            .peers_to_transferred_quota_map
            .iter()
            .filter(|(&(sender, receiver), _transferred_quota)| {
                sender == self.id && sender != receiver
            })
            .map(|(_, &transferred_quota)| transferred_quota)
            .sum();
        let sum_of_consumed_quota: usize =
            *state.peer_id_to_consumed_quota_map.get(&self.id).unwrap();

        sum_of_given_quota - sum_of_transferred_quota - sum_of_consumed_quota >= decrements
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

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    #[serde_as(as = "HashMap<serde_with::json::JsonString, _>")]
    peer_id_to_consumed_quota_map: HashMap<usize, usize>,
    #[serde_as(as = "HashMap<serde_with::json::JsonString, _>")]
    peers_to_transferred_quota_map: HashMap<(usize, usize), usize>,
}

impl State {
    pub fn new(peer_id: usize) -> Self {
        let mut peer_id_to_consumed_quota_map = HashMap::new();
        peer_id_to_consumed_quota_map.insert(peer_id, 0);

        let mut peers_to_transferred_quota_map = HashMap::new();
        peers_to_transferred_quota_map.insert((peer_id, peer_id), 0);

        Self {
            peer_id_to_consumed_quota_map,
            peers_to_transferred_quota_map,
        }
    }

    pub fn merge(&self, other: &Self) -> Self {
        let mut peer_id_to_consumed_quota_map = self.peer_id_to_consumed_quota_map.clone();
        for (&peer_id, &consumed_quota) in other.peer_id_to_consumed_quota_map.iter() {
            if let Some(&original_consumed_quota) = peer_id_to_consumed_quota_map.get(&peer_id) {
                peer_id_to_consumed_quota_map.insert(peer_id, max(original_consumed_quota, consumed_quota));
            } else {
                peer_id_to_consumed_quota_map.insert(peer_id, consumed_quota);
            }
        }

        let mut peers_to_transferred_quota_map = self.peers_to_transferred_quota_map.clone();
        for (&peers, &transferred_quota) in other.peers_to_transferred_quota_map.iter() {
            if let Some(&original_transferred_quota) = peers_to_transferred_quota_map.get(&peers) {
                peers_to_transferred_quota_map.insert(peers, max(original_transferred_quota, transferred_quota));
            } else {
                peers_to_transferred_quota_map.insert(peers, transferred_quota);
            }
        }

        Self {
            peer_id_to_consumed_quota_map,
            peers_to_transferred_quota_map,
        }
    }

    #[tracing::instrument(level = "trace", skip(self, other))]
    pub fn update(&mut self, other: &Self) {
        *self = self.merge(other);
        tracing::trace!("updated state: {:?}", self);
    }
}
