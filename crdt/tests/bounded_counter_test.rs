mod fixtures;

use crate::fixtures::init_tracing;
use crdt::bounded_counter::BCounter;
use crdt::common::*;

pub type Fallible<T> = Result<T>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_bounded_counter() -> Result<()> {
    init_tracing();

    let num_of_peers = 5;

    let peer_futures = (0..num_of_peers).map(|id| {
        async move {
            let zenoh = Arc::new(Zenoh::new(Default::default()).await?);
            let b_counter = BCounter::new(id, num_of_peers, zenoh).await;
            let mut value = 0;

            // Wait for initializing.
            sleep(Duration::from_secs(2)).await;

            b_counter.increase(id * 10).await;
            let _ = b_counter.broadcast_state_merge_event().await;

            // Wait for updating.
            sleep(Duration::from_secs(1)).await;

            value = b_counter.value().await;
            tracing::info!("peer_{}'s current value: {}", id, value);

            b_counter.decrease((id + 1) * 2).await;
            let _ = b_counter.broadcast_state_merge_event().await;

            // Wait for updating.
            sleep(Duration::from_secs(1)).await;

            value = b_counter.value().await;
            tracing::info!("peer_{}'s current value: {}", id, value);

            Fallible::Ok(())
        }
    });

    futures::future::try_join_all(peer_futures).await?;

    Ok(())
}
