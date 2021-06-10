mod fixtures;

use crate::fixtures::init_tracing;
use crdt::common::*;
use crdt::pn_counter::PNCounter;

pub type Fallible<T> = Result<T>;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_pn_counter() -> Result<()> {
    init_tracing();

    let num_of_peers = 5;

    let peer_futures = (0..num_of_peers).map(|id| {
        async move {
            let zenoh = Arc::new(Zenoh::new(Default::default()).await?);
            let pn_counter = PNCounter::new(id, num_of_peers, zenoh).await;
            let mut value = 0;

            // Wait for initializing.
            sleep(Duration::from_secs(2)).await;

            pn_counter.increase(id * 10).await;
            let _ = pn_counter.broadcast_state_merge_event().await;

            // Wait for updating.
            sleep(Duration::from_secs(1)).await;

            value = pn_counter.value().await;
            tracing::info!("peer_{}'s current value: {}", id, value);

            pn_counter.decrease(id * 2).await;
            let _ = pn_counter.broadcast_state_merge_event().await;

            // Wait for updating.
            sleep(Duration::from_secs(1)).await;

            value = pn_counter.value().await;
            tracing::info!("peer_{}'s current value: {}", id, value);

            Fallible::Ok(())
        }
    });

    futures::future::try_join_all(peer_futures).await?;

    Ok(())
}
