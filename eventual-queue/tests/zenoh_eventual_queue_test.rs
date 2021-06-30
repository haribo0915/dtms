#[cfg(test)]
mod tests {

    use eventual_queue::common::*;
    use eventual_queue::zenoh_eventual_queue::ZenohEventualQueue;
    use std::fmt::Error;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_zenoh_eventual_queue() -> Result<()> {
        init_tracing();
        // The maximum workload is running 16 zenoh clients simultaneously on single process (each eventual
        // queue participant owns 2 zenoh clients); otherwise, zenoh will drop the packets.
        let num_of_peers = 3;

        let peer_futures = (0..num_of_peers).map(|id| async move {
            let eventual_queue = ZenohEventualQueue::new(id, num_of_peers).await;

            // Wait for all eventual queue participants to initialize.
            sleep(Duration::from_secs(5)).await;

            let enqueued_item = id;
            tracing::info!("peer_{} enqueue item: {}", id, enqueued_item);
            eventual_queue.enqueue(enqueued_item).await;

            sleep(Duration::from_secs(2)).await;

            if let Some(item) = eventual_queue.dequeue().await {
                tracing::info!("peer_{} dequeue item: {}", id, item);
            } else {
                tracing::info!("peer_{} dequeue item: None", id);
            }

            sleep(Duration::from_secs(5)).await;

            Ok::<(), Error>(())
        });

        futures::future::try_join_all(peer_futures).await?;

        Ok(())
    }
}
