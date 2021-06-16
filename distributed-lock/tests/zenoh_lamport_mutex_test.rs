#[cfg(test)]
mod tests {

    use distributed_lock::common::*;
    use distributed_lock::zenoh_lamport_mutex::ZenohLamportMutex;
    use std::fmt::Error;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_zenoh_lamport_mutex() -> Result<()> {
        init_tracing();
        // The maximum workload is running 18 zenoh clients simultaneously on single process (each lamport
        // mutex participant owns 2 zenoh clients); otherwise, zenoh will drop the packets.
        let num_of_peers = 5;

        let peer_futures = (0..num_of_peers).map(|id| async move {
            let lamport_mutex = ZenohLamportMutex::new(id, num_of_peers).await;

            // Wait for all lamport mutex participants to initialize.
            sleep(Duration::from_secs(10)).await;

            {
                tracing::info!("peer_{} try to lock mutex", id);
                let _lamport_mutex_guard = lamport_mutex.lock().await;
                sleep(Duration::from_secs(1)).await;
                tracing::info!("peer_{} locked mutex", id);
            }

            tracing::info!("peer_{} unlocked mutex", id);

            Ok::<(), Error>(())
        });

        futures::future::try_join_all(peer_futures).await?;

        Ok(())
    }
}
