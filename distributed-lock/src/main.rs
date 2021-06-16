use distributed_lock::common::*;
use distributed_lock::zenoh_lamport_mutex::ZenohLamportMutex;
use distributed_lock::PeerId;
use std::env;

async fn run(id: PeerId, num_of_peers: u64) {
    let lamport_mutex = ZenohLamportMutex::new(id, num_of_peers).await;

    // Wait for all lamport mutex participants to initialize.
    sleep(Duration::from_secs(15)).await;

    {
        tracing::info!("peer_{} try to lock mutex", id);
        let _lamport_mutex_guard = lamport_mutex.lock().await;
        sleep(Duration::from_secs(1)).await;
        tracing::info!("peer_{} locked mutex", id);
    }

    tracing::info!("peer_{} unlocked mutex", id);

    // Close all running async tasks and network sessions.
    lamport_mutex.shutdown();
}

fn main() {
    init_tracing();

    let args: Vec<String> = env::args().collect();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(run(args[1].parse().unwrap(), args[2].parse().unwrap()));
}
