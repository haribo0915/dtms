use eventual_queue::common::*;
use eventual_queue::zenoh_eventual_queue::ZenohEventualQueue;
use eventual_queue::PeerId;
use std::env;

async fn run(id: PeerId, num_of_peers: u64) {
    let eventual_queue = ZenohEventualQueue::new(id, num_of_peers).await;

    // Wait for all eventual queue participants to initialize.
    sleep(Duration::from_secs(10)).await;

    let enqueued_item = id;
    tracing::info!("peer_{} enqueue item: {}", id, enqueued_item);
    eventual_queue.enqueue(enqueued_item).await;

    sleep(Duration::from_secs(2)).await;

    if let Some(item) = eventual_queue.dequeue().await {
        tracing::info!("peer_{} dequeue item: {}", id, item);
    } else {
        tracing::info!("peer_{} dequeue item: None", id);
    }

    // Wait for all eventual queue participants to terminate.
    sleep(Duration::from_secs(10)).await;
}

fn main() {
    init_tracing();

    let args: Vec<String> = env::args().collect();

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(run(args[1].parse().unwrap(), args[2].parse().unwrap()));
}
