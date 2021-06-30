pub use crate::eventual_queue::EQMsg;
pub use crate::PeerId;
pub use anyhow::{anyhow, Result};
pub use serde::{Deserialize, Serialize};
pub use std::convert::TryFrom;
pub use std::convert::TryInto;
pub use std::sync::Arc;
pub use tokio::sync::{mpsc, oneshot, watch};
pub use tokio::task::JoinHandle;
pub use tokio::time::{sleep, Duration};
pub use tracing_subscriber::layer::SubscriberExt;
pub use tracing_subscriber::prelude::*;
pub use zenoh::{Timestamp, Zenoh};

pub fn init_tracing() {
    let fmt_layer = tracing_subscriber::fmt::Layer::default()
        .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NONE)
        .with_ansi(false);
    let subscriber = tracing_subscriber::Registry::default()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(fmt_layer);
    tracing::subscriber::set_global_default(subscriber).expect("error setting global tracing subscriber");
}
