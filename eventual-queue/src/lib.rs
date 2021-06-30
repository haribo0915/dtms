pub mod common;
mod core;
mod error;
pub mod eventual_queue;
pub mod network;
pub mod zenoh_eventual_queue;

/// A eventual queue node's ID.
pub type PeerId = u64;
