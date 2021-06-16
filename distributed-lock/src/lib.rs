pub mod common;
mod core;
mod error;
pub mod lamport_mutex;
pub mod network;
pub mod zenoh_lamport_mutex;

/// A Zenoh node's ID.
pub type PeerId = u64;
