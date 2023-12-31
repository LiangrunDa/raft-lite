pub mod config;
pub mod persister;
mod network;
pub mod raft;
mod raft_log;
mod raft_protocol;
mod timer;

// TODO: use stateright to test raft