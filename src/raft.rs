use crate::config::RaftConfig;
use crate::network::{start_server, RPCClients};
use crate::raft_protocol::RaftProtocol;
use crate::timer::start_timer;
use crate::timer::Timer::{ElectionTimer, ReplicationTimer};
use std::sync::Arc;
use tarpc::server::Channel;
use tokio::sync::mpsc;
use tracing::info;

pub struct Raft {
    config: RaftConfig,
}

impl Raft {
    pub fn new(config: RaftConfig) -> Self {
        Self { config }
    }

    pub fn run(&mut self) {
        let tokio_handle = tokio::runtime::Handle::try_current();
        if tokio_handle.is_err() {
            panic!("Tokio runtime is not ready!");
        } else {
            info!("Use existing tokio runtime")
        }
        let config = self.config.clone();
        tokio_handle.unwrap().spawn(async move {
            let (event_tx, event_rx) = mpsc::channel(100);

            // setup network
            let server_addr = config.peers[config.id as usize].clone();
            let rpc_server = start_server(&server_addr, event_tx.clone());

            // clients
            let clients = RPCClients::new(config.clone());

            // setup timers
            let (election_timer_reset_tx, mut election_timer_reset_rx) = mpsc::channel::<()>(1);
            let (replicate_timer_reset_tx, mut replicate_timer_reset_rx) = mpsc::channel::<()>(1);

            // setup raft protocol
            let mut raft_protocol = RaftProtocol::new(
                Arc::new(config.clone()),
                event_rx,
                Arc::new(election_timer_reset_tx),
                Arc::new(replicate_timer_reset_tx),
                clients,
            );
            let protocol = raft_protocol.run();

            // setup timers
            let election_timer = start_timer(
                election_timer_reset_rx,
                event_tx.clone(),
                config.params.election_timeout,
                ElectionTimer,
            );
            let replication_timer = start_timer(
                replicate_timer_reset_rx,
                event_tx.clone(),
                config.params.replicate_timeout,
                ReplicationTimer,
            );

            tokio::join!(rpc_server, protocol, election_timer, replication_timer);
        });
    }

    pub fn broadcast(&mut self) {}
}
