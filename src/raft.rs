use crate::config::RaftConfig;
use crate::network::{start_server, RPCClients};
use crate::raft_protocol::{Event, RaftProtocol};
use crate::timer::start_timer;
use crate::timer::Timer::{ElectionTimer, ReplicationTimer};
use tokio::sync::mpsc;
use tracing::{info, trace};

pub struct Raft {
    config: RaftConfig,
}

impl Raft {
    pub fn new(config: RaftConfig) -> Self {
        Self { config }
    }

    pub fn run(
        &mut self,
        mut application_broadcast_rx: mpsc::Receiver<Vec<u8>>,
        application_message_tx: mpsc::Sender<Vec<u8>>,
    ) {
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
            let clients = RPCClients::new(config.clone(), event_tx.clone());

            // setup timers
            let (election_timer_reset_tx, election_timer_reset_rx) = mpsc::channel::<()>(1);
            let (replicate_timer_reset_tx, replicate_timer_reset_rx) = mpsc::channel::<()>(1);

            // setup raft protocol
            let mut raft_protocol = RaftProtocol::new(
                config.clone(),
                event_rx,
                election_timer_reset_tx,
                replicate_timer_reset_tx,
                clients,
                application_message_tx,
            )
            .await;
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

            // define an async task to broadcast application messages
            let broadcast_event_task = async move {
                loop {
                    let msg = application_broadcast_rx
                        .recv()
                        .await
                        .expect("broadcast_rx closed");
                    trace!("Received message from application: {:?}", msg);
                    event_tx
                        .send(Event::Broadcast(msg))
                        .await
                        .expect("event_tx closed");
                }
            };

            tokio::join!(
                rpc_server,
                protocol,
                election_timer,
                replication_timer,
                broadcast_event_task
            );
        });
    }

    pub fn broadcast(&mut self) {}
}
