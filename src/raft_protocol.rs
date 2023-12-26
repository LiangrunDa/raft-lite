///
/// Event loop implementation for Raft protocol
/// All event handlers are non-blocking
///
use crate::config::RaftConfig;
use crate::network::RPCClients;
use crate::raft_log::LogEntry;
use std::sync::Arc;
use tarpc::serde;
use tarpc::serde::ser::SerializeStruct;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

pub(crate) enum Role {
    Follower,
    Candidate,
    Leader,
}

pub(crate) struct NodeState {
    pub(crate) id: u64,
    pub(crate) current_term: u64,
    pub(crate) voted_for: Option<u64>,
    pub(crate) log: Vec<LogEntry>,
    pub(crate) commit_length: u64,
    pub(crate) current_role: Role,
    pub(crate) current_leader: Option<u64>,
    pub(crate) votes_received: Vec<u64>,
    pub(crate) sent_length: Vec<u64>,
    pub(crate) acked_length: Vec<u64>,
}

impl NodeState {
    pub(crate) fn new(id: u64) -> Self {
        Self {
            id,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_length: 0,
            current_role: Role::Follower,
            current_leader: None,
            votes_received: Vec::new(),
            sent_length: Vec::new(),
            acked_length: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct LogRequestArgs {
    pub(crate) leader_id: u64,
    pub(crate) term: u64,
    pub(crate) prefix_len: u64,
    pub(crate) prefix_term: u64,
    pub(crate) leader_commit: u64,
    pub(crate) suffix: Vec<LogEntry>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct LogResponseArgs {
    pub(crate) follower: u64,
    pub(crate) term: u64,
    pub(crate) ack: u64,
    pub(crate) success: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct VoteRequestArgs {
    pub(crate) cid: u64,
    pub(crate) cterm: u64,
    pub(crate) clog_length: u64,
    pub(crate) clog_term: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct VoteResponseArgs {
    pub(crate) voter_id: u64,
    pub(crate) term: u64,
    pub(crate) granted: bool,
}

pub(crate) enum Event {
    ElectionTimeout,
    ReplicationTimeout,
    VoteRequest(VoteRequestArgs, oneshot::Sender<VoteResponseArgs>),
    VoteResponse(VoteResponseArgs),
    LogRequest(LogRequestArgs, oneshot::Sender<LogResponseArgs>),
    LogResponse(LogResponseArgs),
    Broadcast,
}

pub(crate) struct RaftProtocol {
    /// Raft-specific state
    state: NodeState,
    /// raft config for the cluster
    config: Arc<RaftConfig>,
    /// election timer
    election_timer_reset_tx: Arc<mpsc::Sender<()>>,
    /// replicate timer
    replicate_timer_reset_tx: Arc<mpsc::Sender<()>>,
    /// peers
    peers: RPCClients,
    /// Request channel
    event_rx: mpsc::Receiver<Event>,
}

impl RaftProtocol {
    pub(crate) fn new(
        config: Arc<RaftConfig>,
        event_rx: mpsc::Receiver<Event>,
        election_timer_reset_tx: Arc<mpsc::Sender<()>>,
        replicate_timer_reset_tx: Arc<mpsc::Sender<()>>,
        peers: RPCClients,
    ) -> Self {
        let id = config.id;
        Self {
            config,
            state: NodeState::new(id),
            election_timer_reset_tx,
            replicate_timer_reset_tx,
            event_rx,
            peers,
        }
    }

    pub(crate) async fn run(&mut self) {
        // event loop
        loop {
            let event = self.event_rx.recv().await.expect("event_rx closed");
            match event {
                Event::ElectionTimeout => {
                    self.start_election().await;
                }
                Event::ReplicationTimeout => {
                    self.replicate_log().await;
                }
                Event::VoteRequest(args, answer) => {
                    self.handle_vote_request(args).await;
                }
                Event::VoteResponse(args) => {
                    self.handle_vote_response(args).await;
                }
                Event::LogRequest(args, answer) => {
                    self.handle_log_request(args).await;
                }
                Event::LogResponse(args) => {
                    self.handle_log_response(args).await;
                }
                Event::Broadcast => {
                    todo!()
                }
            }
        }
    }

    async fn start_election(&mut self) {
        debug!("start election");
    }

    async fn replicate_log(&mut self) {
        debug!("replicate log");
    }

    async fn handle_vote_request(&mut self, args: VoteRequestArgs) {
        todo!()
    }

    async fn handle_vote_response(&mut self, args: VoteResponseArgs) {
        todo!()
    }

    async fn handle_log_request(&mut self, args: LogRequestArgs) {
        todo!()
    }

    async fn handle_log_response(&mut self, args: LogResponseArgs) {
        todo!()
    }
}
