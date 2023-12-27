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
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, trace};

#[derive(PartialEq, Eq, Clone, Debug)]
pub(crate) enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug)]
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
    state: Arc<Mutex<NodeState>>,
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
            state: Arc::new(Mutex::new(NodeState::new(id))),
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
                    self.handle_vote_request(args, answer).await;
                }
                Event::VoteResponse(args) => {
                    self.handle_vote_response(args).await;
                }
                Event::LogRequest(args, answer) => {
                    self.handle_log_request(args, answer).await;
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
        let mut state = self.state.lock().await;
        if state.current_role == Role::Leader {
            return;
        }
        let id = state.id;
        state.current_term += 1;
        state.voted_for = Some(id);
        state.current_role = Role::Candidate;
        state.votes_received.clear();
        state.votes_received.push(id);

        let mut last_term = 0;
        if state.log.len() > 0 {
            last_term = state.log.last().unwrap().term;
        }

        let msg = VoteRequestArgs {
            cid: id,
            cterm: state.current_term.clone(),
            clog_length: state.log.len() as u64,
            clog_term: last_term,
        };

        drop(state);

        self.peers.vote_request(msg).await;
    }

    async fn replicate_log(&mut self) {
        // trace!("replicate log");
    }

    async fn handle_vote_request(&mut self, args: VoteRequestArgs, answer: oneshot::Sender<VoteResponseArgs>) {
        let mut state = self.state.lock().await;
        trace!("received {:?} and current state is {:?}", args, state);
        if args.cterm > state.current_term {
            state.current_term = args.cterm;
            state.current_role = Role::Follower;
            state.voted_for = None;
        }

        let mut last_term = 0;
        if state.log.len() > 0 {
            last_term = state.log.last().unwrap().term;
        }

        let log_ok = args.clog_term > last_term
            || (args.clog_term == last_term && args.clog_length >= state.log.len() as u64);

        let mut granted = false;
        if args.cterm == state.current_term && log_ok && (state.voted_for.is_none() || state.voted_for.unwrap() == args.cid) {
            state.voted_for = Some(args.cid);
            granted = true;
        }

        let msg = VoteResponseArgs {
            voter_id: state.id,
            term: state.current_term,
            granted,
        };
        trace!("sent: {:?}", msg);

        drop(state);

        answer.send(msg).expect("VoteResponse channel closed");
    }

    async fn handle_vote_response(&mut self, args: VoteResponseArgs) {
        let mut state = self.state.lock().await;
        trace!("received vote response and current state is {:?}", state);
        if state.current_role == Role::Candidate && args.term == state.current_term && args.granted {
            state.votes_received.push(args.voter_id);
            trace!("votes received: {:?}", state.votes_received);
            // rounded up
            if state.votes_received.len() >= ((self.config.peers.len() + 1) + 1) / 2 {
                state.current_role = Role::Leader;
                state.current_leader = Some(state.id);
                // TODO: cancel election timer
                debug!("take over leadership with term {}", state.current_term);
                // state.sent_length.clear();
                // state.acked_length.clear();
                // for _ in 0..self.config.peers.len() {
                //     state.sent_length.push(0);
                //     state.acked_length.push(0);
                // }
            }
        } else if args.term > state.current_term {
            state.current_term = args.term;
            state.current_role = Role::Follower;
            state.voted_for = None;
            self.election_timer_reset_tx.send(()).await.expect("election_timer_reset_tx closed");
        }
    }

    async fn handle_log_request(&mut self, args: LogRequestArgs, answer: oneshot::Sender<LogResponseArgs>) {
        todo!()
    }

    async fn handle_log_response(&mut self, args: LogResponseArgs) {
        todo!()
    }
}
