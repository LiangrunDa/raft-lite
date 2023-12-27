///
/// Event loop implementation for Raft protocol
/// All event handlers are non-blocking
///

use std::cmp::min;
use std::collections::HashSet;
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
    pub(crate) fn new(id: u64, num_peers: usize) -> Self {
        Self {
            id,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_length: 0,
            current_role: Role::Follower,
            current_leader: None,
            votes_received: Vec::new(),
            sent_length: vec![0; num_peers],
            acked_length: vec![0; num_peers],
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
            config: config.clone(),
            state: Arc::new(Mutex::new(NodeState::new(id, config.peers.len()))),
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
                    self.handle_replicate_log().await;
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

    async fn handle_replicate_log(&mut self) {
        let mut state = self.state.lock().await;
        if state.current_role != Role::Leader {
            return;
        }
        let id = state.id;

        for i in 0..self.config.peers.len() {
            if i == id as usize {
                continue;
            }
            // we don't need to make them in parallel, since all of them will need to acquire the lock
            Self::replicate_log(&mut state, id, i as u64, &self.peers).await;
        }
    }

    async fn replicate_log(state: &mut NodeState, leader_id: u64, follower_id: u64, peers: &RPCClients) {
        let prefix_len = state.sent_length[follower_id as usize];
        let suffix = state.log[prefix_len as usize..].to_vec();
        let mut prefix_term = 0;
        if prefix_len > 0 {
            prefix_term = state.log[prefix_len as usize - 1].term;
        }
        let msg = LogRequestArgs {
            leader_id,
            term: state.current_term,
            prefix_len,
            prefix_term,
            leader_commit: state.commit_length,
            suffix,
        };
        drop(state);
        peers.log_request(msg, follower_id).await;
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

                for i in 0..self.config.peers.len() {
                    if i == state.id as usize {
                        continue;
                    }
                    state.sent_length[i] = state.log.len() as u64;
                    state.acked_length[i] = 0;
                }
                drop(state);
                self.handle_replicate_log().await;
            }
        } else if args.term > state.current_term {
            state.current_term = args.term;
            state.current_role = Role::Follower;
            state.voted_for = None;
            self.election_timer_reset_tx.send(()).await.expect("election_timer_reset_tx closed");
        }
    }

    async fn handle_log_request(&mut self, args: LogRequestArgs, answer: oneshot::Sender<LogResponseArgs>) {
        let mut state = self.state.lock().await;
        trace!("received log request and current state is {:?}", state);
        if args.term > state.current_term {
            state.current_term = args.term;
            state.voted_for = None;
            self.election_timer_reset_tx.send(()).await.expect("election_timer_reset_tx closed");
        }
        if args.term == state.current_term {
            if state.current_leader != Some(args.leader_id) {
                debug!("become follower of leader {}", args.leader_id);
            }
            state.current_role = Role::Follower;
            state.current_leader = Some(args.leader_id);
            self.election_timer_reset_tx.send(()).await.expect("election_timer_reset_tx closed");
        }
        let log_ok = (state.log.len() >= args.prefix_len as usize) &&
            (args.prefix_len == 0 || state.log[args.prefix_len as usize - 1].term == args.prefix_term);

        let mut ack = 0;
        let mut success = false;
        if args.term == state.current_term && log_ok {
            Self::append_entries(&mut state, args.prefix_len, args.leader_commit, args.suffix.clone());
            ack = args.prefix_len + args.suffix.len() as u64;
            success = true;
        }
        let msg = LogResponseArgs {
            follower: state.id,
            term: state.current_term,
            ack,
            success,
        };
        drop(state);
        trace!("sent: {:?}", msg);
        answer.send(msg).expect("LogResponse channel closed");
    }

    fn append_entries(state: &mut NodeState, prefix_len: u64, leader_commit: u64, suffix: Vec<LogEntry>) {
        if suffix.len() > 0 && state.log.len() > prefix_len as usize {
            let index = min(state.log.len(), prefix_len as usize + suffix.len()) - 1;
            if state.log[index].term != suffix[index - prefix_len as usize].term {
                state.log = state.log[..prefix_len as usize].to_vec();
            }
        }
        if prefix_len as usize + suffix.len() > state.log.len() {
            for i in state.log.len() - prefix_len as usize..suffix.len() {
                state.log.push(suffix[i].clone());
            }
        }
        if leader_commit > state.commit_length {
            for i in state.commit_length..leader_commit {
                // TODO: deliver to application
            }
            state.commit_length = leader_commit;
        }

    }

    async fn handle_log_response(&mut self, args: LogResponseArgs) {
        let mut state = self.state.lock().await;
        trace!("received log response and current state is {:?}", state);
        if args.term > state.current_term && state.current_role == Role::Leader {
            if args.success && args.ack > state.acked_length[args.follower as usize] {
                state.sent_length[args.follower as usize] = args.ack;
                state.acked_length[args.follower as usize] = args.ack;
                Self::commit_log_entries(&mut state, &self.config).await;
            } else if state.sent_length[args.follower as usize] > 0 {
                state.sent_length[args.follower as usize] -= 1;
                let id = state.id;
                Self::replicate_log(&mut state, id, args.follower, &self.peers).await;
            }
        } else if args.term > state.current_term {
            state.current_term = args.term;
            state.current_role = Role::Follower;
            state.voted_for = None;
            self.election_timer_reset_tx.send(()).await.expect("election_tx closed");
        }
    }

    async fn commit_log_entries(state: &mut NodeState, config: &RaftConfig) {
        let min_acks = ((config.peers.len() + 1) + 1) / 2;
        let mut ready = HashSet::new();
        let mut ready_max = 0;
        for i in 1..state.log.len() + 1 {
            if Self::acks(&state.acked_length, i as u64) >= min_acks as u64 {
                ready.insert(i);
                ready_max = i;
            }
        }
        if !ready.is_empty() && ready_max > state.commit_length as usize &&
            state.log[ready_max - 1].term == state.current_term {
            for i in state.commit_length as usize..ready_max {
                // TODO: deliver to application
            }
            state.commit_length = ready_max as u64;
        }
    }

    fn acks(acked_length: &Vec<u64>, length: u64) -> u64 {
        let mut acks = 0;
        for i in 0..acked_length.len() {
            if acked_length[i] > length {
                acks += 1;
            }
        }
        acks
    }
}
