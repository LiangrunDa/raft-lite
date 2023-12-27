///
/// Event loop implementation for Raft protocol
/// All event handlers are non-blocking except for `forward_to_new_leader`.
/// `forward_to_new_leader` is blocking because it needs to wait for the leader
/// to confirm the message is received (otherwise the application message might
/// get lost), so it is implemented as a separate async task. IMO, there is no
/// need to make all event handlers concurrent, because they need to access the
/// same node state.
///
use crate::config::RaftConfig;
use crate::network::RPCClients;
use crate::persister::{PersistRaftState, Persister};
use crate::raft_log::LogEntry;
use futures::TryFutureExt;
use std::cmp::min;
use std::collections::HashSet;
use std::sync::Arc;
use tarpc::serde;
use tarpc::serde::ser::SerializeStruct;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::{debug, error, trace};

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
    pub(crate) votes_received: HashSet<u64>,
    pub(crate) sent_length: Vec<u64>,
    pub(crate) acked_length: Vec<u64>,
}

impl NodeState {
    pub(crate) async fn new(id: u64, num_peers: usize, persister: Box<dyn Persister>) -> Self {
        let persisted_state = persister
            .load_raft_state().await.unwrap_or_else(|_| PersistRaftState::default());
        let PersistRaftState {
            current_term,
            voted_for,
            log,
            commit_length,
        } = persisted_state;
        Self {
            id,
            current_term,
            voted_for,
            log,
            commit_length,
            current_role: Role::Follower,
            current_leader: None,
            votes_received: HashSet::new(),
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct BroadcastArgs {
    pub(crate) payload: Vec<u8>,
}

pub(crate) enum Event {
    ElectionTimeout,
    ReplicationTimeout,
    VoteRequest(VoteRequestArgs, oneshot::Sender<VoteResponseArgs>),
    VoteResponse(VoteResponseArgs),
    LogRequest(LogRequestArgs, oneshot::Sender<LogResponseArgs>),
    LogResponse(LogResponseArgs),
    Broadcast(Vec<u8>),
}

pub(crate) struct RaftProtocol {
    /// Raft-specific state
    state: NodeState,
    /// raft config for the cluster
    config: RaftConfig,
    /// election timer
    election_timer_reset_tx: mpsc::Sender<()>,
    /// replicate timer
    replicate_timer_reset_tx: mpsc::Sender<()>,
    /// peers
    peers: RPCClients,
    /// Request channel
    event_rx: mpsc::Receiver<Event>,
    /// Application message channel
    application_message_tx: mpsc::Sender<Vec<u8>>,
    /// FIFO queue for application broadcast
    message_buffer: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl RaftProtocol {
    pub(crate) async fn new(
        config: RaftConfig,
        event_rx: mpsc::Receiver<Event>,
        election_timer_reset_tx: mpsc::Sender<()>,
        replicate_timer_reset_tx: mpsc::Sender<()>,
        peers: RPCClients,
        application_message_tx: mpsc::Sender<Vec<u8>>,
    ) -> Self {
        let id = config.id;
        Self {
            config: config.clone(),
            state: NodeState::new(id, config.peers.len(), config.persister).await,
            election_timer_reset_tx,
            replicate_timer_reset_tx,
            event_rx,
            peers,
            application_message_tx,
            message_buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(crate) async fn run(&mut self) {
        // event loop
        loop {
            let event = self.event_rx.recv().await.expect("event_rx closed");
            match event {
                Event::ElectionTimeout => {
                    self.start_election().await;
                    Self::persist_state(self.state.clone(), self.config.persister.clone()).await;
                }
                Event::ReplicationTimeout => {
                    self.handle_replicate_log().await;
                }
                Event::VoteRequest(args, answer) => {
                    self.handle_vote_request(args, answer).await;
                    Self::persist_state(self.state.clone(), self.config.persister.clone()).await;
                }
                Event::VoteResponse(args) => {
                    self.handle_vote_response(args).await;
                    Self::persist_state(self.state.clone(), self.config.persister.clone()).await;
                }
                Event::LogRequest(args, answer) => {
                    self.handle_log_request(args, answer).await;
                    Self::persist_state(self.state.clone(), self.config.persister.clone()).await;
                }
                Event::LogResponse(args) => {
                    self.handle_log_response(args).await;
                    Self::persist_state(self.state.clone(), self.config.persister.clone()).await;
                }
                Event::Broadcast(payload) => {
                    self.handle_broadcast(payload).await;
                    Self::persist_state(self.state.clone(), self.config.persister.clone()).await;
                }
            }
        }
    }

    /// Any handler that modifies the states that are supposed to be persisted should call this function
    async fn persist_state(state: NodeState, persister: Box<dyn Persister>) {
        // TODO: Do we really need to persist the term and voted_for?
        tokio::spawn(async move {
            let persisted_state = PersistRaftState {
                current_term: state.current_term,
                voted_for: state.voted_for,
                log: state.log.clone(),
                commit_length: state.commit_length,
            };
            match persister.save_raft_state(&persisted_state).await {
                Ok(_) => {}
                Err(e) => {
                    error!("Failed to persist state: {}", e);
                }
            }
        });
    }

    async fn start_election(&mut self) {
        let state = &mut self.state;
        if state.current_role == Role::Leader {
            return;
        }
        let id = state.id;
        state.current_term += 1;
        state.voted_for = Some(id);
        state.current_role = Role::Candidate;
        state.votes_received.clear();
        state.votes_received.insert(id);

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

        self.peers.vote_request(msg).await;
    }

    async fn handle_replicate_log(&mut self) {
        let state = &self.state;
        if state.current_role != Role::Leader {
            return;
        }
        let id = state.id;

        for i in 0..self.config.peers.len() {
            if i == id as usize {
                continue;
            }
            Self::replicate_log(state, id, i as u64, &self.peers).await;
        }
    }

    async fn replicate_log(
        state: &NodeState,
        leader_id: u64,
        follower_id: u64,
        peers: &RPCClients,
    ) {
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
        peers.log_request(msg, follower_id).await;
    }

    async fn handle_vote_request(
        &mut self,
        args: VoteRequestArgs,
        answer: oneshot::Sender<VoteResponseArgs>,
    ) {
        let state = &mut self.state;
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
        if args.cterm == state.current_term
            && log_ok
            && (state.voted_for.is_none() || state.voted_for.unwrap() == args.cid)
        {
            state.voted_for = Some(args.cid);
            granted = true;
        }

        let msg = VoteResponseArgs {
            voter_id: state.id,
            term: state.current_term,
            granted,
        };
        trace!("sent: {:?}", msg);

        answer.send(msg).expect("VoteResponse channel closed");
    }

    async fn handle_vote_response(&mut self, args: VoteResponseArgs) {
        let state = &mut self.state;
        trace!("received vote response and current state is {:?}", state);
        if state.current_role == Role::Candidate && args.term == state.current_term && args.granted
        {
            state.votes_received.insert(args.voter_id);
            trace!("votes received: {:?}", state.votes_received);
            // rounded up
            if state.votes_received.len() >= ((self.config.peers.len() + 1) + 1) / 2 {
                state.current_role = Role::Leader;
                state.current_leader = Some(state.id);
                // TODO: cancel election timer
                debug!("take over leadership with term {}", state.current_term);
                // append all pending messages to log
                let mut message_buffer = self.message_buffer.lock().await;
                for msg in message_buffer.iter() {
                    let entry = LogEntry {
                        term: state.current_term,
                        payload: msg.clone(),
                    };
                    state.log.push(entry);
                    let id = state.id as usize;
                    state.acked_length[id] = state.log.len() as u64;
                }
                message_buffer.clear();
                drop(message_buffer); // otherwise the compiler will complain about multiple borrows

                for i in 0..self.config.peers.len() {
                    if i == state.id as usize {
                        continue;
                    }
                    state.sent_length[i] = state.log.len() as u64;
                    state.acked_length[i] = 0;
                }
                self.handle_replicate_log().await;
            }
        } else if args.term > state.current_term {
            state.current_term = args.term;
            state.current_role = Role::Follower;
            state.voted_for = None;
            self.election_timer_reset_tx
                .send(())
                .await
                .expect("election_timer_reset_tx closed");
        }
    }

    async fn handle_log_request(
        &mut self,
        args: LogRequestArgs,
        answer: oneshot::Sender<LogResponseArgs>,
    ) {
        let state = &mut self.state;
        trace!("received log request and current state is {:?}", state);
        if args.term > state.current_term {
            state.current_term = args.term;
            state.voted_for = None;
            self.election_timer_reset_tx
                .send(())
                .await
                .expect("election_timer_reset_tx closed");
        }
        if args.term == state.current_term {
            if state.current_leader != Some(args.leader_id) {
                debug!("become follower of leader {}", args.leader_id);
                // TODO: is there a better way to do this?
                let message_buffer = self.message_buffer.clone();
                let peers = Arc::new(self.peers.clone());
                tokio::spawn(async move {
                    Self::forward_to_new_leader(message_buffer, peers, args.leader_id).await;
                });
            }
            state.current_role = Role::Follower;
            state.current_leader = Some(args.leader_id);
            self.election_timer_reset_tx
                .send(())
                .await
                .expect("election_timer_reset_tx closed");
        }
        let log_ok = (state.log.len() >= args.prefix_len as usize)
            && (args.prefix_len == 0
                || state.log[args.prefix_len as usize - 1].term == args.prefix_term);

        let mut ack = 0;
        let mut success = false;
        if args.term == state.current_term && log_ok {
            Self::append_entries(
                state,
                args.prefix_len,
                args.leader_commit,
                args.suffix.clone(),
                self.application_message_tx.clone(),
            )
            .await;
            ack = args.prefix_len + args.suffix.len() as u64;
            success = true;
        }
        let msg = LogResponseArgs {
            follower: state.id,
            term: state.current_term,
            ack,
            success,
        };
        trace!("sent: {:?}", msg);
        answer.send(msg).expect("LogResponse channel closed");
    }

    async fn append_entries(
        state: &mut NodeState,
        prefix_len: u64,
        leader_commit: u64,
        suffix: Vec<LogEntry>,
        application_message_tx: mpsc::Sender<Vec<u8>>,
    ) {
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
                application_message_tx
                    .send(state.log[i as usize].payload.clone())
                    .await
                    .expect("application_message_tx closed");
                debug!(
                    "deliver message in append_entries {:?}",
                    state.log[i as usize].payload
                );
            }
            state.commit_length = leader_commit;
        }
    }

    async fn handle_log_response(&mut self, args: LogResponseArgs) {
        let state = &mut self.state;
        trace!("received log response and current state is {:?}", state);
        if args.term == state.current_term && state.current_role == Role::Leader {
            if args.success && args.ack > state.acked_length[args.follower as usize] {
                state.sent_length[args.follower as usize] = args.ack;
                state.acked_length[args.follower as usize] = args.ack;
                Self::commit_log_entries(state, &self.config, self.application_message_tx.clone())
                    .await;
            } else if state.sent_length[args.follower as usize] > 0 {
                state.sent_length[args.follower as usize] -= 1;
                let id = state.id;
                Self::replicate_log(state, id, args.follower, &self.peers).await;
            }
        } else if args.term > state.current_term {
            state.current_term = args.term;
            state.current_role = Role::Follower;
            state.voted_for = None;
            self.election_timer_reset_tx
                .send(())
                .await
                .expect("election_tx closed");
        }
    }

    async fn handle_broadcast(&mut self, payload: Vec<u8>) {
        let state = &mut self.state;
        if state.current_role == Role::Leader {
            let entry = LogEntry {
                term: state.current_term,
                payload,
            };
            state.log.push(entry);
            let id = state.id as usize;
            state.acked_length[id] = state.log.len() as u64;
            self.handle_replicate_log().await;
        } else {
            // if leader is not known, buffer the message
            // else forward the message to leader
            // spawn a new task to avoid blocking the event loop
            let state = self.state.clone();
            let peers = self.peers.clone();
            let message_buffer = self.message_buffer.clone();
            tokio::spawn(async move {
                match state.current_leader {
                    Some(leader_id) => {
                        let msg = BroadcastArgs { payload };
                        let success = peers.forward_broadcast(msg.clone(), leader_id).await;
                        if !success {
                            // keep FIFO order
                            message_buffer.lock().await.push(msg.payload);
                        }
                    }
                    None => {
                        message_buffer.lock().await.push(payload);
                    }
                }
            });
        }
    }

    async fn forward_to_new_leader(
        message_buffer: Arc<Mutex<Vec<Vec<u8>>>>,
        peers: Arc<RPCClients>,
        new_leader: u64,
    ) {
        let mut message_buffer = message_buffer.lock().await;
        loop {
            let message = message_buffer.pop();
            if message.is_none() {
                break;
            }
            let msg = BroadcastArgs {
                payload: message.unwrap(),
            };
            let success = peers.forward_broadcast(msg.clone(), new_leader).await;
            if !success {
                // keep FIFO order
                message_buffer.insert(0, msg.payload);
                break;
            }
        }
    }

    async fn commit_log_entries(
        state: &mut NodeState,
        config: &RaftConfig,
        application_message_tx: mpsc::Sender<Vec<u8>>,
    ) {
        let min_acks = ((config.peers.len() + 1) + 1) / 2;
        let mut ready_max = 0;
        // here is an optimization: we don't need to iterate through the whole log
        for i in state.commit_length as usize..state.log.len() {
            if Self::acks(&state.acked_length, i as u64) >= min_acks as u64 {
                ready_max = i + 1;
            }
        }
        if ready_max > 0
            && ready_max > state.commit_length as usize
            && state.log[ready_max - 1].term == state.current_term
        {
            for i in state.commit_length as usize..ready_max {
                application_message_tx
                    .send(state.log[i].payload.clone())
                    .await
                    .expect("application_message_tx closed");
                debug!(
                    "deliver message in commit_log_entries {:?}",
                    state.log[i].payload
                );
            }
            state.commit_length = ready_max as u64;
        }
    }

    fn acks(acked_length: &Vec<u64>, length: u64) -> u64 {
        let mut acks = 0;
        for i in 0..acked_length.len() {
            if acked_length[i] >= length {
                acks += 1;
            }
        }
        acks
    }
}
