/// Event loop implementation for Raft protocol
use crate::config::RaftConfig;
use crate::persister::PersistRaftState;
use crate::raft_log::LogEntry;
use crate::runner::{CheckerRunner, RealRunner, Runner};
use core::hash::Hash;
use std::cmp::min;
use std::collections::HashSet;
use std::sync::Arc;
use tarpc::serde;
use tracing::{debug, trace};
use std::fmt;

#[derive(PartialEq, Hash, Eq, Clone, Debug)]
pub(crate) enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeState {
    pub(crate) id: u64,
    pub(crate) current_term: u64,
    pub(crate) voted_for: Option<u64>,
    pub(crate) log: Vec<LogEntry>,
    // TODO: snapshot: use a wrapper to keep the log and the snapshot_index
    pub(crate) commit_length: u64,
    pub(crate) current_role: Role,
    pub(crate) current_leader: Option<u64>,
    pub(crate) votes_received: HashSet<u64>,
    pub(crate) sent_length: Vec<u64>,
    pub(crate) acked_length: Vec<u64>,
}

/// Hash is implemented for Stateright model checking
impl Hash for NodeState {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
        self.current_term.hash(state);
        self.voted_for.hash(state);
        self.log.hash(state);
        self.commit_length.hash(state);
        self.current_role.hash(state);
        self.current_leader.hash(state);
        // sort the votes_received to make sure the hash is deterministic
        let mut votes_received: Vec<u64> = self.votes_received.iter().cloned().collect();
        votes_received.sort();
        votes_received.hash(state);
        self.sent_length.hash(state);
        self.acked_length.hash(state);
    }
}

impl NodeState {
    pub(crate) fn new(id: u64, num_peers: usize) -> Self {
        let persisted_state = PersistRaftState::default();
        Self::from_persisted_state(persisted_state, id, num_peers)
    }

    pub(crate) fn from_persisted_state(
        persisted_state: PersistRaftState,
        id: u64,
        num_peers: usize,
    ) -> NodeState {
        let PersistRaftState {
            current_term,
            voted_for,
            log,
        } = persisted_state;
        Self {
            id,
            current_term,
            voted_for,
            log,
            commit_length: 0,
            current_role: Role::Follower,
            current_leader: None,
            votes_received: HashSet::new(),
            sent_length: vec![0; num_peers],
            acked_length: vec![0; num_peers],
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Hash, PartialEq, Eq)]
pub struct LogRequestArgs {
    pub(crate) leader_id: u64,
    pub(crate) term: u64,
    pub(crate) prefix_len: u64,
    pub(crate) prefix_term: u64,
    pub(crate) leader_commit: u64,
    pub(crate) suffix: Vec<LogEntry>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Hash, PartialEq, Eq)]
pub struct LogResponseArgs {
    pub(crate) follower: u64,
    pub(crate) term: u64,
    pub(crate) ack: u64,
    pub(crate) success: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Hash, PartialEq, Eq)]
pub struct VoteRequestArgs {
    pub(crate) cid: u64,
    pub(crate) cterm: u64,
    pub(crate) clog_length: u64,
    pub(crate) clog_term: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Hash, PartialEq, Eq)]
pub struct VoteResponseArgs {
    pub(crate) voter_id: u64,
    pub(crate) term: u64,
    pub(crate) granted: bool,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Hash, PartialEq, Eq)]
pub struct BroadcastArgs {
    pub(crate) payload: Vec<u8>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum Event {
    ElectionTimeout,
    ReplicationTimeout,
    VoteRequest(VoteRequestArgs),
    VoteResponse(VoteResponseArgs),
    LogRequest(LogRequestArgs),
    LogResponse(LogResponseArgs),
    Broadcast(Vec<u8>),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub(crate) enum StepOutput {
    VoteRequest(u64, VoteRequestArgs),
    VoteResponse(u64, VoteResponseArgs),
    LogRequest(u64, LogRequestArgs),
    LogResponse(u64, LogResponseArgs),
    Broadcast(u64, Vec<u8>),
    ElectionTimerReset,
    ReplicateTimerReset,
    DeliverMessage(Vec<u8>),
}

#[derive(Clone, Hash, PartialEq, Eq)]
pub(crate) struct RaftProtocol<T: Runner> {
    /// Raft-specific state
    pub(crate) state: NodeState,
    /// raft config for the cluster
    config: RaftConfig,
    /// runner on top of the raft protocol
    runner: T,
}

/// For interactive model checking
impl fmt::Debug for RaftProtocol<CheckerRunner> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaftProtocol")
            .field("state", &self.state)
            .field("runner", &self.runner)
            .finish()
    }
}

impl RaftProtocol<RealRunner> {
    pub(crate) fn run(&mut self) {
        // event loop
        loop {
            let event = self.runner.wait_for_event();
            if let Some(event) = event {
                self.dispatch_event(event);
            } else {
                trace!("event_rx closed, exit event loop");
                break;
            }
        }
    }
}

impl RaftProtocol<CheckerRunner> {
    pub(crate) fn step(&mut self, event: Event) -> Vec<StepOutput> {
        self.dispatch_event(event);
        self.runner.collect_output()
    }
}

impl<T: Runner> RaftProtocol<T> {
    pub(crate) fn new(config: RaftConfig, mut runner: T) -> Self {
        Self {
            config: config.clone(),
            state: runner.init_state(),
            runner,
        }
    }

    fn dispatch_event(&mut self, event: Event) {
        debug!("dispatch event: {:?}", event);
        match event {
            Event::ElectionTimeout => {
                self.start_election();
            }
            Event::ReplicationTimeout => {
                self.handle_replicate_log();
            }
            Event::VoteRequest(args) => {
                self.handle_vote_request(args);
            }
            Event::VoteResponse(args) => {
                self.handle_vote_response(args);
            }
            Event::LogRequest(args) => {
                self.handle_log_request(args);
            }
            Event::LogResponse(args) => {
                self.handle_log_response(args);
            }
            Event::Broadcast(payload) => {
                self.handle_broadcast(payload);
            }
        }
    }

    fn start_election(&mut self) {
        let state = &mut self.state;
        if state.current_role == Role::Leader {
            return;
        }
        let id = state.id;
        state.current_term += 1;
        self.runner.change_term(state.current_term);
        state.voted_for = Some(id);
        self.runner.change_voted_for(Some(id));
        state.current_role = Role::Candidate;
        state.votes_received.clear();
        state.votes_received.insert(id);

        let mut last_term = 0;
        if state.log.len() > 0 {
            last_term = state.log.last().unwrap().term;
        }

        let msg = VoteRequestArgs {
            cid: id,
            cterm: state.current_term,
            clog_length: state.log.len() as u64,
            clog_term: last_term,
        };
        for i in 0..self.config.peers.len() {
            if i == id as usize {
                continue;
            }
            self.runner.vote_request(i as u64, msg.clone());
        }
    }

    fn handle_replicate_log(&mut self) {
        let state = &self.state;
        let runner = &mut self.runner;
        if state.current_role != Role::Leader {
            return;
        }
        let id = state.id;

        for i in 0..self.config.peers.len() {
            if i == id as usize {
                continue;
            }
            Self::replicate_log(state, id, i as u64, runner);
        }
    }

    fn replicate_log(state: &NodeState, leader_id: u64, follower_id: u64, runner: &mut T) {
        let prefix_len = state.sent_length[follower_id as usize];
        debug!("prefix_len: {}", prefix_len);
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
        runner.log_request(follower_id, msg);
    }

    fn handle_vote_request(&mut self, args: VoteRequestArgs) {
        let state = &mut self.state;
        trace!("received {:?} and current state is {:?}", args, state);
        if args.cterm > state.current_term {
            state.current_term = args.cterm;
            self.runner.change_term(state.current_term);
            state.current_role = Role::Follower;
            state.voted_for = None;
            self.runner.change_voted_for(None);
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
            self.runner.change_voted_for(Some(args.cid));
            granted = true;
        }

        let msg = VoteResponseArgs {
            voter_id: state.id,
            term: state.current_term,
            granted,
        };
        trace!("sent: {:?}", msg);
        self.runner.vote_response(args.cid, msg);
    }

    fn handle_vote_response(&mut self, args: VoteResponseArgs) {
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
                self.runner.change_current_leader(state.current_leader);
                // TODO: cancel election timer
                debug!(
                    "{} take over leadership with term {}",
                    state.id, state.current_term
                );
                for i in 0..self.config.peers.len() {
                    if i == state.id as usize {
                        continue;
                    }
                    state.sent_length[i] = state.log.len() as u64;
                    state.acked_length[i] = 0;
                }
                self.handle_replicate_log();
            }
        } else if args.term > state.current_term {
            state.current_term = args.term;
            self.runner.change_term(state.current_term);
            state.current_role = Role::Follower;
            state.voted_for = None;
            self.runner.change_voted_for(None);
            self.runner.election_timer_reset();
        }
    }

    fn handle_log_request(&mut self, args: LogRequestArgs) {
        let state = &mut self.state;
        trace!("received log request and current state is {:?}", state);
        if args.term > state.current_term {
            state.current_term = args.term;
            self.runner.change_term(state.current_term);
            state.voted_for = None;
            self.runner.change_voted_for(None);
            self.runner.election_timer_reset();
        }
        if args.term == state.current_term {
            state.current_role = Role::Follower;
            state.current_leader = Some(args.leader_id);
            self.runner.change_current_leader(state.current_leader);
            self.runner.election_timer_reset();
        }
        let log_ok = (state.log.len() >= args.prefix_len as usize)
            && (args.prefix_len == 0
                || state.log[args.prefix_len as usize - 1].term == args.prefix_term);

        let mut ack = 0;
        let mut success = false;
        let runner = &mut self.runner;
        if args.term == state.current_term && log_ok {
            Self::append_entries(
                state,
                args.prefix_len,
                args.leader_commit,
                args.suffix.clone(),
                runner,
            );
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

        self.runner.log_response(args.leader_id, msg);
    }

    fn append_entries(
        state: &mut NodeState,
        prefix_len: u64,
        leader_commit: u64,
        suffix: Vec<LogEntry>,
        runner: &mut T,
    ) {
        debug!("follower {} append entries: {:?}", state.id, suffix);
        if suffix.len() > 0 && state.log.len() > prefix_len as usize {
            let index = min(state.log.len(), prefix_len as usize + suffix.len()) - 1;
            if state.log[index].term != suffix[index - prefix_len as usize].term {
                // state.log = state.log[..prefix_len as usize].to_vec();
                // below is an optimization
                state.log.truncate(prefix_len as usize);
                runner.truncate_logs(prefix_len as usize);
            }
        }
        if prefix_len as usize + suffix.len() > state.log.len() {
            for i in state.log.len() - prefix_len as usize..suffix.len() {
                state.log.push(suffix[i].clone());
                runner.append_log(suffix[i].clone());
            }
        }
        if leader_commit > state.commit_length {
            for i in state.commit_length..leader_commit {
                runner.deliver_message(state.log[i as usize].payload.to_vec());
            }
            state.commit_length = leader_commit;
        }
    }

    fn handle_log_response(&mut self, args: LogResponseArgs) {
        let state = &mut self.state;
        let runner = &mut self.runner;
        trace!("received log response and current state is {:?}", state);
        if args.term == state.current_term && state.current_role == Role::Leader {
            debug!("leader {} received log response: {:?}", state.id, args);
            if args.success && args.ack >= state.acked_length[args.follower as usize] {
                state.sent_length[args.follower as usize] = args.ack;
                state.acked_length[args.follower as usize] = args.ack;
                Self::commit_log_entries(state, &self.config, runner);
            } else if state.sent_length[args.follower as usize] > 0 {
                state.sent_length[args.follower as usize] -= 1;
                let id = state.id;
                Self::replicate_log(state, id, args.follower, runner);
            }
        } else if args.term > state.current_term {
            state.current_term = args.term;
            self.runner.change_term(state.current_term);
            state.current_role = Role::Follower;
            state.voted_for = None;
            self.runner.change_voted_for(None);
            self.runner.election_timer_reset();
        }
    }

    fn handle_broadcast(&mut self, payload: Vec<u8>) {
        let state = &mut self.state;
        if state.current_role == Role::Leader {
            let entry = LogEntry {
                term: state.current_term,
                payload: Arc::new(payload),
            };
            debug!("leader {} append log entry: {:?}", state.id, entry);
            state.log.push(entry.clone());
            self.runner.append_log(entry.clone());
            let id = state.id as usize;
            state.acked_length[id] = state.log.len() as u64;
            self.handle_replicate_log();
        } else {
            self.runner.forward_broadcast(BroadcastArgs { payload });
        }
    }

    fn commit_log_entries(state: &mut NodeState, config: &RaftConfig, runner: &mut T) {
        let min_acks = ((config.peers.len() + 1) + 1) / 2;
        let mut ready_max = 0;
        // here is an optimization: we don't need to iterate through the whole log
        for i in state.commit_length as usize + 1..state.log.len() + 1 {
            if Self::acks(&state.acked_length, i as u64) >= min_acks as u64 {
                ready_max = i;
            }
        }
        debug!("leader {} ready_max: {}", state.id, ready_max);
        if ready_max > 0 && state.log[ready_max - 1].term == state.current_term {
            for i in state.commit_length as usize..ready_max {
                runner.deliver_message(state.log[i].payload.to_vec());
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
