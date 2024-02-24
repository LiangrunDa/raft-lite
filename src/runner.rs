use crate::config::RaftConfig;
use crate::network::RPCClients;
use crate::persister::Persister;
use crate::raft_log::LogEntry;
use crate::raft_protocol::{
    BroadcastArgs, Event, LogRequestArgs, LogResponseArgs, NodeState, StepOutput, VoteRequestArgs,
    VoteResponseArgs,
};
use tokio::sync::mpsc;
use std::fmt;
use tracing::debug;

pub trait Runner {
    fn init_state(&mut self) -> NodeState;
    fn vote_request(&mut self, peer_id: u64, vote_request_args: VoteRequestArgs);
    fn log_request(&mut self, peer_id: u64, log_request_args: LogRequestArgs);
    fn forward_broadcast(&mut self, broadcast_args: BroadcastArgs);
    fn vote_response(&mut self, peer_id: u64, vote_response_args: VoteResponseArgs);
    fn log_response(&mut self, peer_id: u64, log_response_args: LogResponseArgs);
    fn election_timer_reset(&mut self);
    fn replicate_timer_reset(&mut self);
    fn deliver_message(&mut self, message: Vec<u8>);
    fn truncate_logs(&mut self, index: usize);
    fn append_log(&mut self, entry: LogEntry);
    fn change_term(&mut self, term: u64);
    fn change_voted_for(&mut self, voted_for: Option<u64>);
    fn change_current_leader(&mut self, leader: Option<u64>);
}

pub(crate) struct RealRunner {
    config: RaftConfig,
    persister: Persister,
    current_leader: Option<u64>,
    event_rx: mpsc::UnboundedReceiver<Event>,
    rpc_clients: RPCClients,
    message_buffer: Vec<Vec<u8>>,
    election_timer_reset_tx: mpsc::UnboundedSender<()>,
    replicate_timer_reset_tx: mpsc::UnboundedSender<()>,
    application_message_tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl RealRunner {
    pub(crate) fn new(
        config: RaftConfig,
        persister: Persister,
        event_rx: mpsc::UnboundedReceiver<Event>,
        rpc_clients: RPCClients,
        election_timer_reset_tx: mpsc::UnboundedSender<()>,
        replicate_timer_reset_tx: mpsc::UnboundedSender<()>,
        application_message_tx: mpsc::UnboundedSender<Vec<u8>>,
    ) -> Self {
        Self {
            config,
            persister,
            current_leader: None,
            event_rx,
            rpc_clients,
            message_buffer: vec![],
            election_timer_reset_tx,
            replicate_timer_reset_tx,
            application_message_tx,
        }
    }

    pub(crate) fn wait_for_event(&mut self) -> Option<Event> {
        self.event_rx.blocking_recv()
    }
}

impl Runner for RealRunner {
    fn change_current_leader(&mut self, leader: Option<u64>) {
        self.current_leader = leader;
        if let Some(peer_id) = self.current_leader {
            for message in self.message_buffer.drain(..) {
                debug!(
                    "Leader {} is elected, forward buffered message {:?} to leader",
                    peer_id, message
                );
                self.rpc_clients
                    .forward_broadcast(BroadcastArgs { payload: message }, peer_id);
            }
        }
    }

    fn truncate_logs(&mut self, index: usize) {
        self.persister.truncate_disk_log(index);
    }

    fn append_log(&mut self, entry: LogEntry) {
        self.persister.append_disk_log(entry);
    }

    fn change_term(&mut self, term: u64) {
        match self.persister.persist_term(term) {
            Ok(_) => {}
            Err(e) => {
                panic!("Failed to persist term: {}", e);
            }
        }
    }

    fn change_voted_for(&mut self, voted_for: Option<u64>) {
        // self.state.voted_for = voted_for;
        match self.persister.persist_voted_for(voted_for) {
            Ok(_) => {}
            Err(e) => {
                panic!("Failed to persist voted_for: {}", e);
            }
        }
    }

    fn vote_request(&mut self, peer_id: u64, vote_request_args: VoteRequestArgs) {
        self.rpc_clients.vote_request(vote_request_args, peer_id);
    }

    fn log_request(&mut self, peer_id: u64, log_request_args: LogRequestArgs) {
        self.rpc_clients.log_request(log_request_args, peer_id);
    }

    fn forward_broadcast(&mut self, broadcast_args: BroadcastArgs) {
        if let Some(peer_id) = self.current_leader {
            debug!(
                "Leader is elected, forward message {:?} to leader",
                broadcast_args.payload
            );
            self.rpc_clients.forward_broadcast(broadcast_args, peer_id);
        } else {
            debug!(
                "No leader elected, buffer the message {:?} for later delivery",
                broadcast_args.payload
            );
            self.message_buffer.push(broadcast_args.payload);
        }
    }

    fn vote_response(&mut self, peer_id: u64, vote_response_args: VoteResponseArgs) {
        self.rpc_clients.vote_response(vote_response_args, peer_id);
    }

    fn log_response(&mut self, peer_id: u64, log_response_args: LogResponseArgs) {
        self.rpc_clients.log_response(log_response_args, peer_id);
    }

    fn election_timer_reset(&mut self) {
        self.election_timer_reset_tx
            .send(())
            .expect("election timer reset channel closed");
    }

    fn replicate_timer_reset(&mut self) {
        self.replicate_timer_reset_tx
            .send(())
            .expect("replicate timer reset channel closed");
    }

    fn init_state(&mut self) -> NodeState {
        let state = self.persister.load_from_disk().unwrap_or_default();
        NodeState::from_persisted_state(state, self.config.id, self.config.peers.len())
    }

    fn deliver_message(&mut self, message: Vec<u8>) {
        debug!("Deliver message to application layer: {:?}", message);
        self.application_message_tx
            .send(message)
            .expect("application message channel closed");
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct CheckerRunner {
    returned_events: Vec<StepOutput>,
    message_buffer: Vec<Vec<u8>>,
    current_leader: Option<u64>,
    id: u64,
    peers: usize,
}

impl fmt::Debug for CheckerRunner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CheckerRunner")
            .field("message_buffer", &self.message_buffer)
            .finish()
    }
}

impl CheckerRunner {
    pub(crate) fn new(id: u64, peers: usize) -> Self {
        Self {
            returned_events: vec![],
            message_buffer: vec![],
            current_leader: None,
            id,
            peers,
        }
    }

    pub(crate) fn collect_output(&mut self) -> Vec<StepOutput> {
        std::mem::take(&mut self.returned_events)
    }
}

/// CheckerRunner is a mock runner for verifying purpose. It collects all output events returned by the RaftProtocol.
impl Runner for CheckerRunner {
    fn change_current_leader(&mut self, leader: Option<u64>) {
        self.current_leader = leader;
        if let Some(peer_id) = self.current_leader {
            for message in self.message_buffer.drain(..) {
                self.returned_events
                    .push(StepOutput::Broadcast(peer_id, message));
            }
        }
    }

    fn change_term(&mut self, _term: u64) {
        // do nothing because stateright doesn't support stop-recovery model
    }

    fn change_voted_for(&mut self, _voted_for: Option<u64>) {
        // do nothing because stateright doesn't support stop-recovery model
    }

    fn truncate_logs(&mut self, _index: usize) {
        // do nothing, because stateright doesn't support stop-recovery model
    }

    fn append_log(&mut self, _entry: LogEntry) {
        // do nothing because stateright doesn't support stop-recovery model
    }

    fn vote_request(&mut self, peer_id: u64, vote_request_args: VoteRequestArgs) {
        self.returned_events
            .push(StepOutput::VoteRequest(peer_id, vote_request_args));
    }

    fn log_request(&mut self, peer_id: u64, log_request_args: LogRequestArgs) {
        self.returned_events
            .push(StepOutput::LogRequest(peer_id, log_request_args));
    }

    fn forward_broadcast(&mut self, broadcast_args: BroadcastArgs) {
        if let Some(peer_id) = self.current_leader {
            self.returned_events
                .push(StepOutput::Broadcast(peer_id, broadcast_args.payload));
        } else {
            self.message_buffer.push(broadcast_args.payload);
        }
    }

    fn vote_response(&mut self, peer_id: u64, vote_response_args: VoteResponseArgs) {
        self.returned_events
            .push(StepOutput::VoteResponse(peer_id, vote_response_args));
    }

    fn log_response(&mut self, peer_id: u64, log_response_args: LogResponseArgs) {
        self.returned_events
            .push(StepOutput::LogResponse(peer_id, log_response_args));
    }

    fn election_timer_reset(&mut self) {
        self.returned_events.push(StepOutput::ElectionTimerReset);
    }

    fn replicate_timer_reset(&mut self) {
        self.returned_events.push(StepOutput::ReplicateTimerReset);
    }

    fn init_state(&mut self) -> NodeState {
        NodeState::new(self.id, self.peers)
    }

    fn deliver_message(&mut self, message: Vec<u8>) {
        self.returned_events
            .push(StepOutput::DeliverMessage(message));
    }
}
