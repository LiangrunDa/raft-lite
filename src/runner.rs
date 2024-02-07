use crate::config::RaftConfig;
use crate::network::RPCClients;
use crate::raft_protocol::{
    BroadcastArgs, Event, LogRequestArgs, LogResponseArgs, NodeState, VoteRequestArgs,
    VoteResponseArgs,
};
use tokio::sync::mpsc;
use tracing::debug;

pub(crate) trait Runner {
    fn wait_for_event(&mut self) -> Option<Event>;
    fn update_state(&mut self, state: &NodeState);
    fn vote_request(&self, peer_id: u64, vote_request_args: VoteRequestArgs);
    fn log_request(&self, peer_id: u64, log_request_args: LogRequestArgs);
    fn forward_broadcast(&mut self, broadcast_args: BroadcastArgs);
    fn vote_response(&self, peer_id: u64, vote_response_args: VoteResponseArgs);
    fn log_response(&self, peer_id: u64, log_response_args: LogResponseArgs);
    fn election_timer_reset(&self);
    fn replicate_timer_reset(&self);
    fn deliver_message(&self, message: Vec<u8>);
    fn init_state(&self) -> NodeState;
}

pub(crate) struct RealRunner {
    _config: RaftConfig,
    state: NodeState,
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
        state: NodeState,
        event_rx: mpsc::UnboundedReceiver<Event>,
        rpc_clients: RPCClients,
        election_timer_reset_tx: mpsc::UnboundedSender<()>,
        replicate_timer_reset_tx: mpsc::UnboundedSender<()>,
        application_message_tx: mpsc::UnboundedSender<Vec<u8>>,
    ) -> Self {
        Self {
            _config: config,
            state,
            event_rx,
            rpc_clients,
            message_buffer: vec![],
            election_timer_reset_tx,
            replicate_timer_reset_tx,
            application_message_tx,
        }
    }
}

impl Runner for RealRunner {
    fn wait_for_event(&mut self) -> Option<Event> {
        self.event_rx.blocking_recv()
    }

    fn update_state(&mut self, state: &NodeState) {
        // copy all fields except log
        self.state.id = state.id;
        self.state.current_term = state.current_term;
        self.state.voted_for = state.voted_for;
        self.state.commit_length = state.commit_length;
        self.state.current_role = state.current_role.clone();
        self.state.current_leader = state.current_leader;
        self.state.votes_received = state.votes_received.clone();
        self.state.sent_length = state.sent_length.clone();
        self.state.acked_length = state.acked_length.clone();

        if let Some(peer_id) = self.state.current_leader {
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

    fn vote_request(&self, peer_id: u64, vote_request_args: VoteRequestArgs) {
        self.rpc_clients.vote_request(vote_request_args, peer_id);
    }

    fn log_request(&self, peer_id: u64, log_request_args: LogRequestArgs) {
        self.rpc_clients.log_request(log_request_args, peer_id);
    }

    fn forward_broadcast(&mut self, broadcast_args: BroadcastArgs) {
        if let Some(peer_id) = self.state.current_leader {
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

    fn vote_response(&self, peer_id: u64, vote_response_args: VoteResponseArgs) {
        self.rpc_clients.vote_response(vote_response_args, peer_id);
    }

    fn log_response(&self, peer_id: u64, log_response_args: LogResponseArgs) {
        self.rpc_clients.log_response(log_response_args, peer_id);
    }

    fn election_timer_reset(&self) {
        self.election_timer_reset_tx
            .send(())
            .expect("election timer reset channel closed");
    }

    fn replicate_timer_reset(&self) {
        self.replicate_timer_reset_tx
            .send(())
            .expect("replicate timer reset channel closed");
    }

    fn init_state(&self) -> NodeState {
        self.state.clone()
    }

    fn deliver_message(&self, message: Vec<u8>) {
        debug!("Deliver message to application layer: {:?}", message);
        self.application_message_tx
            .send(message)
            .expect("application message channel closed");
    }
}

// TODO: model checker runner
