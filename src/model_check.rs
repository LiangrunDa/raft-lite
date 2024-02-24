use crate::config::{RaftConfig, RaftParams};
use crate::raft_protocol::Event;
use crate::raft_protocol::{
    LogRequestArgs, LogResponseArgs, RaftProtocol, StepOutput, VoteRequestArgs, VoteResponseArgs,
};
use crate::runner::{CheckerRunner};
use stateright::actor::model_timeout;
use stateright::actor::{Actor, ActorModel, Id, Network, Out};
use stateright::Expectation;
use std::borrow::Cow;
use std::collections::HashSet;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CheckerActor {
    peer_ids: Vec<String>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CheckerMessage {
    VoteRequest(VoteRequestArgs),
    VoteResponse(VoteResponseArgs),
    LogRequest(LogRequestArgs),
    LogResponse(LogResponseArgs),
    Broadcast(Vec<u8>),
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum CheckerTimer {
    ElectionTimeout,
    ReplicationTimeout,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CheckerState {
    delivered_messages: Vec<Vec<u8>>,
    raft_protocol: RaftProtocol<CheckerRunner>,
}

fn process_event(
    state: &mut Cow<CheckerState>,
    event: Event,
    o: &mut Out<CheckerActor>,
) {
    let state = state.to_mut();
    let step_output = state.raft_protocol.step(event);
    for output in step_output {
        match output {
            StepOutput::DeliverMessage(payload) => {
                state.delivered_messages.push(payload);
            }
            StepOutput::VoteRequest(peer_id, vote_request_args) => {
                o.send(
                    Id::from(peer_id as usize),
                    CheckerMessage::VoteRequest(vote_request_args),
                );
            }
            StepOutput::VoteResponse(peer_id, vote_response_args) => {
                o.send(
                    Id::from(peer_id as usize),
                    CheckerMessage::VoteResponse(vote_response_args),
                );
            }
            StepOutput::LogRequest(peer_id, log_request_args) => {
                o.send(
                    Id::from(peer_id as usize),
                    CheckerMessage::LogRequest(log_request_args),
                );
            }
            StepOutput::LogResponse(peer_id, log_response_args) => {
                o.send(
                    Id::from(peer_id as usize),
                    CheckerMessage::LogResponse(log_response_args),
                );
            }
            StepOutput::Broadcast(peer_id, payload) => {
                o.send(
                    Id::from(peer_id as usize),
                    CheckerMessage::Broadcast(payload),
                );
            }
            StepOutput::ElectionTimerReset => {
                o.set_timer(CheckerTimer::ElectionTimeout, model_timeout());
            }
            StepOutput::ReplicateTimerReset => {
                o.set_timer(CheckerTimer::ReplicationTimeout, model_timeout());
            }
        }
    }
}

impl Actor for CheckerActor {
    type Msg = CheckerMessage;
    type Timer = CheckerTimer;
    type State = CheckerState;

    fn on_start(&self, id: Id, o: &mut Out<Self>) -> Self::State {
        let id: usize = id.into();
        let checker_runner = CheckerRunner::new(id as u64, self.peer_ids.len());

        let raft_config = RaftConfig::new(
            self.peer_ids.clone(),
            id.to_string(),
            RaftParams::default(),
            None,
        );
        let raft_protocol = RaftProtocol::new(raft_config, checker_runner);
        let state = CheckerState {
            delivered_messages: vec![],
            raft_protocol,
        };

        // set timer
        o.set_timer(CheckerTimer::ElectionTimeout, model_timeout());
        o.set_timer(CheckerTimer::ReplicationTimeout, model_timeout());
        // broadcast a message (the id of the actor)
        o.send(
            Id::from(id),
            CheckerMessage::Broadcast(id.to_string().into_bytes()),
        );
        state
    }

    fn on_msg(
        &self,
        _id: Id,
        state: &mut Cow<Self::State>,
        _src: Id,
        msg: Self::Msg,
        o: &mut Out<Self>,
    ) {
        let event = match msg {
            CheckerMessage::VoteRequest(vote_request_args) => Event::VoteRequest(vote_request_args),
            CheckerMessage::VoteResponse(vote_response_args) => {
                Event::VoteResponse(vote_response_args)
            }
            CheckerMessage::LogRequest(log_request_args) => Event::LogRequest(log_request_args),
            CheckerMessage::LogResponse(log_response_args) => Event::LogResponse(log_response_args),
            CheckerMessage::Broadcast(payload) => Event::Broadcast(payload),
        };
        process_event(state, event, o);
    }

    fn on_timeout(
        &self,
        _id: Id,
        state: &mut Cow<Self::State>,
        timer: &Self::Timer,
        o: &mut Out<Self>,
    ) {
        let event = match timer {
            CheckerTimer::ElectionTimeout => Event::ElectionTimeout,
            CheckerTimer::ReplicationTimeout => Event::ReplicationTimeout,
        };
        process_event(state, event, o);
    }
}

#[derive(Clone)]
pub struct RaftModelCfg {
    pub server_count: usize,
    pub network: Network<<CheckerActor as Actor>::Msg>,
}

impl RaftModelCfg {
    pub fn into_model(self) -> ActorModel<CheckerActor, Self> {
        let peers: Vec<String> = (0..self.server_count).map(|i| i.to_string()).collect();
        ActorModel::new(self.clone(), ())
            .max_crashes((self.server_count - 1) / 2)
            .actors((0..self.server_count).map(|_| CheckerActor {
                peer_ids: peers.clone(),
            }))
            .init_network(self.network)
            .property(Expectation::Sometimes, "Election Liveness", |_, state| {
                state.actor_states.iter().any(|s| {
                    s.raft_protocol.state.current_role == crate::raft_protocol::Role::Leader
                })
            })
            .property(Expectation::Sometimes, "Log Liveness", |_, state| {
                state
                    .actor_states
                    .iter()
                    .any(|s| s.raft_protocol.state.commit_length > 0)
            })
            .property(Expectation::Always, "Election Safety", |_, state| {
                // at most one leader can be elected in a given term

                let mut leaders_term = HashSet::new();
                for s in &state.actor_states {
                    if s.raft_protocol.state.current_role == crate::raft_protocol::Role::Leader
                        && !leaders_term.insert(s.raft_protocol.state.current_term)
                    {
                        return false;
                    }
                }
                true
            })
            .property(Expectation::Always, "State Machine Safety", |_, state| {
                // if a server has applied a log entry at a given index to its state machine, no other server will
                // ever apply a different log entry for the same index.

                let mut max_commit_length = 0;
                let mut max_commit_length_actor_id = 0;
                for (i, s) in state.actor_states.iter().enumerate() {
                    if s.delivered_messages.len() > max_commit_length {
                        max_commit_length = s.delivered_messages.len();
                        max_commit_length_actor_id = i;
                    }
                }
                if max_commit_length == 0 {
                    return true;
                }

                for i in 0..max_commit_length {
                    let ref_log = state.actor_states[max_commit_length_actor_id]
                        .delivered_messages
                        .get(i)
                        .unwrap();
                    for s in &state.actor_states {
                        if let Some(log) = s.delivered_messages.get(i) {
                            if log != ref_log {
                                println!("log mismatch: {:?} != {:?}", log, ref_log);
                                return false;
                            }
                        }
                    }
                }
                true
            })
    }
}
