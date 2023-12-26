use crate::config::RaftConfig;
use crate::raft_protocol::Event;
use crate::raft_protocol::{LogRequestArgs, LogResponseArgs, VoteRequestArgs, VoteResponseArgs};
use futures::{future, prelude::*};
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;
use std::time::Duration;
use stubborn_io::{ReconnectOptions, StubbornTcpStream};
use tarpc::client::Config;
use tarpc::context::Context;
use tarpc::serde_transport::Transport;
use tarpc::server::incoming::Incoming;
use tarpc::server::Channel;
use tarpc::tokio_serde::formats::Json;
use tarpc::{context, server};
use tokio::sync::mpsc;
use tracing::info;

pub(crate) struct RPCServer(pub(crate) Arc<tokio::sync::mpsc::Sender<Event>>);

/// RPC for Raft
#[tarpc::service]
pub(crate) trait RaftRPC {
    /// VoteRequest RPC
    async fn vote_request(args: VoteRequestArgs) -> VoteResponseArgs;
    /// LogRequest RPC
    async fn log_request(args: LogRequestArgs) -> LogResponseArgs;
}

#[tarpc::server]
impl RaftRPC for RPCServer {
    async fn vote_request(self, _: Context, args: VoteRequestArgs) -> VoteResponseArgs {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.0
            .send(Event::VoteRequest(args, tx))
            .await
            .expect("event_tx closed");
        rx.await.expect("VoteResponse channel closed")
    }

    async fn log_request(self, _: Context, args: LogRequestArgs) -> LogResponseArgs {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.0
            .send(Event::LogRequest(args, tx))
            .await
            .expect("event_tx closed");
        rx.await.expect("LogResponse channel closed")
    }
}

pub(crate) async fn start_server(server_addr: &str, event_tx: mpsc::Sender<Event>) {
    let mut listener = tarpc::serde_transport::tcp::listen(server_addr, Json::default)
        .await
        .unwrap();
    info!("Listening on port {}", listener.local_addr().port());
    listener.config_mut().max_frame_length(usize::MAX);
    listener
        .filter_map(|r| future::ready(r.ok()))
        .map(server::BaseChannel::with_defaults)
        .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
        .map(|channel| {
            let server = RPCServer(Arc::new(event_tx.clone()));
            channel.execute(server.serve())
        })
        .for_each(|_| async {})
        .await
}

pub(crate) enum RpcRequestArgs {
    LogRequest(LogRequestArgs),
    VoteRequest(VoteRequestArgs),
}

pub(crate) struct Connection {
    peer_addr: String,
    rpc_request_rx: mpsc::Receiver<RpcRequestArgs>,
}

impl Connection {
    pub(crate) fn new(
        peer_addr: String,
        rpc_request_rx: mpsc::Receiver<RpcRequestArgs>,
    ) -> Self {
        Self {
            peer_addr,
            rpc_request_rx,
        }
    }

    pub(crate) async fn handle(&mut self) {
        // TODO: this guarantees all messages will be sent in order and never got lost, which is unnecessary
        let reconnect_opts = ReconnectOptions::new()
            .with_exit_if_first_connect_fails(false)
            .with_retries_generator(|| iter::repeat(Duration::from_secs(5)));
        let tcp_stream =
            StubbornTcpStream::connect_with_options(self.peer_addr.clone(), reconnect_opts)
                .await
                .unwrap();
        let transport = Transport::from((tcp_stream, Json::default()));
        let client_stub = RaftRPCClient::new(Config::default(), transport).spawn();

        loop {
            let args = self
                .rpc_request_rx
                .recv()
                .await
                .expect("rpc_request_rx closed");
            match args {
                RpcRequestArgs::LogRequest(args) => {
                    client_stub
                        .log_request(context::current(), args)
                        .await
                        .unwrap();
                }
                RpcRequestArgs::VoteRequest(args) => {
                    client_stub
                        .vote_request(context::current(), args)
                        .await
                        .unwrap();
                }
            };
        }
    }
}

pub(crate) struct RPCClients {
    pub(crate) clients_tx: Arc<HashMap<u64, mpsc::Sender<RpcRequestArgs>>>,
}

impl RPCClients {
    pub(crate) fn new(config: RaftConfig) -> Self {
        let mut clients_tx = HashMap::new();
        for (idx, peer) in config.peers.iter().enumerate() {
            if idx == config.id as usize {
                continue;
            }
            let (tx, rx) = mpsc::channel(100);
            clients_tx.insert(idx as u64, tx);
            let p = peer.clone();
            tokio::spawn(async move {
                let mut peer_client = Connection::new(p, rx);
                peer_client.handle().await;
            });
        }
        Self {
            clients_tx: Arc::new(clients_tx),
        }
    }

    pub(crate) fn log_request(&self, args: LogRequestArgs) {
        for (_, tx) in self.clients_tx.iter() {
            tx.try_send(RpcRequestArgs::LogRequest(args.clone()))
                .unwrap();
        }
    }

    pub(crate) fn vote_request(&self, args: VoteRequestArgs) {
        for (_, tx) in self.clients_tx.iter() {
            tx.try_send(RpcRequestArgs::VoteRequest(args.clone()))
                .unwrap();
        }
    }
}
