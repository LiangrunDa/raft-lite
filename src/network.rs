use crate::config::RaftConfig;
use crate::raft_protocol::{BroadcastArgs, Event};
use crate::raft_protocol::{LogRequestArgs, LogResponseArgs, VoteRequestArgs, VoteResponseArgs};
use anyhow::Result;
use futures::{future, prelude::*};
use std::collections::HashMap;
use std::sync::Arc;
use tarpc::client::Config;
use tarpc::context::Context;
use tarpc::serde_transport::Transport;
use tarpc::server::incoming::Incoming;
use tarpc::server::Channel;
use tarpc::tokio_serde::formats::Json;
use tarpc::{context, server};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, trace, warn};

#[derive(Clone)]
pub(crate) struct RPCServer(pub(crate) Arc<mpsc::Sender<Event>>);

/// RPC for Raft
#[tarpc::service]
pub(crate) trait RaftRPC {
    /// VoteRequest RPC
    async fn vote_request(args: VoteRequestArgs) -> VoteResponseArgs;
    /// LogRequest RPC
    async fn log_request(args: LogRequestArgs) -> LogResponseArgs;
    /// Broadcast RPC
    async fn broadcast(args: BroadcastArgs);
}

#[tarpc::server]
impl RaftRPC for RPCServer {
    async fn vote_request(self, _: Context, args: VoteRequestArgs) -> VoteResponseArgs {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.0
            .send(Event::VoteRequest(args.clone(), tx))
            .await
            .expect("event_tx closed");
        let res = rx.await.expect("VoteResponse channel closed");
        // trace!("Received VoteRequest {:?}, and sent VoteResponse {:?}", args, res);
        res
    }

    async fn log_request(self, _: Context, args: LogRequestArgs) -> LogResponseArgs {
        // trace!("Received log request: {:?}", args);
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.0
            .send(Event::LogRequest(args.clone(), tx))
            .await
            .expect("event_tx closed");
        let res = rx.await.expect("LogResponse channel closed");
        // trace!("Received LogRequest {:?}, and sent LogResponse {:?}", args, res);
        res
    }

    async fn broadcast(self, _: Context, args: BroadcastArgs) {
        trace!("Received broadcast: {:?} from peer", args);
        self.0
            .send(Event::Broadcast(args.payload))
            .await
            .expect("event_tx closed");
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
        // Limit channels to 10 per IP.
        .max_channels_per_key(10, |t| t.transport().peer_addr().unwrap().ip())
        .map(|channel| {
            let server = RPCServer(Arc::new(event_tx.clone()));
            channel.execute(server.serve())
        })
        // max 200 channels
        .buffer_unordered(200)
        .for_each(|_| async {})
        .await
}

pub(crate) enum RpcRequestArgs {
    LogRequest(LogRequestArgs),
    VoteRequest(VoteRequestArgs),
    ForwardBroadcast(BroadcastArgs, oneshot::Sender<bool>),
}

pub(crate) struct Connection {
    peer_addr: String,
    rpc_request_rx: mpsc::Receiver<RpcRequestArgs>,
    event_tx: mpsc::Sender<Event>,
    client_stub: Option<RaftRPCClient>,
}

impl Connection {
    pub(crate) fn new(
        peer_addr: String,
        rpc_request_rx: mpsc::Receiver<RpcRequestArgs>,
        event_tx: mpsc::Sender<Event>,
    ) -> Self {
        Self {
            peer_addr,
            rpc_request_rx,
            event_tx,
            client_stub: None,
        }
    }

    pub(crate) async fn try_reconnect(&mut self) -> Result<&RaftRPCClient> {
        match TcpStream::connect(&self.peer_addr).await {
            Ok(stream) => {
                info!("Connected to {}", self.peer_addr);
                let transport = Transport::from((stream, Json::default()));
                self.client_stub = Some(RaftRPCClient::new(Config::default(), transport).spawn());
                Ok(self.client_stub.as_ref().unwrap())
            }
            Err(e) => {
                warn!("Failed to connect to {}: {:?}", self.peer_addr, e);
                self.client_stub = None;
                Err(e.into())
            }
        }
    }

    pub(crate) async fn handle(&mut self) {
        loop {
            let args = self
                .rpc_request_rx
                .recv()
                .await
                .expect("rpc_request_rx closed");

            // The logic is:
            // 1. If client_stub is not None, use it to send request
            // 1.1 If the request is successful, continue
            // 1.2 If the request is failed, set client_stub to None and continue
            // 2. If client_stub is None, try to reconnect
            // 2.1 If reconnect is successful, continue to send request
            // 2.2 If reconnect is failed, go back to next loop
            let client = if let Some(client) = &self.client_stub {
                client
            } else {
                match self.try_reconnect().await {
                    Ok(client) => client,
                    Err(_) => continue,
                }
            };

            match args {
                RpcRequestArgs::LogRequest(args) => {
                    match client.log_request(context::current(), args.clone()).await {
                        Ok(res) => {
                            trace!(
                                "Sent LogRequest {:?} successfully, and received LogResponse {:?}",
                                args,
                                res
                            );
                            self.event_tx
                                .send(Event::LogResponse(res))
                                .await
                                .expect("event_tx closed");
                        }
                        Err(e) => {
                            warn!("Sent LogRequest {:?} failed: {:?}", args, e);
                            if e.to_string().contains("shutdown") {
                                // force to reconnect next time
                                self.client_stub = None;
                            }
                        }
                    }
                }

                RpcRequestArgs::VoteRequest(args) => {
                    match client.vote_request(context::current(), args.clone()).await {
                        Ok(res) => {
                            trace!("Sent VoteRequest {:?} successfully, and received VoteResponse {:?}", args, res);
                            self.event_tx
                                .send(Event::VoteResponse(res))
                                .await
                                .expect("event_tx closed");
                        }
                        Err(e) => {
                            warn!("Sent VoteRequest {:?} failed: {:?}", args, e);
                            if e.to_string().contains("shutdown") {
                                self.client_stub = None;
                            }
                        }
                    }
                }

                RpcRequestArgs::ForwardBroadcast(args, answer) => {
                    match client.broadcast(context::current(), args.clone()).await {
                        Ok(_) => {
                            trace!("Sent Broadcast {:?} successfully", args);
                            answer.send(true).expect("answer channel closed");
                        }
                        Err(e) => {
                            warn!("Sent Broadcast {:?} failed: {:?}", args, e);
                            if e.to_string().contains("shutdown") {
                                self.client_stub = None;
                            }
                            answer.send(false).expect("answer channel closed");
                        }
                    }
                }
            };
        }
    }
}

#[derive(Clone)]
pub(crate) struct RPCClients {
    pub(crate) clients_tx: Arc<HashMap<u64, mpsc::Sender<RpcRequestArgs>>>,
}

impl RPCClients {
    pub(crate) fn new(config: RaftConfig, event_tx: mpsc::Sender<Event>) -> Self {
        let mut clients_tx = HashMap::new();
        for (idx, peer) in config.peers.iter().enumerate() {
            if idx == config.id as usize {
                continue;
            }
            let (tx, rx) = mpsc::channel(100);
            clients_tx.insert(idx as u64, tx);
            let p = peer.clone();
            let event_tx = event_tx.clone();
            tokio::spawn(async move {
                let mut peer_client = Connection::new(p, rx, event_tx);
                peer_client.handle().await;
            });
        }
        Self {
            clients_tx: Arc::new(clients_tx),
        }
    }

    pub(crate) async fn log_request(&self, args: LogRequestArgs, idx: u64) {
        if let Some(tx) = self.clients_tx.get(&idx) {
            tx.send(RpcRequestArgs::LogRequest(args))
                .await
                .expect("rpc_request_tx closed");
        }
        // TODO: else panic?
    }

    pub(crate) async fn vote_request(&self, args: VoteRequestArgs) {
        for (_, tx) in self.clients_tx.iter() {
            tx.send(RpcRequestArgs::VoteRequest(args.clone()))
                .await
                .expect("rpc_request_tx closed");
        }
    }

    pub(crate) async fn forward_broadcast(&self, args: BroadcastArgs, idx: u64) -> bool {
        if let Some(tx) = self.clients_tx.get(&idx) {
            let (answer_tx, answer_rx) = oneshot::channel();
            tx.send(RpcRequestArgs::ForwardBroadcast(args, answer_tx))
                .await
                .expect("rpc_request_tx closed");
            return answer_rx.await.expect("answer channel closed");
        }
        false
    }
}
