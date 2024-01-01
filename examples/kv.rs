use raft_lite::config::{RaftConfig, RaftParams};
use raft_lite::persister::AsyncFilePersister;
use raft_lite::raft::Raft;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::path::PathBuf;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let peers_port = [10624, 10625, 10626];
    let peers = peers_port
        .iter()
        .map(|port| format!("localhost:{}", port))
        .collect::<Vec<String>>();
    for i in 0..peers.len() {
        let path: PathBuf = PathBuf::from(format!("./data/raft_lite/{}", i));
        let mut kv = KVInstance::new(peers.clone(), i, path);
        tokio::spawn(async move {
            kv.set("greeting".to_string(), format!("hello world from {}", i))
                .await;
            kv.run().await;
        });
    }
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    Ok(())
}

struct KVInstance {
    store: HashMap<String, String>,
    raft_broadcast_tx: mpsc::Sender<Vec<u8>>,
    raft_receive_rx: mpsc::Receiver<Vec<u8>>,
    raft: Raft,
    index: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct KVCommand {
    key: String,
    value: String,
}

impl KVInstance {
    fn new(peers: Vec<String>, index: usize, path: PathBuf) -> Self {
        let config = RaftConfig::new(
            peers.clone(),
            peers[index].clone(),
            RaftParams::default(),
            Box::new(AsyncFilePersister::new(path)),
        );
        let mut raft = Raft::new(config);
        let (raft_broadcast_tx, raft_receive_rx) = raft.run();
        Self {
            store: HashMap::new(),
            raft_broadcast_tx,
            raft_receive_rx,
            raft,
            index,
        }
    }

    async fn run(&mut self) {
        // all nodes should have the same order of commands
        loop {
            let msg = self.raft_receive_rx.recv().await.unwrap();
            let cmd = bincode::deserialize::<KVCommand>(&msg).unwrap();

            println!(
                "{:?}: received command [{:?}, {:?}]",
                self.index, cmd.key, cmd.value
            );
            self.store.insert(cmd.key, cmd.value);
        }
    }

    async fn set(&mut self, key: String, value: String) {
        let cmd = KVCommand { key, value };
        let msg = bincode::serialize(&cmd).unwrap();
        self.raft_broadcast_tx.send(msg).await.unwrap();
    }
}
