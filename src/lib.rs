pub mod config;
pub mod persister;
pub mod raft;
mod raft_log;
mod network;
mod raft_protocol;
mod timer;

#[cfg(test)]
mod tests {
    use crate::config::{RaftConfig, RaftParams};
    use crate::persister::AsyncFilePersister;
    use crate::raft::Raft;
    use tracing::Level;
    use tracing_subscriber::prelude::*;

    #[test]
    fn it_works() {
        // set logging level
        tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .init();
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let config = RaftConfig::new(
                vec!["127.0.0.1:8080".to_string(), "127.0.0.1:8081".to_string()],
                "127.0.0.1:8080".to_string(),
                RaftParams::default(),
                Box::new(AsyncFilePersister::default()),
            );
            let mut raft = Raft::new(config);
            let (mtx, mut mrx) = tokio::sync::mpsc::channel::<Vec<u8>>(100);
            let (btx, brx) = tokio::sync::mpsc::channel::<Vec<u8>>(100);
            raft.run(brx, mtx);
            // block forever
            loop {
                // receive message from application
                let message = mrx.recv().await.unwrap();
                println!("Received message from application: {:?}", message);
            }
        });
    }
}
