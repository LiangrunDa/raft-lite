pub mod config;
mod network;
pub mod raft;
mod raft_log;
mod raft_protocol;
mod timer;

#[cfg(test)]
mod tests {
    use crate::config::{RaftConfig, RaftParams};
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
            );
            let mut raft = Raft::new(config);
            raft.run();
            // block forever
            loop {}
        });
    }
}