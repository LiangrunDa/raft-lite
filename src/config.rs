use std::path::PathBuf;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct RaftConfig {
    /// For real runner: vector of ip:port
    /// For checker: vector of ids
    pub peers: Vec<String>,
    /// index of self in peers
    pub(crate) id: u64,
    /// parameters of raft protocol
    pub(crate) params: RaftParams,
    /// persister path
    pub(crate) path: Option<PathBuf>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct RaftParams {
    /// in milliseconds
    pub election_timeout: u64,
    pub replicate_timeout: u64,
}

impl Default for RaftParams {
    fn default() -> Self {
        Self {
            election_timeout: 1000,
            replicate_timeout: 500,
        }
    }
}

impl RaftConfig {
    pub fn new(
        mut peers: Vec<String>,
        self_str: String,
        params: RaftParams,
        path: Option<PathBuf>,
    ) -> Self {
        peers.sort();
        // find the index of self_ip in peers, and use it as id
        let idx = match peers.binary_search(&self_str) {
            Ok(idx) => idx,
            Err(_) => panic!("Self ip not found in peers!"),
        };
        let id = idx as u64;
        Self {
            peers,
            id,
            params,
            path,
        }
    }

    pub(crate) fn get_persister_path(&self) -> PathBuf {
        self.path.clone().unwrap_or_else(|| PathBuf::from("./data"))
    }
}
