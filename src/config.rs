use crate::persister::Persister;

#[derive(Clone)]
pub struct RaftConfig {
    /// vector of ip:port
    pub peers: Vec<String>,
    /// index of self ip in peers
    pub(crate) id: u64,
    /// parameters of raft protocol
    pub(crate) params: RaftParams,
    /// persister implementing trait Persister
    pub(crate) persister: Box<dyn Persister>, // don't want to make RaftConfig generic so dyn trait is used
}

#[derive(Clone)]
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
        self_ip: String,
        params: RaftParams,
        persister: Box<dyn Persister>,
    ) -> Self {
        peers.sort();
        // find the index of self_ip in peers, and use it as id
        let idx = match peers.binary_search(&self_ip) {
            Ok(idx) => idx,
            Err(_) => panic!("Self ip not found in peers!"),
        };
        let id = idx as u64;
        Self {
            peers,
            id,
            params,
            persister,
        }
    }
}
