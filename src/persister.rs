use crate::raft_log::LogEntry;
use anyhow::Error;
use async_trait::async_trait;
use dyn_clone::DynClone;
use serde_json;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistRaftState {
    pub(crate) current_term: u64,
    pub(crate) voted_for: Option<u64>,
    pub(crate) log: Vec<LogEntry>,
    pub(crate) commit_length: u64,
}

impl Default for PersistRaftState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: vec![],
            commit_length: 0,
        }
    }
}

#[async_trait]
pub trait Persister: DynClone + Send + Sync {
    async fn save_raft_state(&self, state: &PersistRaftState) -> Result<(), Error>;
    async fn load_raft_state(&self) -> Result<PersistRaftState, Error>;

    // TODO: snapshot peresister
}

// otherwise, we can't use Box<dyn Persister> in RaftConfig
dyn_clone::clone_trait_object!(Persister);

trait ClonePersister {
    fn clone_persister<'a>(&self) -> Box<dyn Persister>;
}

#[derive(Clone)]
pub struct AsyncFilePersister {
    path: PathBuf,
}

#[async_trait]
impl Persister for AsyncFilePersister {
    async fn save_raft_state(&self, state: &PersistRaftState) -> Result<(), Error> {
        let serialized_state = serde_json::to_string(state)?;

        // Create directories if they don't exist
        if let Some(parent) = std::path::Path::new(&self.path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(true);

        let mut file = options.open(&self.path).await?;
        file.write_all(serialized_state.as_bytes()).await?;

        Ok(())
    }

    async fn load_raft_state(&self) -> Result<PersistRaftState, Error> {
        if !self.path.exists() {
            return Err(Error::msg("Persister file does not exist"));
        }

        let mut file = File::open(&self.path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        let deserialized_state: PersistRaftState = serde_json::from_slice(&buffer)?;

        Ok(deserialized_state)
    }
}

impl AsyncFilePersister {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Default for AsyncFilePersister {
    fn default() -> Self {
        Self::new(PathBuf::from("./data/raft_storage"))
    }
}
