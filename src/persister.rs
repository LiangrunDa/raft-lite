use crate::raft_log::{LogEntry, LogManagerCommand};
use anyhow::Error;
use core::hash::Hash;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::io::Write;
use std::path::PathBuf;
use tokio::sync::mpsc;
use std::sync::Arc;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistRaftState {
    pub(crate) current_term: u64,
    pub(crate) voted_for: Option<u64>,
    pub(crate) log: Vec<Arc<LogEntry>>,
}

// TODO: snapshot persister

impl Default for PersistRaftState {
    fn default() -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            log: vec![],
        }
    }
}

#[derive(Clone, Debug)]
pub struct Persister {
    path: PathBuf,
    logs: Vec<Arc<LogEntry>>,
    log_manager_tx: mpsc::UnboundedSender<LogManagerCommand>,
}

impl PartialEq for Persister {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
    }
}

impl Hash for Persister {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

/// We need to persist the term and voted_for in a synchronous way (persisted before sending VoteResponse),
/// otherwise a node may vote for two different candidates in the same term.
/// However, we can persist the log in an asynchronous way, because it doesn't affect the correctness of the protocol.
/// Currently, we don't persist the commit_length. Reasons:
/// 1. If the application doesn't consume the message, the application layer will lose the message if we persist the state.
/// 2. If the application consumes the message before the state is persisted, the application layer will receive the message again.
/// So the application must handle the message idempotently.

impl Persister {
    pub fn new(path: PathBuf, log_manager_tx: mpsc::UnboundedSender<LogManagerCommand>) -> Self {
        Self {
            path,
            log_manager_tx,
            logs: vec![],
        }
    }

    pub(crate) fn persist_term(&self, term: u64) -> Result<(), Error> {
        // create the directory if not exists
        if !self.path.exists() {
            std::fs::create_dir_all(&self.path)?;
        }
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(self.path.join("term"))?;
        file.write_all(term.to_string().as_bytes())?;
        file.sync_all()?; // flush the file to disk
        Ok(())
    }

    fn load_term(&self) -> Result<u64, Error> {
        let mut file = File::open(self.path.join("term"))?;
        let mut buffer = vec![];
        file.read_to_end(&mut buffer)?;
        let term = String::from_utf8(buffer)?;
        Ok(term.parse()?)
    }

    pub(crate) fn persist_voted_for(&self, voted_for: Option<u64>) -> Result<(), Error> {
        if !self.path.exists() {
            std::fs::create_dir_all(&self.path)?;
        }
        // create a new file if it exists
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(self.path.join("voted_for"))?;
        let bytes = match voted_for {
            Some(value) => value.to_le_bytes().to_vec(),
            None => vec![],
        };
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(())
    }

    fn load_voted_for(&self) -> Result<Option<u64>, Error> {
        let mut file = File::open(self.path.join("voted_for"))?;
        let mut buffer = vec![];
        file.read_to_end(&mut buffer)?;
        let voted_for = if buffer.is_empty() {
            None
        } else {
            Some(u64::from_le_bytes(
                buffer[..std::mem::size_of::<u64>()].try_into().unwrap(),
            ))
        };
        Ok(voted_for)
    }

    pub(crate) fn load_from_disk(&mut self) -> Result<PersistRaftState, Error> {
        let term = self.load_term()?;
        let voted_for = self.load_voted_for()?;
        // println!("load voted_for: {:?}", voted_for);
        // memtake
        let log = std::mem::take(&mut self.logs);
        Ok(PersistRaftState {
            current_term: term,
            voted_for,
            log,
        })
    }

    pub(crate) fn truncate_disk_log(&self, index: usize) {
        let truncate_command = LogManagerCommand::Truncate(index);
        self.log_manager_tx
            .send(truncate_command)
            .expect("log manager channel closed");
    }

    pub(crate) fn append_disk_log(&self, entry: Arc<LogEntry>) {
        let append_command = LogManagerCommand::Append(entry);
        self.log_manager_tx
            .send(append_command)
            .expect("log manager channel closed");
    }

    pub(crate) fn set_logs(&mut self, logs: Vec<Arc<LogEntry>>) {
        // maybe another way to do this?
        self.logs = logs;
    }
}
