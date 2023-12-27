use tarpc::serde;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct LogEntry {
    pub(crate) term: u64,
    pub(crate) payload: Vec<u8>,
}
