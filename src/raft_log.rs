use tarpc::serde;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct LogEntry {
    pub(crate) term: u64,
    pub(crate) index: u64,
    payload: Vec<u8>,
}
