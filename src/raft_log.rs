use tarpc::serde;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct LogEntry {
    term: u64,
    index: u64,
    payload: Vec<u8>,
}
