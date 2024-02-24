use crc::{Crc, CRC_32_CKSUM};
use std::path::PathBuf;
use std::sync::Arc;
use tarpc::serde;
use tokio::fs::OpenOptions;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tracing::{error, info};

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_CKSUM);
pub(crate) type ByteSize = u64;

/// LogEntry for runtime
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, Hash, PartialEq, Eq)]
#[warn(private_interfaces)]
pub struct LogEntry {
    pub(crate) term: u64,
    pub(crate) payload: Vec<u8>,
}

/// Index for fast truncation
pub(crate) struct DiskLogIndex {
    pub(crate) offset: u64,
    pub(crate) size: u64,
}

/// The actual data of the log entry in the disk log file
/// check_sum: 4 bytes
/// term: 8 bytes
/// payload size: 8 bytes
/// payload: variable size
#[derive(Clone, Debug)]
pub(crate) struct DiskLogEntry {
    pub(crate) check_sum: u32,
    pub(crate) term: u64,
    pub(crate) payload: Vec<u8>,
}

impl DiskLogEntry {
    pub(crate) fn new_entry(entry: Arc<LogEntry>) -> Self {
        let payload = entry.payload.to_vec();
        let check_sum = CRC32.checksum(&payload);
        Self {
            check_sum,
            term: entry.term,
            payload,
        }
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.check_sum == CRC32.checksum(&self.payload)
    }

    const fn check_sum_byte_size() -> ByteSize {
        4
    }

    const fn term_byte_size() -> ByteSize {
        8
    }

    const fn payload_size_byte_size() -> ByteSize {
        8
    }

    pub(crate) fn total_byte_size(&self) -> ByteSize {
        Self::check_sum_byte_size()
            + Self::term_byte_size()
            + Self::payload_size_byte_size()
            + self.payload.len() as u64
    }

    async fn serialize<T: AsyncWrite + std::marker::Unpin>(
        &self,
        buf: &mut T,
    ) -> anyhow::Result<()> {
        // checksum
        buf.write_all(&self.check_sum.to_be_bytes()).await?;
        // term
        buf.write_all(&self.term.to_be_bytes()).await?;
        // payload size
        buf.write_all(&self.payload.len().to_be_bytes()).await?;
        // payload
        buf.write_all(&self.payload).await?;
        Ok(())
    }

    async fn deserialize<T: AsyncRead + std::marker::Unpin>(buf: &mut T) -> anyhow::Result<Self> {
        let mut check_sum_buf = [0u8; 4];
        buf.read_exact(&mut check_sum_buf).await?;
        let check_sum = u32::from_be_bytes(check_sum_buf);
        let mut term_buf = [0u8; 8];
        buf.read_exact(&mut term_buf).await?;
        let term = u64::from_be_bytes(term_buf);
        let mut payload_size_buf = [0u8; 8];
        buf.read_exact(&mut payload_size_buf).await?;
        let payload_size = u64::from_be_bytes(payload_size_buf);
        let mut payload = vec![0u8; payload_size as usize];
        buf.read_exact(&mut payload).await?;
        let res = Self {
            check_sum,
            term,
            payload,
        };
        if res.is_valid() {
            Ok(res)
        } else {
            Err(anyhow::anyhow!("Invalid checksum"))
        }
    }
}

pub(crate) struct LogManager {
    pub(crate) log_index: Vec<DiskLogIndex>,
    pub(crate) log_file: tokio::fs::File,
}

#[derive(Debug)]
pub enum LogManagerCommand {
    Append(Arc<LogEntry>),
    Truncate(usize),
}

impl LogManager {
    pub(crate) async fn new<T: Into<PathBuf>>(data_dir: T) -> anyhow::Result<Self> {
        let path = data_dir.into();
        // create the directory if it doesn't exist
        tokio::fs::create_dir_all(&path).await?;
        // create a new file "./data_dir/raft_log" if it doesn't exist
        let log_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path.join("raft_log"))
            .await?;
        Ok(Self {
            log_index: vec![],
            log_file,
        })
    }

    pub(crate) async fn initialize(&mut self) -> anyhow::Result<Vec<Arc<LogEntry>>> {
        let mut cursor = 0u64;
        let file_size = self.log_file.metadata().await?.len();
        let mut result = vec![];
        let mut buffered_reader = tokio::io::BufReader::new(&mut self.log_file);
        buffered_reader
            .seek(std::io::SeekFrom::Start(cursor))
            .await?;
        loop {
            if cursor >= file_size {
                break;
            }
            let entry = DiskLogEntry::deserialize(&mut buffered_reader).await?;
            result.push(Arc::new(LogEntry {
                term: entry.term,
                payload: entry.payload.clone(),
            }));
            let entry_size = entry.payload.len() as u64;
            self.log_index.push(DiskLogIndex {
                offset: cursor,
                size: entry_size,
            });
            cursor += entry.total_byte_size();
            buffered_reader
                .seek(std::io::SeekFrom::Start(cursor))
                .await?;
        }
        info!("Recovered log entries: {:?}", result.len());
        Ok(result)
    }

    pub(crate) async fn append(&mut self, entry: Arc<LogEntry>) -> anyhow::Result<()> {
        let disk_entry = DiskLogEntry::new_entry(entry);
        // println!("disk_entry: {:?}", disk_entry);

        disk_entry.serialize(&mut self.log_file).await?;
        let entry_size = disk_entry.payload.len() as u64;
        if self.log_index.is_empty() {
            self.log_index.push(DiskLogIndex {
                offset: 0,
                size: entry_size,
            });
        } else {
            self.log_index.push(DiskLogIndex {
                offset: self.log_index.last().unwrap().offset + self.log_index.last().unwrap().size,
                size: entry_size,
            });
        }
        Ok(())
    }

    pub(crate) async fn truncate(&mut self, index: usize) -> anyhow::Result<()> {
        // if index is 0, then we truncate the whole log
        if index == 0 {
            self.log_index.clear();
            self.log_file.set_len(0).await?;
            return Ok(());
        }
        // else we keep the log [0, index - 1]
        let offset = self.log_index[index - 1].offset;
        let truncate_len = offset + self.log_index[index - 1].size;
        // keep log_index[0, index - 1]
        self.log_index.truncate(index);
        self.log_file.set_len(truncate_len).await?;
        Ok(())
    }

    pub(crate) async fn run(&mut self, mut disk_rx: mpsc::UnboundedReceiver<LogManagerCommand>) {
        while let Some(cmd) = disk_rx.recv().await {
            // println!("received command: {:?}", cmd);
            match cmd {
                LogManagerCommand::Append(entry) => match self.append(entry).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Error appending log entry: {:?}", e);
                    }
                },
                LogManagerCommand::Truncate(index) => match self.truncate(index).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Error truncating log: {:?}", e);
                    }
                },
            }
        }
    }
}
