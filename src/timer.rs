use crate::raft_protocol::Event;
use tokio::sync::mpsc;
use tokio::time;
use tokio::time::Duration;
use tracing::trace;

pub(crate) enum Timer {
    ElectionTimer,
    ReplicationTimer,
}

pub(crate) async fn start_timer(
    mut reset_rx: mpsc::Receiver<()>,
    event_tx: mpsc::Sender<Event>,
    timeout: u64,
    timer: Timer,
) {
    loop {
        tokio::select! {
            _ = reset_rx.recv() => {}
            _ = time::sleep(Duration::from_millis(timeout)) => {
                match timer {
                    Timer::ElectionTimer => {
                        event_tx.send(Event::ElectionTimeout).await.expect("event_tx closed");
                    }
                    Timer::ReplicationTimer => {
                        event_tx.send(Event::ReplicationTimeout).await.expect("event_tx closed");
                    }
                }
            }
        }
    }
}
