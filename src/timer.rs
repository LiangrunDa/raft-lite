use crate::raft_protocol::Event;
use rand::rngs::{OsRng, StdRng};
use rand::{Rng, SeedableRng};
use tokio::sync::mpsc;
use tokio::time;
use tokio::time::Duration;

#[derive(Eq, PartialEq)]
pub(crate) enum Timer {
    ElectionTimer,
    ReplicationTimer,
}

pub(crate) async fn start_timer(
    mut reset_rx: mpsc::UnboundedReceiver<()>,
    event_tx: mpsc::UnboundedSender<Event>,
    timeout: u64,
    timer: Timer,
) {
    // set seed
    let mut os_rng = OsRng;
    let seed: [u8; 32] = os_rng.gen();
    let mut rng = StdRng::from_seed(seed);

    loop {
        // election timer is randomized
        let mut duration = timeout;
        if timer == Timer::ElectionTimer {
            let random = rng.gen::<u64>();
            duration = (random % timeout) + timeout;
        }
        tokio::select! {
            _ = reset_rx.recv() => {}
            _ = time::sleep(Duration::from_millis(duration)) => {
                match timer {
                    Timer::ElectionTimer => {
                        event_tx.send(Event::ElectionTimeout).expect("event_tx closed");
                    }
                    Timer::ReplicationTimer => {
                        event_tx.send(Event::ReplicationTimeout).expect("event_tx closed");
                    }
                }
            }
        }
    }
}
