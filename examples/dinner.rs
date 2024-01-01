use raft_lite::config::{RaftConfig, RaftParams};
use raft_lite::persister::AsyncFilePersister;
use raft_lite::raft::Raft;
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let guests = vec![
        "localhost:10624".to_string(),
        "localhost:10625".to_string(),
        "localhost:10626".to_string(),
    ];
    let dinner_options = vec![
        b"Ratskeller".to_vec(),
        b"Pizzeria Ristorante Lachoni".to_vec(),
        b"KFC".to_vec(),
    ];

    // spawn a raft instance for each guest
    for i in 0..guests.len() {
        let guests = guests.clone();
        let dinner_options = dinner_options.clone();
        tokio::spawn(async move {
            let mut raft = get_raft_instance(guests.clone(), guests[i].clone());
            // one for broadcast, one for receive
            let (raft_broadcast_tx, mut raft_receive_rx) = raft.run();

            // send dinner option of this guest
            let place = dinner_options[i].clone();
            raft_broadcast_tx.send(place).await.unwrap();

            // receive dinner options of all guests
            let mut count = 0;
            loop {
                let msg = raft_receive_rx.recv().await.unwrap();
                count += 1;
                if count == 2 {
                    // The last message is the final decision
                    let final_decision = String::from_utf8(msg).unwrap();
                    println!("Guest {}: we'll have dinner at {}!", i, final_decision);
                }
            }
        });
    }
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
}

fn get_raft_instance(peers: Vec<String>, self_addr: String) -> Raft {
    let path: PathBuf = PathBuf::from(format!("./data/dinner]/{self_addr}"));
    let raft_config = RaftConfig::new(
        peers.clone(),
        self_addr.clone(),
        RaftParams::default(),
        Box::new(AsyncFilePersister::new(path)),
    );
    Raft::new(raft_config)
}
