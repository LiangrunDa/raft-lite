# Raft Lite

Raft Lite is a very simple and understandable implementation of the Raft consensus algorithm. It is intended to be used as a learning tool for those who are interested in understanding how Raft works internally.

The algorithm is implemented in a event-driven way, which is different from the original paper. The idea is borrowed from my master supervisor [Martin Kleppmann](https://martin.kleppmann.com/)'s course "Distributed Systems".

[![Crates.io][crates-badge]][crates-url]
[![MIT licensed][mit-badge]][mit-url]

[crates-badge]: https://img.shields.io/crates/v/raft-lite
[crates-url]: https://crates.io/crates/raft-lite
[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: https://github.com/LiangrunDa/raft-lite/blob/main/LICENSE

## Usage

Add this to your `Cargo.toml`:

```toml
[dependencies]
raft-lite = "0.1.0"
```

To use it in your project, you can initialize a `Raft` instance with a `RaftConfig`. The way you interact with the Raft instance is to send messages to it and receive messages from it. The message type is `Vec<u8>`. The Raft protocol will guarantee the message delivery is in total order. 

The following example shows how to use Raft Lite to achieve a single value consensus. The example is in `examples/dinner.rs`.

```rust
use raft_lite::config::{RaftConfig, RaftParams};
use raft_lite::persister::AsyncFilePersister;
use raft_lite::raft::Raft;
use std::path::PathBuf;

#[tokio::main]
async fn main() {
    let guests = vec!["localhost:10624".to_string(), "localhost:10625".to_string(), "localhost:10626".to_string()];
    let dinner_options = ["Ratskeller", "Pizzeria Ristorante Lachoni", "KFC"];

    // spawn a raft instance for each guest
    for i in 0..guests.len() {
        let guests = guests.clone();
        tokio::spawn(async move {
            let mut raft = get_raft_instance(guests.clone(), guests[i].clone());
            // one for broadcast, one for receive
            let (raft_broadcast_tx, mut raft_receive_rx) = raft.run();

            // send dinner option of this guest
            raft_broadcast_tx.send(dinner_options[i].to_string().into_bytes()).await.unwrap();

            // receive dinner options of all guests
            let mut count = 0;
            loop {
                let msg = raft_receive_rx.recv().await.unwrap();
                count += 1;
                if count == 2 {
                    // The last message is the final decision
                    println!("Guest {}: Today we'll have dinner at {}!", i, String::from_utf8(msg).unwrap());
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
```


## License
The project is under [MIT license](https://github.com/LiangrunDa/raft-lite/blob/main/LICENSE).

## Related Projects
TODO

