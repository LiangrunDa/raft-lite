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

To use it in your project, you can initialize a `Raft` instance with a `RaftConfig`. The way you interact with the Raft instance is to send messages to it and receive messages from it. The message type is `Vec<u8>`. The Raft protocol will guarantee the message delivery is in total order. The following example shows how to use it: 

```rust
use raft_lite::config::{RaftConfig, RaftParams};
use raft_lite::persister::AsyncFilePersister;
use raft_lite::raft::Raft;

#[tokio::main]
async fn main() {
    let config = RaftConfig::new(
        vec!["localhost:8080".to_string(), "localhost:8081".to_string()],
        "localhost:8080".to_string(),
        RaftParams::default(),
        Box::new(AsyncFilePersister::default()),
    );
    let mut raft = Raft::new(config);
    let (mtx, mut mrx) = tokio::sync::mpsc::channel::<Vec<u8>>(100);
    let (btx, brx) = tokio::sync::mpsc::channel::<Vec<u8>>(100);
    raft.run(brx, mtx);
    // broadcast message to raft
    btx.send("hello".to_string().into_bytes()).await.unwrap();
    loop {
        // receive message from raft
        let message = mrx.recv().await.unwrap();
        println!("Received message from application: {:?}", message);
    }
}
```
## License
The project is under [MIT license](https://github.com/LiangrunDa/raft-lite/blob/main/LICENSE).

## Related Projects
TODO

