use clap::{Parser, ValueEnum};
use raft_lite::model_check::*;
use stateright::actor::Network;
use stateright::report::WriteReporter;
use stateright::Checker;
use stateright::Model;
use std::fmt::Display;

#[derive(Parser, Clone, Debug)]
#[command(author, version, about, long_about = None)]
struct CheckerArgs {
    /// Number of servers in the cluster.
    /// Should be at least 3.
    #[arg(short, long, default_value = "3")]
    server_count: usize,

    /// `check` for Breadth First Search to verify the model.
    /// `explore` to run the model in an interactive web UI.
    #[arg(short, long)]
    mode: RunMode,

    /// Only needed for check mode.
    #[arg(short, long, default_value = "10")]
    depth: Option<usize>,

    /// Underlying network assumptions
    /// We don't provide lossy network, since a lost packet can be considered as a delayed packet that is never delivered.
    #[arg(short, long, default_value_t = NetworkType::UnorderedNonduplicating)]
    network: NetworkType,
}

#[derive(Clone, Debug, ValueEnum)]
enum RunMode {
    Check,
    Explore,
}

#[derive(Clone, Debug, ValueEnum)]
enum NetworkType {
    UnorderedNonduplicating,
    UnorderedDuplicating,
    Ordered,
}

impl Display for NetworkType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NetworkType::UnorderedNonduplicating => write!(f, "unordered-nonduplicating"),
            NetworkType::UnorderedDuplicating => write!(f, "unordered-duplicating,"),
            NetworkType::Ordered => write!(f, "ordered"),
        }
    }
}

// Please note that this is not a crash-recovery model but a crash-stop model.
fn main() -> anyhow::Result<()> {
    let args = CheckerArgs::parse();
    let network = match args.network {
        NetworkType::UnorderedNonduplicating => Network::new_unordered_nonduplicating([]),
        NetworkType::UnorderedDuplicating => Network::new_unordered_duplicating([]),
        NetworkType::Ordered => Network::new_ordered([]),
    };
    match args.mode {
        RunMode::Check => {
            RaftModelCfg {
                server_count: args.server_count,
                network,
            }
            .into_model()
            .checker()
            .threads(num_cpus::get())
            .target_max_depth(args.depth.unwrap_or(13))
            .spawn_bfs()
            .report(&mut WriteReporter::new(&mut std::io::stdout()));
        }
        RunMode::Explore => {
            println!("Exploring state space for Raft on http://localhost:3000");
            RaftModelCfg {
                server_count: args.server_count,
                network,
            }
            .into_model()
            .checker()
            .threads(num_cpus::get())
            .serve("localhost:3000");
        }
    }
    Ok(())
}
