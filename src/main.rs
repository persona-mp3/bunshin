use anyhow::Result;
use bunshin::{Node, run};
use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
struct Cli {
    #[arg(short, long)]
    port: u64,

    #[arg(short, long)]
    node_id: u64,

    #[arg(long, value_delimiter = ',')]
    peers: Option<Vec<String>>,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let address = format!("localhost:{}", args.port);
    println!("initialising node with id: {} on {}", args.node_id, address);
    let node = Node::new(args.node_id, address);
    run(node)
}
