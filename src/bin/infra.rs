use clap::{CommandFactory, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::BufWriter;
use std::process::Command;
use sysinfo::{Pid, System};

const CONFIG_PATH: &str = "infra-config.json";

#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
struct Cli {
    #[arg(short, long, value_delimiter = ',')]
    nodes: Vec<String>,

    network: Option<bool>,

    #[command(subcommand)]
    cmd: Option<Operations>,
}

#[derive(Subcommand, Debug, Clone)]
enum Operations {
    Stop { idx: usize },
    StopAll,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BunshinInstance {
    idx: usize,
    process_id: u32,
    listen_addr: String,
}

type Instances = HashMap<String, BunshinInstance>;

fn main() {
    let args = Cli::parse();

    let result = match args.cmd {
        Some(Operations::Stop { idx }) => stop_bunshin(idx),
        Some(Operations::StopAll) => {
            stop_all()
        }
        None => {
            if args.nodes.is_empty() {
                Cli::command()
                    .error(
                        clap::error::ErrorKind::TooFewValues,
                        "please provide at least one node address",
                    )
                    .exit();
            }

            if let Some(_network) = args.network {
                run_network(&args.nodes)
            } else {
                run_bunshin(&args.nodes)
            }
        }
    };

    if let Err(err) = result {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn load_instances() -> Result<Instances, Box<dyn std::error::Error>> {
    let contents = fs::read_to_string(CONFIG_PATH)
        .map_err(|e| format!("could not read {CONFIG_PATH}: {e}"))?;
    Ok(serde_json::from_str(&contents)?)
}

fn save_instances(instances: &Instances) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(CONFIG_PATH)?;
    serde_json::to_writer_pretty(BufWriter::new(file), instances)?;
    Ok(())
}

fn stop_bunshin(idx: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut instances = load_instances()?;
    let key = idx.to_string();

    let Some(instance) = instances.get(&key).cloned() else {
        return Err(format!("no instance with idx {idx}").into());
    };

    let sys = System::new_all();
    match sys.process(Pid::from_u32(instance.process_id)) {
        Some(process) if process.kill() => println!("killed {}", instance.process_id),
        Some(_) => return Err(format!("could not kill {}", instance.process_id).into()),
        None => println!(
            "process {} not running; removing from config",
            instance.process_id
        ),
    }

    instances.remove(&key);
    save_instances(&instances)
}

fn stop_all() -> Result<(), Box<dyn std::error::Error>> {
    let mut instances = load_instances()?;
    let sys = System::new_all();
    for info in instances.iter_mut() {
        let (_idx, instance) = info;
        match sys.process(Pid::from_u32(instance.process_id)) {
            Some(process) if process.kill() => println!("killed {}", instance.process_id),
            Some(_) => return Err(format!("could not kill {}", instance.process_id).into()),
            None => println!(
                "process {} not running; removing from config",
                instance.process_id
            ),
        }
    }

    save_instances(&Instances::new())
}

fn map_peers(addrs: &[String]) -> HashMap<String, Vec<String>> {
    addrs
        .iter()
        .map(|addr| {
            let others = addrs
                .iter()
                .filter(|other| *other != addr)
                .cloned()
                .collect();
            (addr.clone(), others)
        })
        .collect()
}

fn run_network(addrs: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let mut instances = Instances::new();
    let peers = map_peers(addrs);

    for (idx, item) in peers.iter().enumerate() {
        let mut command = Command::new("./bunshin");
        let (port, peers) = item.clone();

        command
            .arg("--node-id")
            .arg(idx.to_string())
            .arg("--port")
            .arg(port)
            .arg("--peers")
            .arg(peers.join(","));

        match command.spawn() {
            Ok(child) => {
                instances.insert(
                    idx.to_string(),
                    BunshinInstance {
                        idx,
                        listen_addr: port.clone(),
                        process_id: child.id(),
                    },
                );
            }
            Err(e) => eprintln!("failed to start bunshin {port}: {e}"),
        }
    }

    save_instances(&instances)
}

fn run_bunshin(addrs: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let mut instances = Instances::new();

    for (idx, addr) in addrs.iter().enumerate() {
        match Command::new("./bunshin")
            .arg("--addr")
            .arg(addr)
            .arg("--node-id")
            .arg(idx.to_string())
            .spawn()
        {
            Ok(child) => {
                instances.insert(
                    idx.to_string(),
                    BunshinInstance {
                        idx,
                        listen_addr: addr.clone(),
                        process_id: child.id(),
                    },
                );
            }
            Err(e) => eprintln!("failed to start {addr}: {e}"),
        }
    }

    save_instances(&instances)
}
