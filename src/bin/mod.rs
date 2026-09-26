use std::{
    fs,
    io::{self, Read, Write},
    path::PathBuf,
};

use clap::{Parser, Subcommand};
use lepatch::{
    command::{backup, restore},
    reader::ChunkerConfig,
    storage,
};
use tracing::level_filters::LevelFilter;
use walkdir::WalkDir;

const INDEX_EXTENSION: &str = "idx";
const BLOB_EXTENSION: &str = "bin";

#[derive(Debug, Clone, Parser)]
struct Args {
    #[command(subcommand)]
    command: Commands,
    #[arg(long, default_value_t = false)]
    verbose: bool,
}

#[derive(Debug, Clone, Subcommand)]
enum Commands {
    Backup {
        source: PathBuf,
        name: String,
    },
    Restore {
        destination: PathBuf,
        name: String,
        version: Option<u16>,
    },
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = Args::parse();

    let log_level = if args.verbose {
        LevelFilter::TRACE
    } else {
        LevelFilter::INFO
    };

    tracing_subscriber::fmt().with_max_level(log_level).init();

    match args.command {
        Commands::Backup { source, name } => {
            let last_version = get_last_version(&name).unwrap_or(1);
            let index_extension = format!("{:03}.{}", last_version + 1, INDEX_EXTENSION);
            let index_path = PathBuf::from(&name).with_extension(index_extension);

            let mut index_file = {
                fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(index_path)?
            };

            let config = ChunkerConfig {
                min_size: 8 * 1024,
                avg_size: 16 * 1024,
                max_size: 64 * 1024,
            };

            let storage_path = PathBuf::from(&name).with_extension(BLOB_EXTENSION);
            let storage = storage::BlobFileStorage::new(storage_path).await?;

            let key = backup(
                source,
                lepatch::command::Options {
                    chunker: config,
                    ..Default::default()
                },
                storage,
            )
            .await?;

            index_file.write_all(key.as_bytes())?;
            index_file.flush()?;
        }
        Commands::Restore {
            destination,
            name,
            version,
        } => {
            let version = match version {
                Some(v) => v,
                None => get_last_version(&name).unwrap_or(1),
            };

            let index_extension = format!("{:03}.{}", version, INDEX_EXTENSION);
            let index_path = PathBuf::from(&name).with_extension(index_extension);
            let mut index_file = fs::File::open(index_path)?;

            let mut key = String::new();
            index_file.read_to_string(&mut key)?;

            let storage_path = PathBuf::from(&name).with_extension(BLOB_EXTENSION);
            let storage = storage::BlobFileStorage::<false>::new(storage_path).await?;

            restore(destination, key, storage).await?;
        }
    }

    Ok(())
}

fn get_last_version(name: &str) -> Option<u16> {
    WalkDir::new(".")
        .max_depth(1)
        .into_iter()
        .filter_map(|v| v.ok())
        .filter(|v| v.file_type().is_file())
        .filter_map(|v| {
            let filename = v.file_name().to_str()?.split('.').collect::<Vec<_>>();
            match filename.as_slice() {
                [basename, num, INDEX_EXTENSION] => {
                    if *basename != name {
                        return None;
                    }
                    if num.len() != 3 {
                        return None;
                    }
                    let num: u16 = num.parse().ok()?;
                    Some(num)
                }
                _ => None,
            }
        })
        .max()
}
