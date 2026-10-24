//! The `blu` command line interface.

mod cli;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    cli::entry().await
}
