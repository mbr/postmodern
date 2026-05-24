//! CLI for postmodern job queue management.

mod cli;
mod cmd;
mod display;
mod path;
mod payload;

use anyhow::{Context, Result};
use clap::Parser;
use postmodern::{config, Queue};

use crate::cli::{Cli, Command};

/// Runs the CLI.
async fn run(cli: Cli) -> Result<()> {
    let database_url = config::resolve_database_url(cli.db)?;

    // Handle pg commands (don't need a Queue connection).
    if let Command::Pg { command } = cli.command {
        return cmd::pg::run(&database_url, command);
    }

    let queue = Queue::connect(&database_url)
        .await
        .context("failed to connect")?;

    match cli.command {
        Command::Queue { command } => cmd::queue::run(&queue, command).await?,
        Command::Job { command } => cmd::job::run(&queue, command).await?,
        Command::Db { command } => cmd::db::run(&queue, command).await?,
        Command::Pg { .. } => unreachable!(),
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    run(cli).await
}
