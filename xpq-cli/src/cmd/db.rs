//! Database statistics and maintenance commands.

use anyhow::{Context, Result};
use xpq::Queue;

use crate::{
    cli::DbCommand,
    display::{show_table, StatsRow},
};

/// Runs a database command.
pub async fn run(queue: &Queue, command: DbCommand) -> Result<()> {
    match command {
        DbCommand::Stats => {
            let stats = queue.stats().await.context("failed to get stats")?;

            if stats.is_empty() {
                println!("No queues.");
            } else {
                let rows: Vec<StatsRow> = stats
                    .into_iter()
                    .map(|s| StatsRow {
                        queue: s.queue,
                        pending: s.pending,
                        paused: s.paused,
                        in_progress: s.in_progress,
                        finished: s.finished,
                        failed: s.failed,
                    })
                    .collect();
                show_table(rows);
            }
        }

        DbCommand::Reap => {
            let (reaped, next) = queue.reap().await.context("reap failed")?;

            println!("Reaped {reaped} jobs.");
            if let Some(duration) = next {
                println!("Next reap in {:?}.", duration);
            }
        }
    }

    Ok(())
}
