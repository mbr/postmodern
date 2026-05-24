//! Queue management commands.

use anyhow::{Context, Result};
use postmodern::Queue;

use crate::{
    cli::QueueCommand,
    display::{show_table, QueueRow},
};

/// Runs a queue command.
pub async fn run(queue: &Queue, command: QueueCommand) -> Result<()> {
    match command {
        QueueCommand::Ls => {
            let queues = queue.list_queues().await.context("failed to list queues")?;

            if queues.is_empty() {
                println!("No queues.");
            } else {
                let rows: Vec<QueueRow> = queues
                    .into_iter()
                    .map(|q| QueueRow {
                        queue: q.queue,
                        paused: q.paused,
                    })
                    .collect();
                show_table(rows);
            }
        }

        QueueCommand::Create {
            queue: name,
            paused,
        } => {
            let created = queue
                .create_queue(&name, paused)
                .await
                .context("failed to create queue")?;

            if created {
                println!("Created queue '{name}'.");
            } else {
                println!("Queue '{name}' already exists.");
            }
        }

        QueueCommand::Delete { queue: name } => {
            let deleted = queue
                .delete_queue(&name)
                .await
                .context("failed to delete queue")?;
            println!("Deleted queue '{name}' and {deleted} jobs.");
        }

        QueueCommand::Pause { queue: name } => {
            let count = queue
                .pause_queue(&name)
                .await
                .context("failed to pause queue")?;
            println!("Paused queue '{name}', {count} jobs paused.");
        }

        QueueCommand::Resume { queue: name } => {
            let count = queue
                .resume_queue(&name)
                .await
                .context("failed to resume queue")?;
            println!("Resumed queue '{name}', {count} jobs resumed.");
        }
    }

    Ok(())
}
