//! PostgreSQL backup and restore commands.

use std::process::Command;

use anyhow::{Context, Result};

use crate::cli::PgCommand;

/// Runs a PostgreSQL command.
pub fn run(database_url: &str, command: PgCommand) -> Result<()> {
    match command {
        PgCommand::Backup => {
            let status = Command::new("pg_dump")
                .args(["-Fc", "-v", "-d", database_url])
                .status()
                .context("failed to run pg_dump")?;

            if !status.success() {
                anyhow::bail!("pg_dump failed with {status}");
            }
        }
        PgCommand::Restore => {
            let status = Command::new("pg_restore")
                .args(["-v", "-d", database_url])
                .status()
                .context("failed to run pg_restore")?;

            if !status.success() {
                anyhow::bail!("pg_restore failed with {status}");
            }
        }
    }

    Ok(())
}
