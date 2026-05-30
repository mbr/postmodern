//! CLI argument parsing.

use clap::{Parser, Subcommand};
use postmodern::job::JobStatus;
use uuid_suffix::UuidSuffix;

/// Postgres-backed job queue CLI.
#[derive(Debug, Parser)]
#[command(version)]
pub struct Cli {
    /// Database connection string (overrides config file).
    #[arg(long, global = true)]
    pub db: Option<String>,

    /// Subcommand to run.
    #[command(subcommand)]
    pub command: Command,
}

/// Available commands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Queue management.
    Queue {
        #[command(subcommand)]
        command: QueueCommand,
    },
    /// Job inspection and manipulation.
    Job {
        #[command(subcommand)]
        command: JobCommand,
    },
    /// Database statistics and maintenance.
    Db {
        #[command(subcommand)]
        command: DbCommand,
    },
    /// PostgreSQL backup and restore.
    Pg {
        #[command(subcommand)]
        command: PgCommand,
    },
}

/// Queue management commands.
#[derive(Debug, Subcommand)]
pub enum QueueCommand {
    /// List all queues.
    Ls,
    /// Create a new queue.
    Create {
        /// Name of the queue to create.
        queue: String,
        /// Create the queue in paused state.
        #[arg(long)]
        paused: bool,
    },
    /// Delete a queue and all its jobs.
    Delete {
        /// Name of the queue to delete.
        queue: String,
    },
    /// Pause a queue.
    Pause {
        /// Name of the queue to pause.
        queue: String,
    },
    /// Resume a queue.
    Resume {
        /// Name of the queue to resume.
        queue: String,
    },
    /// Rename a queue.
    Rename {
        /// Current queue name.
        from: String,
        /// New queue name.
        to: String,
    },
}

/// Job inspection and manipulation commands.
#[derive(Debug, Subcommand)]
pub enum JobCommand {
    /// List jobs.
    Ls {
        /// Filter by queue name.
        #[arg(long, short)]
        queue: Option<String>,
        /// Filter by status.
        #[arg(long, short)]
        status: Option<JobStatusArg>,
        /// Maximum number of jobs to show.
        #[arg(long, short, default_value = "100")]
        limit: u32,
    },
    /// Show details about a specific job.
    Show {
        /// Job ID or suffix.
        id: UuidSuffix,
    },
    /// Get the next pending job from a queue.
    Next {
        /// Queue to get from.
        queue: String,
        /// Release back to pending after displaying (atomic peek).
        #[arg(long, conflicts_with = "ack")]
        peek: bool,
        /// Mark as finished immediately after displaying.
        #[arg(long, conflicts_with = "peek")]
        ack: bool,
    },
    /// Restart jobs (reset to pending).
    Restart {
        /// Job IDs or suffixes.
        id: Vec<UuidSuffix>,
        /// Force restart even if job is in_progress (breaks lock).
        #[arg(long)]
        force: bool,
    },
    /// Delete jobs.
    Delete {
        /// Job IDs or suffixes.
        id: Vec<UuidSuffix>,
    },
    /// Hard fail jobs.
    Fail {
        /// Job IDs or suffixes.
        id: Vec<UuidSuffix>,
        /// Error message.
        #[arg(long, short)]
        message: String,
    },
    /// Mark jobs as finished.
    Done {
        /// Job IDs or suffixes.
        id: Vec<UuidSuffix>,
    },
    /// Search job payloads for a pattern.
    Search {
        /// Pattern to search for (case-insensitive substring match).
        pattern: String,
        /// Filter by queue name.
        #[arg(long, short)]
        queue: Option<String>,
        /// Filter by status.
        #[arg(long, short)]
        status: Option<JobStatusArg>,
        /// Continue even if total payload size exceeds 50MB.
        #[arg(long)]
        no_limit: bool,
    },
    /// Get a value from a job's payload.
    Get {
        /// Path to the value (e.g., `items[0].pdf`).
        path: String,
        /// Job ID or suffix.
        id: UuidSuffix,
    },
}

/// Database statistics and maintenance commands.
#[derive(Debug, Subcommand)]
pub enum DbCommand {
    /// Show queue statistics.
    Stats,
    /// Run the reaper once.
    Reap,
}

/// PostgreSQL backup and restore commands.
#[derive(Debug, Subcommand)]
pub enum PgCommand {
    /// Backup database using pg_dump (writes to stdout).
    Backup,
    /// Restore database using pg_restore (reads from stdin).
    Restore,
}

/// Job status argument for filtering.
#[derive(Clone, Copy, Debug)]
pub enum JobStatusArg {
    /// Pending jobs.
    Pending,
    /// Paused jobs.
    Paused,
    /// In-progress jobs.
    InProgress,
    /// Finished jobs.
    Finished,
    /// Failed jobs.
    Failed,
}

impl std::str::FromStr for JobStatusArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "paused" => Ok(Self::Paused),
            "in-progress" => Ok(Self::InProgress),
            "finished" => Ok(Self::Finished),
            "failed" => Ok(Self::Failed),
            _ => Err(format!("unknown status: {s}")),
        }
    }
}

impl From<JobStatusArg> for JobStatus {
    fn from(arg: JobStatusArg) -> Self {
        match arg {
            JobStatusArg::Pending => JobStatus::Pending,
            JobStatusArg::Paused => JobStatus::Paused,
            JobStatusArg::InProgress => JobStatus::InProgress,
            JobStatusArg::Finished => JobStatus::Finished,
            JobStatusArg::Failed => JobStatus::Failed,
        }
    }
}
