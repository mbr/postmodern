//! Table display types and formatting utilities.

use std::io::IsTerminal;

use tabled::{
    settings::{
        height::CellHeightLimit,
        object::{Columns, Object, Rows},
        width::Width,
        Format, Modify, Style,
    },
    Table, Tabled,
};
use terminal_size::{terminal_size, Width as TermWidth};

/// Queue row for display.
#[derive(Debug, Tabled)]
pub struct QueueRow {
    /// Queue name.
    #[tabled(rename = "Queue")]
    pub queue: String,
    /// Whether the queue is paused.
    #[tabled(rename = "Paused")]
    pub paused: bool,
}

/// Job row for display.
#[derive(Debug, Tabled)]
pub struct JobRow {
    /// Short job ID (7 hex characters).
    #[tabled(rename = "ID")]
    pub id: String,
    /// Queue name.
    #[tabled(rename = "Queue")]
    pub queue: String,
    /// Job status.
    #[tabled(rename = "Status")]
    pub status: String,
    /// Retry count.
    #[tabled(rename = "Retries")]
    pub retry_count: i32,
    /// Creation time.
    #[tabled(rename = "Created")]
    pub created_at: String,
    /// Description (truncated).
    #[tabled(rename = "Description")]
    pub description: String,
}

/// Statistics row for display.
#[derive(Debug, Tabled)]
pub struct StatsRow {
    /// Queue name.
    #[tabled(rename = "Queue")]
    pub queue: String,
    /// Pending count.
    #[tabled(rename = "Pending")]
    pub pending: i64,
    /// Paused count.
    #[tabled(rename = "Paused")]
    pub paused: i64,
    /// In-progress count.
    #[tabled(rename = "In Progress")]
    pub in_progress: i64,
    /// Finished count.
    #[tabled(rename = "Finished")]
    pub finished: i64,
    /// Failed count.
    #[tabled(rename = "Failed")]
    pub failed: i64,
}

/// Returns the current terminal width, defaulting to 240 columns for non-TTY output.
///
/// Checks `COLUMNS` env var first (set by `watch` and other tools), then stderr, then stdout.
/// Uses 240 as the default for piped output to avoid unnecessary wrapping in scripts.
fn terminal_width() -> usize {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&w: &usize| w > 0)
        .or_else(|| {
            terminal_size::terminal_size_of(std::io::stderr())
                .or_else(terminal_size)
                .map(|(TermWidth(w), _)| w as usize)
        })
        .unwrap_or(240)
}

/// Returns whether stdout is connected to a TTY.
fn is_tty() -> bool {
    std::io::stdout().is_terminal()
}

/// Prints a table to stdout, constraining width to the terminal.
pub fn show_table<T: Tabled>(rows: Vec<T>) {
    let width = terminal_width();
    let mut table = Table::new(rows);
    if is_tty() {
        table.with(Style::modern());
    } else {
        table.with(Style::ascii());
    }
    table.with(Width::wrap(width));
    println!("{table}");
}

/// Column indices for [`JobRow`].
mod job_columns {
    pub const ID: usize = 0;
    pub const QUEUE: usize = 1;
    pub const STATUS: usize = 2;
    pub const RETRIES: usize = 3;
    pub const CREATED: usize = 4;
    pub const DESCRIPTION: usize = 5;
}

/// Layout parameters for the jobs table.
struct JobsLayout {
    id: usize,
    queue: usize,
    status: usize,
    retries: usize,
    created: usize,
    desc: usize,
    /// Whether Created should split date/time onto separate lines.
    created_split: bool,
    /// Whether Description should be limited to 2 lines.
    desc_limit_height: bool,
}

impl JobsLayout {
    /// Wide layout: short ID, full timestamp, natural column widths.
    fn wide(term_width: usize, queue_natural: usize, status_natural: usize) -> Self {
        // 19 overhead + 7 ID + 16 Created + queue + status + 7 retries + desc
        let fixed = 19 + 7 + 16 + queue_natural + status_natural + 7;
        let desc = term_width.saturating_sub(1).saturating_sub(fixed).max(10);
        Self {
            id: 7,
            queue: queue_natural,
            status: status_natural,
            retries: 7,
            created: 16,
            desc,
            created_split: false,
            desc_limit_height: false,
        }
    }

    /// Medium layout: short ID, wrapped timestamp, minimum column widths.
    fn medium(term_width: usize) -> Self {
        // 19 overhead + 7 ID + 10 Created + 3 queue + 3 status + 2 retries + desc
        let fixed = 19 + 7 + 10 + 3 + 3 + 2;
        let desc = term_width.saturating_sub(1).saturating_sub(fixed).max(10);
        Self {
            id: 7,
            queue: 3,
            status: 3,
            retries: 2,
            created: 10,
            desc,
            created_split: true,
            desc_limit_height: true,
        }
    }

    /// Narrow layout: like medium but with minimal description width.
    fn narrow() -> Self {
        Self {
            id: 7,
            queue: 3,
            status: 3,
            retries: 2,
            created: 10,
            desc: 5,
            created_split: true,
            desc_limit_height: true,
        }
    }
}

/// Renders a jobs table to a string with the given terminal width and style.
fn render_jobs_table_styled(rows: Vec<JobRow>, term_width: usize, use_unicode: bool) -> String {
    use job_columns::{CREATED, DESCRIPTION, ID, QUEUE, RETRIES, STATUS};

    // Calculate natural widths from data for wide layout
    let queue_natural = rows
        .iter()
        .map(|r| r.queue.chars().count())
        .max()
        .unwrap_or(3)
        .max(3);
    let status_natural = rows
        .iter()
        .map(|r| r.status.chars().count())
        .max()
        .unwrap_or(3)
        .max(3);

    // Pick layout based on terminal width
    let layout = if term_width >= 120 {
        JobsLayout::wide(term_width, queue_natural, status_natural)
    } else if term_width >= 65 {
        JobsLayout::medium(term_width)
    } else {
        JobsLayout::narrow()
    };

    let mut table = Table::new(rows);
    if use_unicode {
        table.with(Style::modern());
    } else {
        table.with(Style::ascii());
    }
    table
        .with(Modify::new(Columns::single(ID)).with(Width::wrap(layout.id)))
        .with(
            Modify::new(Columns::single(QUEUE))
                .with(Width::wrap(layout.queue))
                .with(CellHeightLimit::new(2)),
        )
        .with(
            Modify::new(Columns::single(STATUS))
                .with(Width::wrap(layout.status))
                .with(CellHeightLimit::new(2)),
        )
        .with(Modify::new(Columns::single(RETRIES)).with(Width::wrap(layout.retries)));

    // Limit Retries header to 2 lines in narrow layouts (values are numbers, always fit)
    if layout.retries < 7 {
        table.with(
            Modify::new(Rows::first().intersect(Columns::single(RETRIES)))
                .with(CellHeightLimit::new(2)),
        );
    }

    if layout.created_split {
        table.with(
            Modify::new(Columns::single(CREATED))
                .with(Format::content(|s| s.replace(' ', "\n")))
                .with(Width::wrap(layout.created)),
        );
    } else {
        table.with(Modify::new(Columns::single(CREATED)).with(Width::wrap(layout.created)));
    }

    if layout.desc_limit_height {
        table.with(
            Modify::new(Columns::single(DESCRIPTION))
                .with(Width::wrap(layout.desc))
                .with(CellHeightLimit::new(2)),
        );
    } else {
        table.with(Modify::new(Columns::single(DESCRIPTION)).with(Width::wrap(layout.desc)));
    }

    table.to_string()
}

/// Prints a jobs table with fixed column widths.
pub fn show_jobs_table(rows: Vec<JobRow>) {
    println!(
        "{}",
        render_jobs_table_styled(rows, terminal_width(), is_tty())
    );
}

#[cfg(test)]
mod tests {
    use super::{render_jobs_table_styled, JobRow};

    fn fixture() -> Vec<JobRow> {
        vec![
            JobRow {
                id: "9fdbdc6".to_string(),
                queue: "ingest".to_string(),
                status: "Pending".to_string(),
                retry_count: 0,
                created_at: "2026-05-18 01:16".to_string(),
                description: "ingest new documents from source".to_string(),
            },
            JobRow {
                id: "6789abc".to_string(),
                queue: "process".to_string(),
                status: "InProgress".to_string(),
                retry_count: 2,
                created_at: "2026-05-17 14:30".to_string(),
                description: "process batch".to_string(),
            },
            JobRow {
                id: "a987654".to_string(),
                queue: "export".to_string(),
                status: "Failed".to_string(),
                retry_count: 5,
                created_at: "2026-05-16 09:00".to_string(),
                description: "export results to external API endpoint".to_string(),
            },
            JobRow {
                id: "f012345".to_string(),
                queue: "notify".to_string(),
                status: "Finished".to_string(),
                retry_count: 0,
                created_at: "2026-05-15 22:45".to_string(),
                description: "".to_string(),
            },
        ]
    }

    #[test]
    fn jobs_table_width_60() {
        insta::assert_snapshot!(render_jobs_table_styled(fixture(), 60, true));
    }

    #[test]
    fn jobs_table_width_80() {
        insta::assert_snapshot!(render_jobs_table_styled(fixture(), 80, true));
    }

    #[test]
    fn jobs_table_width_120() {
        insta::assert_snapshot!(render_jobs_table_styled(fixture(), 120, true));
    }
}
