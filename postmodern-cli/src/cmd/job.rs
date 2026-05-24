//! Job inspection and manipulation commands.

use std::{
    io::{self, Write},
    pin::pin,
};

use anyhow::{Context, Result};
use futures::StreamExt;
use postmodern::{EnqueueOptions, JobDetails, JobFilter, Queue};
use rmpv::Value;
use serde::Serialize;
use uuid::Uuid;
use uuid_suffix::UuidSuffix;

use crate::{
    cli::JobCommand,
    display::{show_jobs_table, JobRow},
    path, payload,
};

/// Returns a short representation of a UUID (7 hex characters).
fn short_id(id: Uuid) -> String {
    UuidSuffix::new(&id).to_string()
}

/// Job representation for YAML output.
#[derive(Serialize)]
struct JobYaml<'a> {
    id: String,
    queue: &'a str,
    status: String,
    priority: i64,
    created: String,
    retries: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    lock: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a str>,
    payload: Value,
}

/// Runs a job command.
pub async fn run(queue: &Queue, command: JobCommand) -> Result<()> {
    match command {
        JobCommand::Ls {
            queue: queue_filter,
            status,
            limit,
        } => {
            let filter = JobFilter {
                queue: queue_filter,
                status: status.map(Into::into),
                limit,
            };
            let jobs = queue
                .list_jobs(filter)
                .await
                .context("failed to list jobs")?;

            if jobs.is_empty() {
                println!("No jobs.");
            } else {
                let rows: Vec<JobRow> = jobs
                    .into_iter()
                    .map(|j| JobRow {
                        id: short_id(j.id),
                        queue: j.queue,
                        status: format!("{:?}", j.status),
                        retry_count: j.retry_count,
                        created_at: j.created_at.format("%Y-%m-%d %H:%M").to_string(),
                        description: j.description.unwrap_or_default(),
                    })
                    .collect();
                show_jobs_table(rows);
            }
        }

        JobCommand::Show { id } => {
            let resolved = queue
                .resolve_job_id(&id)
                .await
                .context("resolving job ID")?;
            let job = queue
                .get_job(resolved)
                .await
                .context("failed to get job")?
                .ok_or_else(|| anyhow::anyhow!("job {} not found", short_id(resolved)))?;

            print_job_details(queue, &job).await?;
        }

        JobCommand::Next {
            queue: queue_name,
            peek,
            ack,
        } => {
            let (details, payload_bytes, job_ack) = queue
                .pull_next(&queue_name)
                .await
                .context("failed to get next job")?
                .ok_or_else(|| anyhow::anyhow!("no pending jobs in queue '{queue_name}'"))?;

            print_job_details_with_payload(&details, &payload_bytes);

            if peek {
                job_ack.restart().await.context("failed to release job")?;
            } else if ack {
                job_ack.commit().await.context("failed to ack job")?;
            } else {
                job_ack.forget();
            }
        }

        JobCommand::Move { id, to } => {
            let resolved = queue
                .resolve_job_ids(&id)
                .await
                .context("resolving job IDs")?;
            let count = queue
                .move_jobs(&resolved, &to)
                .await
                .context("failed to move jobs")?;
            println!("Moved {count} job(s) to queue '{to}'.");
        }

        JobCommand::Copy { id, to } => {
            let resolved = queue
                .resolve_job_id(&id)
                .await
                .context("resolving job ID")?;
            let new_id = queue
                .copy_job(resolved, &to, EnqueueOptions::default())
                .await
                .context("failed to copy job")?;
            println!(
                "Copied job {} to queue '{to}' as {}.",
                short_id(resolved),
                short_id(new_id)
            );
        }

        JobCommand::Restart { id, force } => {
            let resolved = queue
                .resolve_job_ids(&id)
                .await
                .context("resolving job IDs")?;
            let count = queue
                .restart_jobs(&resolved, force)
                .await
                .context("failed to restart jobs")?;

            if count == 0 && !force {
                // Check if any jobs are in_progress
                let in_progress: Vec<_> = futures::future::join_all(
                    resolved
                        .iter()
                        .map(|&job_id| async move { queue.get_job(job_id).await.ok().flatten() }),
                )
                .await
                .into_iter()
                .flatten()
                .filter(|j| j.status == postmodern::job::JobStatus::InProgress)
                .collect();

                if !in_progress.is_empty() {
                    let ids: Vec<_> = in_progress.iter().map(|j| short_id(j.id)).collect();
                    anyhow::bail!(
                        "job(s) {} in_progress (locked). Use --force to restart anyway.",
                        ids.join(", ")
                    );
                }
            }

            println!("Restarted {count} job(s).");
        }

        JobCommand::Delete { id } => {
            let resolved = queue
                .resolve_job_ids(&id)
                .await
                .context("resolving job IDs")?;
            let count = queue
                .delete_jobs(&resolved)
                .await
                .context("failed to delete jobs")?;
            println!("Deleted {count} job(s).");
        }

        JobCommand::Fail { id, message } => {
            let resolved = queue
                .resolve_job_ids(&id)
                .await
                .context("resolving job IDs")?;
            let count = queue
                .fail_jobs(&resolved, &message)
                .await
                .context("failed to fail jobs")?;
            println!("Failed {count} job(s).");
        }

        JobCommand::Done { id } => {
            let resolved = queue
                .resolve_job_ids(&id)
                .await
                .context("resolving job IDs")?;
            let count = queue
                .finish_jobs(&resolved)
                .await
                .context("failed to finish jobs")?;
            println!("Finished {count} job(s).");
        }

        JobCommand::Search {
            pattern,
            queue: queue_filter,
            status,
            no_limit,
        } => {
            const SIZE_LIMIT: u64 = 50 * 1024 * 1024; // 50MB

            let filter = JobFilter {
                queue: queue_filter,
                status: status.map(Into::into),
                limit: u32::MAX,
            };

            let needle: Vec<u8> = pattern
                .chars()
                .filter(|c| !c.is_whitespace())
                .flat_map(|c| c.to_lowercase())
                .collect::<String>()
                .into_bytes();
            let mut total_bytes: u64 = 0;
            let mut matches = Vec::new();
            let mut jobs_scanned: u64 = 0;

            let mut stream = pin!(queue.stream_jobs_with_payload(filter));
            while let Some(result) = stream.next().await {
                let (job, payload_bytes) = match result {
                    Ok(pair) => pair,
                    Err(e) => {
                        eprintln!("warning: failed to fetch job: {e}");
                        continue;
                    }
                };

                jobs_scanned += 1;
                total_bytes += payload_bytes.len() as u64;
                if !no_limit && total_bytes > SIZE_LIMIT {
                    anyhow::bail!(
                        "exceeded 50MB payload limit ({} bytes so far, {} jobs scanned). \
                         Use --no-limit to continue.",
                        total_bytes,
                        jobs_scanned
                    );
                }

                // Search in metadata
                let metadata_match = str_contains_needle(&job.queue, &needle)
                    || job
                        .description
                        .as_deref()
                        .is_some_and(|d| str_contains_needle(d, &needle))
                    || job
                        .error
                        .as_deref()
                        .is_some_and(|e| str_contains_needle(e, &needle));

                if metadata_match {
                    matches.push(job);
                    continue;
                }

                // Search in payload
                let value = match rmpv::decode::read_value(&mut &payload_bytes[..]) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                if rmpv_contains(&value, &needle) {
                    matches.push(job);
                }
            }

            if matches.is_empty() {
                println!("No matches.");
            } else {
                let rows: Vec<JobRow> = matches
                    .into_iter()
                    .map(|j| JobRow {
                        id: short_id(j.id),
                        queue: j.queue,
                        status: format!("{:?}", j.status),
                        retry_count: j.retry_count,
                        created_at: j.created_at.format("%Y-%m-%d %H:%M").to_string(),
                        description: j.description.unwrap_or_default(),
                    })
                    .collect();
                show_jobs_table(rows);
            }
        }

        JobCommand::Get { path: path_str, id } => {
            let resolved = queue
                .resolve_job_id(&id)
                .await
                .context("resolving job ID")?;
            let payload_bytes = queue
                .get_job_payload(resolved)
                .await
                .context("failed to get payload")?
                .ok_or_else(|| anyhow::anyhow!("job {} has no payload", short_id(resolved)))?;

            let payload =
                payload::from_msgpack(&payload_bytes).context("failed to decode payload")?;

            let value = path::get(&payload, &path_str)?;

            match value {
                Value::String(s) => {
                    let s = s.as_str().context("invalid UTF-8 in string")?;
                    print!("{s}");
                }
                Value::Binary(b) => {
                    io::stdout().write_all(b).context("failed to write")?;
                }
                other => {
                    anyhow::bail!(
                        "expected string or binary at path, got {}",
                        value_type_name(other)
                    );
                }
            }
        }
    }

    Ok(())
}

/// Returns a human-readable type name for a value.
fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Nil => "null",
        Value::Boolean(_) => "boolean",
        Value::Integer(_) => "integer",
        Value::F32(_) | Value::F64(_) => "float",
        Value::String(_) => "string",
        Value::Binary(_) => "binary",
        Value::Array(_) => "array",
        Value::Map(_) => "map",
        Value::Ext(_, _) => "ext",
    }
}

/// Prints job details with payload bytes as YAML.
fn print_job_details_with_payload(job: &JobDetails, payload_bytes: &[u8]) {
    let payload_value = payload::from_msgpack(payload_bytes)
        .map(|v| payload::prepare_for_yaml(&v))
        .unwrap_or(Value::Nil);

    let job_yaml = JobYaml {
        id: job.id.to_string(),
        queue: &job.queue,
        status: format!("{:?}", job.status),
        priority: job.priority,
        created: job.created_at.format("%Y-%m-%d %H:%M:%S").to_string(),
        retries: job.retry_count,
        lock: job.lock.map(|l| l.format("%Y-%m-%d %H:%M:%S").to_string()),
        description: job.description.as_deref(),
        error: job.error.as_deref(),
        payload: payload_value,
    };

    match serde_saphyr::to_string(&job_yaml) {
        Ok(yaml) => print!("{yaml}"),
        Err(e) => eprintln!("failed to serialize job: {e}"),
    }
}

/// Prints job details, fetching payload from database.
async fn print_job_details(queue: &Queue, job: &JobDetails) -> Result<()> {
    if let Some(payload_bytes) = queue
        .get_job_payload(job.id)
        .await
        .context("failed to get payload")?
    {
        print_job_details_with_payload(job, &payload_bytes);
    } else {
        print_job_details_with_payload(job, &[]);
    }
    Ok(())
}

/// Searches an rmpv Value tree for a needle (lowercase, no whitespace, UTF-8 bytes).
/// Returns true on first match.
fn rmpv_contains(v: &Value, needle: &[u8]) -> bool {
    match v {
        Value::String(s) => {
            if let Some(s) = s.as_str() {
                str_contains_needle(s, needle)
            } else {
                false
            }
        }
        Value::Binary(b) => {
            if let Ok(s) = std::str::from_utf8(b) {
                str_contains_needle(s, needle)
            } else {
                false
            }
        }
        Value::Array(arr) => arr.iter().any(|v| rmpv_contains(v, needle)),
        Value::Map(map) => map
            .iter()
            .any(|(k, v)| rmpv_contains(k, needle) || rmpv_contains(v, needle)),
        _ => false,
    }
}

/// Checks if a string (normalized: lowercase, no whitespace) contains the needle bytes.
fn str_contains_needle(s: &str, needle: &[u8]) -> bool {
    let haystack: Vec<u8> = s
        .chars()
        .filter(|c| !c.is_whitespace())
        .flat_map(|c| c.to_lowercase())
        .collect::<String>()
        .into_bytes();
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}
