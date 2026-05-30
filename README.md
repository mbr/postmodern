# postmodern: Postgres-backed job queue

`postmodern` is a Postgres job queue. There are many like it, but this one is mine. Use it for ~~fun~~ small to medium projects where you can get away with [Just Use Postgres](https://www.manning.com/books/just-use-postgres) (now a book, apparently).

Push jobs onto queues, pull them off, acknowledge when done. Failed jobs retry with backoff; crashed workers get reaped.

The CLI lets you inspect jobs directly:

```console
$ pm job ls -q ingest
┌────────────────────┬─────┬─────┬────┬────────────┬────────────────────────────────────┐
│ ID                 │ Que │ Sta │ Re │ Created    │ Description                        │
│                    │ ue  │ tus │ tr │            │                                    │
├────────────────────┼─────┼─────┼────┼────────────┼────────────────────────────────────┤
│ 019e5463-fa5b-7910 │ ing │ Fin │ 0  │ 2026-05-23 │ ingest new PDF (hash: ee224bf): ad │
│ -bc25-26c55139e9c6 │ est │ ish │    │ 10:31      │ s-2800wBA026F_20260523_123105_0045 │
├────────────────────┼─────┼─────┼────┼────────────┼────────────────────────────────────┤
│ 019e5460-d876-7603 │ ing │ Fin │ 0  │ 2026-05-23 │ ingest new PDF (hash: 8b735c4): ad │
│ -b291-917d703f5f52 │ est │ ish │    │ 10:28      │ s-2800wBA026F_20260523_122808_0045 │
├────────────────────┼─────┼─────┼────┼────────────┼────────────────────────────────────┤
...

$ pm job show 019e5463-fa5b-7910-bc25-26c55139e9c6
id: 019e5463-fa5b-7910-bc25-26c55139e9c6
queue: ingest
status: Finished
priority: 0
created: 2026-05-23 10:31:38
retries: 0
lock: 2026-05-23 10:34:16
description: "ingest new PDF (hash: ee224bf): ads-2800wBA026F_20260523_123105_004582.pdf"
payload:
  document_id: "0x019e5463fa4870a1b0718703d5de96fe"
  filename: ads-2800wBA026F_20260523_123105_004582.pdf
  upload_time: 2026-05-23T10:31:38.824914622Z
  sha256: "0xee224bfe932f541b63762dbed50d5482f6a80e3533b2f067f1a71ed691ce64fb"
  pdf: <10190057 bytes>
```

## Five reasons to use it

* **No magic**: jobs are plain serde types, no macros, no traits, no worker pool. Call `enqueue()` and `try_stream_jobs()` and process with standard async Rust.
* **CLI tooling**: list, search, inspect, restart, and fail jobs without writing code. If your payload implements `Deserialize`, you can inspect it.
* **Jobs, not notifications**: explicit lifecycle with `soft_fail()` (retry with backoff) and `hard_fail()`.
* **Operational control**: pause queues for maintenance or backpressure; refresh locks for jobs that run longer than the default 20-minute lease.
* **Durable by design**: Rust's type system enforces acknowledgment via `JobAck`; the reaper and retry logic handle crashes and transient errors. As long as Postgres lives, every job eventually finishes or hard-fails.

## Alternatives

Something that might suit your project even better:

* [`pgmq`](https://docs.rs/pgmq) is an SQS-style message queue with visibility timeouts and archive/replay. `postmodern` is a job queue with explicit status lifecycle, built-in retry/backoff, priority, and a CLI for operations.
* [`sqlxmq`](https://docs.rs/sqlxmq) uses `#[job]` macros and a registry to define tasks. `postmodern` takes the opposite approach: jobs are plain serde types with no macros, traits, or registration required.
* [`graphile-worker`](https://crates.io/crates/graphile-worker) is a feature-rich framework: implement `TaskHandler` on your types, register them with `.define_job::<T>()`, and the worker calls your `run()` method with all the bells and whistles. `postmodern` is a library, not a framework: jobs are plain serde types, you write your own processing loop, and there's less to learn.
* [`background-jobs`](https://docs.rs/background-jobs) integrates with Actix-web, supports pluggable storage backends, and provides per-job-type retry configuration via the `Job` trait. `postmodern` is framework-agnostic with no special traits; any serde type is a job.

## Usage

Example code you can fit on the back of a postcard:

```rust,no_run
use postmodern::{Queue, EnqueueOptions};
use futures::StreamExt;
use std::pin::pin;
# type MyPayload = String;
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
# let payload = String::new();

let queue = Queue::connect("postgres://...").await?;
queue.create_queue("tasks", false).await?;

// Enqueue a job
let id = queue.enqueue("tasks", payload, EnqueueOptions::default()).await?;

// Process jobs
let mut stream = pin!(queue.stream_jobs::<MyPayload, _, _>(["tasks"]));
while let Some(job) = stream.next().await {
    let (payload, ack) = job.into_parts();

    // Process payload...

    ack.commit().await?;  // Mark finished
    // Or: ack.soft_fail("reason").await?;  // Retry with backoff
    // Or: ack.hard_fail("reason").await?;  // Permanent failure
}
# Ok(())
# }
```

This is enough to schedule a job and also contains the code to implement a custom processor/worker. Dropping a `JobAck` without calling any method triggers `soft_fail("dropped without ack")`. If you need concurrency, spawn tasks or use `buffer_unordered` on the stream.

## Queue management

Queues must be created before enqueueing jobs. A queue can be paused to prevent new jobs from being processed; existing pending jobs transition to `Paused` and new jobs inherit that state by default.

```rust,no_run
# use postmodern::Queue;
# async fn example(queue: &Queue) -> Result<(), Box<dyn std::error::Error>> {
queue.create_queue("tasks", false).await?;  // Returns true if created
queue.pause_queue("tasks").await?;          // Returns count of paused jobs
queue.resume_queue("tasks").await?;         // Returns count of resumed jobs
queue.rename_queue("tasks", "jobs").await?; // Renames queue and all its jobs
# Ok(())
# }
```

The `EnqueueOptions::initial_state` field controls whether jobs start as `Pending`, `Paused`, or `Auto` (inherits from queue state, the default).

## Idempotency keys

Use `EnqueueOptions::key` to prevent duplicate jobs. When a key is set, enqueueing returns `None` if a job with that key already exists in the queue:

```rust,no_run
# use postmodern::{Queue, EnqueueOptions};
# async fn example(queue: &Queue) -> Result<(), Box<dyn std::error::Error>> {
let opts = EnqueueOptions {
    key: Some("user-123-welcome-email".into()),
    ..Default::default()
};

// First enqueue succeeds
let id = queue.enqueue("emails", "welcome", opts.clone()).await?;
assert!(id.is_some());

// Duplicate key returns None instead of creating a second job
let dup = queue.enqueue("emails", "welcome", opts).await?;
assert!(dup.is_none());

// Look up a job by its key
let job = queue.get_job_by_key("emails", "user-123-welcome-email").await?;
# Ok(())
# }
```

Keys are scoped to their queue. Deleting a job frees its key for reuse.

## Job operations

Beyond streaming, jobs can be fetched by ID, listed, moved, or copied:

```rust,no_run
# use postmodern::{Queue, EnqueueOptions};
# use uuid::Uuid;
# type T = String;
# async fn example(queue: &Queue, id: Uuid) -> Result<(), Box<dyn std::error::Error>> {
// Fetch and lock a specific job
let job = queue.fetch_job::<T>(id).await?;

// List pending jobs (metadata only, no payload)
let jobs = queue.list_pending("tasks").await?;
# Ok(())
# }
```

## Lock refresh

For jobs exceeding `LOCK_DURATION`, call `refresh_lock()` periodically to prevent reaping:

```rust,no_run
# use postmodern::job::PendingJob;
# async fn example(job: PendingJob<String>) -> Result<(), Box<dyn std::error::Error>> {
let (payload, mut ack) = job.into_parts();
loop {
    // Do work chunk...
    ack.refresh_lock().await?;
#   break;
}
ack.commit().await?;
# Ok(())
# }
```

## Retry behavior

Soft failures trigger exponential backoff: immediate retry on first failure, then 25min, 50min, 100min, etc. After 8 retries (~53 hours total), the job transitions to `Failed`. Use `hard_fail` for unrecoverable errors that should not be retried.

## Reaper

Jobs stuck in `in_progress` (e.g., after worker crash) must be reaped. The reaper soft-fails expired jobs, respecting retry limits:

```rust,no_run
# use postmodern::Queue;
# async fn example(queue: &Queue) -> Result<(), Box<dyn std::error::Error>> {
// Run once
let (reaped_count, next_expiry) = queue.reap().await?;

// Or run continuously (never returns)
queue.run_reaper().await;
# }
```

The reaper wakes at most every 10 minutes, or earlier if a lock is about to expire.

## CLI

The `pm` binary provides queue and job management commands. Configure the database URL in `~/.config/postmodern/config.toml`:

```toml
database_url = "postgres://..."
```

Or pass `--db` on each invocation.

### Queue management

- `pm queue ls`: List all queues
- `pm queue create <name> [--paused]`: Create a queue
- `pm queue delete <name>`: Delete a queue and all its jobs
- `pm queue pause <name>`: Pause a queue
- `pm queue resume <name>`: Resume a queue
- `pm queue rename <from> <to>`: Rename a queue

### Job operations

- `pm job ls [-q queue] [-s status] [-l limit]`: List jobs (status: `pending`, `paused`, `in-progress`, `finished`, `failed`)
- `pm job show <id>`: Show job details
- `pm job next <queue> [--peek] [--ack]`: Get next job from queue (locks it by default; `--peek` releases back to pending, `--ack` marks finished)
- `pm job restart <id>... [--force]`: Restart jobs (reset to pending; `--force` breaks in_progress locks)
- `pm job delete <id>...`: Delete jobs
- `pm job fail <id>... -m <message>`: Hard fail jobs with error message
- `pm job done <id>...`: Mark pending/in-progress jobs as finished
- `pm job search [-q queue] [-s status] <pattern>`: Search payloads (aborts at 50MB, use `--no-limit` to override)
- `pm job get <path> <id>`: Extract value from payload (e.g., `items[0].pdf`)

### Database maintenance

- `pm db stats`: Show queue statistics
- `pm db reap`: Run the reaper once

### PostgreSQL backup/restore

- `pm pg backup`: Backup database using pg_dump (writes to stdout)
- `pm pg restore`: Restore database using pg_restore (reads from stdin)

## Limitations aka future features

- **Scheduled jobs**: enqueue jobs to run at a specific time or on a cron schedule
- **LISTEN/NOTIFY**: use Postgres notifications for lower-latency job delivery instead of polling
- **Configurable settings**: lock duration, retry limits, and backoff parameters are currently hardcoded with sane defaults
- **Automatic pruning**: finished jobs must be deleted explicitly; no built-in retention policy yet
- **Standalone reaper**: a long-running `pm reap` command that can be deployed separately instead of scheduling the reaper in your application
