# xpq

Postgres-backed job queue with status-based locking.

Jobs are locked by transitioning to `in_progress` status with a lock timestamp. The lock acts as a lease that must be refreshed for long-running jobs. Jobs not acknowledged within `LOCK_DURATION` (20 minutes) are eligible for reaping.

## Usage

```rust,no_run
use xpq::{Queue, EnqueueOptions};
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
let mut stream = pin!(queue.try_stream_jobs::<MyPayload>("tasks"));
while let Some(result) = stream.next().await {
    let job = result?;
    let (payload, ack) = job.into_parts();

    // Process payload...

    ack.commit().await?;  // Mark finished
    // Or: ack.soft_fail("reason").await?;  // Retry with backoff
    // Or: ack.hard_fail("reason").await?;  // Permanent failure
}
# Ok(())
# }
```

Dropping a `JobAck` without calling any method triggers `soft_fail("dropped without ack")`.

## Queue management

Queues must be created before enqueueing jobs. A queue can be paused to prevent new jobs from being processed; existing pending jobs transition to `Paused` and new jobs inherit that state by default.

```rust,no_run
# use xpq::Queue;
# async fn example(queue: &Queue) -> Result<(), Box<dyn std::error::Error>> {
queue.create_queue("tasks", false).await?;  // Returns true if created
queue.pause_queue("tasks").await?;          // Returns count of paused jobs
queue.resume_queue("tasks").await?;         // Returns count of resumed jobs
# Ok(())
# }
```

The `EnqueueOptions::initial_state` field controls whether jobs start as `Pending`, `Paused`, or `Auto` (inherits from queue state, the default).

## Job operations

Beyond streaming, jobs can be fetched by ID, listed, moved, or copied:

```rust,no_run
# use xpq::{Queue, EnqueueOptions};
# use uuid::Uuid;
# type T = String;
# async fn example(queue: &Queue, id: Uuid) -> Result<(), Box<dyn std::error::Error>> {
// Fetch and lock a specific job
let job = queue.fetch_job::<T>(id).await?;

// List pending jobs (metadata only, no payload)
let jobs = queue.list_pending("tasks").await?;

// Move job to another queue (same payload, same ID)
queue.move_jobs(&[id], "other-queue").await?;

// Copy job to another queue (new ID, shared payload via refcount)
let new_id = queue.copy_job(id, "other-queue", EnqueueOptions::default()).await?;
# Ok(())
# }
```

## Lock refresh

For jobs exceeding `LOCK_DURATION`, call `refresh_lock()` periodically to prevent reaping:

```rust,no_run
# use xpq::job::PendingJob;
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

Soft failures trigger exponential backoff: immediate retry on first failure, then 25min, 50min, 100min, etc. After 8 retries (~53 hours total), the job transitions to `Failed`. Use `hard_fail` for poison pills or unrecoverable errors that should not be retried.

## Reaper

Jobs stuck in `in_progress` (e.g., after worker crash) must be reaped. The reaper soft-fails expired jobs, respecting retry limits:

```rust,no_run
# use xpq::Queue;
# async fn example(queue: &Queue) -> Result<(), Box<dyn std::error::Error>> {
// Run once
let (reaped_count, next_expiry) = queue.reap().await?;

// Or run continuously (never returns)
queue.run_reaper().await;
# }
```

The reaper wakes at most every 10 minutes, or earlier if a lock is about to expire.

## Payload storage

Payloads are serialized with MessagePack and deduplicated by content hash. Multiple jobs can reference the same payload; a reference count tracks usage for cleanup.

## CLI

The `xpq` binary provides queue and job management commands. Configure the database URL in `~/.config/xpq/config.toml`:

```toml
database_url = "postgres://..."
```

Or pass `--db` on each invocation.

### Queue management

- `xpq queue ls` — List all queues
- `xpq queue create <name> [--paused]` — Create a queue
- `xpq queue delete <name>` — Delete a queue and all its jobs
- `xpq queue pause <name>` — Pause a queue
- `xpq queue resume <name>` — Resume a queue

### Job operations

- `xpq job ls [-q queue] [-s status] [-l limit]` — List jobs (status: `pending`, `paused`, `in-progress`, `finished`, `failed`)
- `xpq job show <id>` — Show job details
- `xpq job next <queue> [--peek] [--ack]` — Get next job from queue (locks it by default; `--peek` releases back to pending, `--ack` marks finished)
- `xpq job move <id>... -t <queue>` — Move jobs to another queue
- `xpq job copy <id> -t <queue>` — Copy a job to another queue
- `xpq job restart <id>... [--force]` — Restart jobs (reset to pending; `--force` breaks in_progress locks)
- `xpq job delete <id>...` — Delete jobs
- `xpq job fail <id>... -m <message>` — Hard fail jobs with error message
- `xpq job done <id>...` — Mark pending/in-progress jobs as finished
- `xpq job search [-q queue] [-s status] <pattern>` — Search payloads (aborts at 50MB, use `--no-limit` to override)
- `xpq job get <path> <id>` — Extract value from payload (e.g., `items[0].pdf`)

### Database maintenance

- `xpq db stats` — Show queue statistics
- `xpq db reap` — Run the reaper once

### PostgreSQL backup/restore

- `xpq pg backup` — Backup database using pg_dump (writes to stdout)
- `xpq pg restore` — Restore database using pg_restore (reads from stdin)

## Limitations

- Polling-based (no LISTEN/NOTIFY)
- No automatic cleanup of finished jobs
- Reaper must be scheduled by the application
