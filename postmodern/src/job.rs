//! Job types and acknowledgment handles.

use std::{collections::HashMap, fmt::Display, future::Future, time::Duration};

use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::{AckError, AdvanceError, CheckpointError};

/// Duration before an in-progress job is considered crashed and eligible for reaping.
///
/// Workers processing jobs longer than this should call [`JobAck::refresh_lock`].
pub const LOCK_DURATION: Duration = Duration::from_mins(20);

/// Base delay for exponential backoff on soft failures.
///
/// Delay doubles with each retry: 0, 25min, 50min, 100min, ...
pub const RETRY_BACKOFF_BASE: Duration = Duration::from_mins(25);

/// Maximum number of automatic retries before a job is permanently failed.
///
/// With [`RETRY_BACKOFF_BASE`] of 25 minutes and 8 retries, total retry window is ~53 hours.
pub const MAX_RETRIES: u32 = 8;

/// Maximum interval between reaper runs.
///
/// The reaper also wakes when the next lock is about to expire, whichever comes first.
pub const REAPER_INTERVAL: Duration = Duration::from_mins(10);

/// Job status in the queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq, sqlx::Type)]
#[sqlx(type_name = "job_status", rename_all = "snake_case")]
pub enum JobStatus {
    /// Job is available for processing.
    Pending,
    /// Job is paused.
    Paused,
    /// Job is currently being processed.
    InProgress,
    /// Job has been processed successfully.
    Finished,
    /// Job processing failed.
    Failed,
}

/// Initial state for enqueued jobs.
#[derive(Clone, Copy, Debug, Default)]
pub enum InitialState {
    /// Check queue's paused state; resolve to Pending or Paused accordingly.
    #[default]
    Auto,
    /// Job is immediately available for processing (ignores queue state).
    Pending,
    /// Job is paused and must be unpaused before processing (ignores queue state).
    Paused,
}

/// Options for advancing to the next pipeline stage.
#[derive(Clone, Debug, Default)]
pub struct AdvanceOptions {
    /// Human-readable description for the new job.
    pub description: Option<String>,
    /// Priority for ordering (higher = more urgent).
    pub priority: i64,
}

/// Job metadata without the payload.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct JobMetadata {
    /// Job identifier.
    pub id: Uuid,
    /// Queue this job belongs to.
    pub queue: String,
    /// Human-readable description.
    pub description: Option<String>,
    /// Current job status.
    pub status: JobStatus,
    /// When the job was created.
    pub created_at: DateTime<Utc>,
    /// Priority for ordering (higher = more urgent).
    pub priority: i64,
}

/// A job retrieved from the queue, ready for processing.
///
/// The job is marked as in-progress in the database. Use [`into_parts`](Self::into_parts) to
/// extract the payload and acknowledgment handle.
pub struct PendingJob<T> {
    /// Job metadata.
    pub meta: JobMetadata,
    /// Deserialized payload.
    pub payload: T,
    /// Acknowledgment handle.
    ack: JobAck,
}

impl<T> PendingJob<T> {
    /// Creates a pending job from raw parts.
    pub(crate) fn from_raw(meta: JobMetadata, payload: T, ack: JobAck) -> Self {
        Self { meta, payload, ack }
    }

    /// Separates the job into its components.
    ///
    /// Returns the metadata, payload, and a [`JobAck`] for signaling completion, failure, or retry.
    pub fn into_parts(self) -> (JobMetadata, T, JobAck) {
        (self.meta, self.payload, self.ack)
    }

    /// Runs a function with the payload and acknowledges the job based on its result.
    ///
    /// On success, commits the job and returns the value. On failure, marks the job for retry
    /// with the error message (using alternate `Display` formatting) and returns the error.
    ///
    /// # Error formatting
    ///
    /// The error is stored in the database using `{:#}` (alternate `Display`). For
    /// [`anyhow::Error`](https://docs.rs/anyhow), this includes the full causal chain.
    /// For [`std::error::Error`] types, use
    /// [`DisplayFullErrorExt::to_string_full`](https://docs.rs/display-full-error) to
    /// capture the chain:
    ///
    /// ```no_run
    /// # use postmodern::job::PendingJob;
    /// use display_full_error::DisplayFullErrorExt;
    /// use futures::TryFutureExt;
    ///
    /// # async fn do_work(_: ()) -> Result<(), std::io::Error> { Ok(()) }
    /// # async fn example(job: PendingJob<()>) {
    /// let _ = job.run(|payload| do_work(payload).map_err(|e| e.to_string_full())).await;
    /// # }
    /// ```
    pub async fn run<F, Fut, R, E>(self, f: F) -> Result<R, JobAckError<R, E>>
    where
        F: FnOnce(T) -> Fut,
        Fut: Future<Output = Result<R, E>>,
        E: Display,
    {
        let (_meta, payload, ack) = self.into_parts();
        ack.run(f(payload)).await
    }
}

/// Handle for acknowledging job completion, failure, or retry.
///
/// Must be used to signal the job outcome. Dropping without calling any method marks the job as
/// failed with a "dropped without ack" error.
pub struct JobAck {
    /// Job identifier.
    id: Uuid,
    /// Connection pool, `None` if already consumed.
    pool: Option<PgPool>,
    /// Lock token for this checkout.
    lock_token: Uuid,
    /// Tracks checkpoint encounter count within this execution (name → count).
    encounters: HashMap<String, u32>,
}

impl JobAck {
    /// Creates a new acknowledgment handle.
    pub(crate) fn new(id: Uuid, pool: PgPool, lock_token: Uuid) -> Self {
        Self {
            id,
            pool: Some(pool),
            lock_token,
            encounters: HashMap::new(),
        }
    }

    /// Returns the job identifier.
    pub fn id(&self) -> Uuid {
        self.id
    }

    /// Returns the lock token for this checkout.
    pub fn lock_token(&self) -> Uuid {
        self.lock_token
    }

    /// Marks the job as successfully finished.
    ///
    /// Returns [`AckError::LockLost`] if the lock was lost due to timeout.
    pub async fn commit(mut self) -> Result<(), AckError> {
        let pool = self.pool.take().expect("ack already consumed");
        mark_finished(&pool, self.id, self.lock_token).await
    }

    /// Marks the job as permanently failed with an error message.
    ///
    /// Use this for unrecoverable errors. The job will not be retried.
    /// Returns [`AckError::LockLost`] if the lock was lost due to timeout.
    pub async fn hard_fail(mut self, reason: &str) -> Result<(), AckError> {
        let pool = self.pool.take().expect("ack already consumed");
        mark_hard_failed(&pool, self.id, self.lock_token, reason).await
    }

    /// Marks the job for retry with exponential backoff, or permanently failed if exhausted.
    ///
    /// Increments `retry_count` and schedules a retry with exponential backoff. If retries
    /// are exhausted ([`MAX_RETRIES`]), the job transitions to failed state instead.
    /// Returns [`AckError::LockLost`] if the lock was lost due to timeout.
    pub async fn soft_fail(mut self, reason: &str) -> Result<(), AckError> {
        let pool = self.pool.take().expect("ack already consumed");
        mark_soft_failed(&pool, self.id, self.lock_token, reason).await
    }

    /// Releases the job back to pending state without counting as a failure.
    ///
    /// Use this to return a job to the queue without processing it. The retry count is preserved.
    /// Returns [`AckError::LockLost`] if the lock was lost due to timeout.
    pub async fn restart(mut self) -> Result<(), AckError> {
        let pool = self.pool.take().expect("ack already consumed");
        mark_restarted(&pool, self.id, self.lock_token).await
    }

    /// Atomically commits this job and enqueues a new job in the next stage.
    ///
    /// Respects the target queue's paused state. Returns the new job's ID on success.
    pub async fn advance(
        mut self,
        next_queue: &str,
        payload: &[u8],
        options: AdvanceOptions,
    ) -> Result<Uuid, AdvanceError> {
        let pool = self.pool.take().expect("ack already consumed");
        let next_id = Uuid::now_v7();

        let row: Option<(Uuid,)> = sqlx::query_as(
            "WITH finished AS ( \
                 UPDATE jobs SET status = 'finished', lock = now(), lock_token = NULL \
                 WHERE id = $1 AND lock_token = $2 \
                 RETURNING id \
             ), \
             target_queue AS ( \
                 SELECT paused FROM queues WHERE queue = $3 \
             ) \
             INSERT INTO jobs (id, queue, status, payload, priority, description) \
             SELECT $4, $3, \
                    CASE WHEN q.paused THEN 'paused'::job_status ELSE 'pending'::job_status END, \
                    $5, $6, $7 \
             FROM finished f, target_queue q \
             RETURNING id",
        )
        .bind(self.id)
        .bind(self.lock_token)
        .bind(next_queue)
        .bind(next_id)
        .bind(payload)
        .bind(options.priority)
        .bind(&options.description)
        .fetch_optional(&pool)
        .await
        .map_err(AdvanceError::Database)?;

        row.map(|(id,)| id).ok_or(AdvanceError::Failed)
    }

    /// Consumes the handle without taking any action.
    ///
    /// The job remains in its current state (typically in_progress). Use this when you want to
    /// keep the job locked for later resolution via other means.
    pub fn forget(mut self) {
        self.pool.take();
    }

    /// Extends the lock to prevent the job from being reaped.
    ///
    /// Call this periodically for long-running jobs that exceed [`LOCK_DURATION`]. Returns
    /// [`AckError::LockLost`] if the lock was already lost.
    pub async fn refresh_lock(&mut self) -> Result<(), AckError> {
        let pool = self.pool.as_ref().expect("ack already consumed");
        let result = sqlx::query(
            "UPDATE jobs SET lock = now() \
             WHERE id = $1 AND lock_token = $2 AND status = 'in_progress'",
        )
        .bind(self.id)
        .bind(self.lock_token)
        .execute(pool)
        .await
        .map_err(AckError::Database)?;

        if result.rows_affected() == 0 {
            return Err(AckError::LockLost);
        }
        Ok(())
    }

    /// Creates a checkpoint that memoizes work across retries.
    ///
    /// On first encounter, runs the closure and stores the result. On replay (retry after
    /// failure), returns the stored value without executing the closure.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::DuplicateCheckpoint`] if called twice with the same name in one
    /// execution. Returns [`CheckpointError::LockLost`] if the job lock was lost before the
    /// checkpoint could be written. Returns [`CheckpointError::Closure`] if the closure returns
    /// `Err` (nothing is stored, next attempt re-runs the closure).
    pub async fn checkpoint<T, F, Fut, E>(
        &mut self,
        name: &str,
        f: F,
    ) -> Result<T, CheckpointError<E>>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let pool = self.pool.as_ref().expect("ack already consumed");

        // Track encounters within this execution
        let count = self.encounters.entry(name.to_string()).or_insert(0);
        if *count > 0 {
            return Err(CheckpointError::DuplicateCheckpoint(name.to_string()));
        }
        *count += 1;

        // Check for existing checkpoint from prior run (replay hit)
        let existing: Option<(Vec<u8>,)> = sqlx::query_as(
            "SELECT value FROM checkpoints WHERE job_id = $1 AND name = $2 AND seq = 0",
        )
        .bind(self.id)
        .bind(name)
        .fetch_optional(pool)
        .await
        .map_err(CheckpointError::Database)?;

        if let Some((bytes,)) = existing {
            return rmp_serde::from_slice(&bytes).map_err(CheckpointError::Deserialize);
        }

        // First execution: run the closure
        let value = f().await.map_err(CheckpointError::Closure)?;

        // Serialize the result
        let bytes = rmp_serde::to_vec_named(&value).map_err(CheckpointError::Serialize)?;

        // Store checkpoint, but only if we still hold the lock.
        // If lock was lost (reaper reclaimed, another worker took over) or another writer
        // raced us, this INSERT will affect 0 rows and we return LockLost.
        let result = sqlx::query(
            "INSERT INTO checkpoints (job_id, name, seq, value) \
             SELECT $1, $2, 0, $3 FROM jobs WHERE id = $1 AND lock_token = $4 \
             ON CONFLICT (job_id, name, seq) DO NOTHING",
        )
        .bind(self.id)
        .bind(name)
        .bind(&bytes)
        .bind(self.lock_token)
        .execute(pool)
        .await
        .map_err(CheckpointError::Database)?;

        if result.rows_affected() == 0 {
            return Err(CheckpointError::LockLost);
        }

        Ok(value)
    }

    /// Creates a sequenced checkpoint for deliberate recurrence (loops).
    ///
    /// Unlike [`checkpoint`](Self::checkpoint), allows the same name multiple times within a
    /// single execution. Each encounter gets an incrementing sequence number (0, 1, 2, ...).
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::LockLost`] if the job lock was lost before the checkpoint could
    /// be written. Returns [`CheckpointError::Closure`] if the closure returns `Err`.
    pub async fn checkpoint_seq<T, F, Fut, E>(
        &mut self,
        name: &str,
        f: F,
    ) -> Result<T, CheckpointError<E>>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let pool = self.pool.as_ref().expect("ack already consumed");

        // Get current sequence (before increment) and then increment
        let count = self.encounters.entry(name.to_string()).or_insert(0);
        let seq = *count;
        *count += 1;

        // Check for existing checkpoint from prior run (replay hit)
        let existing: Option<(Vec<u8>,)> = sqlx::query_as(
            "SELECT value FROM checkpoints WHERE job_id = $1 AND name = $2 AND seq = $3",
        )
        .bind(self.id)
        .bind(name)
        .bind(seq as i32)
        .fetch_optional(pool)
        .await
        .map_err(CheckpointError::Database)?;

        if let Some((bytes,)) = existing {
            return rmp_serde::from_slice(&bytes).map_err(CheckpointError::Deserialize);
        }

        // First execution: run the closure
        let value = f().await.map_err(CheckpointError::Closure)?;

        // Serialize the result
        let bytes = rmp_serde::to_vec_named(&value).map_err(CheckpointError::Serialize)?;

        // Store checkpoint, but only if we still hold the lock.
        let result = sqlx::query(
            "INSERT INTO checkpoints (job_id, name, seq, value) \
             SELECT $1, $2, $3, $4 FROM jobs WHERE id = $1 AND lock_token = $5 \
             ON CONFLICT (job_id, name, seq) DO NOTHING",
        )
        .bind(self.id)
        .bind(name)
        .bind(seq as i32)
        .bind(&bytes)
        .bind(self.lock_token)
        .execute(pool)
        .await
        .map_err(CheckpointError::Database)?;

        if result.rows_affected() == 0 {
            return Err(CheckpointError::LockLost);
        }

        Ok(value)
    }

    /// Runs a future and acknowledges the job based on its result.
    ///
    /// On success, commits the job and returns the value. On failure, marks the job for retry
    /// with the error message (using alternate `Display` formatting) and returns the error.
    ///
    /// See [`PendingJob::run`] for details on error formatting.
    pub async fn run<Fut, T, E>(self, fut: Fut) -> Result<T, JobAckError<T, E>>
    where
        Fut: Future<Output = Result<T, E>>,
        E: Display,
    {
        match fut.await {
            Ok(value) => match self.commit().await {
                Ok(()) => Ok(value),
                Err(e) => Err(JobAckError::FailedToCommit(value, e)),
            },
            Err(e) => match self.soft_fail(&format!("{:#}", e)).await {
                Ok(()) => Err(JobAckError::RunError(e)),
                Err(ack_err) => Err(JobAckError::SoftFailError {
                    error: e,
                    source: ack_err,
                }),
            },
        }
    }
}

/// Errors from [`JobAck::run`].
#[derive(Debug, thiserror::Error)]
pub enum JobAckError<T, E> {
    /// Job completed successfully but commit failed.
    #[error("failed to commit job")]
    FailedToCommit(T, #[source] AckError),
    /// Job failed and marking it for retry also failed.
    #[error("failed to mark job as soft-failed (job failed with {error})")]
    SoftFailError {
        /// The original error from job execution.
        error: E,
        /// The error from attempting to soft-fail.
        #[source]
        source: AckError,
    },
    /// Job execution failed (soft-fail succeeded).
    #[error(transparent)]
    RunError(E),
}

impl Drop for JobAck {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            let id = self.id;
            let lock_token = self.lock_token;
            tokio::spawn(async move {
                let _ = mark_soft_failed(&pool, id, lock_token, "dropped without ack").await;
            });
        }
    }
}

/// Marks a job as finished.
async fn mark_finished(pool: &PgPool, id: Uuid, lock_token: Uuid) -> Result<(), AckError> {
    let result = sqlx::query(
        "UPDATE jobs SET status = 'finished', lock = now(), lock_token = NULL \
         WHERE id = $1 AND lock_token = $2",
    )
    .bind(id)
    .bind(lock_token)
    .execute(pool)
    .await
    .map_err(AckError::Database)?;

    if result.rows_affected() == 0 {
        return Err(AckError::LockLost);
    }
    Ok(())
}

/// Marks a job as permanently failed.
async fn mark_hard_failed(
    pool: &PgPool,
    id: Uuid,
    lock_token: Uuid,
    reason: &str,
) -> Result<(), AckError> {
    let result = sqlx::query(
        "UPDATE jobs SET status = 'failed', lock = now(), lock_token = NULL, error = $1 \
         WHERE id = $2 AND lock_token = $3",
    )
    .bind(reason)
    .bind(id)
    .bind(lock_token)
    .execute(pool)
    .await
    .map_err(AckError::Database)?;

    if result.rows_affected() == 0 {
        return Err(AckError::LockLost);
    }
    Ok(())
}

/// Marks a job for retry with backoff, or permanently failed if retries exhausted.
async fn mark_soft_failed(
    pool: &PgPool,
    id: Uuid,
    lock_token: Uuid,
    reason: &str,
) -> Result<(), AckError> {
    let max_retries = MAX_RETRIES as i32;
    let backoff_base_mins = (RETRY_BACKOFF_BASE.as_secs() / 60) as i32;

    // retry_count references the OLD value in all expressions
    // - If old >= max_retries: transition to failed (exhausted)
    // - If old == 0: immediate retry (first failure)
    // - Otherwise: exponential backoff delay
    let result = sqlx::query(
        "UPDATE jobs SET \
             retry_count = retry_count + 1, \
             status = CASE WHEN retry_count >= $3 THEN 'failed'::job_status \
                           ELSE 'pending'::job_status END, \
             lock = CASE \
                 WHEN retry_count >= $3 THEN now() \
                 WHEN retry_count = 0 THEN now() \
                 ELSE now() + make_interval(mins => ($4 * power(2, retry_count - 1))::int) \
             END, \
             lock_token = NULL, \
             error = $5 \
         WHERE id = $1 AND lock_token = $2",
    )
    .bind(id)
    .bind(lock_token)
    .bind(max_retries)
    .bind(backoff_base_mins)
    .bind(reason)
    .execute(pool)
    .await
    .map_err(AckError::Database)?;

    if result.rows_affected() == 0 {
        return Err(AckError::LockLost);
    }
    Ok(())
}

/// Releases a job back to pending without counting as a failure.
async fn mark_restarted(pool: &PgPool, id: Uuid, lock_token: Uuid) -> Result<(), AckError> {
    let result = sqlx::query(
        "UPDATE jobs SET status = 'pending', lock = NULL, lock_token = NULL, error = NULL \
         WHERE id = $1 AND lock_token = $2",
    )
    .bind(id)
    .bind(lock_token)
    .execute(pool)
    .await
    .map_err(AckError::Database)?;

    if result.rows_affected() == 0 {
        return Err(AckError::LockLost);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, pin::pin, sync::atomic::AtomicU32};

    use futures::StreamExt;

    use crate::{EnqueueOptions, Queue};

    async fn setup_db() -> (Queue, pgdb::DbInstance) {
        let db_url = pgdb::db_fixture();
        let queue = Queue::connect(db_url.as_str())
            .await
            .expect("failed to connect to test database");
        queue
            .create_queue("test", false)
            .await
            .expect("failed to create test queue");
        (queue, db_url)
    }

    #[tokio::test]
    async fn checkpoint_store_then_skip() {
        use std::sync::atomic::Ordering;

        let (queue, _db) = setup_db().await;
        static CALL_COUNT: AtomicU32 = AtomicU32::new(0);

        let _id = queue
            .enqueue("test", "payload", EnqueueOptions::default())
            .await
            .unwrap()
            .unwrap();

        // First execution: checkpoint runs the closure
        let mut stream = pin!(queue.try_stream_jobs::<String, _, _>(["test"]));
        let job = stream.next().await.unwrap().unwrap();
        let (_, _, mut ack) = job.into_parts();

        let result: i32 = ack
            .checkpoint("step", || async {
                CALL_COUNT.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Infallible>(42)
            })
            .await
            .unwrap();
        assert_eq!(result, 42);
        assert_eq!(CALL_COUNT.load(Ordering::SeqCst), 1);

        // Soft-fail to trigger a retry
        ack.soft_fail("simulated failure").await.unwrap();

        // Second execution (replay): checkpoint returns stored value without running closure
        let job = stream.next().await.unwrap().unwrap();
        let (_, _, mut ack) = job.into_parts();

        let result: i32 = ack
            .checkpoint("step", || async {
                CALL_COUNT.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Infallible>(99) // different value, but should return stored 42
            })
            .await
            .unwrap();
        assert_eq!(result, 42); // stored value from first run
        assert_eq!(CALL_COUNT.load(Ordering::SeqCst), 1); // closure not called again

        ack.commit().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_intra_run_duplicate() {
        let (queue, _db) = setup_db().await;

        let _ = queue
            .enqueue("test", "payload", EnqueueOptions::default())
            .await
            .unwrap()
            .unwrap();

        let mut stream = pin!(queue.try_stream_jobs::<String, _, _>(["test"]));
        let job = stream.next().await.unwrap().unwrap();
        let (_, _, mut ack) = job.into_parts();

        // First call succeeds
        let _: i32 = ack
            .checkpoint("dup", || async { Ok::<_, Infallible>(1) })
            .await
            .unwrap();

        // Second call with same name in same execution errors
        let err = ack
            .checkpoint("dup", || async { Ok::<_, Infallible>(2) })
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            crate::error::CheckpointError::DuplicateCheckpoint(name) if name == "dup"
        ));

        ack.commit().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_replay_not_duplicate() {
        // Cross-run replay should NOT be treated as a duplicate
        let (queue, _db) = setup_db().await;

        let _ = queue
            .enqueue("test", "payload", EnqueueOptions::default())
            .await
            .unwrap()
            .unwrap();

        // First execution
        let mut stream = pin!(queue.try_stream_jobs::<String, _, _>(["test"]));
        let job = stream.next().await.unwrap().unwrap();
        let (_, _, mut ack) = job.into_parts();

        let _: i32 = ack
            .checkpoint("step", || async { Ok::<_, Infallible>(1) })
            .await
            .unwrap();

        ack.soft_fail("retry").await.unwrap();

        // Second execution (replay)
        let job = stream.next().await.unwrap().unwrap();
        let (_, _, mut ack) = job.into_parts();

        // This should succeed (replay hit), not error as duplicate
        let result: i32 = ack
            .checkpoint("step", || async { Ok::<_, Infallible>(2) })
            .await
            .unwrap();

        assert_eq!(result, 1); // returns stored value, not the closure's 2
        ack.commit().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_seq_loop() {
        use std::sync::atomic::Ordering;

        let (queue, _db) = setup_db().await;
        static CALL_COUNT: AtomicU32 = AtomicU32::new(0);

        let _ = queue
            .enqueue("test", "payload", EnqueueOptions::default())
            .await
            .unwrap()
            .unwrap();

        // First execution: process items 0, 1, then fail
        let mut stream = pin!(queue.try_stream_jobs::<String, _, _>(["test"]));
        let job = stream.next().await.unwrap().unwrap();
        let (_, _, mut ack) = job.into_parts();

        for i in 0..2 {
            let result: i32 = ack
                .checkpoint_seq("item", || async move {
                    CALL_COUNT.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, Infallible>(i * 10)
                })
                .await
                .unwrap();
            assert_eq!(result, i * 10);
        }
        assert_eq!(CALL_COUNT.load(Ordering::SeqCst), 2);

        ack.soft_fail("crash at item 2").await.unwrap();

        // Second execution (replay): items 0, 1 are cached; items 2, 3 run fresh
        let job = stream.next().await.unwrap().unwrap();
        let (_, _, mut ack) = job.into_parts();

        for i in 0..4 {
            let result: i32 = ack
                .checkpoint_seq("item", || async move {
                    CALL_COUNT.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, Infallible>(i * 10)
                })
                .await
                .unwrap();
            assert_eq!(result, i * 10);
        }
        // Only items 2, 3 ran their closures (2 more calls)
        assert_eq!(CALL_COUNT.load(Ordering::SeqCst), 4);

        ack.commit().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_keyed() {
        let (queue, _db) = setup_db().await;

        let _ = queue
            .enqueue("test", "payload", EnqueueOptions::default())
            .await
            .unwrap()
            .unwrap();

        let mut stream = pin!(queue.try_stream_jobs::<String, _, _>(["test"]));
        let job = stream.next().await.unwrap().unwrap();
        let (_, _, mut ack) = job.into_parts();

        // Distinct keys work fine
        for id in ["a", "b", "c"] {
            let _: String = ack
                .checkpoint(&format!("fetch-{id}"), || async move {
                    Ok::<_, Infallible>(format!("result-{id}"))
                })
                .await
                .unwrap();
        }

        // Repeated key in same execution errors
        let err = ack
            .checkpoint("fetch-a", || async { Ok::<_, Infallible>("x".to_string()) })
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            crate::error::CheckpointError::DuplicateCheckpoint(name) if name == "fetch-a"
        ));

        ack.commit().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_error_stores_nothing() {
        use std::sync::atomic::Ordering;

        let (queue, _db) = setup_db().await;
        static CALL_COUNT: AtomicU32 = AtomicU32::new(0);

        let id = queue
            .enqueue("test", "payload", EnqueueOptions::default())
            .await
            .unwrap()
            .unwrap();

        // First execution: closure errors
        let mut stream = pin!(queue.try_stream_jobs::<String, _, _>(["test"]));
        let job = stream.next().await.unwrap().unwrap();
        let (_, _, mut ack) = job.into_parts();

        let err = ack
            .checkpoint("failing", || async {
                CALL_COUNT.fetch_add(1, Ordering::SeqCst);
                Err::<i32, _>("oops")
            })
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            crate::error::CheckpointError::Closure(msg) if msg == "oops"
        ));

        // Verify no checkpoint was stored
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM checkpoints WHERE job_id = $1")
            .bind(id)
            .fetch_one(queue.pool())
            .await
            .unwrap();
        assert_eq!(count.0, 0);

        ack.soft_fail("retry after error").await.unwrap();

        // Second execution: closure runs again (not cached)
        let job = stream.next().await.unwrap().unwrap();
        let (_, _, mut ack) = job.into_parts();

        let result: i32 = ack
            .checkpoint("failing", || async {
                CALL_COUNT.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &str>(42) // succeed this time
            })
            .await
            .unwrap();

        assert_eq!(result, 42);
        assert_eq!(CALL_COUNT.load(Ordering::SeqCst), 2); // closure ran twice

        ack.commit().await.unwrap();
    }

    #[tokio::test]
    async fn checkpoint_cascade_delete() {
        let (queue, _db) = setup_db().await;

        let id = queue
            .enqueue("test", "payload", EnqueueOptions::default())
            .await
            .unwrap()
            .unwrap();

        let mut stream = pin!(queue.try_stream_jobs::<String, _, _>(["test"]));
        let job = stream.next().await.unwrap().unwrap();
        let (_, _, mut ack) = job.into_parts();

        let _: i32 = ack
            .checkpoint("step", || async { Ok::<_, Infallible>(1) })
            .await
            .unwrap();

        ack.commit().await.unwrap();

        // Verify checkpoint exists
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM checkpoints WHERE job_id = $1")
            .bind(id)
            .fetch_one(queue.pool())
            .await
            .unwrap();
        assert_eq!(count.0, 1);

        // Delete the job
        queue.delete_jobs(&[id]).await.unwrap();

        // Verify checkpoint was cascade-deleted
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM checkpoints WHERE job_id = $1")
            .bind(id)
            .fetch_one(queue.pool())
            .await
            .unwrap();
        assert_eq!(count.0, 0);
    }

    #[tokio::test]
    async fn checkpoint_lock_lost_on_stale_token() {
        // Simulates: Worker A is processing, lock expires, Worker B takes over.
        // Worker A (with stale token) should get LockLost when trying to write checkpoint.
        let (queue, _db) = setup_db().await;
        let pool = queue.pool().clone();

        let id = queue
            .enqueue("test", "payload", EnqueueOptions::default())
            .await
            .unwrap()
            .unwrap();

        let stale_token = uuid::Uuid::now_v7();
        let current_token = uuid::Uuid::now_v7();

        // Job is locked by current_token (simulating Worker B took over)
        sqlx::query("UPDATE jobs SET status = 'in_progress', lock_token = $1 WHERE id = $2")
            .bind(current_token)
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();

        // Worker A has stale token - checkpoint should fail
        let mut stale_ack = super::JobAck::new(id, pool.clone(), stale_token);
        let result = stale_ack
            .checkpoint("step", || async { Ok::<_, Infallible>(42) })
            .await;

        assert!(matches!(
            result,
            Err(crate::error::CheckpointError::LockLost)
        ));

        // Worker B has current token - checkpoint should succeed
        let mut current_ack = super::JobAck::new(id, pool.clone(), current_token);
        let value: i32 = current_ack
            .checkpoint("step", || async { Ok::<_, Infallible>(99) })
            .await
            .unwrap();

        assert_eq!(value, 99);

        // Verify only one checkpoint exists (from Worker B)
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM checkpoints WHERE job_id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count.0, 1);

        stale_ack.forget();
        current_ack.forget();
    }
}
