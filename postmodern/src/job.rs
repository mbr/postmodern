//! Job types and acknowledgment handles.

use std::{future::Future, time::Duration};

use chrono::{DateTime, Utc};
use display_full_error::DisplayFullErrorExt;
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::AckError;

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
    /// Connection pool for acknowledgment operations.
    pub(crate) pool: PgPool,
    /// Lock token for this checkout.
    pub(crate) lock_token: Uuid,
}

impl<T> PendingJob<T> {
    /// Separates the payload from the acknowledgment handle.
    ///
    /// Returns the payload and a [`JobAck`] for signaling completion, failure, or retry.
    pub fn into_parts(self) -> (T, JobAck) {
        let ack = JobAck::new(self.meta.id, self.pool, self.lock_token);
        (self.payload, ack)
    }

    /// Runs a function with the payload and acknowledges the job based on its result.
    ///
    /// On success, commits the job and returns the value. On failure, marks the job for retry
    /// with the error message and returns the error.
    pub async fn run<F, Fut, R, E>(self, f: F) -> Result<R, JobAckError<R, E>>
    where
        F: FnOnce(T) -> Fut,
        Fut: Future<Output = Result<R, E>>,
        E: std::error::Error,
    {
        let (payload, ack) = self.into_parts();
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
}

impl JobAck {
    /// Creates a new acknowledgment handle.
    pub(crate) fn new(id: Uuid, pool: PgPool, lock_token: Uuid) -> Self {
        Self {
            id,
            pool: Some(pool),
            lock_token,
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

    /// Runs a future and acknowledges the job based on its result.
    ///
    /// On success, commits the job and returns the value. On failure, marks the job for retry
    /// with the error message and returns the error.
    pub async fn run<Fut, T, E>(self, fut: Fut) -> Result<T, JobAckError<T, E>>
    where
        Fut: Future<Output = Result<T, E>>,
        E: std::error::Error,
    {
        match fut.await {
            Ok(value) => match self.commit().await {
                Ok(()) => Ok(value),
                Err(e) => Err(JobAckError::FailedToCommit(value, e)),
            },
            Err(e) => match self.soft_fail(&e.to_string_full()).await {
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
