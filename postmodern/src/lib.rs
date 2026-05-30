#![doc = include_str!("../README.md")]

pub mod config;
pub mod error;
pub mod job;
mod stream;
mod transform;

use std::time::Duration;

use display_full_error::DisplayFullErrorExt;
use futures::Stream;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{postgres::PgRow, FromRow, PgPool, Row};
pub use transform::TransformResult;
use uuid::Uuid;
use uuid_suffix::{resolve_uuid_suffix, UuidSuffix};

use crate::{
    error::{
        ConnectError, EnqueueError, FetchError, ListError, ModifyError, ReapError, ResolveIdError,
    },
    job::{
        InitialState, JobMetadata, JobStatus, PendingJob, LOCK_DURATION, MAX_RETRIES,
        REAPER_INTERVAL, RETRY_BACKOFF_BASE,
    },
};

/// Options for enqueuing a job.
#[derive(Clone, Debug, Default)]
pub struct EnqueueOptions {
    /// Human-readable job description.
    pub description: Option<String>,
    /// Initial job state.
    pub initial_state: InitialState,
    /// Deduplication key for idempotent enqueue.
    pub key: Option<String>,
    /// Priority for ordering (higher = more urgent).
    pub priority: i64,
}

/// Queue information.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct QueueInfo {
    /// Queue name.
    pub queue: String,
    /// Whether the queue is paused.
    pub paused: bool,
}

/// Detailed job information for inspection.
#[derive(Clone, Debug, sqlx::FromRow)]
pub struct JobDetails {
    /// Job identifier.
    pub id: Uuid,
    /// Queue this job belongs to.
    pub queue: String,
    /// Current job status.
    pub status: JobStatus,
    /// Human-readable description.
    pub description: Option<String>,
    /// When the job was created.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Deduplication key if set.
    pub key: Option<String>,
    /// Lock timestamp.
    pub lock: Option<chrono::DateTime<chrono::Utc>>,
    /// Error message from last failure.
    pub error: Option<String>,
    /// Number of retry attempts.
    pub retry_count: i32,
    /// Priority for ordering (higher = more urgent).
    pub priority: i64,
}

/// Filter options for listing jobs.
#[derive(Clone, Debug, Default)]
pub struct JobFilter {
    /// Filter by queue name.
    pub queue: Option<String>,
    /// Filter by job status.
    pub status: Option<JobStatus>,
    /// Maximum number of jobs to return.
    pub limit: u32,
}

/// Per-queue job statistics.
#[derive(Clone, Debug)]
pub struct QueueStats {
    /// Queue name.
    pub queue: String,
    /// Number of pending jobs.
    pub pending: i64,
    /// Number of paused jobs.
    pub paused: i64,
    /// Number of in-progress jobs.
    pub in_progress: i64,
    /// Number of finished jobs.
    pub finished: i64,
    /// Number of failed jobs.
    pub failed: i64,
}

/// A Postgres-backed job queue.
#[derive(Clone)]
pub struct Queue {
    /// Connection pool.
    pool: PgPool,
}

impl Queue {
    /// Connects to the database and runs migrations.
    pub async fn connect(connection_string: &str) -> Result<Self, ConnectError> {
        let pool = PgPool::connect(connection_string)
            .await
            .map_err(ConnectError::Database)?;
        Self::from_pool(pool).await
    }

    /// Creates a queue from an existing connection pool.
    ///
    /// Runs migrations on the provided pool. Use this when you need to share a pool across
    /// multiple components.
    pub async fn from_pool(pool: PgPool) -> Result<Self, ConnectError> {
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .map_err(ConnectError::Migration)?;
        Ok(Self { pool })
    }

    /// Creates a queue from a pool without running migrations.
    pub(crate) fn from_pool_unchecked(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Enqueues a job with the given payload.
    ///
    /// Returns `Some(id)` with the job's UUID (v7, time-ordered) if created, or `None` if a job
    /// with the same key already exists in this queue.
    /// Returns [`EnqueueError::QueueNotFound`] if the queue does not exist.
    pub async fn enqueue<T: Serialize>(
        &self,
        queue: &str,
        payload: T,
        options: EnqueueOptions,
    ) -> Result<Option<Uuid>, EnqueueError> {
        let id = Uuid::now_v7();
        let payload_bytes = rmp_serde::to_vec_named(&payload).map_err(EnqueueError::Serialize)?;

        let status = self
            .resolve_initial_state(queue, options.initial_state)
            .await
            .map_err(EnqueueError::Database)?
            .ok_or(EnqueueError::QueueNotFound)?;

        let result = sqlx::query(
            "INSERT INTO jobs (id, queue, status, description, payload, priority, key) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (queue, key) WHERE key IS NOT NULL DO NOTHING",
        )
        .bind(id)
        .bind(queue)
        .bind(status)
        .bind(&options.description)
        .bind(&payload_bytes)
        .bind(options.priority)
        .bind(&options.key)
        .execute(&self.pool)
        .await
        .map_err(EnqueueError::Database)?;

        if result.rows_affected() == 0 {
            return Ok(None);
        }

        Ok(Some(id))
    }

    /// Lists all pending jobs in the specified queue.
    ///
    /// Returns job metadata without payloads, ordered by priority (highest first), then creation
    /// time (oldest first).
    pub async fn list_pending(&self, queue: &str) -> Result<Vec<JobMetadata>, ListError> {
        sqlx::query_as(
            "SELECT id, queue, description, status, created_at, priority \
             FROM jobs \
             WHERE queue = $1 AND status = 'pending' AND (lock IS NULL OR lock <= now()) \
             ORDER BY priority DESC, created_at",
        )
        .bind(queue)
        .fetch_all(&self.pool)
        .await
        .map_err(ListError::Query)
    }

    /// Fetches and locks a specific job by ID.
    ///
    /// Returns `None` if the job doesn't exist or is not pending. The job is marked as in-progress
    /// immediately.
    pub async fn fetch_job<T: DeserializeOwned>(
        &self,
        id: Uuid,
    ) -> Result<Option<PendingJob<T>>, FetchError> {
        let lock_token = Uuid::now_v7();
        let row: Option<PgRow> = sqlx::query(
            "UPDATE jobs SET status = 'in_progress', lock = now(), lock_token = $2 \
             WHERE id = $1 AND status = 'pending' AND (lock IS NULL OR lock <= now()) \
             RETURNING id, queue, description, status, created_at, priority, payload",
        )
        .bind(id)
        .bind(lock_token)
        .fetch_optional(&self.pool)
        .await
        .map_err(FetchError::Query)?;

        row.map(|r| row_to_pending_job(r, self.pool.clone(), lock_token))
            .transpose()
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Resolves the initial state for a job in the given queue.
    ///
    /// Returns `None` if the queue does not exist (only possible with `InitialState::Auto`).
    async fn resolve_initial_state(
        &self,
        queue: &str,
        initial_state: InitialState,
    ) -> Result<Option<JobStatus>, sqlx::Error> {
        match initial_state {
            InitialState::Pending => Ok(Some(JobStatus::Pending)),
            InitialState::Paused => Ok(Some(JobStatus::Paused)),
            InitialState::Auto => {
                let row: Option<(bool,)> =
                    sqlx::query_as("SELECT paused FROM queues WHERE queue = $1")
                        .bind(queue)
                        .fetch_optional(&self.pool)
                        .await?;
                Ok(row.map(|(paused,)| {
                    if paused {
                        JobStatus::Paused
                    } else {
                        JobStatus::Pending
                    }
                }))
            }
        }
    }

    /// Creates a queue if it does not exist.
    ///
    /// Returns `true` if the queue was created, `false` if it already existed.
    pub async fn create_queue(&self, queue: &str, paused: bool) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "INSERT INTO queues (queue, paused) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        )
        .bind(queue)
        .bind(paused)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Pauses a queue, preventing new jobs from being processed.
    ///
    /// All pending jobs in the queue are transitioned to paused state.
    /// Returns the number of jobs that were paused.
    pub async fn pause_queue(&self, queue: &str) -> Result<u64, ModifyError> {
        let mut tx = self.pool.begin().await.map_err(ModifyError::Database)?;

        let result = sqlx::query("UPDATE queues SET paused = true WHERE queue = $1")
            .bind(queue)
            .execute(&mut *tx)
            .await
            .map_err(ModifyError::Database)?;

        if result.rows_affected() == 0 {
            return Err(ModifyError::QueueNotFound);
        }

        let updated = sqlx::query(
            "UPDATE jobs SET status = 'paused' WHERE queue = $1 AND status = 'pending'",
        )
        .bind(queue)
        .execute(&mut *tx)
        .await
        .map_err(ModifyError::Database)?;

        tx.commit().await.map_err(ModifyError::Database)?;
        Ok(updated.rows_affected())
    }

    /// Resumes a queue, allowing jobs to be processed.
    ///
    /// All paused jobs in the queue are transitioned to pending state.
    /// Returns the number of jobs that were resumed.
    pub async fn resume_queue(&self, queue: &str) -> Result<u64, ModifyError> {
        let mut tx = self.pool.begin().await.map_err(ModifyError::Database)?;

        let result = sqlx::query("UPDATE queues SET paused = false WHERE queue = $1")
            .bind(queue)
            .execute(&mut *tx)
            .await
            .map_err(ModifyError::Database)?;

        if result.rows_affected() == 0 {
            return Err(ModifyError::QueueNotFound);
        }

        let updated = sqlx::query(
            "UPDATE jobs SET status = 'pending' WHERE queue = $1 AND status = 'paused'",
        )
        .bind(queue)
        .execute(&mut *tx)
        .await
        .map_err(ModifyError::Database)?;

        tx.commit().await.map_err(ModifyError::Database)?;
        Ok(updated.rows_affected())
    }

    /// Reaps expired in-progress jobs.
    ///
    /// Jobs that have been in-progress longer than [`LOCK_DURATION`] are soft-failed (retried with
    /// backoff, or permanently failed if retries exhausted).
    ///
    /// Returns the number of jobs reaped and the duration until the next lock expires (if any
    /// in-progress jobs remain).
    pub async fn reap(&self) -> Result<(u64, Option<Duration>), ReapError> {
        let max_retries = MAX_RETRIES as i32;
        let backoff_base_mins = (RETRY_BACKOFF_BASE.as_secs() / 60) as i32;
        let lock_duration_mins = (LOCK_DURATION.as_secs() / 60) as i32;

        let row: (i64, Option<sqlx::postgres::types::PgInterval>) = sqlx::query_as(
            "WITH reaped AS ( \
                 UPDATE jobs SET \
                     retry_count = retry_count + 1, \
                     status = CASE WHEN retry_count >= $1 THEN 'failed'::job_status \
                                   ELSE 'pending'::job_status END, \
                     lock = CASE \
                         WHEN retry_count >= $1 THEN now() \
                         WHEN retry_count = 0 THEN now() \
                         ELSE now() + make_interval(mins => ($2 * power(2, retry_count - 1))::int) \
                     END, \
                     lock_token = NULL, \
                     error = 'reaped: lock expired' \
                 WHERE status = 'in_progress' \
                   AND lock + make_interval(mins => $3) < now() \
                 RETURNING id \
             ) \
             SELECT \
                 (SELECT COUNT(*) FROM reaped), \
                 GREATEST(MIN(lock) + make_interval(mins => $3) - now(), interval '0') \
             FROM jobs \
             WHERE status = 'in_progress'",
        )
        .bind(max_retries)
        .bind(backoff_base_mins)
        .bind(lock_duration_mins)
        .fetch_one(&self.pool)
        .await
        .map_err(ReapError::Database)?;

        let reaped_count = row.0 as u64;
        let next_reap = row
            .1
            .map(|interval| Duration::from_micros(interval.microseconds.max(0) as u64));

        Ok((reaped_count, next_reap))
    }

    /// Runs the reaper loop indefinitely.
    ///
    /// Periodically checks for and reaps expired in-progress jobs. Sleeps for the minimum of
    /// [`REAPER_INTERVAL`] or the time until the next lock expires.
    ///
    /// This method never returns under normal operation. On database errors, it logs and retries
    /// after a short delay.
    pub async fn run_reaper(&self) -> ! {
        loop {
            match self.reap().await {
                Ok((reaped, next)) => {
                    if reaped > 0 {
                        tracing::info!(reaped, "reaped expired jobs");
                    }
                    let sleep_duration = next.unwrap_or(REAPER_INTERVAL).min(REAPER_INTERVAL);
                    tokio::time::sleep(sleep_duration).await;
                }
                Err(e) => {
                    tracing::error!(error = %e.display_full(), "reaper error");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
        }
    }

    /// Lists all queues.
    pub async fn list_queues(&self) -> Result<Vec<QueueInfo>, ListError> {
        sqlx::query_as("SELECT queue, paused FROM queues ORDER BY queue")
            .fetch_all(&self.pool)
            .await
            .map_err(ListError::Query)
    }

    /// Lists jobs matching the given filter.
    ///
    /// Returns jobs ordered by creation time (newest first), limited by [`JobFilter::limit`].
    pub async fn list_jobs(&self, filter: JobFilter) -> Result<Vec<JobDetails>, ListError> {
        let limit = filter.limit.max(1) as i64;

        match (filter.queue.as_deref(), filter.status) {
            (Some(queue), Some(status)) => {
                sqlx::query_as(
                    "SELECT id, queue, status, description, created_at, key, lock, error, \
                            retry_count, priority \
                     FROM jobs WHERE queue = $1 AND status = $2 \
                     ORDER BY created_at DESC LIMIT $3",
                )
                .bind(queue)
                .bind(status)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
            (Some(queue), None) => {
                sqlx::query_as(
                    "SELECT id, queue, status, description, created_at, key, lock, error, \
                            retry_count, priority \
                     FROM jobs WHERE queue = $1 \
                     ORDER BY created_at DESC LIMIT $2",
                )
                .bind(queue)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
            (None, Some(status)) => {
                sqlx::query_as(
                    "SELECT id, queue, status, description, created_at, key, lock, error, \
                            retry_count, priority \
                     FROM jobs WHERE status = $1 \
                     ORDER BY created_at DESC LIMIT $2",
                )
                .bind(status)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
            (None, None) => {
                sqlx::query_as(
                    "SELECT id, queue, status, description, created_at, key, lock, error, \
                            retry_count, priority \
                     FROM jobs ORDER BY created_at DESC LIMIT $1",
                )
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(ListError::Query)
    }

    /// Streams jobs with their payloads for read-only access.
    ///
    /// Returns a stream of (JobDetails, raw payload bytes) pairs. Jobs are not locked.
    /// Useful for bulk operations like searching where loading all payloads into memory
    /// would be prohibitive.
    pub fn stream_jobs_with_payload(
        &self,
        filter: JobFilter,
    ) -> impl Stream<Item = Result<(JobDetails, Vec<u8>), ListError>> + Send + '_ {
        let pool = self.pool.clone();
        let limit = filter.limit.max(1) as i64;

        async_stream::try_stream! {
            let base = "SELECT id, queue, status, description, created_at, key, \
                               lock, error, retry_count, priority, payload FROM jobs";

            use futures::StreamExt;

            match (filter.queue.as_deref(), filter.status) {
                (Some(queue), Some(status)) => {
                    let sql = format!("{base} WHERE queue = $1 AND status = $2 \
                                       ORDER BY created_at DESC LIMIT $3");
                    let mut rows = sqlx::query(&sql)
                        .bind(queue)
                        .bind(status)
                        .bind(limit)
                        .fetch(&pool);
                    while let Some(row) = rows.next().await {
                        yield row_to_job_payload(row.map_err(ListError::Query)?);
                    }
                }
                (Some(queue), None) => {
                    let sql = format!("{base} WHERE queue = $1 \
                                       ORDER BY created_at DESC LIMIT $2");
                    let mut rows = sqlx::query(&sql)
                        .bind(queue)
                        .bind(limit)
                        .fetch(&pool);
                    while let Some(row) = rows.next().await {
                        yield row_to_job_payload(row.map_err(ListError::Query)?);
                    }
                }
                (None, Some(status)) => {
                    let sql = format!("{base} WHERE status = $1 \
                                       ORDER BY created_at DESC LIMIT $2");
                    let mut rows = sqlx::query(&sql)
                        .bind(status)
                        .bind(limit)
                        .fetch(&pool);
                    while let Some(row) = rows.next().await {
                        yield row_to_job_payload(row.map_err(ListError::Query)?);
                    }
                }
                (None, None) => {
                    let sql = format!("{base} ORDER BY created_at DESC LIMIT $1");
                    let mut rows = sqlx::query(&sql)
                        .bind(limit)
                        .fetch(&pool);
                    while let Some(row) = rows.next().await {
                        yield row_to_job_payload(row.map_err(ListError::Query)?);
                    }
                }
            }
        }
    }

    /// Gets detailed information about a specific job.
    ///
    /// Returns `None` if the job does not exist. Does not lock the job.
    pub async fn get_job(&self, id: Uuid) -> Result<Option<JobDetails>, ListError> {
        sqlx::query_as(
            "SELECT id, queue, status, description, created_at, key, lock, error, retry_count, \
                    priority \
             FROM jobs WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(ListError::Query)
    }

    /// Gets a job by its deduplication key.
    ///
    /// Returns `None` if no job exists with this key in the queue.
    pub async fn get_job_by_key(
        &self,
        queue: &str,
        key: &str,
    ) -> Result<Option<JobDetails>, ListError> {
        sqlx::query_as(
            "SELECT id, queue, status, description, created_at, key, lock, error, retry_count, \
                    priority \
             FROM jobs WHERE queue = $1 AND key = $2",
        )
        .bind(queue)
        .bind(key)
        .fetch_optional(&self.pool)
        .await
        .map_err(ListError::Query)
    }

    /// Resolves a job ID suffix or full UUID to a single job ID.
    ///
    /// Accepts either a full UUID or a hex suffix (1-32 characters). Suffixes are matched against
    /// all job IDs in the database. Returns an error if the suffix is ambiguous or no job matches.
    pub async fn resolve_job_id(&self, suffix: &UuidSuffix) -> Result<Uuid, ResolveIdError> {
        // Fast path: full UUID needs no DB query
        if let Some(uuid) = suffix.to_uuid() {
            return Ok(uuid);
        }

        // Query candidates matching the suffix (limit to 11 to detect ambiguity)
        let pattern = format!("%{suffix}");
        let candidates: Vec<(Uuid,)> =
            sqlx::query_as("SELECT id FROM jobs WHERE id::text LIKE $1 LIMIT 11")
                .bind(&pattern)
                .fetch_all(&self.pool)
                .await
                .map_err(ResolveIdError::Query)?;

        let uuids: Vec<Uuid> = candidates.into_iter().map(|(id,)| id).collect();
        resolve_uuid_suffix(&uuids, suffix).map_err(|e| match e {
            uuid_suffix::ResolveError::NotFound => ResolveIdError::NotFound(suffix.to_string()),
            uuid_suffix::ResolveError::Ambiguous(ids) => {
                let mut matches: Vec<_> = ids
                    .iter()
                    .take(10)
                    .map(|id| UuidSuffix::new(id).to_string())
                    .collect();
                if ids.len() > 10 {
                    matches.push("...".to_string());
                }
                ResolveIdError::Ambiguous {
                    suffix: suffix.to_string(),
                    matches: matches.join(", "),
                }
            }
        })
    }

    /// Resolves multiple job ID suffixes or full UUIDs to job IDs.
    ///
    /// Each input is resolved independently. Returns all resolved IDs or the first error.
    pub async fn resolve_job_ids(
        &self,
        suffixes: &[UuidSuffix],
    ) -> Result<Vec<Uuid>, ResolveIdError> {
        let mut results = Vec::with_capacity(suffixes.len());
        for suffix in suffixes {
            results.push(self.resolve_job_id(suffix).await?);
        }
        Ok(results)
    }

    /// Pulls the next pending job from a queue, locking it for processing.
    ///
    /// Returns job details, raw payload bytes, and an acknowledgment handle. The job transitions
    /// to in_progress and must be resolved via the ack handle (commit, soft_fail, hard_fail, or
    /// restart).
    pub async fn pull_next(
        &self,
        queue: &str,
    ) -> Result<Option<(JobDetails, Vec<u8>, job::JobAck)>, FetchError> {
        let lock_token = Uuid::now_v7();
        Ok(sqlx::query(
            "WITH selected AS ( \
                 SELECT id FROM jobs \
                 WHERE queue = $1 AND status = 'pending' AND (lock IS NULL OR lock <= now()) \
                 ORDER BY priority DESC, created_at \
                 LIMIT 1 \
                 FOR UPDATE SKIP LOCKED \
             ) \
             UPDATE jobs j SET status = 'in_progress', lock = now(), lock_token = $2 \
             FROM selected s \
             WHERE j.id = s.id \
             RETURNING j.id, j.queue, j.status, j.description, j.created_at, j.key, j.lock, \
                       j.error, j.retry_count, j.priority, j.payload",
        )
        .bind(queue)
        .bind(lock_token)
        .fetch_optional(&self.pool)
        .await
        .map_err(FetchError::Query)?
        .map(|row| {
            let details = JobDetails {
                id: row.get("id"),
                queue: row.get("queue"),
                status: row.get("status"),
                description: row.get("description"),
                created_at: row.get("created_at"),
                key: row.get("key"),
                lock: row.get("lock"),
                error: row.get("error"),
                retry_count: row.get("retry_count"),
                priority: row.get("priority"),
            };
            let payload: Vec<u8> = row.get("payload");
            let ack = job::JobAck::new(details.id, self.pool.clone(), lock_token);
            (details, payload, ack)
        }))
    }

    /// Gets the raw msgpack payload bytes for a job.
    ///
    /// Returns `None` if the job does not exist.
    pub async fn get_job_payload(&self, id: Uuid) -> Result<Option<Vec<u8>>, ListError> {
        let row: Option<(Vec<u8>,)> = sqlx::query_as("SELECT payload FROM jobs WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(ListError::Query)?;

        Ok(row.map(|(data,)| data))
    }

    /// Restarts jobs, resetting them to pending state.
    ///
    /// Clears the retry count, lock, and error. By default, only affects jobs in pending, paused,
    /// failed, or finished state. With `force`, also restarts in_progress jobs (breaking their
    /// lock). Returns the number of jobs restarted.
    pub async fn restart_jobs(&self, ids: &[Uuid], force: bool) -> Result<u64, ModifyError> {
        let query = if force {
            "UPDATE jobs SET status = 'pending', retry_count = 0, lock = NULL, \
             lock_token = NULL, error = NULL \
             WHERE id = ANY($1)"
        } else {
            "UPDATE jobs SET status = 'pending', retry_count = 0, lock = NULL, \
             lock_token = NULL, error = NULL \
             WHERE id = ANY($1) AND status IN ('pending', 'failed', 'paused', 'finished')"
        };
        let result = sqlx::query(query)
            .bind(ids)
            .execute(&self.pool)
            .await
            .map_err(ModifyError::Database)?;

        Ok(result.rows_affected())
    }

    /// Marks jobs as permanently failed.
    ///
    /// Sets the job status to failed with the given error message. Only affects jobs that are not
    /// already finished. Returns the number of jobs failed.
    pub async fn fail_jobs(&self, ids: &[Uuid], message: &str) -> Result<u64, ModifyError> {
        let result = sqlx::query(
            "UPDATE jobs SET status = 'failed', lock = now(), lock_token = NULL, error = $1 \
             WHERE id = ANY($2) AND status != 'finished'",
        )
        .bind(message)
        .bind(ids)
        .execute(&self.pool)
        .await
        .map_err(ModifyError::Database)?;

        Ok(result.rows_affected())
    }

    /// Marks jobs as finished.
    ///
    /// Sets the job status to finished and clears any error. Only affects jobs that are
    /// pending or in progress. Returns the number of jobs updated.
    pub async fn finish_jobs(&self, ids: &[Uuid]) -> Result<u64, ModifyError> {
        let result = sqlx::query(
            "UPDATE jobs SET status = 'finished', lock = now(), lock_token = NULL, error = NULL \
             WHERE id = ANY($1) AND status IN ('pending', 'in_progress')",
        )
        .bind(ids)
        .execute(&self.pool)
        .await
        .map_err(ModifyError::Database)?;

        Ok(result.rows_affected())
    }

    /// Returns per-queue job statistics.
    pub async fn stats(&self) -> Result<Vec<QueueStats>, ListError> {
        let rows: Vec<(String, i64, i64, i64, i64, i64)> = sqlx::query_as(
            "SELECT q.queue, \
                 COUNT(*) FILTER (WHERE j.status = 'pending') AS pending, \
                 COUNT(*) FILTER (WHERE j.status = 'paused') AS paused, \
                 COUNT(*) FILTER (WHERE j.status = 'in_progress') AS in_progress, \
                 COUNT(*) FILTER (WHERE j.status = 'finished') AS finished, \
                 COUNT(*) FILTER (WHERE j.status = 'failed') AS failed \
             FROM queues q \
             LEFT JOIN jobs j ON j.queue = q.queue \
             GROUP BY q.queue \
             ORDER BY q.queue",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(ListError::Query)?;

        Ok(rows
            .into_iter()
            .map(
                |(queue, pending, paused, in_progress, finished, failed)| QueueStats {
                    queue,
                    pending,
                    paused,
                    in_progress,
                    finished,
                    failed,
                },
            )
            .collect())
    }

    /// Deletes jobs.
    ///
    /// Returns the number of jobs deleted. Skips non-existent jobs.
    pub async fn delete_jobs(&self, ids: &[Uuid]) -> Result<u64, ModifyError> {
        let result = sqlx::query("DELETE FROM jobs WHERE id = ANY($1)")
            .bind(ids)
            .execute(&self.pool)
            .await
            .map_err(ModifyError::Database)?;

        Ok(result.rows_affected())
    }

    /// Renames a queue.
    ///
    /// Returns [`ModifyError::QueueNotFound`] if the source queue doesn't exist.
    /// Returns [`ModifyError::QueueAlreadyExists`] if the target queue already exists.
    pub async fn rename_queue(&self, from: &str, to: &str) -> Result<(), ModifyError> {
        let result = sqlx::query("UPDATE queues SET queue = $2 WHERE queue = $1")
            .bind(from)
            .bind(to)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                if let sqlx::Error::Database(ref db_err) = e {
                    if db_err.code().as_deref() == Some("23505") {
                        return ModifyError::QueueAlreadyExists;
                    }
                }
                ModifyError::Database(e)
            })?;

        if result.rows_affected() == 0 {
            return Err(ModifyError::QueueNotFound);
        }
        Ok(())
    }

    /// Deletes a queue and all its jobs.
    ///
    /// Returns the number of jobs deleted.
    /// Returns [`ModifyError::QueueNotFound`] if the queue doesn't exist.
    pub async fn delete_queue(&self, queue: &str) -> Result<u64, ModifyError> {
        let mut tx = self.pool.begin().await.map_err(ModifyError::Database)?;

        // Check queue exists
        let exists: Option<(bool,)> = sqlx::query_as("SELECT true FROM queues WHERE queue = $1")
            .bind(queue)
            .fetch_optional(&mut *tx)
            .await
            .map_err(ModifyError::Database)?;
        if exists.is_none() {
            return Err(ModifyError::QueueNotFound);
        }

        // Delete all jobs in the queue
        let result = sqlx::query("DELETE FROM jobs WHERE queue = $1")
            .bind(queue)
            .execute(&mut *tx)
            .await
            .map_err(ModifyError::Database)?;
        let deleted_count = result.rows_affected();

        // Delete the queue
        sqlx::query("DELETE FROM queues WHERE queue = $1")
            .bind(queue)
            .execute(&mut *tx)
            .await
            .map_err(ModifyError::Database)?;

        tx.commit().await.map_err(ModifyError::Database)?;
        Ok(deleted_count)
    }

    /// Lists all job IDs in a queue.
    ///
    /// Returns job IDs ordered by priority (highest first), then creation time (oldest first).
    pub async fn list_job_ids(&self, queue: &str) -> Result<Vec<Uuid>, ListError> {
        let rows: Vec<(Uuid,)> = sqlx::query_as(
            "SELECT id FROM jobs WHERE queue = $1 ORDER BY priority DESC, created_at",
        )
        .bind(queue)
        .fetch_all(&self.pool)
        .await
        .map_err(ListError::Query)?;

        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    /// Transforms a job's payload atomically within a single transaction.
    ///
    /// Locks the job, fetches its payload, calls the transform function, and replaces the payload
    /// if changed. Uses `FOR UPDATE SKIP LOCKED` to avoid blocking workers.
    ///
    /// The transform function receives `(queue_name, payload_bytes)` and returns new payload bytes.
    pub async fn transform_job_payload<F>(
        &self,
        job_id: Uuid,
        transform: F,
    ) -> Result<TransformResult, ModifyError>
    where
        F: FnOnce(&str, &[u8]) -> Result<Vec<u8>, anyhow::Error>,
    {
        transform::transform_job_payload(&self.pool, job_id, transform).await
    }
}

/// Converts a row containing job metadata and payload into a [`PendingJob`].
fn row_to_pending_job<T: DeserializeOwned>(
    row: PgRow,
    pool: PgPool,
    lock_token: Uuid,
) -> Result<PendingJob<T>, FetchError> {
    let meta = JobMetadata::from_row(&row).map_err(FetchError::Query)?;
    let payload_bytes: Vec<u8> = row.get("payload");
    let payload: T =
        rmp_serde::from_slice(&payload_bytes).map_err(|e| FetchError::Deserialize(meta.id, e))?;
    Ok(PendingJob {
        meta,
        payload,
        pool,
        lock_token,
    })
}

/// Fetches the next pending job from the queue, marking it as in-progress.
pub(crate) async fn next_pending_job<T: DeserializeOwned>(
    pool: &PgPool,
    queue: &str,
) -> Result<Option<PendingJob<T>>, FetchError> {
    let lock_token = Uuid::now_v7();
    let row: Option<PgRow> = sqlx::query(
        "WITH selected AS ( \
             SELECT id FROM jobs \
             WHERE queue = $1 AND status = 'pending' AND (lock IS NULL OR lock <= now()) \
             ORDER BY priority DESC, created_at \
             LIMIT 1 \
             FOR UPDATE SKIP LOCKED \
         ) \
         UPDATE jobs j SET status = 'in_progress', lock = now(), lock_token = $2 \
         FROM selected s \
         WHERE j.id = s.id \
         RETURNING j.id, j.queue, j.description, j.status, j.created_at, j.priority, j.payload",
    )
    .bind(queue)
    .bind(lock_token)
    .fetch_optional(pool)
    .await
    .map_err(FetchError::Query)?;

    row.map(|r| row_to_pending_job(r, pool.clone(), lock_token))
        .transpose()
}

/// Converts a database row to (JobDetails, payload bytes).
fn row_to_job_payload(row: PgRow) -> (JobDetails, Vec<u8>) {
    let details = JobDetails {
        id: row.get("id"),
        queue: row.get("queue"),
        status: row.get("status"),
        description: row.get("description"),
        created_at: row.get("created_at"),
        key: row.get("key"),
        lock: row.get("lock"),
        error: row.get("error"),
        retry_count: row.get("retry_count"),
        priority: row.get("priority"),
    };
    let payload: Vec<u8> = row.get("payload");
    (details, payload)
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::StreamExt;
    use uuid_suffix::UuidSuffix;

    use crate::{job::JobStatus, EnqueueOptions, Queue};

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
    async fn hard_fail_and_soft_fail_outcomes() {
        let (queue, _db) = setup_db().await;

        let id1 = queue
            .enqueue("test", 1i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");
        let id2 = queue
            .enqueue("test", 2i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let mut stream = pin!(queue.try_stream_jobs::<i32>("test"));

        // hard_fail goes straight to Failed
        let job1 = stream.next().await.expect("no job").expect("fetch failed");
        job1.into_parts()
            .1
            .hard_fail("unrecoverable error")
            .await
            .expect("hard_fail failed");

        // soft_fail goes to Pending (first retry is immediate)
        let job2 = stream.next().await.expect("no job").expect("fetch failed");
        job2.into_parts()
            .1
            .soft_fail("transient error")
            .await
            .expect("soft_fail failed");

        let (s1, err): (JobStatus, Option<String>) =
            sqlx::query_as("SELECT status, error FROM jobs WHERE id = $1")
                .bind(id1)
                .fetch_one(queue.pool())
                .await
                .expect("query failed");
        assert_eq!(s1, JobStatus::Failed);
        assert_eq!(err, Some("unrecoverable error".to_string()));

        let (s2, retry_count): (JobStatus, i32) =
            sqlx::query_as("SELECT status, retry_count FROM jobs WHERE id = $1")
                .bind(id2)
                .fetch_one(queue.pool())
                .await
                .expect("query failed");
        assert_eq!(s2, JobStatus::Pending);
        assert_eq!(retry_count, 1);
    }

    #[tokio::test]
    async fn fetch_job_by_id() {
        let (queue, _db) = setup_db().await;

        let id = queue
            .enqueue("test", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let job = queue
            .fetch_job::<i32>(id)
            .await
            .expect("fetch failed")
            .expect("job not found");
        assert_eq!(job.payload, 42);

        assert!(queue
            .fetch_job::<i32>(id)
            .await
            .expect("fetch failed")
            .is_none());
        assert!(queue
            .fetch_job::<i32>(uuid::Uuid::nil())
            .await
            .expect("fetch failed")
            .is_none());
    }

    #[tokio::test]
    async fn enqueue_fails_without_queue() {
        let (queue, _db) = setup_db().await;

        let result = queue
            .enqueue("nonexistent", 42i32, EnqueueOptions::default())
            .await;
        assert!(matches!(
            result,
            Err(crate::error::EnqueueError::QueueNotFound)
        ));
    }

    #[tokio::test]
    async fn pause_and_resume_queue() {
        let (queue, _db) = setup_db().await;

        // Enqueue a job while queue is active
        let id1 = queue
            .enqueue("test", 1i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        // Pause the queue - should transition pending job to paused
        let paused_count = queue.pause_queue("test").await.expect("pause failed");
        assert_eq!(paused_count, 1);

        let (status,): (JobStatus,) = sqlx::query_as("SELECT status FROM jobs WHERE id = $1")
            .bind(id1)
            .fetch_one(queue.pool())
            .await
            .unwrap();
        assert_eq!(status, JobStatus::Paused);

        // Enqueue while paused - should be paused due to Auto state
        let id2 = queue
            .enqueue("test", 2i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let (status,): (JobStatus,) = sqlx::query_as("SELECT status FROM jobs WHERE id = $1")
            .bind(id2)
            .fetch_one(queue.pool())
            .await
            .unwrap();
        assert_eq!(status, JobStatus::Paused);

        // Unpause - should transition both jobs to pending
        let resumed_count = queue.resume_queue("test").await.expect("unpause failed");
        assert_eq!(resumed_count, 2);

        let statuses: Vec<(JobStatus,)> =
            sqlx::query_as("SELECT status FROM jobs WHERE queue = 'test' ORDER BY id")
                .fetch_all(queue.pool())
                .await
                .unwrap();
        assert!(statuses.iter().all(|(s,)| *s == JobStatus::Pending));
    }

    #[tokio::test]
    async fn pause_nonexistent_queue_fails() {
        let (queue, _db) = setup_db().await;

        let result = queue.pause_queue("nonexistent").await;
        assert!(matches!(
            result,
            Err(crate::error::ModifyError::QueueNotFound)
        ));
    }

    #[tokio::test]
    async fn create_queue_idempotent() {
        let (queue, _db) = setup_db().await;

        let created = queue
            .create_queue("new_queue", false)
            .await
            .expect("create failed");
        assert!(created);

        let created_again = queue
            .create_queue("new_queue", true)
            .await
            .expect("create failed");
        assert!(!created_again);

        // Original paused state should be preserved
        let (paused,): (bool,) =
            sqlx::query_as("SELECT paused FROM queues WHERE queue = 'new_queue'")
                .fetch_one(queue.pool())
                .await
                .unwrap();
        assert!(!paused);
    }

    #[tokio::test]
    async fn reap_expired_jobs() {
        use crate::job::LOCK_DURATION;

        let (queue, _db) = setup_db().await;

        let id = queue
            .enqueue("test", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        // Simulate an expired in-progress job by setting lock to the past
        let expired_lock = chrono::Utc::now()
            - chrono::Duration::from_std(LOCK_DURATION).unwrap()
            - chrono::Duration::seconds(1);
        sqlx::query(
            "UPDATE jobs SET status = 'in_progress', lock = $1, lock_token = gen_random_uuid() \
             WHERE id = $2",
        )
        .bind(expired_lock)
        .bind(id)
        .execute(queue.pool())
        .await
        .expect("update failed");

        // Reap should find and soft-fail the expired job
        let (reaped, _next) = queue.reap().await.expect("reap failed");
        assert_eq!(reaped, 1);

        let (status, retry_count, error): (JobStatus, i32, Option<String>) =
            sqlx::query_as("SELECT status, retry_count, error FROM jobs WHERE id = $1")
                .bind(id)
                .fetch_one(queue.pool())
                .await
                .expect("query failed");
        assert_eq!(status, JobStatus::Pending);
        assert_eq!(retry_count, 1);
        assert_eq!(error, Some("reaped: lock expired".to_string()));
    }

    #[tokio::test]
    async fn list_job_ids_returns_ordered_ids() {
        let (queue, _db) = setup_db().await;

        let id1 = queue
            .enqueue("test", 1i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");
        let id2 = queue
            .enqueue("test", 2i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let ids = queue.list_job_ids("test").await.expect("list failed");
        assert_eq!(ids, vec![id1, id2]);

        let empty = queue
            .list_job_ids("nonexistent")
            .await
            .expect("list failed");
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn resolve_job_id_full_uuid() {
        let (queue, _db) = setup_db().await;

        let id = queue
            .enqueue("test", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        // Full UUID resolves directly without DB lookup
        let suffix = UuidSuffix::full(&id);
        let resolved = queue.resolve_job_id(&suffix).await.expect("resolve failed");
        assert_eq!(resolved, id);
    }

    #[tokio::test]
    async fn resolve_job_id_suffix_match() {
        let (queue, _db) = setup_db().await;

        let id = queue
            .enqueue("test", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        // Suffix should match the job
        let suffix = UuidSuffix::new(&id);
        let resolved = queue.resolve_job_id(&suffix).await.expect("resolve failed");
        assert_eq!(resolved, id);
    }

    #[tokio::test]
    async fn resolve_job_id_not_found() {
        let (queue, _db) = setup_db().await;

        // Non-matching suffix should fail
        let suffix: UuidSuffix = "0000000".parse().expect("valid suffix");
        let result = queue.resolve_job_id(&suffix).await;
        assert!(matches!(
            result,
            Err(crate::error::ResolveIdError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn deduplication_key() {
        let (queue, _db) = setup_db().await;
        queue.create_queue("other", false).await.unwrap();

        let opts_with_key = EnqueueOptions {
            key: Some("my-key".to_string()),
            ..Default::default()
        };

        // First enqueue succeeds
        let id = queue
            .enqueue("test", 42i32, opts_with_key.clone())
            .await
            .expect("enqueue failed")
            .expect("first enqueue should succeed");

        // Duplicate key returns None
        let dup = queue
            .enqueue("test", 99i32, opts_with_key.clone())
            .await
            .expect("enqueue failed");
        assert!(dup.is_none());

        // Same key in different queue succeeds
        let other_id = queue
            .enqueue("other", 42i32, opts_with_key)
            .await
            .expect("enqueue failed")
            .expect("same key in different queue should succeed");
        assert_ne!(id, other_id);

        // get_job_by_key retrieves the job
        let found = queue
            .get_job_by_key("test", "my-key")
            .await
            .expect("query failed")
            .expect("job should exist");
        assert_eq!(found.id, id);
        assert_eq!(found.key, Some("my-key".to_string()));

        // get_job_by_key returns None for nonexistent key
        let not_found = queue
            .get_job_by_key("test", "nonexistent")
            .await
            .expect("query failed");
        assert!(not_found.is_none());

        // Delete job, re-enqueue with same key succeeds
        queue.delete_jobs(&[id]).await.expect("delete failed");
        let new_id = queue
            .enqueue(
                "test",
                42i32,
                EnqueueOptions {
                    key: Some("my-key".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("enqueue failed")
            .expect("re-enqueue after delete should succeed");
        assert_ne!(id, new_id);
    }
}
