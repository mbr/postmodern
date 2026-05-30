//! Job streaming functionality.

use std::time::Duration;

use backon::{BackoffBuilder, ExponentialBuilder, Retryable};
use display_full_error::DisplayFullErrorExt;
use futures::{stream::unfold, Stream, StreamExt};
use serde::de::DeserializeOwned;

use crate::{
    error::FetchError,
    job::{JobAck, PendingJob},
    JobDetails, Queue,
};

/// Default delay after a query error before retrying.
const QUERY_ERROR_DELAY: Duration = Duration::from_secs(5);

impl Queue {
    /// Returns a fallible stream of raw jobs from the given queues.
    ///
    /// Jobs are returned with raw payload bytes (no deserialization). The stream polls the database
    /// with exponential backoff when all queues are empty (up to 30s). Each fetched job is
    /// immediately marked as in-progress.
    pub fn try_stream_raw<I, S>(
        &self,
        queues: I,
    ) -> impl Stream<Item = Result<(JobDetails, Vec<u8>, JobAck), FetchError>> + Send
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let queue = self.clone();
        let queues: Vec<String> = queues.into_iter().map(Into::into).collect();
        unfold((queue, queues), |(queue, queues)| async move {
            let queue_refs: Vec<&str> = queues.iter().map(|s| s.as_str()).collect();
            let result = (|| poll_next_raw(&queue, &queue_refs))
                .retry(
                    ExponentialBuilder::default()
                        .with_min_delay(Duration::from_millis(100))
                        .with_max_delay(Duration::from_secs(30))
                        .with_max_times(usize::MAX)
                        .build(),
                )
                .when(|e| matches!(e, StreamError::Empty))
                .await;

            match result {
                Ok(job) => Some((Ok(job), (queue, queues))),
                Err(StreamError::Empty) => unreachable!("infinite retry"),
                Err(StreamError::Fetch(e)) => Some((Err(e), (queue, queues))),
            }
        })
    }

    /// Returns a fallible stream of pending jobs from the given queues.
    ///
    /// The stream polls the database with exponential backoff when all queues are empty (up to 30s).
    /// After processing a job, backoff resets to zero for immediate polling. Each fetched job is
    /// immediately marked as in-progress.
    pub fn try_stream_jobs<T, I, S>(
        &self,
        queues: I,
    ) -> impl Stream<Item = Result<PendingJob<T>, FetchError>> + Send
    where
        T: DeserializeOwned + Send + 'static,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.try_stream_raw(queues).map(|result| {
            result.and_then(|(details, payload, ack)| {
                let payload: T = rmp_serde::from_slice(&payload)
                    .map_err(|e| FetchError::Deserialize(details.id, e))?;
                Ok(PendingJob::from_raw(details.into(), payload, ack))
            })
        })
    }

    /// Returns a stream of pending jobs from the given queues.
    ///
    /// Handles errors internally: query errors are logged and retried after a delay, deserialize
    /// errors cause the job to be marked as failed and skipped. Only yields valid jobs.
    pub fn stream_jobs<T, I, S>(&self, queues: I) -> impl Stream<Item = PendingJob<T>> + Send
    where
        T: DeserializeOwned + Send + 'static,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let queue = self.clone();
        let queues: Vec<String> = queues.into_iter().map(Into::into).collect();
        unfold((queue, queues), |(queue, queues)| async move {
            let queue_refs: Vec<&str> = queues.iter().map(|s| s.as_str()).collect();
            loop {
                let result = (|| poll_next_raw(&queue, &queue_refs))
                    .retry(
                        ExponentialBuilder::default()
                            .with_min_delay(Duration::from_millis(100))
                            .with_max_delay(Duration::from_secs(30))
                            .with_max_times(usize::MAX)
                            .build(),
                    )
                    .when(|e| matches!(e, StreamError::Empty))
                    .await;

                match result {
                    Ok((details, payload, ack)) => {
                        let id = details.id;
                        match rmp_serde::from_slice::<T>(&payload) {
                            Ok(payload) => {
                                let job = PendingJob::from_raw(details.into(), payload, ack);
                                return Some((job, (queue, queues)));
                            }
                            Err(e) => {
                                tracing::error!(%id, error = %e.display_full(), "deserialization failed, marking job as failed");
                                if let Err(fail_err) =
                                    queue.fail_jobs(&[id], &e.to_string_full()).await
                                {
                                    tracing::warn!(
                                        %id, error = %fail_err.display_full(),
                                        "failed to mark job as failed, reaper will handle"
                                    );
                                }
                            }
                        }
                    }
                    Err(StreamError::Empty) => unreachable!("infinite retry"),
                    Err(StreamError::Fetch(FetchError::Query(e))) => {
                        tracing::warn!(error = %e.display_full(), "query error, retrying");
                        tokio::time::sleep(QUERY_ERROR_DELAY).await;
                    }
                    Err(StreamError::Fetch(FetchError::Deserialize(_, _))) => {
                        unreachable!("raw stream does not deserialize")
                    }
                }
            }
        })
    }
}

/// Internal error type for stream retry logic.
enum StreamError {
    /// Queue is empty, retry with backoff.
    Empty,
    /// Fetch failed.
    Fetch(FetchError),
}

/// Wrapper that converts `Ok(None)` to `Err(StreamError::Empty)` for retry.
async fn poll_next_raw(
    queue: &Queue,
    queues: &[&str],
) -> Result<(JobDetails, Vec<u8>, JobAck), StreamError> {
    match queue.pull_next(queues).await {
        Ok(Some(job)) => Ok(job),
        Ok(None) => Err(StreamError::Empty),
        Err(e) => Err(StreamError::Fetch(e)),
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::StreamExt;

    use crate::{error::AckError, job::JobStatus, EnqueueOptions, Queue};

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
    async fn enqueue_stream_and_commit() {
        let (queue, _db) = setup_db().await;

        let id = queue
            .enqueue("test", "hello".to_string(), EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let mut stream = pin!(queue.try_stream_jobs::<String, _, _>(["test"]));
        let job = stream.next().await.expect("no job").expect("fetch failed");
        assert_eq!(job.meta.id, id);
        assert_eq!(job.payload, "hello");

        job.into_parts().1.commit().await.expect("commit failed");

        let (status,): (JobStatus,) = sqlx::query_as("SELECT status FROM jobs WHERE id = $1")
            .bind(id)
            .fetch_one(queue.pool())
            .await
            .expect("query failed");
        assert_eq!(status, JobStatus::Finished);
    }

    #[tokio::test]
    async fn concurrent_consumers_get_different_jobs() {
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

        let mut stream1 = pin!(queue.try_stream_jobs::<i32, _, _>(["test"]));
        let mut stream2 = pin!(queue.try_stream_jobs::<i32, _, _>(["test"]));

        let job1 = stream1.next().await.expect("no job").expect("fetch failed");
        let job2 = stream2.next().await.expect("no job").expect("fetch failed");

        let mut ids = [job1.meta.id, job2.meta.id];
        ids.sort();
        assert_eq!(ids, [id1, id2]);
    }

    #[tokio::test]
    async fn lock_lost_on_token_change() {
        let (queue, _db) = setup_db().await;

        let id = queue
            .enqueue("test", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let mut stream = pin!(queue.try_stream_jobs::<i32, _, _>(["test"]));
        let job = stream.next().await.expect("no job").expect("fetch failed");
        let (_, ack) = job.into_parts();

        sqlx::query("UPDATE jobs SET lock_token = gen_random_uuid() WHERE id = $1")
            .bind(id)
            .execute(queue.pool())
            .await
            .expect("update failed");

        assert!(matches!(ack.commit().await, Err(AckError::LockLost)));
    }

    #[tokio::test]
    async fn drop_without_ack_soft_fails() {
        let (queue, _db) = setup_db().await;

        let id = queue
            .enqueue("test", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        {
            let mut stream = pin!(queue.try_stream_jobs::<i32, _, _>(["test"]));
            let job = stream.next().await.expect("no job").expect("fetch failed");
            let (_, _ack) = job.into_parts();
        }

        // JobAck::drop spawns a fire-and-forget task to soft-fail the job. In production, the
        // runtime keeps running and the task completes normally. In tests, exiting immediately
        // would shut down the runtime before the task finishes, so we poll until the status
        // changes from InProgress.
        let mut status = JobStatus::InProgress;
        let mut error = None;
        let mut retry_count = 0;
        for _ in 0..100 {
            (status, error, retry_count) =
                sqlx::query_as("SELECT status, error, retry_count FROM jobs WHERE id = $1")
                    .bind(id)
                    .fetch_one(queue.pool())
                    .await
                    .expect("query failed");
            if status != JobStatus::InProgress {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(status, JobStatus::Pending);
        assert_eq!(error, Some("dropped without ack".to_string()));
        assert_eq!(retry_count, 1);
    }

    #[tokio::test]
    async fn soft_fail_exhausts_retries() {
        use crate::job::MAX_RETRIES;

        let (queue, _db) = setup_db().await;

        let id = queue
            .enqueue("test", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        sqlx::query("UPDATE jobs SET retry_count = $1 WHERE id = $2")
            .bind(MAX_RETRIES as i32)
            .bind(id)
            .execute(queue.pool())
            .await
            .expect("update failed");

        let mut stream = pin!(queue.try_stream_jobs::<i32, _, _>(["test"]));
        let job = stream.next().await.expect("no job").expect("fetch failed");
        job.into_parts()
            .1
            .soft_fail("final failure")
            .await
            .expect("soft_fail failed");

        let (status, retry_count): (JobStatus, i32) =
            sqlx::query_as("SELECT status, retry_count FROM jobs WHERE id = $1")
                .bind(id)
                .fetch_one(queue.pool())
                .await
                .expect("query failed");
        assert_eq!(status, JobStatus::Failed);
        assert_eq!(retry_count, MAX_RETRIES as i32 + 1);
    }
}
