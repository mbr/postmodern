//! Sequential durable workflows.
//!
//! Pipelines provide a layer on top of postmodern's job primitives for building multi-stage
//! workflows. Each stage corresponds to a queue, and jobs advance atomically from one stage to
//! the next.

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use serde::{de::DeserializeOwned, Serialize};

use crate::{
    error::{AckError, AdvanceError},
    job::{AdvanceOptions, JobAck},
    JobDetails,
};

/// Outcome of processing a pipeline stage.
#[derive(Debug)]
pub enum Outcome {
    /// Advance to next stage with serialized payload.
    Advance {
        /// Target queue name.
        queue: String,
        /// Serialized payload bytes.
        payload: Vec<u8>,
    },
    /// Pipeline complete, commit job.
    Done,
    /// Transient failure, retry with backoff.
    Retry(
        /// Error message describing the failure.
        String,
    ),
    /// Permanent failure, do not retry.
    Fail(
        /// Error message describing the failure.
        String,
    ),
}

/// Errors from running a job through the pipeline.
#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    /// Failed to advance to the next stage.
    #[error("failed to advance")]
    Advance(#[source] AdvanceError),
    /// Failed to acknowledge job completion.
    #[error("failed to commit")]
    Commit(#[source] AckError),
    /// Failed to mark job for retry.
    #[error("failed to soft-fail")]
    SoftFail(#[source] AckError),
    /// Failed to mark job as permanently failed.
    #[error("failed to hard-fail")]
    HardFail(#[source] AckError),
}

/// Type-erased stage handler.
type StageHandler = Arc<
    dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, anyhow::Error>> + Send>>
        + Send
        + Sync,
>;

/// Entry for a single pipeline stage.
struct StageEntry {
    /// Handler function.
    handler: StageHandler,
    /// Next stage queue name, or `None` for the final stage.
    next: Option<String>,
}

/// A sequential durable workflow.
///
/// Pipelines dispatch jobs to stage handlers based on their queue name. Each handler transforms
/// the job's payload, and the result determines whether the job advances to the next stage,
/// completes, or fails.
pub struct Pipeline {
    /// Stage entries keyed by queue name.
    stages: HashMap<String, StageEntry>,
    /// Queue names in order.
    queues: Vec<String>,
}

/// Builder for constructing pipelines.
pub struct PipelineBuilder {
    /// Stage entries being built.
    entries: HashMap<String, StageEntry>,
    /// Queue names in order of addition.
    queue_order: Vec<String>,
}

impl Pipeline {
    /// Creates a new pipeline builder.
    pub fn builder() -> PipelineBuilder {
        PipelineBuilder {
            entries: HashMap::new(),
            queue_order: Vec::new(),
        }
    }

    /// Dispatches a job to its stage handler.
    async fn dispatch(&self, queue: &str, payload: Vec<u8>) -> Outcome {
        let entry = match self.stages.get(queue) {
            Some(e) => e,
            None => return Outcome::Fail(format!("unknown stage: {queue}")),
        };

        match (entry.handler)(payload).await {
            Ok(next_payload) => match &entry.next {
                Some(next_queue) => Outcome::Advance {
                    queue: next_queue.clone(),
                    payload: next_payload,
                },
                None => Outcome::Done,
            },
            Err(e) => Outcome::Retry(format!("{e:#}")),
        }
    }

    /// Processes a job through the pipeline.
    ///
    /// Dispatches the job to its stage handler and resolves it based on the outcome.
    pub async fn run(
        &self,
        details: &JobDetails,
        payload: Vec<u8>,
        ack: JobAck,
    ) -> Result<(), PipelineError> {
        match self.dispatch(&details.queue, payload).await {
            Outcome::Advance { queue, payload } => {
                ack.advance(&queue, &payload, AdvanceOptions::default())
                    .await
                    .map_err(PipelineError::Advance)?;
            }
            Outcome::Done => {
                ack.commit().await.map_err(PipelineError::Commit)?;
            }
            Outcome::Retry(msg) => {
                ack.soft_fail(&msg).await.map_err(PipelineError::SoftFail)?;
            }
            Outcome::Fail(msg) => {
                ack.hard_fail(&msg).await.map_err(PipelineError::HardFail)?;
            }
        }
        Ok(())
    }

    /// Returns the queue names this pipeline handles.
    pub fn queues(&self) -> &[String] {
        &self.queues
    }
}

impl PipelineBuilder {
    /// Adds a stage that transforms `In` to `Out`.
    ///
    /// Stages are linked in the order they are added. The handler receives a deserialized payload
    /// and returns the output to be serialized for the next stage.
    pub fn stage<In, Out, F, Fut>(mut self, queue: &str, f: F) -> Self
    where
        In: DeserializeOwned + Send + 'static,
        Out: Serialize + Send + 'static,
        F: Fn(In) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Out, anyhow::Error>> + Send + 'static,
    {
        // Link stage to previous stage.
        if let Some(prev) = self.queue_order.last() {
            if let Some(entry) = self.entries.get_mut(prev) {
                entry.next = Some(queue.to_string());
            }
        }

        // Construct handler, will take and emit raw bytes, but serialize in between.
        let f = Arc::new(f);
        let handler: StageHandler = Arc::new(move |bytes| {
            let f = Arc::clone(&f);
            Box::pin(async move {
                let input: In = rmp_serde::from_slice(&bytes)?;
                let output = f(input).await?;
                Ok(rmp_serde::to_vec_named(&output)?)
            })
        });

        self.entries.insert(
            queue.to_string(),
            StageEntry {
                handler,
                next: None,
            },
        );
        self.queue_order.push(queue.to_string());
        self
    }

    /// Builds the pipeline.
    pub fn build(self) -> Pipeline {
        Pipeline {
            stages: self.entries,
            queues: self.queue_order,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::pin;

    use futures::StreamExt;

    use super::*;
    use crate::{job::JobStatus, EnqueueOptions, Queue};

    async fn setup_db() -> (Queue, pgdb::DbInstance) {
        let db_url = pgdb::db_fixture();
        let queue = Queue::connect(db_url.as_str())
            .await
            .expect("failed to connect to test database");
        (queue, db_url)
    }

    #[tokio::test]
    async fn pipeline_advances_through_stages() {
        let (queue, _db) = setup_db().await;

        queue
            .create_queue("stage1", false)
            .await
            .expect("failed to create queue");
        queue
            .create_queue("stage2", false)
            .await
            .expect("failed to create queue");
        queue
            .create_queue("stage3", false)
            .await
            .expect("failed to create queue");

        let pipeline = Pipeline::builder()
            .stage("stage1", |x: i32| async move { Ok(x + 1) })
            .stage("stage2", |x: i32| async move { Ok(x * 2) })
            .stage("stage3", |_x: i32| async move { Ok(()) })
            .build();

        assert_eq!(pipeline.queues(), &["stage1", "stage2", "stage3"]);

        let id = queue
            .enqueue("stage1", 10i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let queue_refs: Vec<&str> = pipeline.queues().iter().map(|s| s.as_str()).collect();

        let mut stream = pin!(queue.try_stream_jobs_from_raw(&queue_refs));

        let (details, payload, ack) = stream.next().await.expect("no job").expect("fetch failed");
        assert_eq!(details.id, id);
        assert_eq!(details.queue, "stage1");
        pipeline
            .run(&details, payload, ack)
            .await
            .expect("pipeline failed");

        let job = queue.get_job(id).await.expect("get failed").unwrap();
        assert_eq!(job.status, JobStatus::Finished);

        let (details2, payload2, ack2) =
            stream.next().await.expect("no job").expect("fetch failed");
        assert_eq!(details2.queue, "stage2");
        let val: i32 = rmp_serde::from_slice(&payload2).expect("deserialize failed");
        assert_eq!(val, 11);
        pipeline
            .run(&details2, payload2, ack2)
            .await
            .expect("pipeline failed");

        let (details3, payload3, ack3) =
            stream.next().await.expect("no job").expect("fetch failed");
        assert_eq!(details3.queue, "stage3");
        let val3: i32 = rmp_serde::from_slice(&payload3).expect("deserialize failed");
        assert_eq!(val3, 22);
        pipeline
            .run(&details3, payload3, ack3)
            .await
            .expect("pipeline failed");

        let job3 = queue
            .get_job(details3.id)
            .await
            .expect("get failed")
            .unwrap();
        assert_eq!(job3.status, JobStatus::Finished);
    }

    #[tokio::test]
    async fn pipeline_retries_on_error() {
        let (queue, _db) = setup_db().await;

        queue
            .create_queue("flaky", false)
            .await
            .expect("failed to create queue");

        let pipeline = Pipeline::builder()
            .stage("flaky", |_x: i32| async move {
                anyhow::bail!("transient error");
                #[allow(unreachable_code)]
                Ok::<(), _>(())
            })
            .build();

        let id = queue
            .enqueue("flaky", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let queue_refs: Vec<&str> = pipeline.queues().iter().map(|s| s.as_str()).collect();
        let mut stream = pin!(queue.try_stream_jobs_from_raw(&queue_refs));

        let (details, payload, ack) = stream.next().await.expect("no job").expect("fetch failed");
        assert_eq!(details.id, id);
        pipeline
            .run(&details, payload, ack)
            .await
            .expect("pipeline failed");

        let job = queue.get_job(id).await.expect("get failed").unwrap();
        assert_eq!(job.status, JobStatus::Pending);
        assert_eq!(job.retry_count, 1);
        assert!(job.error.as_ref().unwrap().contains("transient error"));
    }

    #[tokio::test]
    async fn pipeline_fails_unknown_stage() {
        let (queue, _db) = setup_db().await;

        queue
            .create_queue("known", false)
            .await
            .expect("failed to create queue");
        queue
            .create_queue("unknown", false)
            .await
            .expect("failed to create queue");

        let pipeline = Pipeline::builder()
            .stage("known", |x: i32| async move { Ok(x) })
            .build();

        let id = queue
            .enqueue("unknown", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let (details, payload, ack) = queue
            .pull_next("unknown")
            .await
            .expect("fetch failed")
            .expect("no job");
        assert_eq!(details.id, id);
        pipeline
            .run(&details, payload, ack)
            .await
            .expect("pipeline failed");

        let job = queue.get_job(id).await.expect("get failed").unwrap();
        assert_eq!(job.status, JobStatus::Failed);
        assert!(job.error.as_ref().unwrap().contains("unknown stage"));
    }
}
