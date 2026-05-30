//! Sequential durable workflows.
//!
//! Pipelines provide a layer on top of postmodern's job primitives for building multi-stage
//! workflows. Each stage corresponds to a queue, and jobs advance from one stage to the next.
//!
//! # Semantics
//!
//! **Stages run in order of addition.** The builder links each stage to the previous one,
//! forming a linear pipeline. There is no support for branching or parallel stages.
//!
//! **At-least-once execution.** The stage handler runs, then the job advances atomically to
//! the next queue. If the process crashes between handler completion and the advance, the
//! job's lease expires and it re-runs from the beginning of the current stage. Stages must
//! be idempotent or tolerate duplicate execution.
//!
//! **All errors retry.** Handler errors trigger [`soft_fail`](crate::job::JobAck::soft_fail)
//! with exponential backoff. After [`MAX_RETRIES`](crate::job::MAX_RETRIES), the job
//! transitions to `Failed`. There is no way to signal permanent failure from within a stage.
//!
//! # Example
//!
//! ```no_run
//! use futures::StreamExt;
//! use postmodern::{Queue, pipeline::Pipeline};
//!
//! # #[derive(serde::Serialize, serde::Deserialize)] struct Scan;
//! # #[derive(serde::Serialize, serde::Deserialize)] struct Ocred;
//! # #[derive(serde::Serialize, serde::Deserialize)] struct Classified;
//! # async fn ocr(_: Scan) -> Result<Ocred, std::io::Error> { Ok(Ocred) }
//! # async fn classify(_: Ocred) -> Result<Classified, std::io::Error> { Ok(Classified) }
//! # async fn archive(_: Classified) -> Result<(), std::io::Error> { Ok(()) }
//!
//! # async fn example(queue: Queue) {
//! let pipeline = Pipeline::builder()
//!     .stage("docs:ocr", |s: Scan| async move { ocr(s).await })
//!     .stage("docs:classify", |o: Ocred| async move { classify(o).await })
//!     .stage("docs:archive", |c: Classified| async move { archive(c).await })
//!     .build();
//!
//! // Process all stages with shared concurrency budget
//! queue
//!     .stream_jobs(pipeline.queues())
//!     .for_each_concurrent(16, |job| pipeline.dispatch(job))
//!     .await;
//! # }
//! ```
//!
//! For per-stage concurrency control, stream each stage separately:
//!
//! ```ignore
//! // Each stage gets its own concurrency budget
//! tokio::join!(
//!     queue.stream_jobs(["docs:ocr"]).for_each_concurrent(8, |job| pipeline.dispatch(job)),
//!     queue.stream_jobs(["docs:classify"]).for_each_concurrent(4, |job| pipeline.dispatch(job)),
//!     queue.stream_jobs(["docs:archive"]).for_each_concurrent(2, |job| pipeline.dispatch(job)),
//! );
//! ```

use std::{collections::HashMap, fmt::Display, future::Future, pin::Pin, sync::Arc};

use display_full_error::DisplayFullErrorExt;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    error::{AckError, AdvanceError},
    job::{AdvanceOptions, JobAck, JobMetadata, PendingJob},
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
///
/// Returns serialized output bytes on success, or an error message string on failure.
type StageHandler = Arc<
    dyn Fn(rmpv::Value) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send>>
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

    /// Runs the stage handler for a queue.
    async fn run_stage(&self, queue: &str, payload: rmpv::Value) -> Outcome {
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
            Err(msg) => Outcome::Retry(msg),
        }
    }

    /// Processes a job through the pipeline.
    ///
    /// Dispatches the job to its stage handler and resolves it based on the outcome.
    pub async fn run(
        &self,
        meta: &JobMetadata,
        payload: rmpv::Value,
        ack: JobAck,
    ) -> Result<(), PipelineError> {
        match self.run_stage(&meta.queue, payload).await {
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

    /// Processes a job through the pipeline, logging errors.
    ///
    /// Like [`run`](Self::run), but errors are logged instead of returned. Use with
    /// [`Queue::stream_jobs`] for fire-and-forget processing:
    ///
    /// ```ignore
    /// queue.stream_jobs(pipeline.queues())
    ///     .for_each_concurrent(16, |job| pipeline.dispatch(job))
    ///     .await;
    /// ```
    pub async fn dispatch(&self, job: PendingJob<rmpv::Value>) {
        let (meta, payload, ack) = job.into_parts();
        if let Err(e) = self.run(&meta, payload, ack).await {
            tracing::error!(id = %meta.id, queue = %meta.queue, error = %e.display_full(), "pipeline error");
        }
    }

    /// Returns the queue names this pipeline handles.
    pub fn queues(&self) -> Vec<String> {
        self.queues.clone()
    }
}

impl PipelineBuilder {
    /// Adds a stage that transforms `In` to `Out`.
    ///
    /// Stages are linked in the order they are added, forming a linear pipeline. The handler
    /// receives a deserialized payload and returns the output to be serialized for the next stage.
    ///
    /// # Panics
    ///
    /// Panics if a stage with the same queue name has already been added.
    pub fn stage<In, Out, E, F, Fut>(mut self, queue: &str, f: F) -> Self
    where
        In: DeserializeOwned + Send + 'static,
        Out: Serialize + Send + 'static,
        E: Display + Send + 'static,
        F: Fn(In) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Out, E>> + Send + 'static,
    {
        assert!(
            !self.entries.contains_key(queue),
            "duplicate stage queue name: {queue}"
        );

        // Link previous stage to this one (linear pipeline assumption).
        if let Some(prev) = self.queue_order.last() {
            if let Some(entry) = self.entries.get_mut(prev) {
                entry.next = Some(queue.to_string());
            }
        }

        // Construct handler: deserialize Value to In, run handler, serialize Out to bytes.
        // Errors are formatted via Display and returned as strings.
        let f = Arc::new(f);
        let handler: StageHandler = Arc::new(move |value| {
            let f = Arc::clone(&f);
            Box::pin(async move {
                let input: In =
                    rmpv::ext::from_value(value).map_err(|e| format!("deserialize: {e}"))?;
                let output = f(input).await.map_err(|e| e.to_string())?;
                rmp_serde::to_vec_named(&output).map_err(|e| format!("serialize: {e}"))
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
            .stage("stage1", |x: i32| async move { Ok::<_, &str>(x + 1) })
            .stage("stage2", |x: i32| async move { Ok::<_, &str>(x * 2) })
            .stage("stage3", |_x: i32| async move { Ok::<_, &str>(()) })
            .build();

        assert_eq!(pipeline.queues(), vec!["stage1", "stage2", "stage3"]);

        let id = queue
            .enqueue("stage1", 10i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let mut stream = pin!(queue.try_stream_jobs::<rmpv::Value, _, _>(pipeline.queues()));

        let job = stream.next().await.expect("no job").expect("fetch failed");
        assert_eq!(job.meta.id, id);
        assert_eq!(job.meta.queue, "stage1");
        let (meta, payload, ack) = job.into_parts();
        pipeline
            .run(&meta, payload, ack)
            .await
            .expect("pipeline failed");

        let stored = queue.get_job(id).await.expect("get failed").unwrap();
        assert_eq!(stored.status, JobStatus::Finished);

        let job2 = stream.next().await.expect("no job").expect("fetch failed");
        assert_eq!(job2.meta.queue, "stage2");
        let val: i32 = rmpv::ext::from_value(job2.payload.clone()).expect("convert failed");
        assert_eq!(val, 11);
        let (meta2, payload2, ack2) = job2.into_parts();
        pipeline
            .run(&meta2, payload2, ack2)
            .await
            .expect("pipeline failed");

        let job3 = stream.next().await.expect("no job").expect("fetch failed");
        assert_eq!(job3.meta.queue, "stage3");
        let val3: i32 = rmpv::ext::from_value(job3.payload.clone()).expect("convert failed");
        assert_eq!(val3, 22);
        let job3_id = job3.meta.id;
        let (meta3, payload3, ack3) = job3.into_parts();
        pipeline
            .run(&meta3, payload3, ack3)
            .await
            .expect("pipeline failed");

        let stored3 = queue.get_job(job3_id).await.expect("get failed").unwrap();
        assert_eq!(stored3.status, JobStatus::Finished);
    }

    #[tokio::test]
    async fn pipeline_retries_on_error() {
        let (queue, _db) = setup_db().await;

        queue
            .create_queue("flaky", false)
            .await
            .expect("failed to create queue");

        let pipeline = Pipeline::builder()
            .stage(
                "flaky",
                |_x: i32| async move { Err::<(), _>("transient error") },
            )
            .build();

        let id = queue
            .enqueue("flaky", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let mut stream = pin!(queue.try_stream_jobs::<rmpv::Value, _, _>(pipeline.queues()));

        let job = stream.next().await.expect("no job").expect("fetch failed");
        assert_eq!(job.meta.id, id);
        let (meta, payload, ack) = job.into_parts();
        pipeline
            .run(&meta, payload, ack)
            .await
            .expect("pipeline failed");

        let stored = queue.get_job(id).await.expect("get failed").unwrap();
        assert_eq!(stored.status, JobStatus::Pending);
        assert_eq!(stored.retry_count, 1);
        assert!(stored.error.as_ref().unwrap().contains("transient error"));
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
            .stage("known", |x: i32| async move { Ok::<_, &str>(x) })
            .build();

        let id = queue
            .enqueue("unknown", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let mut stream = pin!(queue.try_stream_jobs::<rmpv::Value, _, _>(["unknown"]));
        let job = stream.next().await.expect("no job").expect("fetch failed");
        assert_eq!(job.meta.id, id);
        let (meta, payload, ack) = job.into_parts();
        pipeline
            .run(&meta, payload, ack)
            .await
            .expect("pipeline failed");

        let stored = queue.get_job(id).await.expect("get failed").unwrap();
        assert_eq!(stored.status, JobStatus::Failed);
        assert!(stored.error.as_ref().unwrap().contains("unknown stage"));
    }

    #[test]
    #[should_panic(expected = "duplicate stage queue name: stage1")]
    fn duplicate_stage_panics() {
        let _ = Pipeline::builder()
            .stage("stage1", |x: i32| async move { Ok::<_, &str>(x) })
            .stage("stage1", |x: i32| async move { Ok::<_, &str>(x * 2) })
            .build();
    }
}
