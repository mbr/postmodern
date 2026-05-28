//! Atomic payload transformation.

use sqlx::PgPool;
use uuid::Uuid;

use crate::error::ModifyError;

/// Result of a payload transformation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformResult {
    /// Job was locked by another transaction, skipped.
    Skipped,
    /// Payload unchanged after transformation.
    Unchanged,
    /// Payload was transformed and replaced.
    Transformed,
}

/// Transforms a job's payload atomically within a single transaction.
///
/// Locks the job, fetches its payload, calls the transform function, and replaces the payload
/// if changed. Uses `FOR UPDATE SKIP LOCKED` to avoid blocking workers.
///
/// The transform function receives `(queue_name, payload_bytes)` and returns new payload bytes.
pub async fn transform_job_payload<F>(
    pool: &PgPool,
    job_id: Uuid,
    transform: F,
) -> Result<TransformResult, ModifyError>
where
    F: FnOnce(&str, &[u8]) -> Result<Vec<u8>, anyhow::Error>,
{
    let mut tx = pool.begin().await.map_err(ModifyError::Database)?;

    let row: Option<(String, Vec<u8>)> =
        sqlx::query_as("SELECT queue, payload FROM jobs WHERE id = $1 FOR UPDATE SKIP LOCKED")
            .bind(job_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(ModifyError::Database)?;

    let Some((queue_name, old_payload)) = row else {
        return Ok(TransformResult::Skipped);
    };

    let new_payload = transform(&queue_name, &old_payload).map_err(ModifyError::Transform)?;

    if old_payload == new_payload {
        return Ok(TransformResult::Unchanged);
    }

    sqlx::query("UPDATE jobs SET payload = $1 WHERE id = $2")
        .bind(&new_payload)
        .bind(job_id)
        .execute(&mut *tx)
        .await
        .map_err(ModifyError::Database)?;

    tx.commit().await.map_err(ModifyError::Database)?;
    Ok(TransformResult::Transformed)
}

#[cfg(test)]
mod tests {
    use super::{transform_job_payload, TransformResult};
    use crate::{EnqueueOptions, Queue};

    async fn setup_db() -> (Queue, pgdb::DbInstance) {
        let db_url = pgdb::db_fixture();
        let queue = Queue::connect(db_url.as_str())
            .await
            .expect("failed to connect");
        queue
            .create_queue("test", false)
            .await
            .expect("failed to create queue");
        (queue, db_url)
    }

    #[tokio::test]
    async fn transforms_payload() {
        let (queue, _db) = setup_db().await;

        let id = queue
            .enqueue("test", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let old_payload = queue
            .get_job_payload(id)
            .await
            .expect("get payload failed")
            .expect("payload not found");

        let result = transform_job_payload(queue.pool(), id, |_queue, _payload| {
            Ok(rmp_serde::to_vec_named(&99i32).expect("serialize failed"))
        })
        .await
        .expect("transform failed");
        assert_eq!(result, TransformResult::Transformed);

        let updated_payload = queue
            .get_job_payload(id)
            .await
            .expect("get payload failed")
            .expect("payload not found");
        assert_ne!(updated_payload, old_payload);
    }

    #[tokio::test]
    async fn unchanged_when_same() {
        let (queue, _db) = setup_db().await;

        let id = queue
            .enqueue("test", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let result =
            transform_job_payload(queue.pool(), id, |_queue, payload| Ok(payload.to_vec()))
                .await
                .expect("transform failed");
        assert_eq!(result, TransformResult::Unchanged);
    }
}
