//! Atomic payload transformation.

use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::ModifyError;

/// Result of a payload transformation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformResult {
    /// Job was locked by another transaction, skipped.
    Skipped,
    /// Payload unchanged after transformation (same hash).
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
        sqlx::query_as("SELECT queue, payload_hash FROM jobs WHERE id = $1 FOR UPDATE SKIP LOCKED")
            .bind(job_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(ModifyError::Database)?;

    let Some((queue_name, old_hash)) = row else {
        return Ok(TransformResult::Skipped);
    };

    let old_payload: Option<(Vec<u8>,)> =
        sqlx::query_as("SELECT data FROM payloads WHERE hash = $1")
            .bind(&old_hash)
            .fetch_optional(&mut *tx)
            .await
            .map_err(ModifyError::Database)?;

    let Some((old_payload,)) = old_payload else {
        return Err(ModifyError::NotFound);
    };

    let new_payload = transform(&queue_name, &old_payload).map_err(ModifyError::Transform)?;
    let new_hash = Sha256::digest(&new_payload);

    if old_hash == new_hash.as_slice() {
        return Ok(TransformResult::Unchanged);
    }

    sqlx::query(
        "INSERT INTO payloads (hash, data, refcount) VALUES ($1, $2, 0) \
         ON CONFLICT (hash) DO NOTHING",
    )
    .bind(new_hash.as_slice())
    .bind(&new_payload)
    .execute(&mut *tx)
    .await
    .map_err(ModifyError::Database)?;

    sqlx::query("UPDATE jobs SET payload_hash = $1 WHERE id = $2")
        .bind(new_hash.as_slice())
        .bind(job_id)
        .execute(&mut *tx)
        .await
        .map_err(ModifyError::Database)?;

    sqlx::query("UPDATE payloads SET refcount = refcount + 1 WHERE hash = $1")
        .bind(new_hash.as_slice())
        .execute(&mut *tx)
        .await
        .map_err(ModifyError::Database)?;

    crate::release_payload(&mut tx, &old_hash).await?;

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
    async fn updates_hash_and_refcounts() {
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

        let (refcount,): (i32,) = sqlx::query_as(
            "SELECT refcount FROM payloads WHERE hash = (SELECT payload_hash FROM jobs WHERE id = $1)",
        )
        .bind(id)
        .fetch_one(queue.pool())
        .await
        .expect("query failed");
        assert_eq!(refcount, 1);
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

    #[tokio::test]
    async fn handles_shared_payloads() {
        let (queue, _db) = setup_db().await;

        // Enqueue same payload twice - they share the same payload via content deduplication
        let id1 = queue
            .enqueue("test", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let id2 = queue
            .enqueue("test", 42i32, EnqueueOptions::default())
            .await
            .expect("enqueue failed")
            .expect("unexpected duplicate");

        let (refcount_before,): (i32,) = sqlx::query_as(
            "SELECT refcount FROM payloads WHERE hash = (SELECT payload_hash FROM jobs WHERE id = $1)",
        )
        .bind(id1)
        .fetch_one(queue.pool())
        .await
        .expect("query failed");
        assert_eq!(refcount_before, 2);

        transform_job_payload(queue.pool(), id1, |_queue, _payload| {
            Ok(rmp_serde::to_vec_named(&99i32).expect("serialize failed"))
        })
        .await
        .expect("transform failed");

        let (old_refcount,): (i32,) = sqlx::query_as(
            "SELECT refcount FROM payloads WHERE hash = (SELECT payload_hash FROM jobs WHERE id = $1)",
        )
        .bind(id2)
        .fetch_one(queue.pool())
        .await
        .expect("query failed");
        assert_eq!(old_refcount, 1);

        let (new_refcount,): (i32,) = sqlx::query_as(
            "SELECT refcount FROM payloads WHERE hash = (SELECT payload_hash FROM jobs WHERE id = $1)",
        )
        .bind(id1)
        .fetch_one(queue.pool())
        .await
        .expect("query failed");
        assert_eq!(new_refcount, 1);
    }
}
