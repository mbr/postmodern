//! Error types for queue operations.

/// Queue connection errors.
#[derive(Debug, thiserror::Error)]
pub enum ConnectError {
    /// Database connection failed.
    #[error("database connection failed")]
    Database(#[source] sqlx::Error),
    /// Migration failed.
    #[error("migration failed")]
    Migration(#[source] sqlx::migrate::MigrateError),
}

/// Enqueue operation errors.
#[derive(Debug, thiserror::Error)]
pub enum EnqueueError {
    /// Payload serialization failed.
    #[error("failed to serialize payload")]
    Serialize(#[source] rmp_serde::encode::Error),
    /// Database operation failed.
    #[error("database error")]
    Database(#[source] sqlx::Error),
    /// Queue does not exist.
    #[error("queue not found")]
    QueueNotFound,
}

/// Job fetch errors.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// Query execution failed.
    #[error("query failed")]
    Query(#[source] sqlx::Error),
    /// Payload deserialization failed.
    #[error("failed to deserialize payload for job {0}")]
    Deserialize(uuid::Uuid, #[source] rmp_serde::decode::Error),
}

/// Job listing errors.
#[derive(Debug, thiserror::Error)]
pub enum ListError {
    /// Query execution failed.
    #[error("query failed")]
    Query(#[source] sqlx::Error),
}

/// Job modification errors.
#[derive(Debug, thiserror::Error)]
pub enum ModifyError {
    /// Database operation failed.
    #[error("database error")]
    Database(#[source] sqlx::Error),
    /// Job not found.
    #[error("job not found")]
    NotFound,
    /// Queue does not exist.
    #[error("queue not found")]
    QueueNotFound,
    /// Transform function failed.
    #[error("transform failed")]
    Transform(#[source] anyhow::Error),
}

/// Job acknowledgment errors.
#[derive(Debug, thiserror::Error)]
pub enum AckError {
    /// Database operation failed.
    #[error("failed to update job")]
    Database(#[source] sqlx::Error),
    /// Lock was lost, likely due to timeout and reaping.
    #[error("lock lost")]
    LockLost,
}

/// Reaper errors.
#[derive(Debug, thiserror::Error)]
pub enum ReapError {
    /// Database operation failed.
    #[error("database error")]
    Database(#[source] sqlx::Error),
}
