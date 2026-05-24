CREATE TYPE job_status AS ENUM ('pending', 'paused', 'in_progress', 'finished', 'failed');

CREATE TABLE queues (
    queue TEXT PRIMARY KEY,
    paused BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE payloads (
    hash BYTEA PRIMARY KEY,
    data BYTEA NOT NULL,
    refcount INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    queue TEXT NOT NULL REFERENCES queues(queue),
    status job_status NOT NULL DEFAULT 'pending',
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload_hash BYTEA NOT NULL REFERENCES payloads(hash),
    lock TIMESTAMPTZ,
    lock_token UUID,
    error TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX jobs_queue_pending ON jobs(queue, created_at) WHERE status = 'pending';
