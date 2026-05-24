ALTER TABLE jobs ADD COLUMN priority BIGINT NOT NULL DEFAULT 0;

DROP INDEX jobs_queue_pending;
CREATE INDEX jobs_queue_pending ON jobs(queue, priority DESC, created_at) WHERE status = 'pending';
