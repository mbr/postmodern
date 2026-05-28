ALTER TABLE jobs ADD COLUMN key TEXT;
CREATE UNIQUE INDEX jobs_queue_key ON jobs(queue, key)
    WHERE key IS NOT NULL;
