ALTER TABLE jobs DROP CONSTRAINT jobs_queue_fkey;
ALTER TABLE jobs ADD CONSTRAINT jobs_queue_fkey
    FOREIGN KEY (queue) REFERENCES queues(queue) ON UPDATE CASCADE;
