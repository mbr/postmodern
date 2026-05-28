ALTER TABLE jobs ADD COLUMN payload BYTEA;

UPDATE jobs SET payload = p.data
FROM payloads p
WHERE jobs.payload_hash = p.hash;

ALTER TABLE jobs ALTER COLUMN payload SET NOT NULL;
ALTER TABLE jobs DROP COLUMN payload_hash;

DROP TABLE payloads;
