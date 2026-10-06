package queue

import "context"

const schema = `
CREATE TABLE IF NOT EXISTS relay_jobs (
 id text PRIMARY KEY,
 queue text NOT NULL,
 state text NOT NULL CHECK (state IN ('pending','leased','retry_wait','completed','dead_letter')),
 payload json NOT NULL,
 attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
 max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 1000),
 lease_token text,
 worker_id text,
 lease_expires_at timestamptz,
 created_at timestamptz NOT NULL,
 available_at timestamptz NOT NULL,
 completed_at timestamptz,
 last_error text NOT NULL DEFAULT '',
 timeout_ms bigint NOT NULL CHECK (timeout_ms BETWEEN 1 AND 86400000),
 CHECK (attempt <= max_attempts),
 CHECK ((state = 'leased') = (lease_token IS NOT NULL AND worker_id IS NOT NULL AND lease_expires_at IS NOT NULL)),
 CHECK (state = 'leased' OR (lease_token IS NULL AND worker_id IS NULL AND lease_expires_at IS NULL)),
 CHECK ((state IN ('completed','dead_letter')) = (completed_at IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS relay_jobs_ready ON relay_jobs(queue,available_at,created_at,id) WHERE state IN ('pending','retry_wait');
CREATE INDEX IF NOT EXISTS relay_jobs_expired ON relay_jobs(lease_expires_at,id) WHERE state = 'leased';
CREATE INDEX IF NOT EXISTS relay_jobs_queue_state ON relay_jobs(queue,state);
CREATE TABLE IF NOT EXISTS relay_idempotency (
 queue text NOT NULL,
 key text NOT NULL,
 job_id text REFERENCES relay_jobs(id),
 expires_at timestamptz NOT NULL,
 PRIMARY KEY(queue,key)
);
`

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(783492176324)`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, schema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
