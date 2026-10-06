package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
	cfg  Config
}

func Open(ctx context.Context, url string, cfg Config) (*Store, error) {
	if cfg.LeaseDuration < time.Millisecond || cfg.LeaseDuration > 24*time.Hour || cfg.IdempotencyWindow < time.Millisecond || cfg.RetryBase < time.Millisecond || cfg.RetryMax < cfg.RetryBase || cfg.RetryMax > 365*24*time.Hour {
		return nil, fmt.Errorf("%w: invalid queue configuration", ErrInvalid)
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool, cfg: cfg}, nil
}

func (s *Store) Close()                         { s.pool.Close() }
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

const columns = `id,queue,state,payload,attempt,max_attempts,COALESCE(lease_token,''),COALESCE(worker_id,''),lease_expires_at,created_at,available_at,completed_at,last_error,timeout_ms`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.Queue, &j.State, &j.Payload, &j.Attempt, &j.MaxAttempts, &j.LeaseToken, &j.WorkerID, &j.LeaseExpiresAt, &j.CreatedAt, &j.AvailableAt, &j.CompletedAt, &j.LastError, &j.TimeoutMillis)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	return j, err
}

func identifier() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func dbNow(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)
	return now, err
}

func (s *Store) Enqueue(ctx context.Context, name string, req Submit) (Job, bool, error) {
	if err := validateSubmit(name, req); err != nil {
		return Job{}, false, err
	}
	if req.MaxAttempts == 0 {
		req.MaxAttempts = 3
	}
	if req.Timeout == 0 {
		req.Timeout = s.cfg.LeaseDuration
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback(ctx)
	if req.IdempotencyKey != "" {
		_, err = tx.Exec(ctx, `INSERT INTO relay_idempotency(queue,key,expires_at) VALUES($1,$2,clock_timestamp()) ON CONFLICT DO NOTHING`, name, req.IdempotencyKey)
		if err != nil {
			return Job{}, false, err
		}
		var existing string
		var expires time.Time
		err = tx.QueryRow(ctx, `SELECT COALESCE(job_id,''),expires_at FROM relay_idempotency WHERE queue=$1 AND key=$2 FOR UPDATE`, name, req.IdempotencyKey).Scan(&existing, &expires)
		if err != nil {
			return Job{}, false, err
		}
		now, e := dbNow(ctx, tx)
		if e != nil {
			return Job{}, false, e
		}
		if existing != "" && expires.After(now) {
			job, e := scanJob(tx.QueryRow(ctx, `SELECT `+columns+` FROM relay_jobs WHERE id=$1`, existing))
			if e != nil {
				return Job{}, false, e
			}
			if e = tx.Commit(ctx); e != nil {
				return Job{}, false, e
			}
			return job, false, nil
		}
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return Job{}, false, err
	}
	job, err := scanJob(tx.QueryRow(ctx, `INSERT INTO relay_jobs(id,queue,state,payload,max_attempts,created_at,available_at,timeout_ms) VALUES($1,$2,'pending',$3,$4,$5,$6,$7) RETURNING `+columns, identifier(), name, req.Payload, req.MaxAttempts, now, now.Add(req.Delay), req.Timeout.Milliseconds()))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "22") {
			return Job{}, false, fmt.Errorf("%w: payload cannot be stored as PostgreSQL JSON", ErrInvalid)
		}
		return Job{}, false, err
	}
	if req.IdempotencyKey != "" {
		_, err = tx.Exec(ctx, `UPDATE relay_idempotency SET job_id=$3,expires_at=$4 WHERE queue=$1 AND key=$2`, name, req.IdempotencyKey, job.ID, now.Add(s.cfg.IdempotencyWindow))
		if err != nil {
			return Job{}, false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return Job{}, false, err
	}
	return job, true, nil
}

func (s *Store) Claim(ctx context.Context, name, worker string) (Job, error) {
	if err := validateQueue(name); err != nil {
		return Job{}, err
	}
	if len(worker) == 0 || len(worker) > 256 || !utf8.ValidString(worker) || strings.IndexByte(worker, 0) >= 0 {
		return Job{}, fmt.Errorf("%w: worker_id must contain 1–256 valid UTF-8 bytes", ErrInvalid)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback(ctx)
	job, err := scanJob(tx.QueryRow(ctx, `SELECT `+columns+` FROM relay_jobs WHERE queue=$1 AND state IN ('pending','retry_wait') AND available_at<=clock_timestamp() AND attempt<max_attempts ORDER BY available_at,created_at,id LIMIT 1 FOR UPDATE SKIP LOCKED`, name))
	if errors.Is(err, ErrNotFound) {
		return Job{}, ErrEmpty
	}
	if err != nil {
		return Job{}, err
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return Job{}, err
	}
	job, err = scanJob(tx.QueryRow(ctx, `UPDATE relay_jobs SET state='leased',attempt=attempt+1,lease_token=$2,worker_id=$3,lease_expires_at=$4 WHERE id=$1 RETURNING `+columns, job.ID, identifier(), worker, now.Add(time.Duration(job.TimeoutMillis)*time.Millisecond)))
	if err != nil {
		return Job{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return job, nil
}

func (s *Store) Get(ctx context.Context, id string) (Job, error) {
	return scanJob(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM relay_jobs WHERE id=$1`, id))
}

func lockedLease(ctx context.Context, tx pgx.Tx, id, token string) (Job, time.Time, error) {
	job, err := scanJob(tx.QueryRow(ctx, `SELECT `+columns+` FROM relay_jobs WHERE id=$1 FOR UPDATE`, id))
	if err != nil {
		return Job{}, time.Time{}, err
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return Job{}, time.Time{}, err
	}
	if token == "" || job.State != Leased || job.LeaseToken != token || job.LeaseExpiresAt == nil || !job.LeaseExpiresAt.After(now) {
		return Job{}, time.Time{}, ErrLeaseLost
	}
	return job, now, nil
}

func (s *Store) Ack(ctx context.Context, id, token string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, now, err := lockedLease(ctx, tx, id, token)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE relay_jobs SET state='completed',completed_at=$2,lease_token=NULL,worker_id=NULL,lease_expires_at=NULL WHERE id=$1`, id, now)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) retry(ctx context.Context, tx pgx.Tx, job Job, now time.Time, reason string) error {
	state := RetryWait
	available := now.Add(backoff(job.Attempt, s.cfg.RetryBase, s.cfg.RetryMax))
	var completed *time.Time
	if job.Attempt >= job.MaxAttempts {
		state = DeadLetter
		completed = &now
		available = now
	}
	_, err := tx.Exec(ctx, `UPDATE relay_jobs SET state=$2,available_at=$3,completed_at=$4,last_error=$5,lease_token=NULL,worker_id=NULL,lease_expires_at=NULL WHERE id=$1`, job.ID, state, available, completed, reason)
	return err
}

func (s *Store) Fail(ctx context.Context, id, token, reason string) error {
	if len(reason) > 4096 || !utf8.ValidString(reason) || strings.IndexByte(reason, 0) >= 0 {
		return fmt.Errorf("%w: reason must be valid UTF-8 up to 4096 bytes", ErrInvalid)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	job, now, err := lockedLease(ctx, tx, id, token)
	if err != nil {
		return err
	}
	if err = s.retry(ctx, tx, job, now, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Extend(ctx context.Context, id, token string, extension time.Duration) (Job, error) {
	if extension < time.Millisecond || extension > 24*time.Hour {
		return Job{}, fmt.Errorf("%w: extension must be between 1 millisecond and 24 hours", ErrInvalid)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback(ctx)
	job, now, err := lockedLease(ctx, tx, id, token)
	if err != nil {
		return Job{}, err
	}
	expires := job.LeaseExpiresAt.Add(extension)
	if expires.After(now.Add(24 * time.Hour)) {
		return Job{}, fmt.Errorf("%w: lease cannot extend beyond 24 hours from now", ErrInvalid)
	}
	job, err = scanJob(tx.QueryRow(ctx, `UPDATE relay_jobs SET lease_expires_at=$2 WHERE id=$1 RETURNING `+columns, id, expires))
	if err != nil {
		return Job{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return job, nil
}

func (s *Store) Recover(ctx context.Context) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT `+columns+` FROM relay_jobs WHERE state='leased' AND lease_expires_at<=clock_timestamp() ORDER BY lease_expires_at,id LIMIT 100 FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return 0, err
	}
	jobs := make([]Job, 0, 100)
	for rows.Next() {
		job, e := scanJob(rows)
		if e != nil {
			rows.Close()
			return 0, e
		}
		jobs = append(jobs, job)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return 0, err
	}
	for _, job := range jobs {
		if err = s.retry(ctx, tx, job, now, "lease expired"); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(jobs), nil
}

func (s *Store) Stats(ctx context.Context, name string) (Stats, error) {
	if name != "" {
		if err := validateQueue(name); err != nil {
			return Stats{}, err
		}
	}
	var stats Stats
	err := s.pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE state='pending'),count(*) FILTER(WHERE state='leased'),count(*) FILTER(WHERE state='retry_wait'),count(*) FILTER(WHERE state='completed'),count(*) FILTER(WHERE state='dead_letter') FROM relay_jobs WHERE ($1='' OR queue=$1)`, name).Scan(&stats.Pending, &stats.Leased, &stats.RetryWait, &stats.Completed, &stats.DeadLetter)
	return stats, err
}
