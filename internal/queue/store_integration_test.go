package queue

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func integrationStore(t *testing.T) (*Store, string) {
	t.Helper()
	databaseURL := os.Getenv("RELAY_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("RELAY_TEST_DATABASE_URL is not set")
	}
	admin, err := Open(context.Background(), databaseURL, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	name := "queue_test_" + identifier()
	if _, err = admin.pool.Exec(context.Background(), `CREATE SCHEMA `+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.pool.Exec(context.Background(), `DROP SCHEMA `+name+` CASCADE`); err != nil {
			t.Error(err)
		}
	})
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	params := parsed.Query()
	params.Set("search_path", name)
	params.Set("application_name", name)
	parsed.RawQuery = params.Encode()
	s, err := Open(context.Background(), parsed.String(), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err = s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, name
}

func TestIntegrationLeaseExpiryAfterLockWait(t *testing.T) {
	s, name := integrationStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, operation := range []string{"ack", "fail", "extend"} {
		t.Run(operation, func(t *testing.T) {
			submitted, _, err := s.Enqueue(ctx, name, Submit{Payload: json.RawMessage(`{}`), Timeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			job, err := s.Claim(ctx, name, "waiting-worker")
			if err != nil {
				t.Fatal(err)
			}
			if job.ID != submitted.ID {
				t.Fatalf("claimed %s, wanted %s", job.ID, submitted.ID)
			}
			holder, err := s.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer holder.Rollback(ctx)
			if _, err = holder.Exec(ctx, `SELECT id FROM relay_jobs WHERE id=$1 FOR UPDATE`, job.ID); err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() {
				switch operation {
				case "ack":
					result <- s.Ack(ctx, job.ID, job.LeaseToken)
				case "fail":
					result <- s.Fail(ctx, job.ID, job.LeaseToken, "failed")
				case "extend":
					_, err := s.Extend(ctx, job.ID, job.LeaseToken, time.Second)
					result <- err
				}
			}()
			deadline := time.Now().Add(800 * time.Millisecond)
			waiting := false
			for time.Now().Before(deadline) {
				err = s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND application_name=$1 AND wait_event_type='Lock' AND query LIKE '%COALESCE(lease_token%' AND query LIKE '%WHERE id=$1 FOR UPDATE%')`, name).Scan(&waiting)
				if err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				time.Sleep(5 * time.Millisecond)
			}
			if !waiting {
				t.Fatal("lease mutation did not block on the job row")
			}
			if !time.Now().Before(*job.LeaseExpiresAt) {
				t.Fatal("lease expired before mutation entered its lock wait")
			}
			time.Sleep(time.Until(*job.LeaseExpiresAt) + 25*time.Millisecond)
			if err = holder.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case err = <-result:
				if !errors.Is(err, ErrLeaseLost) {
					t.Fatalf("mutation accepted expired lease after waiting: %v", err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			stored, err := s.Get(ctx, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.State != Leased || stored.LeaseToken != job.LeaseToken || !stored.LeaseExpiresAt.Equal(*job.LeaseExpiresAt) {
				t.Fatalf("failed mutation changed job: %+v", stored)
			}
		})
	}
}

func TestIntegrationIndependentStoresClaimOnce(t *testing.T) {
	first, name := integrationStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	second, err := Open(ctx, first.pool.Config().ConnString(), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	job, _, err := first.Enqueue(ctx, name, Submit{Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 32)
	var claimed atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			s := first
			if i%2 == 1 {
				s = second
			}
			got, err := s.Claim(ctx, name, "competing-worker")
			if err == nil {
				claimed.Add(1)
				if got.ID != job.ID {
					results <- errors.New("claimed another job")
				}
				return
			}
			if !errors.Is(err, ErrEmpty) {
				results <- err
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		t.Error(err)
	}
	if got := claimed.Load(); got != 1 {
		t.Fatalf("%d workers claimed one job", got)
	}
	stored, err := first.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Attempt != 1 || stored.State != Leased {
		t.Fatalf("unexpected persisted claim: %+v", stored)
	}
}

func TestIntegrationDatabaseUnavailable(t *testing.T) {
	s, name := integrationStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	closed, err := Open(ctx, s.pool.Config().ConnString(), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	operations := map[string]func() error{
		"ping":    func() error { return closed.Ping(ctx) },
		"migrate": func() error { return closed.Migrate(ctx) },
		"enqueue": func() error {
			_, _, err := closed.Enqueue(ctx, name, Submit{Payload: json.RawMessage(`{}`)})
			return err
		},
		"claim":   func() error { _, err := closed.Claim(ctx, name, "worker"); return err },
		"get":     func() error { _, err := closed.Get(ctx, "missing"); return err },
		"ack":     func() error { return closed.Ack(ctx, "missing", "token") },
		"fail":    func() error { return closed.Fail(ctx, "missing", "token", "failure") },
		"extend":  func() error { _, err := closed.Extend(ctx, "missing", "token", time.Second); return err },
		"recover": func() error { _, err := closed.Recover(ctx); return err },
		"stats":   func() error { _, err := closed.Stats(ctx, name); return err },
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			if err := operation(); err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrEmpty) || errors.Is(err, ErrLeaseLost) {
				t.Fatalf("database failure was hidden: %v", err)
			}
		})
	}
	stats, err := s.Stats(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (Stats{}) {
		t.Fatalf("failed operations persisted jobs: %+v", stats)
	}
}

func TestIntegrationCanceledLeaseMutationDoesNotChangeJob(t *testing.T) {
	s, name := integrationStore(t)
	ctx := context.Background()
	_, _, err := s.Enqueue(ctx, name, Submit{Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.Claim(ctx, name, "worker")
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err = s.Ack(canceled, job.ID, job.LeaseToken); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want cancellation", err)
	}
	if err = s.Ack(ctx, job.ID, job.LeaseToken); err != nil {
		t.Fatal(err)
	}
	stored, err := s.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != Completed {
		t.Fatalf("canceled mutation damaged lease: %+v", stored)
	}
}

func TestIntegrationPayloadBytesPreserved(t *testing.T) {
	s, name := integrationStore(t)
	for _, payload := range []string{
		`{"value":"\u0000"}`,
		`{"value":1e999999}`,
		`{"values":[` + strings.TrimSuffix(strings.Repeat("1e100000,", 30), ",") + `]}`,
		`{ "values": [1, 2, 3] }`,
	} {
		submitted, _, err := s.Enqueue(context.Background(), name, Submit{Payload: json.RawMessage(payload)})
		if err != nil {
			t.Fatal(err)
		}
		if string(submitted.Payload) != payload {
			t.Fatalf("payload changed on submission: got %d bytes, want %d", len(submitted.Payload), len(payload))
		}
		stored, err := s.Get(context.Background(), submitted.ID)
		if err != nil {
			t.Fatal(err)
		}
		if string(stored.Payload) != payload {
			t.Fatalf("payload changed in storage: got %d bytes, want %d", len(stored.Payload), len(payload))
		}
	}
}
