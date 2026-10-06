package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Enoch208/relay/internal/queue"
)

func database(t *testing.T) string {
	t.Helper()
	url := os.Getenv("RELAY_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("RELAY_TEST_DATABASE_URL is not set")
	}
	return url
}

func openStore(t *testing.T) (*queue.Store, queue.Config) {
	t.Helper()
	cfg := queue.DefaultConfig()
	cfg.LeaseDuration = 120 * time.Millisecond
	cfg.RetryBase = 10 * time.Millisecond
	cfg.RetryMax = 40 * time.Millisecond
	cfg.IdempotencyWindow = time.Second
	s, err := queue.Open(context.Background(), database(t), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s, cfg
}

func name(t *testing.T) string {
	return fmt.Sprintf("%s-%d", strings.ReplaceAll(t.Name(), "/", "-"), time.Now().UnixNano())
}

func submit(t *testing.T, s *queue.Store, q string, max int) queue.Job {
	t.Helper()
	j, created, err := s.Enqueue(context.Background(), q, queue.Submit{Payload: json.RawMessage(`{"work":"test"}`), MaxAttempts: max})
	if err != nil || !created {
		t.Fatalf("enqueue: created=%v err=%v", created, err)
	}
	return j
}

func claim(t *testing.T, s *queue.Store, q, w string) queue.Job {
	t.Helper()
	j, err := s.Claim(context.Background(), q, w)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true within five seconds")
}

func recoverClaim(t *testing.T, s *queue.Store, q string) queue.Job {
	t.Helper()
	var got queue.Job
	eventually(t, func() bool {
		if _, err := s.Recover(context.Background()); err != nil {
			t.Fatal(err)
		}
		j, err := s.Claim(context.Background(), q, "replacement")
		if errors.Is(err, queue.ErrEmpty) {
			return false
		}
		if err != nil {
			t.Fatal(err)
		}
		got = j
		return true
	})
	return got
}

func TestConcurrentIdempotency(t *testing.T) {
	s, _ := openStore(t)
	q := name(t)
	const workers = 24
	start := make(chan struct{})
	ids := make(chan string, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			j, _, err := s.Enqueue(context.Background(), q, queue.Submit{Payload: json.RawMessage(`{"n":1}`), IdempotencyKey: "same", MaxAttempts: 3})
			if err != nil {
				errs <- err
				return
			}
			ids <- j.ID
		}()
	}
	close(start)
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	var first string
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatalf("duplicate jobs %s and %s", first, id)
		}
	}
	stats, err := s.Stats(context.Background(), q)
	if err != nil || stats.Pending != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}

func TestIdempotencyWindowAndQueueScope(t *testing.T) {
	s, cfg := openStore(t)
	q := name(t)
	ctx := context.Background()
	req := queue.Submit{Payload: json.RawMessage(`{}`), IdempotencyKey: "key", MaxAttempts: 2}
	first, _, err := s.Enqueue(ctx, q, req)
	if err != nil {
		t.Fatal(err)
	}
	other, created, err := s.Enqueue(ctx, q+"-other", req)
	if err != nil || !created || first.ID == other.ID {
		t.Fatalf("queue scope: %+v %v %v", other, created, err)
	}
	again, created, err := s.Enqueue(ctx, q, req)
	if err != nil || created || first.ID != again.ID {
		t.Fatalf("duplicate: %+v %v %v", again, created, err)
	}
	time.Sleep(cfg.IdempotencyWindow + 20*time.Millisecond)
	fresh, created, err := s.Enqueue(ctx, q, req)
	if err != nil || !created || first.ID == fresh.ID {
		t.Fatalf("expired key: %+v %v %v", fresh, created, err)
	}
}

func TestTwoWorkersOneLease(t *testing.T) {
	s, _ := openStore(t)
	q := name(t)
	submit(t, s, q, 3)
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, w := range []string{"a", "b"} {
		go func(worker string) { <-start; _, err := s.Claim(context.Background(), q, worker); results <- err }(w)
	}
	close(start)
	wins, empty := 0, 0
	for i := 0; i < 2; i++ {
		err := <-results
		if err == nil {
			wins++
		} else if errors.Is(err, queue.ErrEmpty) {
			empty++
		} else {
			t.Fatal(err)
		}
	}
	if wins != 1 || empty != 1 {
		t.Fatalf("wins=%d empty=%d", wins, empty)
	}
}

func TestExpiredLeaseRejectsEveryMutation(t *testing.T) {
	s, _ := openStore(t)
	q := name(t)
	submit(t, s, q, 3)
	old := claim(t, s, q, "a")
	ctx := context.Background()
	time.Sleep(time.Until(*old.LeaseExpiresAt) + 10*time.Millisecond)
	for label, err := range map[string]error{"ack": s.Ack(ctx, old.ID, old.LeaseToken), "fail": s.Fail(ctx, old.ID, old.LeaseToken, "late"), "extend": func() error { _, e := s.Extend(ctx, old.ID, old.LeaseToken, time.Second); return e }()} {
		if !errors.Is(err, queue.ErrLeaseLost) {
			t.Errorf("%s: %v", label, err)
		}
	}
	fresh := recoverClaim(t, s, q)
	if fresh.ID != old.ID || fresh.LeaseToken == old.LeaseToken || fresh.Attempt != 2 {
		t.Fatalf("reclaim: old=%+v new=%+v", old, fresh)
	}
	if err := s.Ack(ctx, old.ID, old.LeaseToken); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("stale ack: %v", err)
	}
	if err := s.Ack(ctx, fresh.ID, fresh.LeaseToken); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, q, "c"); !errors.Is(err, queue.ErrEmpty) {
		t.Fatalf("completed job reclaimed: %v", err)
	}
}

func TestRetryExhaustion(t *testing.T) {
	for _, mode := range []string{"failure", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := openStore(t)
			q := name(t)
			original := submit(t, s, q, 3)
			ctx := context.Background()
			for attempt := 1; attempt <= 3; attempt++ {
				j := recoverClaim(t, s, q)
				if j.Attempt != attempt {
					t.Fatalf("attempt=%d want=%d", j.Attempt, attempt)
				}
				if mode == "failure" {
					if err := s.Fail(ctx, j.ID, j.LeaseToken, "injected"); err != nil {
						t.Fatal(err)
					}
				} else {
					time.Sleep(time.Until(*j.LeaseExpiresAt) + 10*time.Millisecond)
					if _, err := s.Recover(ctx); err != nil {
						t.Fatal(err)
					}
				}
			}
			got, err := s.Get(ctx, original.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != "dead_letter" || got.Attempt != 3 {
				t.Fatalf("exhaustion: %+v", got)
			}
			if _, err := s.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Claim(ctx, q, "extra"); !errors.Is(err, queue.ErrEmpty) {
				t.Fatalf("dead letter reclaimed: %v", err)
			}
		})
	}
}

func TestDelayedJobAndLeaseRenewal(t *testing.T) {
	s, _ := openStore(t)
	q := name(t)
	ctx := context.Background()
	j, _, err := s.Enqueue(ctx, q, queue.Submit{Payload: json.RawMessage(`{}`), MaxAttempts: 3, Delay: 80 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, q, "early"); !errors.Is(err, queue.ErrEmpty) {
		t.Fatalf("early claim: %v", err)
	}
	leased := recoverClaim(t, s, q)
	renewed, err := s.Extend(ctx, j.ID, leased.LeaseToken, 400*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if !renewed.LeaseExpiresAt.After(*leased.LeaseExpiresAt) {
		t.Fatal("renewal did not extend lease")
	}
	time.Sleep(time.Until(*leased.LeaseExpiresAt) + 10*time.Millisecond)
	if _, err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, q, "steal"); !errors.Is(err, queue.ErrEmpty) {
		t.Fatalf("renewed lease stolen: %v", err)
	}
	if err := s.Ack(ctx, j.ID, leased.LeaseToken); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentAcknowledgements(t *testing.T) {
	s, _ := openStore(t)
	q := name(t)
	submit(t, s, q, 2)
	j := claim(t, s, q, "worker")
	start := make(chan struct{})
	results := make(chan error, 16)
	for i := 0; i < 16; i++ {
		go func() { <-start; results <- s.Ack(context.Background(), j.ID, j.LeaseToken) }()
	}
	close(start)
	wins := 0
	for i := 0; i < 16; i++ {
		err := <-results
		if err == nil {
			wins++
		} else if !errors.Is(err, queue.ErrLeaseLost) {
			t.Error(err)
		}
	}
	if wins != 1 {
		t.Fatalf("successful acks=%d", wins)
	}
}

func TestRestartRetainsDurableState(t *testing.T) {
	s, cfg := openStore(t)
	q := name(t)
	ctx := context.Background()
	done := submit(t, s, q, 2)
	lease := claim(t, s, q, "done")
	if err := s.Ack(ctx, lease.ID, lease.LeaseToken); err != nil {
		t.Fatal(err)
	}
	pending := submit(t, s, q, 2)
	s.Close()
	restarted, err := queue.Open(ctx, database(t), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if err := restarted.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	persisted, err := restarted.Get(ctx, done.ID)
	if err != nil || persisted.State != "completed" {
		t.Fatalf("completed: %+v %v", persisted, err)
	}
	got := claim(t, restarted, q, "restarted")
	if got.ID != pending.ID {
		t.Fatalf("pending job lost: %+v", got)
	}
}

func TestKilledWorkerRecovery(t *testing.T) {
	s, _ := openStore(t)
	q := name(t)
	j := submit(t, s, q, 3)
	cmd := exec.Command(os.Args[0], "-test.run=^TestWorkerProcess$")
	cmd.Env = append(os.Environ(), "RELAY_CHILD_QUEUE="+q)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	scan := bufio.NewScanner(out)
	ready := make(chan bool, 1)
	go func() { ready <- scan.Scan() }()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("worker exited before claiming")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not claim")
	}
	var abandoned queue.Job
	if err := json.Unmarshal(scan.Bytes(), &abandoned); err != nil {
		t.Fatal(err)
	}
	if abandoned.ID != j.ID {
		t.Fatal("worker claimed wrong job")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	got := recoverClaim(t, s, q)
	if got.ID != j.ID || got.Attempt != 2 {
		t.Fatalf("lost abandoned job: %+v", got)
	}
	if err := s.Ack(context.Background(), j.ID, abandoned.LeaseToken); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("stale worker ack: %v", err)
	}
	if err := s.Ack(context.Background(), got.ID, got.LeaseToken); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerProcess(t *testing.T) {
	q := os.Getenv("RELAY_CHILD_QUEUE")
	if q == "" {
		t.Skip("subprocess helper")
	}
	s, _ := openStore(t)
	j := claim(t, s, q, "child")
	if err := json.NewEncoder(os.Stdout).Encode(j); err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Hour)
	}
}

func TestConcurrentDrain(t *testing.T) {
	s, _ := openStore(t)
	q := name(t)
	ctx := context.Background()
	const count = 100
	for i := 0; i < count; i++ {
		_, _, err := s.Enqueue(ctx, q, queue.Submit{Payload: json.RawMessage(`{}`), MaxAttempts: 3, Timeout: 10 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
	}
	ids := make(chan string, count)
	errs := make(chan error, 12)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				j, err := s.Claim(ctx, q, fmt.Sprint(w))
				if errors.Is(err, queue.ErrEmpty) {
					return
				}
				if err != nil {
					errs <- err
					return
				}
				if err := s.Ack(ctx, j.ID, j.LeaseToken); err != nil {
					errs <- err
					return
				}
				ids <- j.ID
			}
		}(i)
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Errorf("duplicate lease: %s", id)
		}
		seen[id] = true
	}
	if len(seen) != count {
		t.Fatalf("completed=%d want=%d", len(seen), count)
	}
	stats, err := s.Stats(ctx, q)
	if err != nil || stats.Completed != count || stats.Leased != 0 || stats.Pending != 0 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
}
