package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Enoch208/relay/client"
)

type fakeQueue struct {
	claimed    atomic.Int64
	acks       atomic.Int64
	fails      atomic.Int64
	extensions atomic.Int64
	claimLimit int64
	renewError error
	lease      time.Duration
	renewed    chan struct{}
	mu         sync.Mutex
	expiry     time.Time
}

func (f *fakeQueue) Claim(ctx context.Context, _, _ string) (client.Job, error) {
	if f.claimed.Add(1) > f.claimLimit {
		return client.Job{}, client.ErrEmpty
	}
	expiry := time.Now().Add(f.lease)
	return client.Job{ID: "id", LeaseToken: "token", LeaseExpiresAt: &expiry}, nil
}
func (f *fakeQueue) Ack(context.Context, string, string) error          { f.acks.Add(1); return nil }
func (f *fakeQueue) Fail(context.Context, string, string, string) error { f.fails.Add(1); return nil }
func (f *fakeQueue) Extend(_ context.Context, _, _ string, d time.Duration) (client.Job, error) {
	f.extensions.Add(1)
	if f.renewError != nil {
		return client.Job{}, f.renewError
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.expiry = f.expiry.Add(d)
	if f.renewed != nil {
		select {
		case f.renewed <- struct{}{}:
		default:
		}
	}
	expiry := f.expiry
	return client.Job{LeaseExpiresAt: &expiry}, nil
}
func logger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func leasedJob(d time.Duration) client.Job {
	expiry := time.Now().Add(d)
	return client.Job{ID: "id", LeaseToken: "token", LeaseExpiresAt: &expiry}
}
func TestProcessSettles(t *testing.T) {
	for _, tc := range []struct {
		name        string
		handler     Handler
		acks, fails int64
	}{{"success", func(context.Context, client.Job) error { return nil }, 1, 0}, {"error", func(context.Context, client.Job) error { return errors.New("failed") }, 0, 1}, {"panic", func(context.Context, client.Job) error { panic("oops") }, 0, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			q := &fakeQueue{}
			process(context.Background(), q, leasedJob(time.Minute), tc.handler, logger())
			if q.acks.Load() != tc.acks || q.fails.Load() != tc.fails {
				t.Fatalf("acks=%d fails=%d", q.acks.Load(), q.fails.Load())
			}
		})
	}
}
func TestRenewalWhileHandlerRuns(t *testing.T) {
	q := &fakeQueue{renewed: make(chan struct{}, 1), expiry: time.Now().Add(150 * time.Millisecond)}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	process(ctx, q, leasedJob(150*time.Millisecond), func(ctx context.Context, _ client.Job) error {
		select {
		case <-q.renewed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, logger())
	if q.extensions.Load() == 0 || q.acks.Load() != 1 {
		t.Fatalf("extensions=%d acks=%d", q.extensions.Load(), q.acks.Load())
	}
}
func TestRenewalFailureCancelsAndNeverSettles(t *testing.T) {
	q := &fakeQueue{renewError: errors.New("lease lost")}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	process(ctx, q, leasedJob(150*time.Millisecond), func(ctx context.Context, _ client.Job) error { <-ctx.Done(); return ctx.Err() }, logger())
	if q.extensions.Load() == 0 || q.acks.Load() != 0 || q.fails.Load() != 0 {
		t.Fatalf("unexpected settlement: %+v", q)
	}
}
func TestShutdownLeavesActiveLeasesForRecovery(t *testing.T) {
	q := &fakeQueue{claimLimit: 4, lease: time.Minute}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 4)
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, q, Config{Queue: "test", ID: "worker", Concurrency: 4, PollInterval: time.Millisecond}, func(ctx context.Context, _ client.Job) error { started <- struct{}{}; <-ctx.Done(); return ctx.Err() }, logger())
	}()
	for i := 0; i < 4; i++ {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			cancel()
			t.Fatal("workers did not run concurrently")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown blocked")
	}
	if q.acks.Load() != 0 || q.fails.Load() != 0 {
		t.Fatal("canceled handlers settled jobs")
	}
}
func TestInvalidConfiguration(t *testing.T) {
	if Run(context.Background(), &fakeQueue{}, Config{}, nil, logger()) == nil {
		t.Fatal("invalid configuration accepted")
	}
}

func TestFailureReasonPreservesUTF8(t *testing.T) {
	for _, message := range []string{strings.Repeat("界", 2000), "bad\x00reason", "invalid\xffutf8"} {
		reason := failureReason(errors.New(message))
		if !utf8.ValidString(reason) || len(reason) > 4096 || strings.ContainsRune(reason, 0) {
			t.Fatalf("invalid reason: %q", reason)
		}
	}
}
