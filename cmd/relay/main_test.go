package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/Enoch208/relay/client"
	"github.com/Enoch208/relay/internal/queue"
)

func TestServerProcess(t *testing.T) {
	if os.Getenv("RELAY_TEST_SERVER_CHILD") == "" {
		t.Skip("subprocess helper")
	}
	if err := run(slog.New(slog.NewJSONHandler(os.Stdout, nil))); err != nil {
		t.Fatal(err)
	}
}

func startServer(t *testing.T, database string) (*client.Client, func()) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestServerProcess$")
	cmd.Env = append(os.Environ(), "RELAY_TEST_SERVER_CHILD=1", "DATABASE_URL="+database, "ADDR=127.0.0.1:0", "RECOVERY_INTERVAL=10ms", "RETRY_BASE=10ms", "RETRY_MAX=20ms")
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	scan := bufio.NewScanner(out)
	ready := make(chan string, 1)
	go func() {
		if scan.Scan() {
			ready <- scan.Text()
		} else {
			ready <- ""
		}
	}()
	var line string
	select {
	case line = <-ready:
	case <-time.After(15 * time.Second):
		t.Fatal("server did not start")
	}
	var event struct {
		Address string `json:"address"`
	}
	if err = json.Unmarshal([]byte(line), &event); err != nil || event.Address == "" {
		t.Fatalf("startup log: %q: %v", line, err)
	}
	c := client.New("http://"+event.Address, &http.Client{Timeout: 2 * time.Second})
	stop := func() {
		t.Helper()
		if stopped {
			return
		}
		stopped = true
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("unclean SIGTERM shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Fatal("SIGTERM shutdown timed out")
		}
	}
	return c, stop
}

func TestGracefulServerShutdownPreservesJobs(t *testing.T) {
	database := os.Getenv("RELAY_TEST_DATABASE_URL")
	if database == "" {
		t.Skip("RELAY_TEST_DATABASE_URL is not set")
	}
	ctx := context.Background()
	c, stop := startServer(t, database)
	name := fmt.Sprintf("server-shutdown-%d", time.Now().UnixNano())
	pending, _, err := c.Enqueue(ctx, name+"-pending", client.Submit{Payload: json.RawMessage(`{"pending":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	submitted, _, err := c.Enqueue(ctx, name+"-leased", client.Submit{Payload: json.RawMessage(`{}`), TimeoutMS: 10000})
	if err != nil {
		t.Fatal(err)
	}
	leased, err := c.Claim(ctx, name+"-leased", "worker")
	if err != nil {
		t.Fatal(err)
	}
	stop()
	store, err := queue.Open(ctx, database, queue.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, tc := range []struct{ id, state string }{{pending.ID, "pending"}, {submitted.ID, "leased"}} {
		job, err := store.Get(ctx, tc.id)
		if err != nil || job.State != tc.state {
			t.Fatalf("job after shutdown: %+v %v", job, err)
		}
	}
	if err = store.Ack(ctx, leased.ID, leased.LeaseToken); err != nil {
		t.Fatalf("persisted lease no longer valid: %v", err)
	}
}

func TestServerRecoveryLoop(t *testing.T) {
	database := os.Getenv("RELAY_TEST_DATABASE_URL")
	if database == "" {
		t.Skip("RELAY_TEST_DATABASE_URL is not set")
	}
	c, stop := startServer(t, database)
	defer stop()
	ctx := context.Background()
	name := fmt.Sprintf("server-recovery-%d", time.Now().UnixNano())
	job, _, err := c.Enqueue(ctx, name, client.Submit{Payload: json.RawMessage(`{}`), TimeoutMS: 150, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	old, err := c.Claim(ctx, name, "abandoned")
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		inspected, err := c.Get(ctx, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if inspected.State == "retry_wait" {
			var remote *client.Error
			if err = c.Ack(ctx, old.ID, old.LeaseToken); !errors.As(err, &remote) || remote.Status != 409 {
				t.Fatalf("stale ACK: %v", err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("recovery loop did not release expired job")
}
