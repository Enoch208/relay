package integration

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/Enoch208/relay/client"
	"github.com/Enoch208/relay/internal/api"
)

func startAPIProcess(t *testing.T) (*client.Client, func()) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestAPIProcess$")
	cmd.Env = append(os.Environ(), "RELAY_CHILD_API=1")
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}
	t.Cleanup(stop)
	scan := bufio.NewScanner(out)
	ready := make(chan bool, 1)
	go func() { ready <- scan.Scan() }()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("API process exited before listening")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("API did not start")
	}
	return client.New(scan.Text(), nil), stop
}

func TestAPIProcess(t *testing.T) {
	if os.Getenv("RELAY_CHILD_API") == "" {
		t.Skip("subprocess helper")
	}
	s, _ := openStore(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			_, _ = s.Recover(context.Background())
		}
	}()
	fmt.Println("http://" + listener.Addr().String())
	err = http.Serve(listener, api.New(s, slog.New(slog.NewTextHandler(io.Discard, nil))))
	if err != nil {
		t.Fatal(err)
	}
}

func TestAPIProcessRestart(t *testing.T) {
	_ = database(t)
	ctx := context.Background()
	q := name(t)
	first, kill := startAPIProcess(t)
	j, created, err := first.Enqueue(ctx, q, client.Submit{Payload: json.RawMessage(`{"durable":true}`), MaxAttempts: 3, TimeoutMS: 300})
	if err != nil || !created {
		t.Fatalf("enqueue: %v %v", created, err)
	}
	old, err := first.Claim(ctx, q, "original")
	if err != nil {
		t.Fatal(err)
	}
	kill()
	second, _ := startAPIProcess(t)
	inspected, err := second.Get(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if inspected.LeaseToken != "" {
		t.Fatal("inspection revealed lease token")
	}
	var recovered client.Job
	eventually(t, func() bool {
		job, e := second.Claim(ctx, q, "replacement")
		if errors.Is(e, client.ErrEmpty) {
			return false
		}
		if e != nil {
			t.Fatal(e)
		}
		recovered = job
		return true
	})
	if recovered.ID != j.ID || recovered.Attempt != 2 || recovered.LeaseToken == old.LeaseToken {
		t.Fatalf("recovery: %+v", recovered)
	}
	err = second.Ack(ctx, old.ID, old.LeaseToken)
	var status *client.Error
	if !errors.As(err, &status) || status.Status != 409 {
		t.Fatalf("stale ACK: %v", err)
	}
	if err := second.Ack(ctx, recovered.ID, recovered.LeaseToken); err != nil {
		t.Fatal(err)
	}
	final, err := second.Get(ctx, j.ID)
	if err != nil || final.State != "completed" {
		t.Fatalf("final state: %+v %v", final, err)
	}
}
