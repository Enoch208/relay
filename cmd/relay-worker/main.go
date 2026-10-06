package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Enoch208/relay/client"
	"github.com/Enoch208/relay/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func run(log *slog.Logger) error {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "worker"
	}
	concurrency, err := strconv.Atoi(env("CONCURRENCY", "4"))
	if err != nil {
		return err
	}
	poll, err := time.ParseDuration(env("POLL_INTERVAL", "500ms"))
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := worker.Config{Queue: env("QUEUE", "default"), ID: env("WORKER_ID", hostname), Concurrency: concurrency, PollInterval: poll}
	return worker.Run(ctx, client.New(env("RELAY_URL", "http://localhost:8080"), nil), cfg, handle, log)
}

func handle(ctx context.Context, job client.Job) error {
	var payload struct {
		SleepMS          int64 `json:"sleep_ms"`
		FailUntilAttempt int   `json:"fail_until_attempt"`
	}
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return err
	}
	if payload.SleepMS < 0 || payload.SleepMS > 86400000 {
		return errors.New("sleep_ms must be between 0 and 86400000")
	}
	timer := time.NewTimer(time.Duration(payload.SleepMS) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	if job.Attempt <= payload.FailUntilAttempt {
		return fmt.Errorf("requested failure on attempt %d", job.Attempt)
	}
	return nil
}
