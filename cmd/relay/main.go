package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Enoch208/relay/internal/api"
	"github.com/Enoch208/relay/internal/queue"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := queue.DefaultConfig()
	for _, entry := range []struct {
		name   string
		target *time.Duration
	}{{"LEASE_DURATION", &cfg.LeaseDuration}, {"IDEMPOTENCY_WINDOW", &cfg.IdempotencyWindow}, {"RETRY_BASE", &cfg.RetryBase}, {"RETRY_MAX", &cfg.RetryMax}} {
		if raw := os.Getenv(entry.name); raw != "" {
			value, err := time.ParseDuration(raw)
			if err != nil || value <= 0 {
				return fmt.Errorf("invalid %s", entry.name)
			}
			*entry.target = value
		}
	}
	recoveryInterval := time.Second
	if raw := os.Getenv("RECOVERY_INTERVAL"); raw != "" {
		var err error
		recoveryInterval, err = time.ParseDuration(raw)
		if err != nil || recoveryInterval <= 0 {
			return errors.New("invalid RECOVERY_INTERVAL")
		}
	}
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	startupCtx, startupCancel := context.WithTimeout(ctx, 30*time.Second)
	defer startupCancel()
	store, err := queue.Open(startupCtx, dbURL, cfg)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Migrate(startupCtx); err != nil {
		return err
	}
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{Addr: addr, Handler: api.New(store, log), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	recoveryCtx, endRecovery := context.WithCancel(ctx)
	defer endRecovery()
	recoveryDone := make(chan struct{})
	go func() {
		defer close(recoveryDone)
		ticker := time.NewTicker(recoveryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-recoveryCtx.Done():
				return
			case <-ticker.C:
				callCtx, cancel := context.WithTimeout(recoveryCtx, 10*time.Second)
				n, err := store.Recover(callCtx)
				cancel()
				if err != nil && recoveryCtx.Err() == nil {
					log.Error("recovery failed", "error", err)
				} else if n > 0 {
					log.Info("expired leases recovered", "jobs", n)
				}
			}
		}
	}()
	serveErr := make(chan error, 1)
	go func() {
		log.Info("server listening", "address", listener.Addr().String())
		serveErr <- server.Serve(listener)
	}()
	select {
	case err = <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		err = server.Shutdown(shutdownCtx)
		cancel()
		if err != nil {
			_ = server.Close()
		}
	}
	endRecovery()
	<-recoveryDone
	return err
}
