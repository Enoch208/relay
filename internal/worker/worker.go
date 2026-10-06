package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Enoch208/relay/client"
)

type Queue interface {
	Claim(context.Context, string, string) (client.Job, error)
	Ack(context.Context, string, string) error
	Fail(context.Context, string, string, string) error
	Extend(context.Context, string, string, time.Duration) (client.Job, error)
}

type Handler func(context.Context, client.Job) error
type Config struct {
	Queue, ID    string
	Concurrency  int
	PollInterval time.Duration
}

func Run(ctx context.Context, q Queue, cfg Config, handler Handler, log *slog.Logger) error {
	if cfg.Concurrency < 1 || cfg.Concurrency > 1000 || cfg.PollInterval <= 0 || cfg.Queue == "" || cfg.ID == "" || handler == nil {
		return errors.New("invalid worker configuration")
	}
	if log == nil {
		log = slog.Default()
	}
	var wg sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			id := fmt.Sprintf("%s-%d", cfg.ID, slot)
			for ctx.Err() == nil {
				job, err := q.Claim(ctx, cfg.Queue, id)
				if err != nil {
					if !errors.Is(err, client.ErrEmpty) && ctx.Err() == nil {
						log.Error("claim failed", "worker_id", id, "error", err)
					}
					if !pause(ctx, cfg.PollInterval) {
						return
					}
					continue
				}
				process(ctx, q, job, handler, log)
			}
		}(i)
	}
	wg.Wait()
	return nil
}

func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func process(ctx context.Context, q Queue, job client.Job, handler Handler, log *slog.Logger) {
	if job.LeaseExpiresAt == nil {
		log.Error("claim missing expiry", "job_id", job.ID)
		return
	}
	remaining := time.Until(*job.LeaseExpiresAt)
	if remaining <= 0 {
		return
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	renewalCtx, stopRenewal := context.WithCancel(workCtx)
	renewed := make(chan error, 1)
	interval := remaining / 3
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	go func() {
		deadline := *job.LeaseExpiresAt
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-renewalCtx.Done():
				renewed <- nil
				return
			case <-ticker.C:
			}
			requestCtx, endRequest := context.WithDeadline(renewalCtx, deadline)
			updated, err := q.Extend(requestCtx, job.ID, job.LeaseToken, interval)
			endRequest()
			if err != nil {
				if renewalCtx.Err() != nil {
					renewed <- nil
					return
				}
				cancel()
				renewed <- err
				return
			}
			if updated.LeaseExpiresAt == nil {
				cancel()
				renewed <- errors.New("renewal missing expiry")
				return
			}
			deadline = *updated.LeaseExpiresAt
		}
	}()
	err := invoke(workCtx, handler, job)
	stopRenewal()
	renewalErr := <-renewed
	if ctx.Err() != nil {
		return
	}
	if renewalErr != nil {
		log.Warn("lease renewal failed", "job_id", job.ID, "error", renewalErr)
		return
	}
	if err != nil {
		reason := failureReason(err)
		err = q.Fail(ctx, job.ID, job.LeaseToken, reason)
	} else {
		err = q.Ack(ctx, job.ID, job.LeaseToken)
	}
	if err != nil {
		log.Warn("job settlement failed", "job_id", job.ID, "error", err)
	}
}

func invoke(ctx context.Context, handler Handler, job client.Job) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()
	return handler(ctx, job)
}

func failureReason(err error) string {
	reason := strings.ReplaceAll(strings.ToValidUTF8(err.Error(), "�"), "\x00", "�")
	if len(reason) > 4096 {
		reason = reason[:4096]
		for !utf8.ValidString(reason) {
			reason = reason[:len(reason)-1]
		}
	}
	return reason
}
