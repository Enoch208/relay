package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrNotFound  = errors.New("job not found")
	ErrEmpty     = errors.New("queue is empty")
	ErrLeaseLost = errors.New("lease is no longer valid")
	ErrInvalid   = errors.New("invalid request")
	queuePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)
)

const MaxPayloadBytes = 1 << 20

const (
	Pending    = "pending"
	Leased     = "leased"
	RetryWait  = "retry_wait"
	Completed  = "completed"
	DeadLetter = "dead_letter"
)

type Config struct {
	LeaseDuration     time.Duration
	IdempotencyWindow time.Duration
	RetryBase         time.Duration
	RetryMax          time.Duration
}

func DefaultConfig() Config {
	return Config{LeaseDuration: 30 * time.Second, IdempotencyWindow: 24 * time.Hour, RetryBase: time.Second, RetryMax: time.Minute}
}

type Submit struct {
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	MaxAttempts    int             `json:"max_attempts,omitempty"`
	Delay          time.Duration   `json:"-"`
	Timeout        time.Duration   `json:"-"`
}

type Job struct {
	ID             string          `json:"id"`
	Queue          string          `json:"queue"`
	State          string          `json:"state"`
	Payload        json.RawMessage `json:"payload"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	LeaseToken     string          `json:"lease_token,omitempty"`
	WorkerID       string          `json:"worker_id,omitempty"`
	LeaseExpiresAt *time.Time      `json:"lease_expires_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	AvailableAt    time.Time       `json:"available_at"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	TimeoutMillis  int64           `json:"timeout_ms"`
}

type Stats struct {
	Pending    int64 `json:"pending"`
	Leased     int64 `json:"leased"`
	RetryWait  int64 `json:"retry_wait"`
	Completed  int64 `json:"completed"`
	DeadLetter int64 `json:"dead_letter"`
}

func validateQueue(name string) error {
	if !queuePattern.MatchString(name) {
		return fmt.Errorf("%w: queue must be 1–128 letters, digits, dots, underscores or hyphens and start with a letter or digit", ErrInvalid)
	}
	return nil
}

func validateSubmit(name string, req Submit) error {
	if err := validateQueue(name); err != nil {
		return err
	}
	if len(req.Payload) == 0 || len(req.Payload) > MaxPayloadBytes || !json.Valid(req.Payload) || !utf8.Valid(req.Payload) {
		return fmt.Errorf("%w: payload must be valid JSON up to 1 MiB", ErrInvalid)
	}
	if len(req.IdempotencyKey) > 256 || !utf8.ValidString(req.IdempotencyKey) || strings.IndexByte(req.IdempotencyKey, 0) >= 0 {
		return fmt.Errorf("%w: idempotency key must be valid UTF-8 up to 256 bytes", ErrInvalid)
	}
	if req.MaxAttempts < 0 || req.MaxAttempts > 1000 {
		return fmt.Errorf("%w: max_attempts must be between 1 and 1000", ErrInvalid)
	}
	if req.Delay < 0 || req.Delay > 365*24*time.Hour {
		return fmt.Errorf("%w: delay must be between zero and 365 days", ErrInvalid)
	}
	if req.Timeout < 0 || req.Timeout > 24*time.Hour || (req.Timeout > 0 && req.Timeout < time.Millisecond) {
		return fmt.Errorf("%w: timeout must be between 1 millisecond and 24 hours", ErrInvalid)
	}
	return nil
}

func backoff(attempt int, base, maximum time.Duration) time.Duration {
	delay := base
	for i := 1; i < attempt && delay < maximum; i++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}
