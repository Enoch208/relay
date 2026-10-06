package queue

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	cases := []struct {
		name            string
		attempt         int
		base, max, want time.Duration
	}{
		{"first", 1, time.Second, time.Minute, time.Second},
		{"second", 2, time.Second, time.Minute, 2 * time.Second},
		{"third", 3, time.Second, time.Minute, 4 * time.Second},
		{"capped", 8, time.Second, time.Minute, time.Minute},
		{"nonpower_cap", 5, time.Second, 7 * time.Second, 7 * time.Second},
		{"large_attempt", 1000, time.Second, time.Minute, time.Minute},
		{"overflow", 3, time.Duration(1 << 61), time.Duration(1<<63 - 1), time.Duration(1<<63 - 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := backoff(tc.attempt, tc.base, tc.max); got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestValidateSubmit(t *testing.T) {
	valid := Submit{Payload: json.RawMessage(`{"name":"test"}`), MaxAttempts: 3}
	cases := []struct {
		name, queue string
		change      func(*Submit)
		invalid     bool
	}{
		{"valid", "mail.send-v2", nil, false},
		{"defaults", "default", func(s *Submit) { s.MaxAttempts = 0 }, false},
		{"maximum_attempts", "default", func(s *Submit) { s.MaxAttempts = 1000 }, false},
		{"empty_queue", "", nil, true},
		{"queue_path", "../mail", nil, true},
		{"queue_space", "mail send", nil, true},
		{"queue_long", strings.Repeat("a", 129), nil, true},
		{"missing_payload", "mail", func(s *Submit) { s.Payload = nil }, true},
		{"broken_json", "mail", func(s *Submit) { s.Payload = json.RawMessage(`{`) }, true},
		{"invalid_utf8", "mail", func(s *Submit) { s.Payload = []byte{'"', 0xff, '"'} }, true},
		{"oversized_payload", "mail", func(s *Submit) { s.Payload = json.RawMessage(`"` + strings.Repeat("a", MaxPayloadBytes) + `"`) }, true},
		{"negative_attempts", "mail", func(s *Submit) { s.MaxAttempts = -1 }, true},
		{"excessive_attempts", "mail", func(s *Submit) { s.MaxAttempts = 1001 }, true},
		{"negative_delay", "mail", func(s *Submit) { s.Delay = -time.Second }, true},
		{"excessive_delay", "mail", func(s *Submit) { s.Delay = 366 * 24 * time.Hour }, true},
		{"negative_timeout", "mail", func(s *Submit) { s.Timeout = -time.Second }, true},
		{"tiny_timeout", "mail", func(s *Submit) { s.Timeout = time.Nanosecond }, true},
		{"millisecond_timeout", "mail", func(s *Submit) { s.Timeout = time.Millisecond }, false},
		{"excessive_timeout", "mail", func(s *Submit) { s.Timeout = 25 * time.Hour }, true},
		{"long_key", "mail", func(s *Submit) { s.IdempotencyKey = strings.Repeat("a", 257) }, true},
		{"null_key", "mail", func(s *Submit) { s.IdempotencyKey = "a\x00b" }, true},
		{"invalid_key_utf8", "mail", func(s *Submit) { s.IdempotencyKey = string([]byte{0xff}) }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := valid
			if tc.change != nil {
				tc.change(&req)
			}
			err := validateSubmit(tc.queue, req)
			if (err != nil) != tc.invalid {
				t.Fatalf("unexpected validation: %v", err)
			}
			if err != nil && !errors.Is(err, ErrInvalid) {
				t.Fatalf("validation error does not wrap ErrInvalid: %v", err)
			}
		})
	}
}

func TestInvalidConfig(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Config)
	}{
		{"short_lease", func(c *Config) { c.LeaseDuration = time.Nanosecond }},
		{"long_lease", func(c *Config) { c.LeaseDuration = 25 * time.Hour }},
		{"zero_window", func(c *Config) { c.IdempotencyWindow = 0 }},
		{"negative_base", func(c *Config) { c.RetryBase = -time.Second }},
		{"max_below_base", func(c *Config) { c.RetryMax = time.Millisecond }},
		{"excessive_max", func(c *Config) { c.RetryMax = 366 * 24 * time.Hour }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.change(&cfg)
			store, err := Open(context.Background(), "", cfg)
			if store != nil || !errors.Is(err, ErrInvalid) {
				t.Fatalf("got store=%v, error=%v", store, err)
			}
		})
	}
}
