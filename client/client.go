package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

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
	TimeoutMS      int64           `json:"timeout_ms"`
}

type Submit struct {
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	MaxAttempts    int             `json:"max_attempts,omitempty"`
	DelayMS        int64           `json:"delay_ms,omitempty"`
	TimeoutMS      int64           `json:"timeout_ms,omitempty"`
}

type Stats struct {
	Pending    int64 `json:"pending"`
	Leased     int64 `json:"leased"`
	RetryWait  int64 `json:"retry_wait"`
	Completed  int64 `json:"completed"`
	DeadLetter int64 `json:"dead_letter"`
}

type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("relay: HTTP %d: %s", e.Status, e.Message) }

var ErrEmpty = errors.New("queue empty")

type Client struct {
	baseURL string
	http    *http.Client
}

func New(baseURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &Client{strings.TrimRight(baseURL, "/"), httpClient}
}

func (c *Client) request(ctx context.Context, method, path string, body, out any) (int, error) {
	var reader io.Reader
	if body != nil {
		var buffer bytes.Buffer
		encoder := json.NewEncoder(&buffer)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(body); err != nil {
			return 0, err
		}
		reader = &buffer
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var payload struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&payload)
		return res.StatusCode, &Error{res.StatusCode, payload.Error}
	}
	if out != nil && res.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(io.LimitReader(res.Body, 8<<20)).Decode(out); err != nil {
			return res.StatusCode, err
		}
	}
	return res.StatusCode, nil
}

func (c *Client) Enqueue(ctx context.Context, name string, submit Submit) (Job, bool, error) {
	var job Job
	status, err := c.request(ctx, "POST", "/queues/"+url.PathEscape(name)+"/jobs", submit, &job)
	return job, status == http.StatusCreated, err
}

func (c *Client) Claim(ctx context.Context, name, workerID string) (Job, error) {
	var job Job
	status, err := c.request(ctx, "POST", "/queues/"+url.PathEscape(name)+"/claim", map[string]string{"worker_id": workerID}, &job)
	if err == nil && status == 204 {
		err = ErrEmpty
	}
	return job, err
}

func (c *Client) Ack(ctx context.Context, id, token string) error {
	_, err := c.request(ctx, "POST", "/jobs/"+url.PathEscape(id)+"/ack", map[string]string{"lease_token": token}, nil)
	return err
}

func (c *Client) Fail(ctx context.Context, id, token, reason string) error {
	_, err := c.request(ctx, "POST", "/jobs/"+url.PathEscape(id)+"/fail", map[string]string{"lease_token": token, "reason": reason}, nil)
	return err
}

func (c *Client) Extend(ctx context.Context, id, token string, extension time.Duration) (Job, error) {
	var job Job
	_, err := c.request(ctx, "POST", "/jobs/"+url.PathEscape(id)+"/extend", struct {
		Token       string `json:"lease_token"`
		ExtensionMS int64  `json:"extension_ms"`
	}{token, extension.Milliseconds()}, &job)
	return job, err
}

func (c *Client) Get(ctx context.Context, id string) (Job, error) {
	var job Job
	_, err := c.request(ctx, "GET", "/jobs/"+url.PathEscape(id), nil, &job)
	return job, err
}

func (c *Client) Stats(ctx context.Context, name string) (Stats, error) {
	var stats Stats
	_, err := c.request(ctx, "GET", "/queues/"+url.PathEscape(name)+"/stats", nil, &stats)
	return stats, err
}
