package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Enoch208/relay/client"
	"github.com/Enoch208/relay/internal/queue"
)

type fakeStore struct {
	job     queue.Job
	err     error
	created bool
	submit  queue.Submit
	worker  string
	token   string
}

func (f *fakeStore) Enqueue(_ context.Context, _ string, s queue.Submit) (queue.Job, bool, error) {
	f.submit = s
	return f.job, f.created, f.err
}
func (f *fakeStore) Claim(_ context.Context, _, worker string) (queue.Job, error) {
	f.worker = worker
	return f.job, f.err
}
func (f *fakeStore) Ack(_ context.Context, _, token string) error     { f.token = token; return f.err }
func (f *fakeStore) Fail(_ context.Context, _, token, _ string) error { f.token = token; return f.err }
func (f *fakeStore) Extend(_ context.Context, _, token string, _ time.Duration) (queue.Job, error) {
	f.token = token
	return f.job, f.err
}
func (f *fakeStore) Get(context.Context, string) (queue.Job, error) { return f.job, f.err }
func (f *fakeStore) Stats(context.Context, string) (queue.Stats, error) {
	return queue.Stats{Pending: 3, RetryWait: 2, Leased: 1}, f.err
}
func (f *fakeStore) Ping(context.Context) error { return f.err }
func handler(f *fakeStore) *Server              { return New(f, slog.New(slog.NewTextHandler(io.Discard, nil))) }
func TestMalformedRequests(t *testing.T) {
	for _, body := range []string{"", `{`, `null`, `{"payload":1,"unknown":1}`, `{"payload":1} {"payload":2}`, `{"payload":1,"delay_ms":-1}`, `{"payload":1,"timeout_ms":9223372036854775807}`, `{"payload":"` + strings.Repeat("x", 1<<20) + `"}`} {
		t.Run(body[:min(len(body), 45)], func(t *testing.T) {
			w := httptest.NewRecorder()
			handler(&fakeStore{}).ServeHTTP(w, httptest.NewRequest("POST", "/queues/test/jobs", strings.NewReader(body)))
			if w.Code != 400 {
				t.Fatalf("status %d", w.Code)
			}
		})
	}
}
func TestErrors(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want int
	}{{queue.ErrNotFound, 404}, {queue.ErrLeaseLost, 409}, {queue.ErrInvalid, 400}, {errors.New("password=secret"), 500}, {context.DeadlineExceeded, 503}} {
		t.Run(tc.err.Error(), func(t *testing.T) {
			w := httptest.NewRecorder()
			handler(&fakeStore{err: tc.err}).ServeHTTP(w, httptest.NewRequest("GET", "/jobs/id", nil))
			if w.Code != tc.want {
				t.Fatalf("got %d want %d", w.Code, tc.want)
			}
			if strings.Contains(w.Body.String(), "secret") {
				t.Fatal("internal error leaked")
			}
		})
	}
}
func TestLeaseTokensOnlyReturnedToOwner(t *testing.T) {
	f := &fakeStore{job: queue.Job{ID: "id", LeaseToken: "private-token", Payload: json.RawMessage(`{}`)}}
	for _, tc := range []struct {
		method, path, body string
		wantToken          bool
	}{{"GET", "/jobs/id", "", false}, {"POST", "/queues/test/jobs", `{"payload":{}}`, false}, {"POST", "/queues/test/claim", `{"worker_id":"worker"}`, true}, {"POST", "/jobs/id/extend", `{"lease_token":"private-token","extension_ms":1000}`, true}} {
		w := httptest.NewRecorder()
		handler(f).ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body)))
		if w.Code != 200 {
			t.Fatalf("%s: status %d", tc.path, w.Code)
		}
		if strings.Contains(w.Body.String(), "private-token") != tc.wantToken {
			t.Fatalf("%s: wrong token visibility: %s", tc.path, w.Body.String())
		}
	}
}
func TestClientRoundTrip(t *testing.T) {
	expiry := time.Now().Add(time.Minute)
	f := &fakeStore{created: true, job: queue.Job{ID: "id", Queue: "test", State: "leased", LeaseToken: "token", LeaseExpiresAt: &expiry, Payload: json.RawMessage(`{"message":"hello"}`)}}
	server := httptest.NewServer(handler(f))
	defer server.Close()
	c := client.New(server.URL, server.Client())
	ctx := context.Background()
	job, created, err := c.Enqueue(ctx, "test", client.Submit{Payload: json.RawMessage(`{"message":"hello"}`), DelayMS: 250, TimeoutMS: 2000, MaxAttempts: 3})
	if err != nil || !created || job.ID != "id" || job.LeaseToken != "" {
		t.Fatalf("enqueue: %+v %t %v", job, created, err)
	}
	if f.submit.Delay != 250*time.Millisecond || f.submit.Timeout != 2*time.Second {
		t.Fatalf("duration conversion: %+v", f.submit)
	}
	job, err = c.Claim(ctx, "test", "worker-1")
	if err != nil || job.LeaseToken != "token" || f.worker != "worker-1" {
		t.Fatalf("claim: %+v %v", job, err)
	}
	if err = c.Ack(ctx, "id", "token"); err != nil || f.token != "token" {
		t.Fatalf("ack: %v", err)
	}
	if err = c.Fail(ctx, "id", "token", "failed"); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Extend(ctx, "id", "token", time.Second); err != nil {
		t.Fatal(err)
	}
	if stats, err := c.Stats(ctx, "test"); err != nil || stats.Pending != 3 {
		t.Fatalf("stats: %+v %v", stats, err)
	}
	f.err = queue.ErrEmpty
	if _, err = c.Claim(ctx, "test", "worker-1"); !errors.Is(err, client.ErrEmpty) {
		t.Fatalf("empty: %v", err)
	}
	f.err = queue.ErrLeaseLost
	err = c.Ack(ctx, "id", "token")
	var remote *client.Error
	if !errors.As(err, &remote) || remote.Status != 409 {
		t.Fatalf("lost lease: %v", err)
	}
}
func TestHealthAndMetrics(t *testing.T) {
	h := handler(&fakeStore{})
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 {
			t.Fatalf("%s: %d", path, w.Code)
		}
		if path == "/metrics" && !strings.Contains(w.Body.String(), "relay_queue_depth 5") {
			t.Fatal(w.Body.String())
		}
	}
	f := &fakeStore{err: errors.New("offline")}
	w := httptest.NewRecorder()
	handler(f).ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readiness %d", w.Code)
	}
}

func TestPayloadLimitIncludesEnvelope(t *testing.T) {
	payload := `"` + strings.Repeat("a", queue.MaxPayloadBytes-2) + `"`
	f := &fakeStore{created: true}
	w := httptest.NewRecorder()
	handler(f).ServeHTTP(w, httptest.NewRequest("POST", "/queues/test/jobs", strings.NewReader(`{"payload":`+payload+`,"idempotency_key":"key","delay_ms":31536000000}`)))
	if w.Code != http.StatusCreated {
		t.Fatalf("maximum payload rejected: %d %s", w.Code, w.Body.String())
	}
	if len(f.submit.Payload) != queue.MaxPayloadBytes || f.submit.Delay != 365*24*time.Hour {
		t.Fatalf("wrong parsed payload or delay: %d %s", len(f.submit.Payload), f.submit.Delay)
	}
}

func TestLargeSpecialCharacterPayloadRoundTrip(t *testing.T) {
	for _, character := range []string{"<", "\u2028"} {
		t.Run(character, func(t *testing.T) {
			payload := json.RawMessage(`"` + strings.Repeat(character, (queue.MaxPayloadBytes-2)/len(character)) + `"`)
			f := &fakeStore{created: true, job: queue.Job{ID: "large", Payload: payload}}
			server := httptest.NewServer(handler(f))
			defer server.Close()
			c := client.New(server.URL, server.Client())
			job, _, err := c.Enqueue(context.Background(), "test", client.Submit{Payload: payload})
			if err != nil {
				t.Fatal(err)
			}
			if string(job.Payload) != string(payload) {
				t.Fatal("enqueue altered payload")
			}
			job, err = c.Claim(context.Background(), "test", "worker")
			if err != nil {
				t.Fatal(err)
			}
			if string(job.Payload) != string(payload) {
				t.Fatal("claim altered payload")
			}
		})
	}
}
