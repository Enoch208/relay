package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Enoch208/relay/internal/queue"
)

type Store interface {
	Enqueue(context.Context, string, queue.Submit) (queue.Job, bool, error)
	Claim(context.Context, string, string) (queue.Job, error)
	Ack(context.Context, string, string) error
	Fail(context.Context, string, string, string) error
	Extend(context.Context, string, string, time.Duration) (queue.Job, error)
	Get(context.Context, string) (queue.Job, error)
	Stats(context.Context, string) (queue.Stats, error)
	Ping(context.Context) error
}

type Server struct {
	store   Store
	log     *slog.Logger
	metrics metrics
	handler http.Handler
}

func New(store Store, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{store: store, log: logger}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /queues/{queue}/jobs", s.enqueue)
	mux.HandleFunc("POST /queues/{queue}/claim", s.claim)
	mux.HandleFunc("POST /jobs/{id}/ack", s.ack)
	mux.HandleFunc("POST /jobs/{id}/fail", s.fail)
	mux.HandleFunc("POST /jobs/{id}/extend", s.extend)
	mux.HandleFunc("GET /jobs/{id}", s.get)
	mux.HandleFunc("GET /queues/{queue}/stats", s.stats)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.store.Ping(r.Context()); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "database unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /metrics", s.serveMetrics)
	s.handler = mux
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	s.handler.ServeHTTP(w, r.WithContext(ctx))
}

func decode(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, queue.MaxPayloadBytes+(64<<10))
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("request must contain one JSON object")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(v)
}

func (s *Server) failure(w http.ResponseWriter, r *http.Request, err error) {
	status, msg := http.StatusInternalServerError, "internal server error"
	switch {
	case errors.Is(err, queue.ErrNotFound):
		status, msg = 404, "job not found"
	case errors.Is(err, queue.ErrLeaseLost):
		status, msg = 409, "lease lost"
	case errors.Is(err, queue.ErrInvalid):
		status, msg = 400, "invalid request"
	case errors.Is(err, context.DeadlineExceeded):
		status, msg = 503, "request timed out"
	case errors.Is(err, context.Canceled):
		status, msg = 503, "request canceled"
	default:
		s.log.Error("request failed", "method", r.Method, "path", r.URL.Path, "error", err)
	}
	writeJSON(w, status, map[string]string{"error": msg})
}

func badRequest(w http.ResponseWriter) {
	writeJSON(w, 400, map[string]string{"error": "invalid JSON request"})
}

func validName(v string, maximum int) bool {
	return len(v) > 0 && len(v) <= maximum && strings.TrimSpace(v) == v
}

func millis(v int64, maximum time.Duration) (time.Duration, bool) {
	if v < 0 || v > int64(maximum/time.Millisecond) {
		return 0, false
	}
	return time.Duration(v) * time.Millisecond, true
}

func (s *Server) enqueue(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Payload        json.RawMessage `json:"payload"`
		IdempotencyKey string          `json:"idempotency_key"`
		MaxAttempts    int             `json:"max_attempts"`
		DelayMS        int64           `json:"delay_ms"`
		TimeoutMS      int64           `json:"timeout_ms"`
	}
	if decode(w, r, &body) != nil || !validName(r.PathValue("queue"), 128) || len(body.Payload) == 0 || len(body.Payload) > queue.MaxPayloadBytes {
		badRequest(w)
		return
	}
	delay, ok := millis(body.DelayMS, 365*24*time.Hour)
	timeout, ok2 := millis(body.TimeoutMS, 24*time.Hour)
	if !ok || !ok2 {
		badRequest(w)
		return
	}
	job, created, err := s.store.Enqueue(r.Context(), r.PathValue("queue"), queue.Submit{Payload: body.Payload, IdempotencyKey: body.IdempotencyKey, MaxAttempts: body.MaxAttempts, Delay: delay, Timeout: timeout})
	if err != nil {
		s.failure(w, r, err)
		return
	}
	job.LeaseToken = ""
	status := http.StatusOK
	if created {
		status = http.StatusCreated
		s.metrics.submitted.Add(1)
		s.log.Info("job submitted", "job_id", job.ID, "queue", job.Queue)
	}
	writeJSON(w, status, job)
}

func (s *Server) claim(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkerID string `json:"worker_id"`
	}
	if decode(w, r, &body) != nil || !validName(body.WorkerID, 256) || !validName(r.PathValue("queue"), 128) {
		badRequest(w)
		return
	}
	start := time.Now()
	job, err := s.store.Claim(r.Context(), r.PathValue("queue"), body.WorkerID)
	s.metrics.claimLatency.observe(time.Since(start).Seconds())
	if errors.Is(err, queue.ErrEmpty) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		s.failure(w, r, err)
		return
	}
	s.metrics.attempts.Add(1)
	if job.Attempt > 1 {
		s.metrics.retried.Add(1)
	}
	s.metrics.waitTime.observe(time.Since(job.AvailableAt).Seconds())
	s.log.Info("job claimed", "job_id", job.ID, "queue", job.Queue, "worker_id", job.WorkerID, "attempt", job.Attempt)
	writeJSON(w, 200, job)
}

func (s *Server) ack(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Token string `json:"lease_token"`
	}
	if decode(w, r, &body) != nil || body.Token == "" {
		badRequest(w)
		return
	}
	if err := s.store.Ack(r.Context(), r.PathValue("id"), body.Token); err != nil {
		s.failure(w, r, err)
		return
	}
	s.metrics.completed.Add(1)
	s.log.Info("job completed", "job_id", r.PathValue("id"))
	w.WriteHeader(204)
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Token  string `json:"lease_token"`
		Reason string `json:"reason"`
	}
	if decode(w, r, &body) != nil || body.Token == "" || len(body.Reason) > 4096 {
		badRequest(w)
		return
	}
	if err := s.store.Fail(r.Context(), r.PathValue("id"), body.Token, body.Reason); err != nil {
		s.failure(w, r, err)
		return
	}
	s.metrics.failed.Add(1)
	s.log.Info("job failed", "job_id", r.PathValue("id"))
	w.WriteHeader(204)
}

func (s *Server) extend(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Token       string `json:"lease_token"`
		ExtensionMS int64  `json:"extension_ms"`
	}
	if decode(w, r, &body) != nil || body.Token == "" {
		badRequest(w)
		return
	}
	duration, ok := millis(body.ExtensionMS, 24*time.Hour)
	if !ok || duration == 0 {
		badRequest(w)
		return
	}
	job, err := s.store.Extend(r.Context(), r.PathValue("id"), body.Token, duration)
	if err != nil {
		s.failure(w, r, err)
		return
	}
	writeJSON(w, 200, job)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	job, err := s.store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.failure(w, r, err)
		return
	}
	job.LeaseToken = ""
	writeJSON(w, 200, job)
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context(), r.PathValue("queue"))
	if err != nil {
		s.failure(w, r, err)
		return
	}
	writeJSON(w, 200, stats)
}
