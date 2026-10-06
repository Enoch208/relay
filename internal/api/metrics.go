package api

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
)

var buckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

type histogram struct {
	mu     sync.Mutex
	counts [12]uint64
	count  uint64
	sum    float64
}

func (h *histogram) observe(v float64) {
	if v < 0 {
		v = 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.count++
	h.sum += v
	for i, b := range buckets {
		if v <= b {
			h.counts[i]++
		}
	}
}

func (h *histogram) write(w http.ResponseWriter, name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Fprintf(w, "# TYPE %s histogram\n", name)
	for i, b := range buckets {
		fmt.Fprintf(w, "%s_bucket{le=%q} %d\n", name, fmt.Sprint(b), h.counts[i])
	}
	fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n%s_sum %g\n%s_count %d\n", name, h.count, name, h.sum, name, h.count)
}

type metrics struct {
	submitted, completed, failed, attempts, retried atomic.Uint64
	claimLatency, waitTime                          histogram
}

func (s *Server) serveMetrics(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.Stats(r.Context(), "")
	if err != nil {
		s.failure(w, r, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	for _, m := range []struct {
		name  string
		value uint64
	}{{"relay_jobs_submitted_total", s.metrics.submitted.Load()}, {"relay_jobs_completed_total", s.metrics.completed.Load()}, {"relay_jobs_failed_total", s.metrics.failed.Load()}, {"relay_execution_attempts_total", s.metrics.attempts.Load()}, {"relay_jobs_retried_total", s.metrics.retried.Load()}} {
		fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", m.name, m.name, m.value)
	}
	for _, m := range []struct {
		name  string
		value int64
	}{{"relay_queue_depth", stats.Pending + stats.RetryWait}, {"relay_active_leases", stats.Leased}, {"relay_jobs_retry_wait", stats.RetryWait}, {"relay_jobs_dead_letter", stats.DeadLetter}, {"relay_jobs_completed", stats.Completed}} {
		fmt.Fprintf(w, "# TYPE %s gauge\n%s %d\n", m.name, m.name, m.value)
	}
	s.metrics.claimLatency.write(w, "relay_claim_duration_seconds")
	s.metrics.waitTime.write(w, "relay_job_wait_seconds")
}
