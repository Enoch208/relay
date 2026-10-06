package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Enoch208/relay/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

type measurement struct {
	Workers    int     `json:"workers"`
	Repetition int     `json:"repetition"`
	Enqueue    float64 `json:"enqueue_per_second"`
	Claim      float64 `json:"claim_per_second"`
	Ack        float64 `json:"ack_per_second"`
	EndToEnd   float64 `json:"end_to_end_per_second"`
	P50        float64 `json:"claim_p50_ms"`
	P90        float64 `json:"claim_p90_ms"`
	P99        float64 `json:"claim_p99_ms"`
}

type report struct {
	Time        string            `json:"time_utc"`
	Jobs        int               `json:"jobs_per_phase"`
	Repetitions int               `json:"repetitions"`
	Connections int               `json:"connection_limit"`
	Environment map[string]string `json:"environment"`
	Runs        []measurement     `json:"runs"`
	Medians     []measurement     `json:"medians"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	jobs := flag.Int("jobs", 1000, "jobs per phase")
	workerList := flag.String("workers", "1,10,50,100", "comma-separated worker counts")
	repetitions := flag.Int("repetitions", 3, "complete runs per worker count")
	connections := flag.Int("connections", 32, "maximum store database connections")
	out := flag.String("out", "", "write raw measurements and medians as JSON")
	flag.Parse()
	if *jobs < 1 || *repetitions < 1 || *connections < 1 {
		return errors.New("jobs, repetitions and connections must be positive")
	}
	var workers []int
	for _, item := range strings.Split(*workerList, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(item))
		if err != nil || n < 1 {
			return fmt.Errorf("invalid worker count %q", item)
		}
		workers = append(workers, n)
	}
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return errors.New("DATABASE_URL is required")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		return errors.New("DATABASE_URL must be a PostgreSQL URL")
	}
	params := parsed.Query()
	params.Set("pool_max_conns", strconv.Itoa(*connections))
	parsed.RawQuery = params.Encode()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Minute)
	defer cancel()
	config := queue.DefaultConfig()
	config.LeaseDuration = 5 * time.Minute
	store, err := queue.Open(ctx, parsed.String(), config)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	env := map[string]string{
		"go": runtime.Version(), "platform": runtime.GOOS + "/" + runtime.GOARCH,
		"logical_cpus": strconv.Itoa(runtime.NumCPU()), "gomaxprocs": strconv.Itoa(runtime.GOMAXPROCS(0)),
		"os": command("uname", "-sr"),
	}
	if runtime.GOOS == "darwin" {
		env["cpu"] = command("sysctl", "-n", "machdep.cpu.brand_string")
		env["ram_bytes"] = command("sysctl", "-n", "hw.memsize")
	} else {
		env["cpu"] = command("lscpu")
		data, readErr := os.ReadFile("/proc/meminfo")
		if readErr == nil {
			env["memory"] = strings.SplitN(string(data), "\n", 2)[0]
		}
	}
	for key, query := range map[string]string{
		"postgres":           "SELECT version()",
		"shared_buffers":     "SHOW shared_buffers",
		"max_connections":    "SHOW max_connections",
		"synchronous_commit": "SHOW synchronous_commit",
		"fsync":              "SHOW fsync",
	} {
		var value string
		if err := pool.QueryRow(ctx, query).Scan(&value); err != nil {
			return err
		}
		env[key] = value
	}
	r := report{Time: time.Now().UTC().Format(time.RFC3339), Jobs: *jobs, Repetitions: *repetitions, Connections: *connections, Environment: env}
	for _, count := range workers {
		var group []measurement
		for repetition := 1; repetition <= *repetitions; repetition++ {
			fmt.Fprintf(os.Stderr, "%d workers, run %d/%d\n", count, repetition, *repetitions)
			m, err := measure(ctx, store, pool, *jobs, count)
			if err != nil {
				return err
			}
			m.Repetition = repetition
			group = append(group, m)
			r.Runs = append(r.Runs, m)
		}
		r.Medians = append(r.Medians, aggregate(group))
	}
	if *out != "" {
		data, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(*out, append(data, '\n'), 0644); err != nil {
			return err
		}
	}
	fmt.Printf("Measured %s. %d jobs per phase, %d repetitions, %d store connections.\n\n", r.Time, r.Jobs, r.Repetitions, r.Connections)
	fmt.Println("| Workers | Enqueue/s | Claim/s | ACK/s | End-to-end jobs/s | Claim p50 ms | p90 ms | p99 ms |")
	fmt.Println("|---:|---:|---:|---:|---:|---:|---:|---:|")
	for _, m := range r.Medians {
		fmt.Printf("| %d | %.0f | %.0f | %.0f | %.0f | %.3f | %.3f | %.3f |\n", m.Workers, m.Enqueue, m.Claim, m.Ack, m.EndToEnd, m.P50, m.P90, m.P99)
	}
	fmt.Println()
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		fmt.Printf("%s: %s\n", key, env[key])
	}
	return nil
}

func measure(ctx context.Context, store *queue.Store, pool *pgxpool.Pool, n, workers int) (result measurement, resultErr error) {
	name := fmt.Sprintf("bench-%d-%d", os.Getpid(), time.Now().UnixNano())
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := pool.Exec(cleanupCtx, "DELETE FROM relay_jobs WHERE queue=$1", name)
		resultErr = errors.Join(resultErr, err)
	}()
	result.Workers = workers
	submit := queue.Submit{Payload: json.RawMessage(`{"task":"benchmark","value":42}`), MaxAttempts: 1}
	duration, err := parallel(ctx, n, workers, func(ctx context.Context, i int) error {
		_, _, err := store.Enqueue(ctx, name, submit)
		return err
	})
	if err != nil {
		return result, err
	}
	result.Enqueue = float64(n) / duration.Seconds()
	claimed := make([]queue.Job, n)
	latencies := make([]float64, n)
	duration, err = parallel(ctx, n, workers, func(ctx context.Context, i int) error {
		started := time.Now()
		job, err := claim(ctx, store, name)
		latencies[i] = float64(time.Since(started)) / float64(time.Millisecond)
		claimed[i] = job
		return err
	})
	if err != nil {
		return result, err
	}
	result.Claim = float64(n) / duration.Seconds()
	sort.Float64s(latencies)
	result.P50, result.P90, result.P99 = percentile(latencies, .5), percentile(latencies, .9), percentile(latencies, .99)
	duration, err = parallel(ctx, n, workers, func(ctx context.Context, i int) error {
		return store.Ack(ctx, claimed[i].ID, claimed[i].LeaseToken)
	})
	if err != nil {
		return result, err
	}
	result.Ack = float64(n) / duration.Seconds()
	duration, err = parallel(ctx, n, workers, func(ctx context.Context, i int) error {
		if _, _, err := store.Enqueue(ctx, name, submit); err != nil {
			return err
		}
		job, err := claim(ctx, store, name)
		if err != nil {
			return err
		}
		return store.Ack(ctx, job.ID, job.LeaseToken)
	})
	if err != nil {
		return result, err
	}
	result.EndToEnd = float64(n) / duration.Seconds()
	return result, nil
}

func parallel(ctx context.Context, n, workers int, fn func(context.Context, int) error) (time.Duration, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var next atomic.Int64
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	start := make(chan struct{})
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for ctx.Err() == nil {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				if err := fn(ctx, i); err != nil {
					once.Do(func() { firstErr = err; cancel() })
					return
				}
			}
		}()
	}
	started := time.Now()
	close(start)
	wg.Wait()
	if firstErr != nil {
		return time.Since(started), firstErr
	}
	return time.Since(started), ctx.Err()
}

func claim(ctx context.Context, store *queue.Store, name string) (queue.Job, error) {
	for {
		job, err := store.Claim(ctx, name, "benchmark")
		if !errors.Is(err, queue.ErrEmpty) {
			return job, err
		}
		timer := time.NewTimer(100 * time.Microsecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return queue.Job{}, ctx.Err()
		case <-timer.C:
		}
	}
}

func aggregate(runs []measurement) measurement {
	m := measurement{Workers: runs[0].Workers}
	fields := []func(measurement) float64{
		func(r measurement) float64 { return r.Enqueue }, func(r measurement) float64 { return r.Claim },
		func(r measurement) float64 { return r.Ack }, func(r measurement) float64 { return r.EndToEnd },
		func(r measurement) float64 { return r.P50 }, func(r measurement) float64 { return r.P90 },
		func(r measurement) float64 { return r.P99 },
	}
	values := make([]float64, len(fields))
	for i, field := range fields {
		samples := make([]float64, len(runs))
		for j, run := range runs {
			samples[j] = field(run)
		}
		sort.Float64s(samples)
		middle := len(samples) / 2
		values[i] = samples[middle]
		if len(samples)%2 == 0 {
			values[i] = (samples[middle-1] + samples[middle]) / 2
		}
	}
	m.Enqueue, m.Claim, m.Ack, m.EndToEnd, m.P50, m.P90, m.P99 = values[0], values[1], values[2], values[3], values[4], values[5], values[6]
	return m
}

func percentile(sorted []float64, fraction float64) float64 {
	return sorted[int(math.Ceil(float64(len(sorted))*fraction))-1]
}

func command(name string, args ...string) string {
	output, err := exec.Command(name, args...).Output()
	if err != nil {
		return "unavailable"
	}
	return strings.TrimSpace(string(output))
}
