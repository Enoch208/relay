# Relay

[![CI](https://github.com/Enoch208/relay/actions/workflows/ci.yml/badge.svg)](https://github.com/Enoch208/relay/actions/workflows/ci.yml)

Relay is a durable job queue in Go backed by PostgreSQL. Producers submit JSON jobs over HTTP. Workers claim time-limited leases, acknowledge completed work, or retry failures with exponential backoff. When a worker disappears, a recovery loop returns its unfinished work to the queue. Multiple API instances and workers coordinate through the same database.

## Run

```sh
docker compose up --build -d
curl -s http://localhost:8080/readyz

curl -s http://localhost:8080/queues/default/jobs \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"sleep_ms":100,"fail_until_attempt":1},"max_attempts":3,"idempotency_key":"example-1"}'

docker compose --profile worker up --build -d worker
curl -s http://localhost:8080/queues/default/stats
```

The example job fails once, retries after one second, then completes. Repeating its submission within 24 hours returns the original job. The API listens on localhost:8080 and Prometheus on localhost:9090. PostgreSQL data lives in a named Docker volume.

For a local Go 1.26 build:

```sh
docker compose up -d postgres
export DATABASE_URL='postgres://relay:relay@localhost:5432/relay?sslmode=disable'
make build
./bin/relay
```

Run `./bin/relay-worker` in another terminal. Its handler accepts `sleep_ms` and `fail_until_attempt` for demonstrations; replace the handler in `cmd/relay-worker` with application work. The `client` package provides a typed HTTP client.

## Architecture

```mermaid
flowchart LR
    P[Producer] --> A[Relay HTTP API]
    A --> DB[(PostgreSQL)]
    W[Worker goroutines] -->|claim / ACK / fail / extend| A
    R[Lease recovery loop] -->|expired leases| DB
    A --> R
    M[Prometheus] -->|scrape /metrics| A
```

Queue transitions live in `internal/queue`. HTTP handlers validate requests and delegate to that store. Each API process runs a bounded recovery sweep; PostgreSQL row locks make concurrent sweeps safe. Workers use contexts to cancel handlers and a heartbeat to extend leases during long jobs.

```
pending ──claim──> leased ──ACK──> completed
                     │
                  failure or expiry
                     │
                     ├── attempts remain ──> retry_wait ──claim when due──> leased
                     └── attempts exhausted ──> dead_letter
```

### Claims and stale acknowledgements

A claim locks one eligible row with `FOR UPDATE SKIP LOCKED`, increments its attempt, writes a random lease token and expiry, then commits before returning the job. Other claimers skip that locked row. Once the transaction commits, its state is `leased`, so it no longer matches the claim query. This prevents concurrent valid leases without making unrelated workers wait for the first row. Ordering is best effort under contention, not strict FIFO.

ACK, failure and extension lock the job first, then read PostgreSQL's clock and check the state, token and expiry. An expired token is rejected even before the recovery loop has visited the row. An old worker also cannot acknowledge a replacement worker's lease. Completed and dead-letter jobs are excluded from both claiming and recovery, and database constraints cap attempts and require lease fields to agree with state.

All eligibility and expiry decisions use the database clock. API hosts do not need synchronized clocks to decide lease ownership. A lease protects queue transitions; it cannot stop an old worker from finishing an external side effect after its lease expires. Delivery is **at least once**. Handlers must make side effects idempotent, or use a transactional outbox or downstream fencing where appropriate. If a response is lost after commit, the caller may not know whether the operation succeeded.

### Recovery and retries

The recovery loop locks up to 100 expired jobs per sweep with `SKIP LOCKED`. Both explicit failure and expiry use the same transition: retry after `min(RETRY_BASE × 2^(attempt−1), RETRY_MAX)`, or dead-letter after the last permitted attempt. Defaults are 1s, 2s, 4s and so on, capped at one minute. The attempt count increases only on a successful claim; expiry consumes the attempt already started. There is no automatic dead-letter replay.

Recovery requires a running API process. With its default one-second sweep, a crashed worker's job becomes eligible after the remaining lease, the sweep delay, and retry backoff. Restarting an API leaves leases and queued work in PostgreSQL. The server drains in-flight HTTP requests on shutdown; interrupted worker handlers leave work recoverable through lease expiry. Handlers must observe context cancellation: the worker waits for them during shutdown, and Go cannot forcibly stop an uncooperative handler.

### Idempotent submission

An idempotency key is scoped to a queue. Submissions lock the key's row in the same transaction that inserts the job. Concurrent duplicates return the same job within the configured window, including after that job completes. The window starts when a new job is created and duplicate requests do not extend it. After expiry, the same key can create a new job. A duplicate key returns the original payload even if the new request contains different data.

## HTTP API

| Method | Path | Body / response |
|---|---|---|
| POST | `/queues/{queue}/jobs` | `payload`, optional `idempotency_key`, `max_attempts`, `delay_ms`, `timeout_ms`; job |
| POST | `/queues/{queue}/claim` | `worker_id`; job with lease token, attempt and expiry, or 204 if empty |
| POST | `/jobs/{id}/ack` | `lease_token`; 204 |
| POST | `/jobs/{id}/fail` | `lease_token`, `reason`; 204 |
| POST | `/jobs/{id}/extend` | `lease_token`, `extension_ms`; updated lease |
| GET | `/jobs/{id}` | Job state |
| GET | `/queues/{queue}/stats` | Counts by state |
| GET | `/healthz` | Process liveness |
| GET | `/readyz` | Database readiness |
| GET | `/metrics` | Prometheus exposition |

Submission returns 201 for a new job and 200 for a duplicate. Invalid input returns 400, unknown jobs 404, stale leases 409, request timeouts 503, and other storage failures 500. JSON errors contain an `error` field. The default attempt limit is three. `timeout_ms` controls each lease's duration; it is not a deadline for the job's entire lifetime. Extensions add time to the current expiry, capped at 24 hours from now.

Queue names contain 1–128 letters, digits, dots, underscores or hyphens and start with a letter or digit. JSON payloads are limited to 1 MiB. Idempotency keys and worker IDs are limited to 256 bytes; failure reasons to 4 KiB. Invalid JSON, unknown request fields and trailing JSON values are rejected.

## Configuration

| Server variable | Default |
|---|---|
| `DATABASE_URL` | Required PostgreSQL connection URL |
| `ADDR` | `:8080` |
| `LEASE_DURATION` | `30s` |
| `IDEMPOTENCY_WINDOW` | `24h` |
| `RETRY_BASE` | `1s` |
| `RETRY_MAX` | `1m` |
| `RECOVERY_INTERVAL` | `1s` |

Worker variables: `RELAY_URL=http://localhost:8080`, `QUEUE=default`, `WORKER_ID=<hostname>`, `CONCURRENCY=4`, `POLL_INTERVAL=500ms`. Durations use Go duration syntax. Add `pool_max_conns` to the database URL to control each API instance's connection pool; size all pools against PostgreSQL's connection budget. Use matching queue configuration across API instances.

Logs are structured JSON with job, queue and worker identifiers for job operations. Metrics include submitted/completed/failed/retried counters, attempts, claim latency, job wait time, queue depth, active leases and retry/dead-letter counts. Counters and histograms belong to each API process and reset on restart. State gauges read the shared database; do not sum those gauges across replicas. Lease expiry is reflected in state gauges after recovery.

## Correctness

```sh
export RELAY_TEST_DATABASE_URL='postgres://relay:relay@localhost:5432/relay?sslmode=disable'
make check
```

Integration tests require PostgreSQL and skip when `RELAY_TEST_DATABASE_URL` is unset. Use a disposable test database. CI provides PostgreSQL, runs tests normally and under the Go race detector, checks formatting and `go vet`, builds the binaries and container, runs a containerized server/worker smoke test, and runs a short benchmark.

The suite covers concurrent duplicate submissions, competing claims, stale and expired lease mutations, bounded retries, delayed jobs, concurrent acknowledgements, process crash recovery and store reopening. API tests cover malformed requests and storage failures. Worker tests exercise cancellation and lease renewal. SQL constraints provide another check on transition invariants.

## Measurements

Run the real PostgreSQL benchmark with:

```sh
export DATABASE_URL='postgres://relay:relay@localhost:5432/relay?sslmode=disable'
./scripts/measure.sh
```

The default is 1,000 jobs per phase, 1/10/50/100 worker goroutines and three repetitions per worker count, with a 32-connection store pool. Each repetition measures concurrent enqueue, then claim against the filled queue, then ACK against those leases. A final phase performs enqueue → claim → ACK for each job. Claim percentiles include pool waits, database round trips, transaction commits and any empty-claim retries (100 µs polling). These are direct store calls with a small fixed JSON payload, no HTTP traffic and no application processing. They are closed-loop measurements rather than an open-loop saturation or long-running soak test.

Each cell reports the median of the repetition results; latency cells are medians of each run's nearest-rank percentiles. A fresh queue name isolates every repetition and its rows are removed afterward. Connection setup is included as the pool grows. The script writes raw runs, medians, CPU, RAM, OS, Go and PostgreSQL versions, and database durability settings to `work/benchmarks/results.json`. Use `-jobs`, `-workers`, `-repetitions` and `-connections` to change the workload; use at least three repetitions for a reported result.

Measured on 7 October 2026 with an Apple M5 (10 logical CPUs), 24 GiB RAM, macOS/Darwin 25.6.0, Go 1.26.5 and PostgreSQL 15.17 on the same host over TCP. PostgreSQL used `fsync=on`, `synchronous_commit=on`, `shared_buffers=128MB` and `max_connections=100`. Command: `./scripts/measure.sh` with defaults. [Raw runs and environment](scripts/benchmark-results.json).

| Workers | Enqueue/s | Claim/s | ACK/s | End-to-end jobs/s | Claim p50 ms | p90 ms | p99 ms |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 5856 | 5281 | 5019 | 1530 | 0.175 | 0.221 | 0.345 |
| 10 | 26441 | 18760 | 23079 | 7742 | 0.456 | 0.566 | 1.226 |
| 50 | 30186 | 23260 | 29537 | 9590 | 1.957 | 2.827 | 9.913 |
| 100 | 30289 | 22164 | 28429 | 9377 | 4.322 | 5.123 | 5.882 |

Throughput levels off between 50 and 100 workers in this small workload. With 32 database connections, additional goroutines can wait in the pool; these measurements alone do not isolate the database, scheduler, storage or row-lock contribution. Use PostgreSQL wait events and query plans alongside a Go profile to investigate a sustained workload.

## Deployment and limits

`Dockerfile` builds static server and worker binaries and runs them as an unprivileged user. Compose is a local example with development credentials and localhost ports. For Kubernetes, build and push the image, replace `relay:local` in `deploy/kubernetes/relay.yaml`, and create a `relay-database` Secret with a `url` key pointing to your PostgreSQL service before applying the manifest. The example runs two API replicas with readiness/liveness probes, resource limits and a read-only root filesystem. Schema initialization is serialized with a PostgreSQL advisory lock.

- Durability depends on one PostgreSQL cluster and its WAL, storage and replication settings. Relay does not provide cross-region replication or independent consensus.
- The API has no authentication, authorization or built-in TLS. Keep it private behind an authenticated TLS proxy. Lease tokens authorize mutations and should be treated as secrets. Add tenant isolation, quotas and rate limiting before exposing it to untrusted clients.
- Completed jobs, dead letters and idempotency rows are retained. A production service needs a retention policy, bounded cleanup, backup/restore exercises and versioned schema migrations.
- Polling, per-job transactions and a recovery batch of 100 bound throughput. Retry backoff has no jitter, and there are no priority queues, batch APIs or fairness guarantees. Benchmark the expected workload before changing these choices.
- Metrics state scans grow with retained data. The benchmark uses a small local workload; it does not establish performance for a large backlog, remote database, failover or prolonged contention.

## Layout

```
cmd/relay/           HTTP server and lease recovery loop
cmd/relay-worker/    example worker process
cmd/relay-bench/     PostgreSQL measurements
internal/queue/      schema, transactions, leases and retry transitions
internal/api/        HTTP validation, routing and Prometheus metrics
internal/worker/     concurrent processing, cancellation and renewal
client/              HTTP client
tests/integration/  persistence, races and crash recovery
scripts/measure.sh   repeatable benchmark entry point
deploy/              Prometheus and Kubernetes examples
```
