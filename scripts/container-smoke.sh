#!/bin/sh
set -eu
: "${DATABASE_URL:?Set DATABASE_URL to the CI PostgreSQL database}"
image="${1:-relay:ci}"
server="relay-api-smoke-$$"
worker="relay-worker-smoke-$$"
queue="smoke-$$"
url=http://127.0.0.1:18080
cleanup() {
  result=$?
  if [ "$result" -ne 0 ]; then
    docker logs "$server" || true
    docker logs "$worker" || true
  fi
  docker rm -f "$server" "$worker" >/dev/null 2>&1 || true
  exit "$result"
}
trap cleanup EXIT
trap 'exit 1' INT TERM
docker run -d --name "$server" --network host \
  -e DATABASE_URL -e ADDR=127.0.0.1:18080 "$image" >/dev/null
for attempt in $(seq 1 60); do
  if curl -fsS "$url/readyz" >/dev/null 2>&1; then break; fi
  if [ "$attempt" -eq 60 ]; then exit 1; fi
  sleep 0.5
done
job_id=$(curl -fsS "$url/queues/$queue/jobs" \
  -H 'Content-Type: application/json' \
  -d '{"payload":{"sleep_ms":10,"fail_until_attempt":1},"max_attempts":3}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
docker run -d --name "$worker" --network host --entrypoint relay-worker \
  -e RELAY_URL="$url" -e QUEUE="$queue" -e CONCURRENCY=2 "$image" >/dev/null
for attempt in $(seq 1 60); do
  state=$(curl -fsS "$url/jobs/$job_id" | python3 -c 'import json,sys; job=json.load(sys.stdin); print(job["state"]+":"+str(job["attempt"]))')
  if [ "$state" = completed:2 ]; then
    printf 'Container worker completed job %s after retry\n' "$job_id"
    exit 0
  fi
  case "$state" in completed:*|dead_letter:*) exit 1 ;; esac
  sleep 0.5
done
exit 1
