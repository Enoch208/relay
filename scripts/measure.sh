#!/bin/sh
set -eu
: "${DATABASE_URL:?Set DATABASE_URL to a disposable PostgreSQL database}"
cd "$(dirname "$0")/.."
mkdir -p work/benchmarks
go run ./cmd/relay-bench -out work/benchmarks/results.json "$@"
