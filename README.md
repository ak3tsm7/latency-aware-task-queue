# latency-aware-task-queue
atency-aware distributed task queue with priority scheduling, heterogeneous workers (CPU/GPU-modeled), and speculative execution for straggler mitigation.

## Retries & Dead Letter Queue (DLQ)

Jobs now support bounded retries with backoff and a Redis-backed DLQ.

### Job fields
`internal/models/job.go` adds:
- `retry_count`, `max_retries`
- `retry_backoff`: `"exponential"` (default) or `"linear"`
- `failed_at`, `error_message`

If `max_retries` is `0`, the system defaults to `3`.

### Redis keys
- `retry:scheduled` (sorted set): retry jobs waiting for backoff (score = unix `available_at`).
- `dlq:failed` (sorted set): jobs that exceeded max retries (score = unix `failed_at`).
- `dlq:job:<id>` (hash): failure metadata for a DLQ'd job.

### Workflow
- Worker failure calls `HandleJobFailure(...)` to either:
  - schedule a retry into `retry:scheduled`, or
  - move the job into `dlq:failed` and write `dlq:job:<id>`.
- Scheduler runs `PromoteDueRetries(...)` and moves due jobs back into `queue:*` for workers to pick up.

## Load / Benchmark Tool
- Run the benchmark utility to enqueue a batch and measure drain time:
  ```
  go run ./cmd/bench -jobs 200 -concurrency 20 -queue cpu -timeout 3000
  ```
- Env overrides: `REDIS_ADDR`, `BENCH_JOBS`, `BENCH_CONCURRENCY`, `BENCH_QUEUE`, `BENCH_TIMEOUT_MS`, `BENCH_FAIL` (bool), `BENCH_PAYLOAD_BYTES` (int).
- Output shows remaining/running/queued/dlq counts until all benchmark jobs finish. Use alongside running workers for load testing.

### Sample Benchmark (local)
- Command: `bin/bench.exe -jobs 2000 -concurrency 40 -queue cpu -timeout 6000`
- Environment: 40 workers (RATE_LIMIT_PER_MINUTE=100000), scheduler running, Redis local.
- Results: throughput 8.58 jobs/sec; latency p50/p95/p99 (ms): 157238 / 226262 / 232307; success 2000; failed 0; cancelled 0; dlq 0; duration 233.04 s.
See `BENCHMARKS.md` for details.
