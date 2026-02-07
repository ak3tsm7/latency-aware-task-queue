Benchmark Results (local run)

- Command: `bin/bench.exe -jobs 2000 -concurrency 40 -queue cpu -timeout 6000`
- Environment: 40 workers (RATE_LIMIT_PER_MINUTE=100000), scheduler running, Redis local.
- Results:
  - Throughput: 8.58 jobs/sec
  - Latency p50/p95/p99 (ms): 157238 / 226262 / 232307
  - Success: 2000, Failed: 0, Cancelled: 0, DLQ: 0
  - Duration: 233.04 s
