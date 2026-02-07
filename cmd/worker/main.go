package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/ak3tsm7/latency-aware-task-queue/internal/metrics"
	"github.com/ak3tsm7/latency-aware-task-queue/internal/models"
	redisq "github.com/ak3tsm7/latency-aware-task-queue/internal/redis"
)

var errJobCancelled = errors.New("job cancelled")

func main() {
	ctx := context.Background()
	rateLimitPerMinute := envInt("RATE_LIMIT_PER_MINUTE", 60)

	metrics.Register()

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		fmt.Println("Metrics server started on :2113/metrics")
		if err := http.ListenAndServe(":2113", nil); err != nil {
			fmt.Printf("Failed to start metrics server: %v\n", err)
		}
	}()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Println("Failed to connect to Redis:", err)
		return
	}

	worker := models.NewWorker("cpu")
	rateLimiter := redisq.NewRateLimiter(rdb, rateLimitPerMinute)

	fmt.Printf("Worker %s (%s) started\n", worker.ID, worker.Type)
	fmt.Println("Registering with scheduler...")

	err := rdb.ZAdd(ctx, "workers:latency", redis.Z{
		Score:  1000,
		Member: worker.ID,
	}).Err()

	if err != nil {
		fmt.Println("Failed to register:", err)
		return
	}
	fmt.Println("Registration successful")

	fmt.Println("Waiting for jobs...")

	for {
		job, err := redisq.FetchAndClaimJob(ctx, rdb, worker)
		if err != nil {
			fmt.Println("Error fetching job:", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if job == nil {
			time.Sleep(1 * time.Second)
			continue
		}

		fmt.Printf("\nWorker %s received job %s\n", worker.ID, job.ID)
		fmt.Printf("   Task: %s | Type: %s | Priority: %d\n",
			job.TaskType, job.Requires, job.Priority)

		// Pre-run cancellation check
		if cancelled, _ := rdb.Exists(ctx, fmt.Sprintf("cancelled:%s", job.ID)).Result(); cancelled > 0 {
			fmt.Println("Job marked cancelled before start - skipping")
			markCancelled(ctx, rdb, job)
			continue
		}

		heartbeatKey := fmt.Sprintf("heartbeat:%s", worker.ID)
		runningKey := fmt.Sprintf("running:%s", worker.ID)

		// Rate limit per queue and per worker before starting heartbeats/work.
		queueKey := job.Requires
		if queueKey == "" {
			queueKey = "any"
		}
		allowedQueue, err := rateLimiter.AllowQueue(ctx, queueKey)
		if err != nil {
			fmt.Println("Rate limit check failed (queue):", err)
		}
		allowedWorker, err := rateLimiter.AllowWorker(ctx, worker.ID)
		if err != nil {
			fmt.Println("Rate limit check failed (worker):", err)
		}
		if !allowedQueue || !allowedWorker {
			fmt.Println("Rate limit hit - requeueing job")
			rdb.HDel(ctx, runningKey, job.ID)
			requeueJob(ctx, rdb, job)
			time.Sleep(1 * time.Second)
			continue
		}

		stopHB := make(chan struct{})
		go func() {
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					err := rdb.Set(ctx, heartbeatKey, time.Now().Unix(), 15*time.Second).Err()
					if err != nil {
						fmt.Println("Heartbeat failed:", err)
					}
				case <-stopHB:
					return
				}
			}
		}()

		start := time.Now()
		var execErr error

		// Respect job timeout
		timeout := time.Duration(job.TimeoutMs) * time.Millisecond
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		execCtx, execCancel := context.WithTimeout(ctx, timeout)

		// Simulate work with cancellation/timeout awareness
		executionTime := time.Duration(2+job.Priority%3) * time.Second
		fmt.Printf("   Executing for %v (timeout %v)...\n", executionTime, timeout)

		workDone := make(chan struct{})
		go func() {
			time.Sleep(executionTime)
			close(workDone)
		}()

		select {
		case <-workDone:
			// completed
		case <-execCtx.Done():
			execErr = execCtx.Err()
		}
		execCancel()

		// Check cancellation flag after work/timeout
		cancelled, _ := rdb.Exists(ctx, fmt.Sprintf("cancelled:%s", job.ID)).Result()
		if cancelled > 0 && execErr == nil {
			execErr = errJobCancelled
		}

		// Only apply simulated failure if the job did not already timeout/cancel.
		if execErr == nil {
			if v, ok := job.Payload["should_fail"].(bool); ok && v {
				execErr = errors.New("simulated job failure (payload.should_fail=true)")
			}
		}

		duration := time.Since(start)
		if execErr == nil {
			fmt.Printf("   Completed in %v\n", duration)
		} else {
			fmt.Printf("   Failed in %v: %v\n", duration, execErr)
		}

		metrics.JobDurationSeconds.WithLabelValues(job.Requires, worker.Type).Observe(duration.Seconds())

		successLabel := "true"
		if execErr != nil {
			successLabel = "false"
		}
		metrics.JobsCompletedTotal.WithLabelValues(job.Requires, worker.Type, successLabel).Inc()

		err = redisq.UpdateWorkerMetrics(
			ctx,
			rdb,
			worker.ID,
			worker.Type,
			duration,
		)
		if err != nil {
			fmt.Println("Failed to update metrics:", err)
		}

		close(stopHB)

		rdb.HDel(ctx, runningKey, job.ID)
		rdb.Del(ctx, heartbeatKey)

		jobKey := fmt.Sprintf("job:%s", job.ID)
		if execErr != nil {
			if errors.Is(execErr, context.DeadlineExceeded) {
				execErr = fmt.Errorf("job timeout after %v", timeout)
			}
			if errors.Is(execErr, errJobCancelled) || execErr.Error() == "job cancelled" {
				markCancelled(ctx, rdb, job)
			} else {
				if err := redisq.HandleJobFailure(ctx, rdb, *job, execErr); err != nil {
					fmt.Println("Failed to record job failure:", err)
				}
			}
			// Record completion metadata for bench/source tracking
			rdb.HSet(ctx, jobKey, map[string]interface{}{
				"status":           "failed",
				"completed_at_ns":  time.Now().UnixNano(),
				"error":            execErr.Error(),
			})
		} else {
			// For bench/source=bench, keep job record to allow latency calculation; otherwise delete.
			if job.Metadata["source"] == "bench" {
				rdb.HSet(ctx, jobKey, map[string]interface{}{
					"status":          "succeeded",
					"completed_at_ns": time.Now().UnixNano(),
				})
			} else {
				rdb.Del(ctx, jobKey)
			}
		}

		metricsData, _ := rdb.HGetAll(ctx, "metrics:"+worker.ID).Result()
		if len(metricsData) > 0 {
			fmt.Printf("   Updated Metrics - Avg Latency: %sms | Jobs Done: %s\n",
				metricsData["avg_latency_ms"], metricsData["jobs_done"])
		}
	}
}

func envInt(key string, def int) int {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	if n, err := strconv.Atoi(val); err == nil && n > 0 {
		return n
	}
	return def
}

// requeueJob puts a job back to its appropriate queue when rate limits are hit.
func requeueJob(ctx context.Context, rdb *redis.Client, job *models.Job) {
	queue := "queue:any"
	if job.Requires == "cpu" || job.Requires == "gpu" {
		queue = fmt.Sprintf("queue:%s", job.Requires)
	}
	rdb.ZAdd(ctx, queue, redis.Z{
		Score:  float64(-job.Priority),
		Member: job.ID,
	})
	jobKey := fmt.Sprintf("job:%s", job.ID)
	rdb.HSet(ctx, jobKey, "status", "queued")
}

func markCancelled(ctx context.Context, rdb *redis.Client, job *models.Job) {
	jobKey := fmt.Sprintf("job:%s", job.ID)
	rdb.HSet(ctx, jobKey, map[string]interface{}{
		"status": "cancelled",
		"error":  "job cancelled",
	})
}
