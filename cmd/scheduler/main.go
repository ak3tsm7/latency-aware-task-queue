package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ak3tsm7/latency-aware-task-queue/internal/models"
	"github.com/ak3tsm7/latency-aware-task-queue/internal/metrics"
	redisq "github.com/ak3tsm7/latency-aware-task-queue/internal/redis"
)

func main() {
	ctx := context.Background()

	// Initialize metrics
	metrics.Register()

	// Start Prometheus metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		fmt.Println("📊 Metrics server started on :2112/metrics")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			fmt.Printf("Failed to start metrics server: %v\n", err)
		}
	}()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// Job cancellation endpoint: DELETE /api/jobs/{id}/cancel
	http.HandleFunc("/api/jobs/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/jobs/"), "/")
		if len(parts) != 2 || parts[1] != "cancel" || parts[0] == "" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		jobID := parts[0]
		cancelKey := fmt.Sprintf("cancelled:%s", jobID)
		if err := rdb.Set(ctx, cancelKey, "1", time.Hour).Err(); err != nil {
			http.Error(w, "failed to cancel job", http.StatusInternalServerError)
			return
		}
		rdb.HSet(ctx, fmt.Sprintf("job:%s", jobID), map[string]interface{}{
			"status": "cancelled",
			"error":  "job cancelled",
		})
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("cancelled"))
	})

	// Test Redis connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Println("Failed to connect to Redis:", err)
		return
	}
	fmt.Println("Connected to Redis successfully")

	// Start background metrics collector
	go collectMetrics(ctx, rdb)

	// Promote retry-scheduled jobs back into normal queues.
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if promoted, err := redisq.PromoteDueRetries(ctx, rdb, 100); err != nil {
					fmt.Println("Retry promotion error:", err)
				} else if promoted > 0 {
					fmt.Printf("🔁 Promoted %d retry job(s)\n", promoted)
				}
			}
		}
	}()

	// 1️⃣ Run recovery FIRST to handle any stuck jobs
	fmt.Println("Running recovery scan...")
	recoverStuckJobs(ctx, rdb)

	// 2️⃣ Enqueue test jobs
	jobs := []models.Job{
		{
			ID:        uuid.New().String(),
			TaskType:  "image_processing",
			Requires:  "gpu",
			Priority:  100,
			Payload:   map[string]interface{}{"image": "test1.jpg", "operation": "resize"},
			TimeoutMs: 5000,
			Metadata:  map[string]string{"source": "api"},
			MaxRetries:   3,
			RetryBackoff: "exponential",
		},
		{
			ID:        uuid.New().String(),
			TaskType:  "data_processing",
			Requires:  "cpu",
			Priority:  80,
			Payload:   map[string]interface{}{"dataset": "sales_data.csv"},
			TimeoutMs: 3000,
			Metadata:  map[string]string{"source": "batch"},
			MaxRetries:   3,
			RetryBackoff: "exponential",
		},
		{
			ID:        uuid.New().String(),
			TaskType:  "general_task",
			Requires:  "any",
			Priority:  50,
			Payload:   map[string]interface{}{"task": "cleanup"},
			TimeoutMs: 2000,
			Metadata:  map[string]string{"source": "cron"},
			MaxRetries:   3,
			RetryBackoff: "exponential",
		},
	}

	// Optional: enqueue a guaranteed failing job to exercise retries + DLQ.
	// PowerShell: `$env:ENQUEUE_FAILING_JOB="1"; go run ./cmd/scheduler`
	if os.Getenv("ENQUEUE_FAILING_JOB") == "1" {
		jobs = append(jobs, models.Job{
			ID:           uuid.New().String(),
			TaskType:     "dlq_test",
			Requires:     "cpu",
			Priority:     100,
			Payload:      map[string]interface{}{"should_fail": true},
			TimeoutMs:    1000,
			Metadata:     map[string]string{"source": "scheduler_env"},
			MaxRetries:   2,
			RetryBackoff: "linear",
		})
	}

	for _, job := range jobs {
		err := redisq.EnqueueJob(ctx, rdb, job)
		if err != nil {
			fmt.Printf("Failed to enqueue job %s: %v\n", job.ID, err)
		} else {
			// Increment Prometheus counter
			metrics.JobsSubmittedTotal.WithLabelValues(job.Requires).Inc()
			
			fmt.Printf("✅ Job %s enqueued to queue:%s (priority: %d)\n", 
				job.ID, job.Requires, job.Priority)
		}
	}

	// 3️⃣ Display current worker rankings
	fmt.Println("\n📊 Current Worker Rankings (by latency):")
	workers, err := rdb.ZRangeWithScores(ctx, "workers:latency", 0, -1).Result()
	if err == nil && len(workers) > 0 {
		for i, w := range workers {
			fmt.Printf("  %d. Worker %s - Avg Latency: %.2fms\n", 
				i+1, w.Member, w.Score)
		}
	} else {
		fmt.Println("  No workers registered yet")
	}

	// 4️⃣ Show queue status
	fmt.Println("\n📦 Queue Status:")
	queues := []string{"queue:gpu", "queue:cpu", "queue:any"}
	for _, queue := range queues {
		count, _ := rdb.ZCard(ctx, queue).Result()
		fmt.Printf("  %s: %d jobs\n", queue, count)
	}

	fmt.Println("\n✅ Scheduler completed. Workers can now process these jobs.")
	fmt.Println("📊 Metrics available at http://localhost:2112/metrics")
	
	// Keep running to serve metrics
	select {}
}

// collectMetrics periodically updates gauge metrics from Redis
func collectMetrics(ctx context.Context, rdb *redis.Client) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Update queue lengths
		queues := map[string]string{
			"queue:cpu": "cpu",
			"queue:gpu": "gpu",
			"queue:any": "any",
		}

		for queueKey, queueType := range queues {
			count, err := rdb.ZCard(ctx, queueKey).Result()
			if err == nil {
				metrics.QueueLength.WithLabelValues(queueType).Set(float64(count))
			}
		}

		// Update worker count
		workerCount, err := rdb.ZCard(ctx, "workers:latency").Result()
		if err == nil {
			metrics.WorkersRegistered.Set(float64(workerCount))
		}

		// Update running jobs count
		runningKeys, err := rdb.Keys(ctx, "running:*").Result()
		if err == nil {
			totalRunning := int64(0)
			for _, key := range runningKeys {
				count, _ := rdb.HLen(ctx, key).Result()
				totalRunning += count
			}
			metrics.RunningJobsTotal.Set(float64(totalRunning))
		}
	}
}
