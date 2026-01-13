package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/google/uuid"

	"github.com/ak3tsm7/latency-aware-task-queue/internal/models"
	redisq "github.com/ak3tsm7/latency-aware-task-queue/internal/redis"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// 1️⃣ Run recovery FIRST
	fmt.Println("Running recovery scan...")
	recoverStuckJobs(ctx, rdb)

	// 2️⃣ Enqueue a new job (for testing)
	job := models.Job{
		ID:        uuid.New().String(),
		TaskType:  "test_task",
		Requires:  "gpu",
		Priority:  100,
		Payload:   map[string]interface{}{"msg": "hello"},
		TimeoutMs: 5000,
		Metadata:  map[string]string{},
	}

	err := redisq.EnqueueJob(ctx, rdb, job)
	fmt.Println("Enqueue result:", err)

	// Give Redis time to settle (optional)
	time.Sleep(500 * time.Millisecond)
}
