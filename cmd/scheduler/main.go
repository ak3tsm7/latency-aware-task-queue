package main

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/ak3tsm7/latency-aware-task-queue/internal/models"
	redisq "github.com/ak3tsm7/latency-aware-task-queue/internal/redis"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	job := models.Job{
		ID:       uuid.New().String(),
		TaskType: "test_task",
		Requires: "gpu",
		Priority: 100,
		Payload:  map[string]interface{}{"msg": "hello"},
		TimeoutMs: 5000,
		Metadata:  map[string]string{},
	}

	err := redisq.EnqueueJob(context.Background(), rdb, job)
	fmt.Println("Enqueue result:", err)
}
