package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ak3tsm7/latency-aware-task-queue/internal/models"
	redisq "github.com/ak3tsm7/latency-aware-task-queue/internal/redis"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	worker := models.NewWorker("gpu")

	job, err := redisq.FetchAndClaimJob(ctx, rdb, worker)
	if err != nil {
		panic(err)
	}

	if job == nil {
		fmt.Println("No job available")
		return
	}

	fmt.Printf("Worker %s executing job %s\n", worker.ID, job.ID)

	heartbeatKey := fmt.Sprintf("heartbeat:%s", worker.ID)
	runningKey := fmt.Sprintf("running:%s", worker.ID)

	// Heartbeat goroutine
	stopHB := make(chan struct{})
	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				rdb.Set(ctx, heartbeatKey, time.Now().Unix(), 10*time.Second)
			case <-stopHB:
				return
			}
		}
	}()

	// Simulate execution
	time.Sleep(8 * time.Second)

	// Stop heartbeat
	close(stopHB)

	// Cleanup
	rdb.HDel(ctx, runningKey, job.ID)
	rdb.Del(ctx, heartbeatKey)

	fmt.Printf("Worker %s completed job %s\n", worker.ID, job.ID)
}
