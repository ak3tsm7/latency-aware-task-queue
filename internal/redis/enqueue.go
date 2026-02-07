package redisq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ak3tsm7/latency-aware-task-queue/internal/models"
)


func EnqueueJob(ctx context.Context, rdb *redis.Client, job models.Job) error {
	// 1. Store job payload
	jobKey := fmt.Sprintf("job:%s", job.ID)
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	metadataJSON, err := json.Marshal(job.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	err = rdb.HSet(ctx, jobKey, map[string]interface{}{
		"payload":     string(data),
		"metadata":    string(metadataJSON),
	"status":      "queued",
	"created_at":  time.Now().Unix(),
	}).Err()

	
	if err != nil {
		return fmt.Errorf("failed to store job data: %w", err)
	}

	// 2. Add to appropriate queue based on requirements
	queue := "queue:any"
	if job.Requires == "cpu" || job.Requires == "gpu" {
		queue = fmt.Sprintf("queue:%s", job.Requires)
	}

	// Use negative priority for max-heap behavior (ZPopMax gets highest score)
	// Higher priority jobs should have higher (less negative) scores
	score := float64(-job.Priority)

	err = rdb.ZAdd(ctx, queue, redis.Z{
		Score:  score,
		Member: job.ID,
	}).Err()
	
	if err != nil {
		return fmt.Errorf("failed to add job to queue: %w", err)
	}

	return nil
}
