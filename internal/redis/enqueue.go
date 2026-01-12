package redisq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/ak3tsm7/latency-aware-task-queue/internal/models"
)

func EnqueueJob(ctx context.Context, rdb *redis.Client, job models.Job) error {
	jobKey := fmt.Sprintf("job:%s", job.ID)

	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	if err := rdb.HSet(ctx, jobKey, "payload", data).Err(); err != nil {
		return err
	}

	queue := "queue:any"
	if job.Requires == "cpu" || job.Requires == "gpu" {
		queue = fmt.Sprintf("queue:%s", job.Requires)
	}

	return rdb.ZAdd(ctx, queue, redis.Z{
		Score:  float64(job.Priority),
		Member: job.ID,
	}).Err()
}
