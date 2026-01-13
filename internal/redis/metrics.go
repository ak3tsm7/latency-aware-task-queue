package redisq

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const emaAlpha = 0.2

func UpdateWorkerMetrics(
	ctx context.Context,
	rdb *redis.Client,
	workerID string,
	workerType string,
	executionTime time.Duration,
) error {

	key := "metrics:" + workerID
	currentMs := float64(executionTime.Milliseconds())

	pipe := rdb.TxPipeline()

	existingAvg, err := rdb.HGet(ctx, key, "avg_latency_ms").Result()
	var newAvg float64

	if err == redis.Nil {
		// First job → initialize
		newAvg = currentMs
	} else if err != nil {
		return err
	} else {
		oldAvg, _ := strconv.ParseFloat(existingAvg, 64)
		newAvg = emaAlpha*currentMs + (1-emaAlpha)*oldAvg
	}

	pipe.HSet(ctx, key,
		"avg_latency_ms", newAvg,
		"worker_type", workerType,
	)
	pipe.HIncrBy(ctx, key, "jobs_done", 1)

	_, err = pipe.Exec(ctx)
	return err
}
