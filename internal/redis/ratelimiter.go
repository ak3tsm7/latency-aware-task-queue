package redisq

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter provides simple per-minute counters for queues and workers using Redis.
// Implementation: counter per current minute key with 60s TTL.
type RateLimiter struct {
	maxJobsPerMinute int
	redis            *redis.Client
}

func NewRateLimiter(rdb *redis.Client, maxPerMinute int) *RateLimiter {
	if maxPerMinute <= 0 {
		maxPerMinute = 60
	}
	return &RateLimiter{
		maxJobsPerMinute: maxPerMinute,
		redis:            rdb,
	}
}

func minuteWindow() int64 {
	return time.Now().Unix() / 60
}

func (r *RateLimiter) allow(ctx context.Context, key string) (bool, error) {
	cnt, err := r.redis.Incr(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if cnt == 1 {
		// First hit in this window: set TTL to 60s
		_ = r.redis.Expire(ctx, key, 60*time.Second).Err()
	}
	if int(cnt) > r.maxJobsPerMinute {
		return false, nil
	}
	return true, nil
}

func (r *RateLimiter) AllowQueue(ctx context.Context, queue string) (bool, error) {
	key := fmt.Sprintf("ratelimit:%s:%d", queue, minuteWindow())
	return r.allow(ctx, key)
}

func (r *RateLimiter) AllowWorker(ctx context.Context, workerID string) (bool, error) {
	key := fmt.Sprintf("ratelimit:worker:%s:%d", workerID, minuteWindow())
	return r.allow(ctx, key)
}
