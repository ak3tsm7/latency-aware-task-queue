package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func recoverStuckJobs(ctx context.Context, rdb *redis.Client) {
	keys, _ := rdb.Keys(ctx, "running:*").Result()

	for _, key := range keys {
		workerID := strings.TrimPrefix(key, "running:")
		hbKey := "heartbeat:" + workerID

		lastHB, err := rdb.Get(ctx, hbKey).Int64()
		if err != nil || time.Now().Unix()-lastHB > 10 {
			jobs, _ := rdb.HGetAll(ctx, key).Result()
			for jobID := range jobs {
				fmt.Println("Recovering job", jobID)

				// Requeue as generic
				rdb.ZAdd(ctx, "queue:any", redis.Z{
					Score: 100,
					Member: jobID,
				})
			}

			rdb.Del(ctx, key)
			rdb.Del(ctx, hbKey)
		}
	}
}
