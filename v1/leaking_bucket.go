package v1

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

var _ Ratelimiter = &LeakingBucket{}

type LeakingBucket struct {
	BucketSize  int
	ConsumeRate int
	ConsumeUnit time.Duration

	rdb *redis.Client
}

func NewLeakingBucket(
	rdb *redis.Client,
	bucketSize,
	refillRate int,
	rateUnit time.Duration,
) *LeakingBucket {
	return &LeakingBucket{
		rdb:         rdb,
		BucketSize:  bucketSize,
		ConsumeRate: refillRate,
		ConsumeUnit: rateUnit,
	}
}

type leakingBucketState struct {
	ConsumedCount int   `json:"consumed_count"`
	UpdatedAt     int64 `json:"updated_at"`
}

func (tb *LeakingBucket) Consume(ctx context.Context, key string, count int) bool {
	var state *leakingBucketState
	var current int64 = time.Now().UnixNano()

	stateRaw, err := tb.rdb.HGet(ctx, "ratelimiter", key).Result()
	if err != nil && err != redis.Nil {
		return true
	}

	if err != redis.Nil {
		if err := json.Unmarshal([]byte(stateRaw), &state); err != nil {
			return true
		}
	}

	if state == nil {
		state = &leakingBucketState{
			ConsumedCount: 0,
			UpdatedAt:     current,
		}
	}
	leaked := int(((current - state.UpdatedAt) * int64(tb.ConsumeRate)) / tb.ConsumeUnit.Nanoseconds())
	state.ConsumedCount = max(state.ConsumedCount-leaked, 0)

	if state.ConsumedCount+count > tb.BucketSize {
		return false
	}

	state.ConsumedCount += count
	state.UpdatedAt = current

	stateEncoded, _ := json.Marshal(state)
	tb.rdb.HSet(ctx, "ratelimiter", key, stateEncoded)
	return true
}
