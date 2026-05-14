package v1

import (
	"encoding/json"
	"time"

	"github.com/go-redis/redis"
)

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

type LeakingBucketState struct {
	ConsumedCount int   `json:"consumed_count"`
	UpdatedAt     int64 `json:"updated_at"`
}

func (tb *LeakingBucket) Consume(key string, count int) bool {
	var state *LeakingBucketState
	var current int64 = time.Now().UnixMicro()

	stateRaw, err := tb.rdb.HGet("ratelimiter", key).Result()
	if err != nil && err != redis.Nil {
		return true
	}

	if err != redis.Nil {
		if err := json.Unmarshal([]byte(stateRaw), &state); err != nil {
			return true
		}
	}

	if state == nil {
		state = &LeakingBucketState{
			ConsumedCount: 0,
			UpdatedAt:     current,
		}
	}
	leaked := int(((current - state.UpdatedAt) * int64(tb.ConsumeRate)) / tb.ConsumeUnit.Microseconds())
	state.ConsumedCount = max(state.ConsumedCount-leaked, 0)

	if state.ConsumedCount+count > tb.BucketSize {
		return false
	}

	state.ConsumedCount += count
	state.UpdatedAt = current

	stateEncoded, _ := json.Marshal(state)
	tb.rdb.HSet("ratelimiter", key, stateEncoded)
	return true
}
