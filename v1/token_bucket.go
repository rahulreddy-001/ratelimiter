package v1

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

var _ Ratelimiter = &TokenBucket{}

type TokenBucket struct {
	BucketSize int
	RefillRate int
	RateUnit   time.Duration

	rdb *redis.Client
}

func NewTokenBucket(
	rdb *redis.Client,
	bucketSize,
	refillRate int,
	rateUnit time.Duration,
) *TokenBucket {
	return &TokenBucket{
		rdb:        rdb,
		BucketSize: bucketSize,
		RefillRate: refillRate,
		RateUnit:   rateUnit,
	}
}

type tokenBucketState struct {
	TokensRemaining int   `json:"tokens_remaining"`
	UpdatedAt       int64 `json:"updated_at"`
}

func (tb *TokenBucket) Consume(ctx context.Context, key string, count int) bool {
	var state *tokenBucketState
	var current int64 = time.Now().UnixNano()

	stateRaw, err := tb.rdb.HGet(ctx, "ratelimiter", key).Result()
	if err != nil && err != redis.Nil {
		return true
	}

	if err == nil || err != redis.Nil {
		if err := json.Unmarshal([]byte(stateRaw), &state); err != nil {
			return true
		}
	}

	if state == nil {
		state = &tokenBucketState{
			TokensRemaining: tb.BucketSize,
			UpdatedAt:       current,
		}
	}

	tokens := int(int64(state.TokensRemaining) + ((current - state.UpdatedAt) * (int64(tb.RefillRate)) / tb.RateUnit.Nanoseconds()))
	tokens = min(tokens, tb.BucketSize)

	if tokens < count {
		return false
	}
	state = &tokenBucketState{
		TokensRemaining: tokens - count,
		UpdatedAt:       current,
	}

	stateEncoded, _ := json.Marshal(state)
	tb.rdb.HSet(ctx, "ratelimiter", key, stateEncoded)
	return true
}
