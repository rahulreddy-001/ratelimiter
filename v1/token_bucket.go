package v1

import (
	"encoding/json"
	"time"

	"github.com/go-redis/redis"
)

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

type State struct {
	TokensRemaining int   `json:"tokens_remaining"`
	UpdatedAt       int64 `json:"updated_at"`
}

func (tb *TokenBucket) Consume(key string, count int) bool {
	var state *State
	var current int64 = time.Now().UnixNano()

	stateRaw, err := tb.rdb.HGet("ratelimiter", key).Result()
	if err != nil && err != redis.Nil {
		return true
	}

	if err == nil || err != redis.Nil {
		if err := json.Unmarshal([]byte(stateRaw), &state); err != nil {
			return true
		}
	}

	if state == nil {
		state = &State{
			TokensRemaining: tb.BucketSize,
			UpdatedAt:       current,
		}
	}

	tokens := int(int64(state.TokensRemaining) + ((current - state.UpdatedAt) * (int64(tb.RefillRate)) / tb.RateUnit.Nanoseconds()))
	tokens = min(tokens, tb.BucketSize)

	if tokens < count {
		return false
	}
	state = &State{
		TokensRemaining: tokens - count,
		UpdatedAt:       current,
	}

	stateEncoded, _ := json.Marshal(state)
	tb.rdb.HSet("ratelimiter", key, stateEncoded)
	return true
}
