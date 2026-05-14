package v1

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

var _ Ratelimiter = &FixedWindow{}

type FixedWindow struct {
	Limit    int
	Interval time.Duration

	rdb *redis.Client
}

func NewFixedWindow(
	rdb *redis.Client,
	limit int,
	interval time.Duration,
) *FixedWindow {
	return &FixedWindow{
		rdb:      rdb,
		Limit:    limit,
		Interval: interval,
	}
}

type fixedWindowState struct {
	TokensConsumed int   `json:"tokens_consumed"`
	UpdatedAt      int64 `json:"updated_at"`
}

func (tb *FixedWindow) Consume(ctx context.Context, key string, count int) bool {
	var state *fixedWindowState
	var current int64 = time.Now().UnixNano()

	stateRaw, err := tb.rdb.Get(ctx, rateLimiterKey(key)).Result()
	if err != nil && err != redis.Nil {
		return true
	}

	if err == nil || err != redis.Nil {
		if err := json.Unmarshal([]byte(stateRaw), &state); err != nil {
			return true
		}
	}

	if state == nil {
		state = &fixedWindowState{
			TokensConsumed: 0,
			UpdatedAt:      current,
		}
	}

	if state.UpdatedAt+tb.Interval.Nanoseconds() <= current {
		state = &fixedWindowState{
			TokensConsumed: 0,
			UpdatedAt:      current,
		}

	}

	tokens := tb.Limit - state.TokensConsumed
	if tokens < count {
		return false
	}
	state = &fixedWindowState{
		TokensConsumed: state.TokensConsumed + count,
		UpdatedAt:      current,
	}

	stateEncoded, _ := json.Marshal(state)
	tb.rdb.Set(ctx, rateLimiterKey(key), stateEncoded, tb.Interval)
	return true
}
