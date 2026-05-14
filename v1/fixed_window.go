package v1

import (
	"encoding/json"
	"time"

	"github.com/go-redis/redis"
)

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

type FixedWindowState struct {
	TokensConsumed int   `json:"tokens_consumed"`
	UpdatedAt      int64 `json:"updated_at"`
}

func (tb *FixedWindow) Consume(key string, count int) bool {
	var state *FixedWindowState
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
		state = &FixedWindowState{
			TokensConsumed: 0,
			UpdatedAt:      current,
		}
	}

	if state.UpdatedAt+tb.Interval.Microseconds() <= current {
		state = &FixedWindowState{
			TokensConsumed: 0,
			UpdatedAt:      current,
		}

	}

	tokens := tb.Limit - state.TokensConsumed
	if tokens < count {
		return false
	}
	state = &FixedWindowState{
		TokensConsumed: state.TokensConsumed + count,
		UpdatedAt:      current,
	}

	stateEncoded, _ := json.Marshal(state)
	tb.rdb.HSet("ratelimiter", key, stateEncoded)
	return true
}
