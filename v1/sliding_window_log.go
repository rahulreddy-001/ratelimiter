package v1

import (
	"encoding/json"
	"time"

	"github.com/go-redis/redis"
)

var _ Ratelimiter = &SlidingWindowLog{}

type SlidingWindowLog struct {
	Limit    int
	Interval time.Duration

	rdb *redis.Client
}

func NewSlidingWindowLog(
	rdb *redis.Client,
	limit int,
	interval time.Duration,
) *SlidingWindowLog {
	return &SlidingWindowLog{
		rdb:      rdb,
		Limit:    limit,
		Interval: interval,
	}
}

type slidingWindowLogState struct {
	ConsumedTS []int64 `json:"consumed_ts"`
}

func (tb *SlidingWindowLog) Consume(key string, count int) bool {
	var state *slidingWindowLogState
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
		state = &slidingWindowLogState{
			ConsumedTS: []int64{},
		}
	}

	cutoff := current - tb.Interval.Nanoseconds()

	idx := 0
	for idx < len(state.ConsumedTS) &&
		state.ConsumedTS[idx] < cutoff {
		idx++
	}
	state.ConsumedTS = state.ConsumedTS[idx:]

	if len(state.ConsumedTS)+count > tb.Limit {
		return false
	}
	for _ = range count {
		state.ConsumedTS = append(state.ConsumedTS, current)
	}

	stateEncoded, _ := json.Marshal(state)
	tb.rdb.HSet("ratelimiter", key, stateEncoded)
	return true
}
