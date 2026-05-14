package v1

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis"
)

func setupTokenBucket(t *testing.T) *TokenBucket {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	return NewTokenBucket(
		rdb,
		3,
		1,
		2*time.Second,
	)
}

func TestTokenBucket_ConsumeWithinLimit(t *testing.T) {
	tb := setupTokenBucket(t)

	key := "user-1"

	if !tb.Consume(key, 1) {
		t.Fatal("expected first consume to succeed")
	}

	if !tb.Consume(key, 1) {
		t.Fatal("expected second consume to succeed")
	}

	if !tb.Consume(key, 1) {
		t.Fatal("expected third consume to succeed")
	}
}

func TestTokenBucket_ExceedLimit(t *testing.T) {
	tb := setupTokenBucket(t)

	key := "user-2"

	tb.Consume(key, 1)
	tb.Consume(key, 1)
	tb.Consume(key, 1)

	if tb.Consume(key, 1) {
		t.Fatal("expected fourth consume to fail")
	}
}

func TestTokenBucket_RefillAfterRateUnit(t *testing.T) {
	tb := setupTokenBucket(t)

	key := "user-3"

	tb.Consume(key, 1)
	tb.Consume(key, 1)
	tb.Consume(key, 1)

	if tb.Consume(key, 1) {
		t.Fatal("expected bucket to be empty")
	}

	time.Sleep(3 * time.Second)

	if !tb.Consume(key, 1) {
		t.Fatal("expected consume after refill")
	}
}

func TestTokenBucket_DifferentKeys(t *testing.T) {
	tb := setupTokenBucket(t)

	user1 := "user-1"
	user2 := "user-2"

	tb.Consume(user1, 1)
	tb.Consume(user1, 1)
	tb.Consume(user1, 1)

	if tb.Consume(user1, 1) {
		t.Fatal("expected user1 bucket exhausted")
	}

	if !tb.Consume(user2, 1) {
		t.Fatal("expected user2 to still have tokens")
	}
}
