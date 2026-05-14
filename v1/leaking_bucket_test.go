package v1

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupLeakingBucket(
	t *testing.T,
	bucketSize,
	consumeRate int,
	consumeUnit time.Duration,
) *LeakingBucket {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	return NewLeakingBucket(
		rdb,
		bucketSize,
		consumeRate,
		consumeUnit,
	)
}

func TestLeakingBucket_LeaksGradually(t *testing.T) {
	lb := setupLeakingBucket(
		t,
		10,
		2,
		time.Second,
	)

	if !lb.Consume(context.Background(), "user", 10) {
		t.Fatal("expected initial consume to succeed")
	}

	time.Sleep(500 * time.Millisecond)

	// only ~1 token leaked
	if lb.Consume(context.Background(), "user", 2) {
		t.Fatal("expected consume to fail")
	}

	time.Sleep(600 * time.Millisecond)

	// now ~2 tokens leaked total
	if !lb.Consume(context.Background(), "user", 2) {
		t.Fatal("expected consume to succeed")
	}
}

func TestLeakingBucket_ConsumeWithinLimit(t *testing.T) {
	lb := setupLeakingBucket(
		t,
		10,
		1,
		time.Second,
	)

	allowed := lb.Consume(context.Background(), "user-1", 5)

	if !allowed {
		t.Fatal("expected consume to succeed")
	}
}

func TestLeakingBucket_RejectWhenBucketFull(t *testing.T) {
	lb := setupLeakingBucket(
		t,
		10,
		1,
		time.Second,
	)

	if !lb.Consume(context.Background(), "user-1", 8) {
		t.Fatal("expected first consume to succeed")
	}

	if lb.Consume(context.Background(), "user-1", 5) {
		t.Fatal("expected second consume to fail")
	}
}

func TestLeakingBucket_LeakOverTime(t *testing.T) {
	lb := setupLeakingBucket(
		t,
		10,
		2,
		time.Second,
	)

	if !lb.Consume(context.Background(), "user-1", 10) {
		t.Fatal("expected initial consume to succeed")
	}

	if lb.Consume(context.Background(), "user-1", 1) {
		t.Fatal("expected bucket to be full")
	}

	time.Sleep(3 * time.Second)

	if !lb.Consume(context.Background(), "user-1", 5) {
		t.Fatal("expected consume after leaking")
	}
}

func TestLeakingBucket_DifferentKeys(t *testing.T) {
	lb := setupLeakingBucket(
		t,
		10,
		1,
		time.Second,
	)

	if !lb.Consume(context.Background(), "user-1", 10) {
		t.Fatal("expected user-1 consume to succeed")
	}

	if lb.Consume(context.Background(), "user-1", 1) {
		t.Fatal("expected user-1 bucket full")
	}

	if !lb.Consume(context.Background(), "user-2", 5) {
		t.Fatal("expected user-2 to have independent bucket")
	}
}

func TestLeakingBucket_ExactCapacity(t *testing.T) {
	lb := setupLeakingBucket(
		t,
		10,
		1,
		time.Second,
	)

	if !lb.Consume(context.Background(), "user-1", 10) {
		t.Fatal("expected exact capacity consume to succeed")
	}
}

func TestLeakingBucket_OverflowByOne(t *testing.T) {
	lb := setupLeakingBucket(
		t,
		10,
		1,
		time.Second,
	)

	if lb.Consume(context.Background(), "user-1", 11) {
		t.Fatal("expected overflow consume to fail")
	}
}

func TestLeakingBucket_ActualGradualLeak(t *testing.T) {
	lb := setupLeakingBucket(
		t,
		10,
		2,
		time.Second,
	)

	if !lb.Consume(context.Background(), "user", 10) {
		t.Fatal()
	}

	time.Sleep(1100 * time.Millisecond)

	if lb.Consume(context.Background(), "user", 3) {
		t.Fatal("expected consume to fail")
	}

	// consume(2) should succeed
	if !lb.Consume(context.Background(), "user", 2) {
		t.Fatal("expected consume to succeed")
	}
}
