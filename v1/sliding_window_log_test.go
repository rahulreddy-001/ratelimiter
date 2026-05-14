package v1

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis"
)

func setupSlidingWindowLog(
	t *testing.T,
	limit int,
	interval time.Duration,
) *SlidingWindowLog {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	return NewSlidingWindowLog(
		rdb,
		limit,
		interval,
	)
}

func TestSlidingWindowLog_ConsumeWithinLimit(t *testing.T) {
	sw := setupSlidingWindowLog(
		t,
		5,
		time.Second,
	)

	for range 5 {
		if !sw.Consume("user-1", 1) {
			t.Fatal("expected consume to succeed")
		}
	}
}

func TestSlidingWindowLog_RejectWhenLimitExceeded(t *testing.T) {
	sw := setupSlidingWindowLog(
		t,
		5,
		time.Second,
	)

	for range 5 {
		if !sw.Consume("user-1", 1) {
			t.Fatal("expected consume to succeed")
		}
	}

	if sw.Consume("user-1", 1) {
		t.Fatal("expected consume to fail")
	}
}

func TestSlidingWindowLog_WindowSlides(t *testing.T) {
	sw := setupSlidingWindowLog(
		t,
		5,
		time.Second,
	)

	for range 5 {
		if !sw.Consume("user-1", 1) {
			t.Fatal("expected consume to succeed")
		}
	}

	if sw.Consume("user-1", 1) {
		t.Fatal("expected consume to fail")
	}

	time.Sleep(1100 * time.Millisecond)

	if !sw.Consume("user-1", 1) {
		t.Fatal("expected consume after window slide")
	}
}

func TestSlidingWindowLog_DifferentKeys(t *testing.T) {
	sw := setupSlidingWindowLog(
		t,
		5,
		time.Second,
	)

	for range 5 {
		if !sw.Consume("user-1", 1) {
			t.Fatal("expected user-1 consume to succeed")
		}
	}

	if sw.Consume("user-1", 1) {
		t.Fatal("expected user-1 limit exceeded")
	}

	if !sw.Consume("user-2", 1) {
		t.Fatal("expected user-2 to have separate window")
	}
}

func TestSlidingWindowLog_ExactLimit(t *testing.T) {
	sw := setupSlidingWindowLog(
		t,
		5,
		time.Second,
	)

	for range 5 {
		if !sw.Consume("user-1", 1) {
			t.Fatal("expected consume within exact limit")
		}
	}
}

func TestSlidingWindowLog_OverflowByOne(t *testing.T) {
	sw := setupSlidingWindowLog(
		t,
		1,
		time.Second,
	)

	if !sw.Consume("user-1", 1) {
		t.Fatal("expected first consume")
	}

	if sw.Consume("user-1", 1) {
		t.Fatal("expected second consume to fail")
	}
}

func TestSlidingWindowLog_OldEntriesRemoved(t *testing.T) {
	sw := setupSlidingWindowLog(
		t,
		2,
		time.Second,
	)

	if !sw.Consume("user-1", 1) {
		t.Fatal("expected first consume")
	}

	time.Sleep(1100 * time.Millisecond)

	if !sw.Consume("user-1", 1) {
		t.Fatal("expected second consume")
	}

	time.Sleep(1100 * time.Millisecond)

	// first timestamp should now be gone
	if !sw.Consume("user-1", 1) {
		t.Fatal("expected old timestamps cleaned")
	}
}
