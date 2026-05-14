package v1

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis"
)

func setupFixedWindow(
	t *testing.T,
	limit int,
	interval time.Duration,
) *FixedWindow {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	return NewFixedWindow(
		rdb,
		limit,
		interval,
	)
}

func TestFixedWindow_ConsumeWithinLimit(t *testing.T) {
	fw := setupFixedWindow(
		t,
		10,
		time.Second,
	)

	if !fw.Consume("user-1", 5) {
		t.Fatal("expected consume to succeed")
	}
}

func TestFixedWindow_RejectWhenLimitExceeded(t *testing.T) {
	fw := setupFixedWindow(
		t,
		10,
		time.Second,
	)

	if !fw.Consume("user-1", 8) {
		t.Fatal("expected first consume to succeed")
	}

	if fw.Consume("user-1", 5) {
		t.Fatal("expected second consume to fail")
	}
}

func TestFixedWindow_ResetAfterInterval(t *testing.T) {
	fw := setupFixedWindow(
		t,
		10,
		time.Second,
	)

	if !fw.Consume("user-1", 10) {
		t.Fatal("expected consume to succeed")
	}

	if fw.Consume("user-1", 1) {
		t.Fatal("expected limit exceeded")
	}

	time.Sleep(1100 * time.Millisecond)

	if !fw.Consume("user-1", 5) {
		t.Fatal("expected consume after reset")
	}
}

func TestFixedWindow_DifferentKeys(t *testing.T) {
	fw := setupFixedWindow(
		t,
		10,
		time.Second,
	)

	if !fw.Consume("user-1", 10) {
		t.Fatal("expected user-1 consume to succeed")
	}

	if fw.Consume("user-1", 1) {
		t.Fatal("expected user-1 limit exceeded")
	}

	if !fw.Consume("user-2", 5) {
		t.Fatal("expected user-2 to have separate window")
	}
}

func TestFixedWindow_ExactLimit(t *testing.T) {
	fw := setupFixedWindow(
		t,
		10,
		time.Second,
	)

	if !fw.Consume("user-1", 10) {
		t.Fatal("expected exact limit consume to succeed")
	}
}

func TestFixedWindow_OverflowByOne(t *testing.T) {
	fw := setupFixedWindow(
		t,
		10,
		time.Second,
	)

	if fw.Consume("user-1", 11) {
		t.Fatal("expected overflow consume to fail")
	}
}

func TestFixedWindow_MultipleConsumes(t *testing.T) {
	fw := setupFixedWindow(
		t,
		10,
		time.Second,
	)

	if !fw.Consume("user-1", 3) {
		t.Fatal("expected first consume")
	}

	if !fw.Consume("user-1", 3) {
		t.Fatal("expected second consume")
	}

	if !fw.Consume("user-1", 4) {
		t.Fatal("expected third consume")
	}

	if fw.Consume("user-1", 1) {
		t.Fatal("expected limit exceeded")
	}
}
