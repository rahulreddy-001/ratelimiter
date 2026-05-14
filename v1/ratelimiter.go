package v1

import "context"

type Ratelimiter interface {
	Consume(ctx context.Context, key string, count int) bool
}
