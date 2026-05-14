package v1

type Ratelimiter interface {
	Consume(key string, count int) bool
}
