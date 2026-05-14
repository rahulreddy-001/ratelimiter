package v1

func rateLimiterKey(key string) string {
	return "ratelimiter:" + key
}
