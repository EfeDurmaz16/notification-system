package stream

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// A slot covers the entire dispatch, then one additional second. Starting the
// window only at admission would let delayed claims bunch up at the provider.
// The 2 minute crash lease is longer than the 30 second send deadline.
var reserve = redis.NewScript(`
local pause = redis.call("PTTL", KEYS[3])
if pause > 0 then
  return pause * 1000
end
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
redis.call("ZREMRANGEBYSCORE", KEYS[1], "-inf", now)
local available = tonumber(redis.call("GET", KEYS[2]) or "0")
if redis.call("ZCARD", KEYS[1]) >= 100 then
  local oldest = redis.call("ZRANGE", KEYS[1], 0, 0, "WITHSCORES")
  available = math.max(available, tonumber(oldest[2]))
end
if available > now then
  return available - now
end
redis.call("ZADD", KEYS[1], now + 120000000, ARGV[1])
redis.call("PEXPIRE", KEYS[1], 121000)
redis.call("SET", KEYS[2], now + 10000, "PX", 2000)
return 0
`)

var finishSlot = redis.NewScript(`
local t = redis.call("TIME")
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
redis.call("ZADD", KEYS[1], "XX", now + 1000000, ARGV[1])
return 1
`)

type limiter struct {
	client *redis.Client
	prefix string
}

type slot struct {
	key   string
	token string
}

// tryAcquire asks Redis once. A positive delay parks only this channel in
// the scheduler, so waiting for its quota occupies no delivery handler.
func (l limiter) tryAcquire(ctx context.Context, channel string) (slot, time.Duration, error) {
	lease := slot{key: l.prefix + "{" + channel + "}", token: rand.Text()}
	wait, err := reserve.Run(ctx, l.client, []string{lease.key, lease.key + ":next", lease.key + ":pause"}, lease.token).Int64()
	if err != nil {
		return slot{}, 0, fmt.Errorf("reserve %s quota: %w", channel, err)
	}
	if wait > 0 {
		// An active permit can be released before its crash lease expires.
		return slot{}, min(time.Duration(wait)*time.Microsecond, 100*time.Millisecond), nil
	}
	return lease, 0, nil
}

func (l limiter) release(ctx context.Context, lease slot) error {
	return finishSlot.Run(ctx, l.client, []string{lease.key}, lease.token).Err()
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
