package delivery

import (
	"math/rand"
	"time"
)

type Backoff struct {
	Base   time.Duration
	Max    time.Duration
	jitter func(time.Duration) time.Duration
}

func NewBackoff(base, max time.Duration) Backoff {
	return Backoff{
		Base: base,
		Max:  max,
		jitter: func(limit time.Duration) time.Duration {
			if limit <= 0 {
				return 0
			}
			return time.Duration(rand.Int63n(int64(limit) + 1))
		},
	}
}

func (b Backoff) Delay(attempt int, retryAfter time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	limit := b.Base
	for current := 1; current < attempt && limit < b.Max; current++ {
		if limit > b.Max/2 {
			limit = b.Max
			break
		}
		limit *= 2
	}
	if limit > b.Max {
		limit = b.Max
	}

	delay := b.jitter(limit)
	if retryAfter > delay {
		delay = retryAfter
	}
	if delay > b.Max {
		return b.Max
	}
	return delay
}
