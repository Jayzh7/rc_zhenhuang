package delivery

import (
	"testing"
	"time"
)

func TestBackoffCapsExponentialDelay(t *testing.T) {
	t.Parallel()

	backoff := Backoff{
		Base: time.Second,
		Max:  5 * time.Second,
		jitter: func(limit time.Duration) time.Duration {
			return limit
		},
	}

	if delay := backoff.Delay(1, 0); delay != time.Second {
		t.Fatalf("first delay = %s", delay)
	}
	if delay := backoff.Delay(4, 0); delay != 5*time.Second {
		t.Fatalf("capped delay = %s", delay)
	}
	if delay := backoff.Delay(1, 3*time.Second); delay != 3*time.Second {
		t.Fatalf("Retry-After delay = %s", delay)
	}
	if delay := backoff.Delay(1, 10*time.Second); delay != 5*time.Second {
		t.Fatalf("capped Retry-After delay = %s", delay)
	}
}
