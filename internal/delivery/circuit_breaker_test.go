package delivery

import (
	"testing"
	"time"
)

func TestCircuitBreakerTripsAndCooldowns(t *testing.T) {
	cb := NewCircuitBreaker(3, 100*time.Millisecond)

	allowed, _ := cb.Allow("dest1")
	if !allowed {
		t.Fatal("expected allowed initially")
	}

	cb.RecordFailure("dest1")
	cb.RecordFailure("dest1")

	allowed, _ = cb.Allow("dest1")
	if !allowed {
		t.Fatal("expected allowed before threshold")
	}

	cb.RecordFailure("dest1") // reaches 3 failures

	allowed, remaining := cb.Allow("dest1")
	if allowed {
		t.Fatal("expected blocked after 3 failures")
	}
	if remaining <= 0 {
		t.Fatalf("expected remaining cooldown, got %v", remaining)
	}

	time.Sleep(120 * time.Millisecond)

	allowed, _ = cb.Allow("dest1")
	if !allowed {
		t.Fatal("expected allowed after cooldown")
	}

	cb.RecordSuccess("dest1")
	allowed, _ = cb.Allow("dest1")
	if !allowed {
		t.Fatal("expected allowed after success")
	}
}
