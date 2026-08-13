package delivery

import (
	"sync"
	"time"
)

type CircuitBreaker struct {
	mu              sync.Mutex
	failures        map[string]int
	openUntil       map[string]time.Time
	failureThreshold int
	cooldown        time.Duration
}

func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failures:         make(map[string]int),
		openUntil:        make(map[string]time.Time),
		failureThreshold: threshold,
		cooldown:         cooldown,
	}
}

func (cb *CircuitBreaker) Allow(destinationID string) (bool, time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	until, exists := cb.openUntil[destinationID]
	if !exists {
		return true, 0
	}

	now := time.Now()
	if now.Before(until) {
		return false, until.Sub(now)
	}

	delete(cb.openUntil, destinationID)
	return true, 0
}

func (cb *CircuitBreaker) RecordSuccess(destinationID string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	delete(cb.failures, destinationID)
	delete(cb.openUntil, destinationID)
}

func (cb *CircuitBreaker) RecordFailure(destinationID string) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures[destinationID]++
	if cb.failures[destinationID] >= cb.failureThreshold {
		cb.openUntil[destinationID] = time.Now().Add(cb.cooldown)
		cb.failures[destinationID] = 0
	}
}
