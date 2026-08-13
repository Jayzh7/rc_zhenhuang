package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"rc-notifier/internal/delivery"
	"rc-notifier/internal/store"
)

type captureRepository struct {
	completion store.Completion
}

func (*captureRepository) ClaimNext(context.Context, string, time.Duration) (*store.DeliveryJob, error) {
	return nil, store.ErrNoJob
}

func (r *captureRepository) CompleteAttempt(_ context.Context, completion store.Completion) error {
	r.completion = completion
	return nil
}

type fixedDeliverer struct {
	result delivery.Result
}

func (d fixedDeliverer) Deliver(context.Context, *store.DeliveryJob) delivery.Result {
	return d.result
}

type fixedBackoff time.Duration

func (b fixedBackoff) Delay(int, time.Duration) time.Duration {
	return time.Duration(b)
}

func TestProcessSchedulesRetry(t *testing.T) {
	t.Parallel()

	repository := &captureRepository{}
	runner := Runner{
		Repository: repository,
		Deliverer: fixedDeliverer{result: delivery.Result{
			Retryable:    true,
			ErrorCode:    "network_error",
			ErrorMessage: "connection reset",
		}},
		Backoff: fixedBackoff(time.Minute),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	before := time.Now().Add(59 * time.Second)
	runner.process(context.Background(), "worker-1", &store.DeliveryJob{
		ID:           "notification-1",
		AttemptID:    7,
		LeaseToken:   "lease-1",
		AttemptCount: 2,
		MaxAttempts:  4,
	})

	if repository.completion.Kind != store.CompletionRetry {
		t.Fatalf("completion = %+v", repository.completion)
	}
	if repository.completion.NextAttemptAt.Before(before) {
		t.Fatalf("NextAttemptAt = %s", repository.completion.NextAttemptAt)
	}
}

func TestProcessDeadLettersFinalAttempt(t *testing.T) {
	t.Parallel()

	repository := &captureRepository{}
	runner := Runner{
		Repository: repository,
		Deliverer: fixedDeliverer{result: delivery.Result{
			Retryable: true,
			ErrorCode: "timeout",
		}},
		Backoff: fixedBackoff(time.Minute),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	runner.process(context.Background(), "worker-1", &store.DeliveryJob{
		ID:           "notification-1",
		AttemptID:    7,
		LeaseToken:   "lease-1",
		AttemptCount: 4,
		MaxAttempts:  4,
	})

	if repository.completion.Kind != store.CompletionDead {
		t.Fatalf("completion = %+v", repository.completion)
	}
}
