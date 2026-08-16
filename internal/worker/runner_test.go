package worker

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
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

type recordingBackoff struct {
	attempt    int
	retryAfter time.Duration
	delay      time.Duration
}

func (b *recordingBackoff) Delay(attempt int, retryAfter time.Duration) time.Duration {
	b.attempt = attempt
	b.retryAfter = retryAfter
	return b.delay
}

type oneJobRepository struct {
	claimed    atomic.Bool
	job        *store.DeliveryJob
	completion chan store.Completion
}

func (r *oneJobRepository) ClaimNext(context.Context, string, time.Duration) (*store.DeliveryJob, error) {
	if r.claimed.CompareAndSwap(false, true) {
		return r.job, nil
	}
	return nil, store.ErrNoJob
}

func (r *oneJobRepository) CompleteAttempt(ctx context.Context, completion store.Completion) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.completion <- completion
	return nil
}

type blockingDeliverer struct {
	started chan struct{}
	release chan struct{}
}

func (d blockingDeliverer) Deliver(ctx context.Context, _ *store.DeliveryJob) delivery.Result {
	close(d.started)
	<-d.release
	if err := ctx.Err(); err != nil {
		return delivery.Result{
			ErrorCode:    "context_canceled",
			ErrorMessage: err.Error(),
		}
	}
	status := 204
	return delivery.Result{Success: true, StatusCode: &status}
}

func TestProcessSchedulesRetry(t *testing.T) {
	t.Parallel()

	repository := &captureRepository{}
	backoff := &recordingBackoff{
		delay: time.Minute,
	}
	runner := Runner{
		Repository: repository,
		Deliverer: fixedDeliverer{result: delivery.Result{
			Retryable:    true,
			ErrorCode:    "network_error",
			ErrorMessage: "connection reset",
			RetryAfter:   30 * time.Second,
		}},
		Backoff: backoff,
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
	if backoff.attempt != 2 || backoff.retryAfter != 30*time.Second {
		t.Fatalf("backoff attempt = %d, Retry-After = %s", backoff.attempt, backoff.retryAfter)
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

func TestProcessRecordsSuccessAndPermanentFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		result   delivery.Result
		wantKind store.CompletionKind
	}{
		{
			name: "success",
			result: delivery.Result{
				Success: true,
			},
			wantKind: store.CompletionSucceeded,
		},
		{
			name: "permanent failure",
			result: delivery.Result{
				ErrorCode: "permanent_status",
			},
			wantKind: store.CompletionDead,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			repository := &captureRepository{}
			runner := Runner{
				Repository: repository,
				Deliverer:  fixedDeliverer{result: test.result},
				Backoff:    fixedBackoff(time.Minute),
				Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
			}
			runner.process(context.Background(), "worker-1", &store.DeliveryJob{
				ID:           "notification-1",
				AttemptID:    7,
				LeaseToken:   "lease-1",
				AttemptCount: 1,
				MaxAttempts:  4,
				Timeout:      time.Second,
			})

			if repository.completion.Kind != test.wantKind {
				t.Fatalf("completion = %+v", repository.completion)
			}
		})
	}
}

func TestRunWaitsForInFlightDeliveryDuringShutdown(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	repository := &oneJobRepository{
		job: &store.DeliveryJob{
			ID:           "notification-1",
			AttemptID:    1,
			LeaseToken:   "lease-1",
			AttemptCount: 1,
			MaxAttempts:  3,
			Timeout:      time.Second,
		},
		completion: make(chan store.Completion, 1),
	}
	runner := Runner{
		Repository:    repository,
		Deliverer:     blockingDeliverer{started: started, release: release},
		Backoff:       fixedBackoff(time.Second),
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		WorkerID:      "worker",
		Concurrency:   1,
		PollInterval:  10 * time.Millisecond,
		LeaseDuration: time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx)
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("delivery did not start")
	}
	cancel()

	select {
	case err := <-done:
		t.Fatalf("runner stopped before in-flight delivery completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case completion := <-repository.completion:
		if completion.Kind != store.CompletionSucceeded {
			t.Fatalf("completion = %+v", completion)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery outcome was not recorded")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not stop after in-flight delivery completed")
	}
}

func TestRunRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := (&Runner{}).Run(context.Background()); err == nil {
		t.Fatal("expected missing dependency error")
	}
	if err := (&Runner{
		Repository:  &captureRepository{},
		Deliverer:   fixedDeliverer{},
		Backoff:     fixedBackoff(time.Second),
		Logger:      logger,
		Concurrency: 0,
	}).Run(context.Background()); err == nil {
		t.Fatal("expected invalid concurrency error")
	}
}
