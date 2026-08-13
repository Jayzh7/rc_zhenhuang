package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"rc-notifier/internal/delivery"
	"rc-notifier/internal/store"
)

type Repository interface {
	ClaimNext(context.Context, string, time.Duration) (*store.DeliveryJob, error)
	CompleteAttempt(context.Context, store.Completion) error
}

type Deliverer interface {
	Deliver(context.Context, *store.DeliveryJob) delivery.Result
}

type Backoff interface {
	Delay(int, time.Duration) time.Duration
}

type Runner struct {
	Repository    Repository
	Deliverer     Deliverer
	Backoff       Backoff
	Logger        *slog.Logger
	WorkerID      string
	Concurrency   int
	PollInterval  time.Duration
	LeaseDuration time.Duration
}

func (r *Runner) Run(ctx context.Context) error {
	if r.Repository == nil || r.Deliverer == nil || r.Backoff == nil || r.Logger == nil {
		return fmt.Errorf("worker dependencies are incomplete")
	}
	if r.Concurrency <= 0 {
		return fmt.Errorf("worker concurrency must be positive")
	}

	var waitGroup sync.WaitGroup
	for index := 0; index < r.Concurrency; index++ {
		waitGroup.Add(1)
		go func(workerIndex int) {
			defer waitGroup.Done()
			r.loop(ctx, fmt.Sprintf("%s-%d", r.WorkerID, workerIndex+1))
		}(index)
	}

	<-ctx.Done()
	waitGroup.Wait()
	return nil
}

func (r *Runner) loop(ctx context.Context, workerID string) {
	for ctx.Err() == nil {
		job, err := r.Repository.ClaimNext(ctx, workerID, r.LeaseDuration)
		switch {
		case errors.Is(err, store.ErrNoJob):
			if !sleep(ctx, r.PollInterval) {
				return
			}
			continue
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			r.Logger.Error("claim delivery job", "worker_id", workerID, "error", err)
			if !sleep(ctx, r.PollInterval) {
				return
			}
			continue
		}

		r.process(ctx, workerID, job)
	}
}

func (r *Runner) process(_ context.Context, workerID string, job *store.DeliveryJob) {
	deliverTimeout := job.Timeout + 2*time.Second
	deliverCtx, cancelDeliver := context.WithTimeout(context.Background(), deliverTimeout)
	defer cancelDeliver()

	result := r.Deliverer.Deliver(deliverCtx, job)
	completion := store.Completion{
		NotificationID: job.ID,
		AttemptID:      job.AttemptID,
		LeaseToken:     job.LeaseToken,
		HTTPStatus:     result.StatusCode,
		ErrorCode:      result.ErrorCode,
		ErrorMessage:   result.ErrorMessage,
	}

	switch {
	case result.Success:
		completion.Kind = store.CompletionSucceeded
	case result.Retryable && job.AttemptCount < job.MaxAttempts:
		completion.Kind = store.CompletionRetry
		completion.NextAttemptAt = time.Now().Add(r.Backoff.Delay(job.AttemptCount, result.RetryAfter))
	default:
		completion.Kind = store.CompletionDead
	}

	completeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.Repository.CompleteAttempt(completeCtx, completion); err != nil {
		if errors.Is(err, store.ErrLeaseLost) {
			r.Logger.Warn("delivery outcome ignored after lease loss",
				"worker_id", workerID,
				"notification_id", job.ID,
				"attempt", job.AttemptCount,
			)
			return
		}
		r.Logger.Error("record delivery outcome",
			"worker_id", workerID,
			"notification_id", job.ID,
			"attempt", job.AttemptCount,
			"error", err,
		)
		return
	}

	r.Logger.Info("delivery attempt completed",
		"worker_id", workerID,
		"notification_id", job.ID,
		"destination_id", job.DestinationID,
		"attempt", job.AttemptCount,
		"outcome", completion.Kind,
		"status_code", result.StatusCode,
	)
}

func sleep(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
