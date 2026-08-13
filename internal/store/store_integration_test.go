package store

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestStoreLifecycle(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	defer pool.Close()

	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, "TRUNCATE notification_attempts, notifications, destinations"); err != nil {
		t.Fatalf("clear test database: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO destinations (
			id, version, active, url, method, headers, timeout_ms, max_attempts
		)
		VALUES ('crm', 1, true, 'https://example.com/webhook', 'POST', '{}'::jsonb, 1000, 3)
	`); err != nil {
		t.Fatalf("insert destination: %v", err)
	}

	repository := New(pool)
	input := Submission{
		CallerID:       "orders",
		IdempotencyKey: "payment-1",
		DestinationID:  "crm",
		ContentType:    "application/json",
		Body:           []byte(`{"paid":true}`),
		RequestHash:    bytes.Repeat([]byte{1}, 32),
	}

	notification, created, err := repository.Submit(ctx, input)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if !created || notification.Status != "pending" {
		t.Fatalf("notification = %+v, created = %v", notification, created)
	}

	duplicate, created, err := repository.Submit(ctx, input)
	if err != nil {
		t.Fatalf("submit duplicate: %v", err)
	}
	if created || duplicate.ID != notification.ID {
		t.Fatalf("duplicate = %+v, created = %v", duplicate, created)
	}

	conflicting := input
	conflicting.RequestHash = bytes.Repeat([]byte{2}, 32)
	if _, _, err := repository.Submit(ctx, conflicting); err != ErrIdempotencyConflict {
		t.Fatalf("conflicting submission error = %v", err)
	}

	job, err := repository.ClaimNext(ctx, "test-worker", 5*time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if job.ID != notification.ID || job.AttemptCount != 1 {
		t.Fatalf("job = %+v", job)
	}

	status := 204
	if err := repository.CompleteAttempt(ctx, Completion{
		NotificationID: job.ID,
		AttemptID:      job.AttemptID,
		LeaseToken:     job.LeaseToken,
		Kind:           CompletionSucceeded,
		HTTPStatus:     &status,
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}

	delivered, err := repository.GetNotification(ctx, notification.ID, "orders")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if delivered.Status != "succeeded" || delivered.DeliveredAt == nil {
		t.Fatalf("delivered notification = %+v", delivered)
	}
}
