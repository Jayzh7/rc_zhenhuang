package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var requireIntegrationDatabase = flag.Bool(
	"require-integration-database",
	false,
	"fail instead of skipping when TEST_DATABASE_URL is not set",
)

var integrationSchemaSequence atomic.Uint64

type integrationFixture struct {
	ctx             context.Context
	pool            *pgxpool.Pool
	adminPool       *pgxpool.Pool
	applicationName string
	store           *Store
}

type destinationConfig struct {
	ID          string
	Version     int
	Active      bool
	URL         string
	Method      string
	Headers     string
	TimeoutMS   int
	MaxAttempts int
}

type attemptRecord struct {
	AttemptNo  int
	Outcome    string
	HTTPStatus *int
	ErrorCode  *string
}

func TestMigrateIsIdempotent(t *testing.T) {
	fixture := newIntegrationFixture(t)

	if err := Migrate(fixture.ctx, fixture.pool); err != nil {
		t.Fatalf("migrate again: %v", err)
	}

	var count int
	if err := fixture.pool.QueryRow(fixture.ctx, `
		SELECT COUNT(*)
		FROM schema_migrations
		WHERE name = '001_init.sql'
	`).Scan(&count); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if count != 1 {
		t.Fatalf("migration records = %d", count)
	}
}

func TestStoreSubmissionIdempotencyAndCallerIsolation(t *testing.T) {
	fixture := newIntegrationFixture(t)
	insertDestination(t, fixture, defaultDestination("crm"))

	input := defaultSubmission("orders", "payment-1", "crm")
	notification, created, err := fixture.store.Submit(fixture.ctx, input)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if !created || notification.Status != "pending" {
		t.Fatalf("notification = %+v, created = %v", notification, created)
	}

	duplicate, created, err := fixture.store.Submit(fixture.ctx, input)
	if err != nil {
		t.Fatalf("submit duplicate: %v", err)
	}
	if created || duplicate.ID != notification.ID {
		t.Fatalf("duplicate = %+v, created = %v", duplicate, created)
	}

	conflicting := input
	conflicting.RequestHash = bytes.Repeat([]byte{2}, 32)
	if _, _, err := fixture.store.Submit(fixture.ctx, conflicting); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting submission error = %v", err)
	}

	if _, err := fixture.store.GetNotification(fixture.ctx, notification.ID, "other-caller"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-caller lookup error = %v", err)
	}

	missingDestination := defaultSubmission("orders", "payment-2", "missing")
	if _, _, err := fixture.store.Submit(fixture.ctx, missingDestination); !errors.Is(err, ErrDestinationNotFound) {
		t.Fatalf("missing destination error = %v", err)
	}
}

func TestStoreConcurrentSubmissionDeduplicates(t *testing.T) {
	fixture := newIntegrationFixture(t)
	insertDestination(t, fixture, defaultDestination("crm"))

	const submitters = 8
	lockID := time.Now().UnixNano()
	installSubmissionBarrier(t, fixture, lockID)
	releaseBarrier := holdAdvisoryLock(t, fixture, lockID)

	input := defaultSubmission("orders", "concurrent-payment", "crm")
	start := make(chan struct{})
	results := make(chan struct {
		notification Notification
		created      bool
		err          error
	}, submitters)

	var waitGroup sync.WaitGroup
	for index := 0; index < submitters; index++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			notification, created, err := fixture.store.Submit(fixture.ctx, input)
			results <- struct {
				notification Notification
				created      bool
				err          error
			}{notification: notification, created: created, err: err}
		}()
	}

	close(start)
	waitForBlockedQueries(t, fixture, submitters)
	releaseBarrier()
	waitGroup.Wait()
	close(results)

	createdCount := 0
	ids := make(map[string]struct{})
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent submit: %v", result.err)
		}
		if result.created {
			createdCount++
		}
		ids[result.notification.ID] = struct{}{}
	}
	if createdCount != 1 || len(ids) != 1 {
		t.Fatalf("created count = %d, IDs = %v", createdCount, ids)
	}

	var rowCount int
	if err := fixture.pool.QueryRow(fixture.ctx, `
		SELECT COUNT(*)
		FROM notifications
		WHERE caller_id = 'orders' AND idempotency_key = 'concurrent-payment'
	`).Scan(&rowCount); err != nil {
		t.Fatalf("count notifications: %v", err)
	}
	if rowCount != 1 {
		t.Fatalf("notification rows = %d", rowCount)
	}
}

func TestStoreSnapshotsDestinationConfiguration(t *testing.T) {
	fixture := newIntegrationFixture(t)
	v1 := defaultDestination("crm")
	v1.URL = "https://v1.example.com/webhook"
	v1.Headers = `{"X-Destination-Version":"1"}`
	v1.TimeoutMS = 1500
	v1.MaxAttempts = 3
	insertDestination(t, fixture, v1)

	input := defaultSubmission("orders", "snapshot-payment", "crm")
	notification, _, err := fixture.store.Submit(fixture.ctx, input)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	tx, err := fixture.pool.Begin(fixture.ctx)
	if err != nil {
		t.Fatalf("begin destination update: %v", err)
	}
	if _, err := tx.Exec(fixture.ctx, `
		UPDATE destinations
		SET active = false
		WHERE id = 'crm' AND version = 1
	`); err != nil {
		_ = tx.Rollback(fixture.ctx)
		t.Fatalf("disable destination v1: %v", err)
	}
	if _, err := tx.Exec(fixture.ctx, `
		INSERT INTO destinations (
			id, version, active, url, method, headers, timeout_ms, max_attempts
		)
		VALUES (
			'crm', 2, true, 'https://v2.example.com/webhook',
			'PUT', '{"X-Destination-Version":"2"}'::jsonb, 3000, 9
		)
	`); err != nil {
		_ = tx.Rollback(fixture.ctx)
		t.Fatalf("insert destination v2: %v", err)
	}
	if err := tx.Commit(fixture.ctx); err != nil {
		t.Fatalf("commit destination update: %v", err)
	}

	job, err := fixture.store.ClaimNext(fixture.ctx, "snapshot-worker", time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if job.ID != notification.ID ||
		job.DestinationVersion != 1 ||
		job.TargetURL != v1.URL ||
		job.Method != v1.Method ||
		job.Timeout != 1500*time.Millisecond ||
		job.MaxAttempts != 3 {
		t.Fatalf("job did not preserve v1 snapshot: %+v", job)
	}

	var headers map[string]string
	if err := json.Unmarshal(job.HeadersJSON, &headers); err != nil {
		t.Fatalf("decode headers: %v", err)
	}
	if headers["X-Destination-Version"] != "1" {
		t.Fatalf("headers = %v", headers)
	}
}

func TestStoreConcurrentClaimIsExclusive(t *testing.T) {
	fixture := newIntegrationFixture(t)
	insertDestination(t, fixture, defaultDestination("crm"))
	if _, _, err := fixture.store.Submit(
		fixture.ctx,
		defaultSubmission("orders", "claim-payment", "crm"),
	); err != nil {
		t.Fatalf("submit: %v", err)
	}

	lockID := time.Now().UnixNano()
	installClaimBarrier(t, fixture, lockID)
	releaseBarrier := holdAdvisoryLock(t, fixture, lockID)

	start := make(chan struct{})
	results := make(chan struct {
		job *DeliveryJob
		err error
	}, 2)

	for index := 0; index < 2; index++ {
		go func(worker int) {
			<-start
			job, err := fixture.store.ClaimNext(
				fixture.ctx,
				fmt.Sprintf("worker-%d", worker),
				time.Second,
			)
			results <- struct {
				job *DeliveryJob
				err error
			}{job: job, err: err}
		}(index)
	}
	close(start)

	waitForBlockedQueries(t, fixture, 1)
	var emptyResult struct {
		job *DeliveryJob
		err error
	}
	select {
	case emptyResult = <-results:
		if emptyResult.job != nil || !errors.Is(emptyResult.err, ErrNoJob) {
			releaseBarrier()
			t.Fatalf("claim while row locked: job=%+v err=%v", emptyResult.job, emptyResult.err)
		}
	case <-time.After(time.Second):
		releaseBarrier()
		t.Fatal("second claim blocked instead of skipping the locked notification")
	}

	releaseBarrier()
	claimedResult := <-results
	if claimedResult.err != nil || claimedResult.job == nil {
		t.Fatalf("claimed result: job=%+v err=%v", claimedResult.job, claimedResult.err)
	}
}

func TestStoreRetryThenSuccessPersistsAttemptHistory(t *testing.T) {
	fixture := newIntegrationFixture(t)
	insertDestination(t, fixture, defaultDestination("crm"))
	notification, _, err := fixture.store.Submit(
		fixture.ctx,
		defaultSubmission("orders", "retry-payment", "crm"),
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	first, err := fixture.store.ClaimNext(fixture.ctx, "worker-1", time.Second)
	if err != nil {
		t.Fatalf("claim first attempt: %v", err)
	}
	retryStatus := 503
	if err := fixture.store.CompleteAttempt(fixture.ctx, Completion{
		NotificationID: notification.ID,
		AttemptID:      first.AttemptID,
		LeaseToken:     first.LeaseToken,
		Kind:           CompletionRetry,
		NextAttemptAt:  time.Now().Add(time.Hour),
		HTTPStatus:     &retryStatus,
		ErrorCode:      "retryable_status",
		ErrorMessage:   "supplier unavailable",
	}); err != nil {
		t.Fatalf("complete retry: %v", err)
	}

	pending, err := fixture.store.GetNotification(fixture.ctx, notification.ID, "orders")
	if err != nil {
		t.Fatalf("get pending notification: %v", err)
	}
	if pending.Status != "pending" ||
		pending.AttemptCount != 1 ||
		pending.LastStatusCode == nil ||
		*pending.LastStatusCode != retryStatus ||
		pending.LastErrorCode == nil ||
		*pending.LastErrorCode != "retryable_status" {
		t.Fatalf("pending notification = %+v", pending)
	}
	if _, err := fixture.store.ClaimNext(fixture.ctx, "worker-2", time.Second); !errors.Is(err, ErrNoJob) {
		t.Fatalf("claim before retry due = %v", err)
	}

	if _, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE notifications
		SET next_attempt_at = now()
		WHERE id = $1
	`, notification.ID); err != nil {
		t.Fatalf("make retry due: %v", err)
	}

	second, err := fixture.store.ClaimNext(fixture.ctx, "worker-2", time.Second)
	if err != nil {
		t.Fatalf("claim second attempt: %v", err)
	}
	successStatus := 204
	if err := fixture.store.CompleteAttempt(fixture.ctx, Completion{
		NotificationID: notification.ID,
		AttemptID:      second.AttemptID,
		LeaseToken:     second.LeaseToken,
		Kind:           CompletionSucceeded,
		HTTPStatus:     &successStatus,
	}); err != nil {
		t.Fatalf("complete success: %v", err)
	}

	delivered, err := fixture.store.GetNotification(fixture.ctx, notification.ID, "orders")
	if err != nil {
		t.Fatalf("get delivered notification: %v", err)
	}
	if delivered.Status != "succeeded" ||
		delivered.AttemptCount != 2 ||
		delivered.DeliveredAt == nil ||
		delivered.LastErrorCode != nil {
		t.Fatalf("delivered notification = %+v", delivered)
	}

	attempts := loadAttempts(t, fixture, notification.ID)
	assertAttempt(t, attempts, 0, 1, "retry_scheduled", retryStatus, "retryable_status")
	assertAttempt(t, attempts, 1, 2, "succeeded", successStatus, "")
}

func TestStorePermanentFailureDeadLetters(t *testing.T) {
	fixture := newIntegrationFixture(t)
	insertDestination(t, fixture, defaultDestination("crm"))
	notification, _, err := fixture.store.Submit(
		fixture.ctx,
		defaultSubmission("orders", "dead-payment", "crm"),
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	job, err := fixture.store.ClaimNext(fixture.ctx, "worker-1", time.Second)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	status := 400
	if err := fixture.store.CompleteAttempt(fixture.ctx, Completion{
		NotificationID: notification.ID,
		AttemptID:      job.AttemptID,
		LeaseToken:     job.LeaseToken,
		Kind:           CompletionDead,
		HTTPStatus:     &status,
		ErrorCode:      "permanent_status",
		ErrorMessage:   "supplier rejected request",
	}); err != nil {
		t.Fatalf("complete dead: %v", err)
	}

	dead, err := fixture.store.GetNotification(fixture.ctx, notification.ID, "orders")
	if err != nil {
		t.Fatalf("get dead notification: %v", err)
	}
	if dead.Status != "dead" ||
		dead.LastStatusCode == nil ||
		*dead.LastStatusCode != status ||
		dead.LastErrorCode == nil ||
		*dead.LastErrorCode != "permanent_status" {
		t.Fatalf("dead notification = %+v", dead)
	}
	if _, err := fixture.store.ClaimNext(fixture.ctx, "worker-2", time.Second); !errors.Is(err, ErrNoJob) {
		t.Fatalf("claim dead notification = %v", err)
	}

	attempts := loadAttempts(t, fixture, notification.ID)
	assertAttempt(t, attempts, 0, 1, "dead", status, "permanent_status")
}

func TestStoreExpiredLeaseIsReclaimedAndRejectsStaleCompletion(t *testing.T) {
	fixture := newIntegrationFixture(t)
	insertDestination(t, fixture, defaultDestination("crm"))
	notification, _, err := fixture.store.Submit(
		fixture.ctx,
		defaultSubmission("orders", "lease-payment", "crm"),
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	first, err := fixture.store.ClaimNext(fixture.ctx, "worker-1", time.Second)
	if err != nil {
		t.Fatalf("claim first attempt: %v", err)
	}
	expireLease(t, fixture, notification.ID)

	second, err := fixture.store.ClaimNext(fixture.ctx, "worker-2", time.Second)
	if err != nil {
		t.Fatalf("reclaim expired lease: %v", err)
	}
	if second.AttemptCount != 2 || second.LeaseToken == first.LeaseToken {
		t.Fatalf("reclaimed job = %+v", second)
	}

	status := 204
	if err := fixture.store.CompleteAttempt(fixture.ctx, Completion{
		NotificationID: notification.ID,
		AttemptID:      first.AttemptID,
		LeaseToken:     first.LeaseToken,
		Kind:           CompletionSucceeded,
		HTTPStatus:     &status,
	}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale completion error = %v", err)
	}

	if err := fixture.store.CompleteAttempt(fixture.ctx, Completion{
		NotificationID: notification.ID,
		AttemptID:      second.AttemptID,
		LeaseToken:     second.LeaseToken,
		Kind:           CompletionSucceeded,
		HTTPStatus:     &status,
	}); err != nil {
		t.Fatalf("complete reclaimed attempt: %v", err)
	}

	attempts := loadAttempts(t, fixture, notification.ID)
	assertAttempt(t, attempts, 0, 1, "lease_expired", 0, "lease_expired")
	assertAttempt(t, attempts, 1, 2, "succeeded", status, "")
}

func TestStoreExpiredFinalLeaseDeadLetters(t *testing.T) {
	fixture := newIntegrationFixture(t)
	destination := defaultDestination("crm")
	destination.MaxAttempts = 1
	insertDestination(t, fixture, destination)
	notification, _, err := fixture.store.Submit(
		fixture.ctx,
		defaultSubmission("orders", "final-lease-payment", "crm"),
	)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	if _, err := fixture.store.ClaimNext(fixture.ctx, "worker-1", time.Second); err != nil {
		t.Fatalf("claim: %v", err)
	}
	expireLease(t, fixture, notification.ID)

	if _, err := fixture.store.ClaimNext(fixture.ctx, "worker-2", time.Second); !errors.Is(err, ErrNoJob) {
		t.Fatalf("claim after final lease expiry = %v", err)
	}

	dead, err := fixture.store.GetNotification(fixture.ctx, notification.ID, "orders")
	if err != nil {
		t.Fatalf("get dead notification: %v", err)
	}
	if dead.Status != "dead" ||
		dead.LastErrorCode == nil ||
		*dead.LastErrorCode != "lease_expired" {
		t.Fatalf("dead notification = %+v", dead)
	}

	attempts := loadAttempts(t, fixture, notification.ID)
	assertAttempt(t, attempts, 0, 1, "lease_expired", 0, "lease_expired")
}

func newIntegrationFixture(t *testing.T) integrationFixture {
	t.Helper()

	databaseURL := integrationDatabaseURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	adminPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		cancel()
		t.Fatalf("create admin pool: %v", err)
	}

	schema := fmt.Sprintf(
		"rc_notifier_test_%d_%d",
		os.Getpid(),
		integrationSchemaSequence.Add(1),
	)
	schemaIdentifier := pgx.Identifier{schema}.Sanitize()
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schemaIdentifier); err != nil {
		adminPool.Close()
		cancel()
		t.Fatalf("create test schema: %v", err)
	}

	var pool *pgxpool.Pool
	t.Cleanup(func() {
		if pool != nil {
			pool.Close()
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := adminPool.Exec(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+schemaIdentifier+" CASCADE",
		); err != nil {
			t.Errorf("drop test schema: %v", err)
		}
		adminPool.Close()
		cancel()
	})

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	config.MaxConns = 16
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	config.ConnConfig.RuntimeParams["application_name"] = schema
	pool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create isolated pool: %v", err)
	}
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	return integrationFixture{
		ctx:             ctx,
		pool:            pool,
		adminPool:       adminPool,
		applicationName: schema,
		store:           New(pool),
	}
}

func installSubmissionBarrier(t *testing.T, fixture integrationFixture, lockID int64) {
	t.Helper()

	_, err := fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`
		CREATE FUNCTION block_notification_insert() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(%d);
			RETURN NEW;
		END
		$$;

		CREATE TRIGGER block_notification_insert
		BEFORE INSERT ON notifications
		FOR EACH ROW EXECUTE FUNCTION block_notification_insert()
	`, lockID))
	if err != nil {
		t.Fatalf("install submission barrier: %v", err)
	}
}

func installClaimBarrier(t *testing.T, fixture integrationFixture, lockID int64) {
	t.Helper()

	_, err := fixture.pool.Exec(fixture.ctx, fmt.Sprintf(`
		CREATE FUNCTION block_notification_claim() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(%d);
			RETURN NEW;
		END
		$$;

		CREATE TRIGGER block_notification_claim
		BEFORE UPDATE ON notifications
		FOR EACH ROW
		WHEN (OLD.status = 'pending' AND NEW.status = 'processing')
		EXECUTE FUNCTION block_notification_claim()
	`, lockID))
	if err != nil {
		t.Fatalf("install claim barrier: %v", err)
	}
}

func holdAdvisoryLock(t *testing.T, fixture integrationFixture, lockID int64) func() {
	t.Helper()

	conn, err := fixture.pool.Acquire(fixture.ctx)
	if err != nil {
		t.Fatalf("acquire barrier connection: %v", err)
	}
	if _, err := conn.Exec(fixture.ctx, "SELECT pg_advisory_lock($1)", lockID); err != nil {
		conn.Release()
		t.Fatalf("acquire advisory barrier: %v", err)
	}

	var once sync.Once
	release := func() {
		once.Do(func() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var unlocked bool
			if err := conn.QueryRow(
				releaseCtx,
				"SELECT pg_advisory_unlock($1)",
				lockID,
			).Scan(&unlocked); err != nil {
				t.Errorf("release advisory barrier: %v", err)
			} else if !unlocked {
				t.Errorf("advisory barrier %d was not held", lockID)
			}
			conn.Release()
		})
	}
	t.Cleanup(release)
	return release
}

func waitForBlockedQueries(t *testing.T, fixture integrationFixture, expected int) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var blocked int
		if err := fixture.adminPool.QueryRow(fixture.ctx, `
			SELECT COUNT(*)
			FROM pg_stat_activity
			WHERE application_name = $1
			  AND wait_event_type = 'Lock'
		`, fixture.applicationName).Scan(&blocked); err != nil {
			t.Fatalf("count blocked queries: %v", err)
		}
		if blocked >= expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("did not observe %d concurrently blocked queries", expected)
}

func integrationDatabaseURL(t *testing.T) string {
	t.Helper()

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL != "" {
		return databaseURL
	}
	if *requireIntegrationDatabase {
		t.Fatal("TEST_DATABASE_URL is required")
	}
	t.Skip("TEST_DATABASE_URL is not set")
	return ""
}

func defaultDestination(id string) destinationConfig {
	return destinationConfig{
		ID:          id,
		Version:     1,
		Active:      true,
		URL:         "https://example.com/webhook",
		Method:      "POST",
		Headers:     `{}`,
		TimeoutMS:   1000,
		MaxAttempts: 3,
	}
}

func insertDestination(t *testing.T, fixture integrationFixture, destination destinationConfig) {
	t.Helper()

	if _, err := fixture.pool.Exec(fixture.ctx, `
		INSERT INTO destinations (
			id, version, active, url, method, headers, timeout_ms, max_attempts
		)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
	`,
		destination.ID,
		destination.Version,
		destination.Active,
		destination.URL,
		destination.Method,
		destination.Headers,
		destination.TimeoutMS,
		destination.MaxAttempts,
	); err != nil {
		t.Fatalf("insert destination: %v", err)
	}
}

func defaultSubmission(callerID, idempotencyKey, destinationID string) Submission {
	body := []byte(`{"paid":true}`)
	return Submission{
		CallerID:       callerID,
		IdempotencyKey: idempotencyKey,
		DestinationID:  destinationID,
		ContentType:    "application/json",
		Body:           body,
		RequestHash:    bytes.Repeat([]byte{1}, 32),
	}
}

func expireLease(t *testing.T, fixture integrationFixture, notificationID string) {
	t.Helper()

	if _, err := fixture.pool.Exec(fixture.ctx, `
		UPDATE notifications
		SET lease_until = now() - interval '1 second'
		WHERE id = $1
	`, notificationID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
}

func loadAttempts(t *testing.T, fixture integrationFixture, notificationID string) []attemptRecord {
	t.Helper()

	rows, err := fixture.pool.Query(fixture.ctx, `
		SELECT attempt_no, outcome, http_status, error_code
		FROM notification_attempts
		WHERE notification_id = $1
		ORDER BY attempt_no
	`, notificationID)
	if err != nil {
		t.Fatalf("query attempts: %v", err)
	}
	defer rows.Close()

	var attempts []attemptRecord
	for rows.Next() {
		var attempt attemptRecord
		if err := rows.Scan(
			&attempt.AttemptNo,
			&attempt.Outcome,
			&attempt.HTTPStatus,
			&attempt.ErrorCode,
		); err != nil {
			t.Fatalf("scan attempt: %v", err)
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read attempts: %v", err)
	}
	return attempts
}

func assertAttempt(
	t *testing.T,
	attempts []attemptRecord,
	index int,
	attemptNo int,
	outcome string,
	status int,
	errorCode string,
) {
	t.Helper()

	if len(attempts) <= index {
		t.Fatalf("attempt %d missing from %+v", index, attempts)
	}
	attempt := attempts[index]
	if attempt.AttemptNo != attemptNo || attempt.Outcome != outcome {
		t.Fatalf("attempt %d = %+v", index, attempt)
	}
	if status == 0 {
		if attempt.HTTPStatus != nil {
			t.Fatalf("attempt %d HTTP status = %v", index, *attempt.HTTPStatus)
		}
	} else if attempt.HTTPStatus == nil || *attempt.HTTPStatus != status {
		t.Fatalf("attempt %d HTTP status = %v", index, attempt.HTTPStatus)
	}
	if errorCode == "" {
		if attempt.ErrorCode != nil {
			t.Fatalf("attempt %d error code = %q", index, *attempt.ErrorCode)
		}
	} else if attempt.ErrorCode == nil || *attempt.ErrorCode != errorCode {
		t.Fatalf("attempt %d error code = %v", index, attempt.ErrorCode)
	}
}
