package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"rc-notifier/internal/identifier"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) Submit(ctx context.Context, input Submission) (Notification, bool, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Notification{}, false, fmt.Errorf("begin submission transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	existing, existingHash, err := getByIdempotencyKey(ctx, tx, input.CallerID, input.IdempotencyKey)
	switch {
	case err == nil:
		if !bytes.Equal(existingHash, input.RequestHash) {
			return Notification{}, false, ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return Notification{}, false, fmt.Errorf("commit idempotent read: %w", err)
		}
		return existing, false, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return Notification{}, false, fmt.Errorf("look up idempotency key: %w", err)
	}

	var destinationVersion int
	var targetURL, method, headersJSON string
	var secretHeaderName, secretEnvKey *string
	var timeoutMS, maxAttempts int
	err = tx.QueryRow(ctx, `
		SELECT version, url, method, headers::text, secret_header_name, secret_env_key,
		       timeout_ms, max_attempts
		FROM destinations
		WHERE id = $1 AND active = true
		ORDER BY version DESC
		LIMIT 1
	`, input.DestinationID).Scan(
		&destinationVersion,
		&targetURL,
		&method,
		&headersJSON,
		&secretHeaderName,
		&secretEnvKey,
		&timeoutMS,
		&maxAttempts,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Notification{}, false, ErrDestinationNotFound
	}
	if err != nil {
		return Notification{}, false, fmt.Errorf("load destination: %w", err)
	}

	id, err := identifier.New()
	if err != nil {
		return Notification{}, false, err
	}

	inserted, _, err := scanStoredNotification(tx.QueryRow(ctx, `
		INSERT INTO notifications (
			id, caller_id, idempotency_key, request_hash,
			destination_id, destination_version, target_url, method, headers,
			secret_header_name, secret_env_key, content_type, body,
			timeout_ms, max_attempts
		)
		VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8, $9::jsonb,
			$10, $11, $12, $13,
			$14, $15
		)
		ON CONFLICT (caller_id, idempotency_key) DO NOTHING
		RETURNING
			id, destination_id, destination_version, status,
			attempt_count, max_attempts, next_attempt_at,
			last_status_code, last_error_code, last_error_message,
			created_at, updated_at, delivered_at, request_hash
	`,
		id,
		input.CallerID,
		input.IdempotencyKey,
		input.RequestHash,
		input.DestinationID,
		destinationVersion,
		targetURL,
		method,
		headersJSON,
		secretHeaderName,
		secretEnvKey,
		input.ContentType,
		input.Body,
		timeoutMS,
		maxAttempts,
	))
	switch {
	case err == nil:
		if err := tx.Commit(ctx); err != nil {
			return Notification{}, false, fmt.Errorf("commit notification: %w", err)
		}
		return inserted, true, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return Notification{}, false, fmt.Errorf("insert notification: %w", err)
	}

	existing, existingHash, err = getByIdempotencyKey(ctx, tx, input.CallerID, input.IdempotencyKey)
	if err != nil {
		return Notification{}, false, fmt.Errorf("load concurrent notification: %w", err)
	}
	if !bytes.Equal(existingHash, input.RequestHash) {
		return Notification{}, false, ErrIdempotencyConflict
	}
	if err := tx.Commit(ctx); err != nil {
		return Notification{}, false, fmt.Errorf("commit concurrent idempotent read: %w", err)
	}
	return existing, false, nil
}

func (s *Store) GetNotification(ctx context.Context, id, callerID string) (Notification, error) {
	notification, _, err := scanStoredNotification(s.pool.QueryRow(ctx, `
		SELECT
			id, destination_id, destination_version, status,
			attempt_count, max_attempts, next_attempt_at,
			last_status_code, last_error_code, last_error_message,
			created_at, updated_at, delivered_at, request_hash
		FROM notifications
		WHERE id = $1 AND caller_id = $2
	`, id, callerID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Notification{}, ErrNotFound
	}
	if err != nil {
		return Notification{}, fmt.Errorf("get notification: %w", err)
	}
	return notification, nil
}

func (s *Store) ClaimNext(ctx context.Context, workerID string, baseLease time.Duration) (*DeliveryJob, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin claim transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var exhaustedID string
	err = tx.QueryRow(ctx, `
		SELECT id
		FROM notifications
		WHERE status = 'processing'
		  AND lease_until <= now()
		  AND attempt_count >= max_attempts
		ORDER BY lease_until
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`).Scan(&exhaustedID)
	if err == nil {
		if _, err := tx.Exec(ctx, `
			UPDATE notifications
			SET status = 'dead',
			    lease_owner = NULL,
			    lease_token = NULL,
			    lease_until = NULL,
			    last_error_code = 'lease_expired',
			    last_error_message = 'worker lease expired during the final attempt',
			    updated_at = now()
			WHERE id = $1
		`, exhaustedID); err != nil {
			return nil, fmt.Errorf("dead-letter exhausted lease: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE notification_attempts
			SET finished_at = now(),
			    outcome = 'lease_expired',
			    error_code = 'lease_expired',
			    error_message = 'worker lease expired before recording an outcome'
			WHERE notification_id = $1 AND finished_at IS NULL
		`, exhaustedID); err != nil {
			return nil, fmt.Errorf("close exhausted attempt: %w", err)
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("find exhausted lease: %w", err)
	}

	var id, previousStatus string
	var timeoutMS int
	err = tx.QueryRow(ctx, `
		SELECT id, status, timeout_ms
		FROM notifications
		WHERE (
			(
				status = 'pending'
				AND next_attempt_at <= now()
			) OR (
				status = 'processing'
				AND lease_until <= now()
			)
		)
		  AND attempt_count < max_attempts
		ORDER BY
			CASE
				WHEN status = 'processing' THEN lease_until
				ELSE next_attempt_at
			END,
			created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`).Scan(&id, &previousStatus, &timeoutMS)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit empty claim: %w", err)
		}
		return nil, ErrNoJob
	}
	if err != nil {
		return nil, fmt.Errorf("select delivery job: %w", err)
	}

	if previousStatus == "processing" {
		if _, err := tx.Exec(ctx, `
			UPDATE notification_attempts
			SET finished_at = now(),
			    outcome = 'lease_expired',
			    error_code = 'lease_expired',
			    error_message = 'worker lease expired before recording an outcome'
			WHERE notification_id = $1 AND finished_at IS NULL
		`, id); err != nil {
			return nil, fmt.Errorf("close expired attempt: %w", err)
		}
	}

	leaseToken, err := identifier.New()
	if err != nil {
		return nil, err
	}
	effectiveLease := baseLease
	minimumLease := time.Duration(timeoutMS)*time.Millisecond + 5*time.Second
	if effectiveLease < minimumLease {
		effectiveLease = minimumLease
	}

	job := DeliveryJob{
		ID:         id,
		LeaseToken: leaseToken,
	}
	var headersJSON string
	err = tx.QueryRow(ctx, `
		UPDATE notifications
		SET status = 'processing',
		    attempt_count = attempt_count + 1,
		    lease_owner = $2,
		    lease_token = $3,
		    lease_until = now() + ($4::bigint * interval '1 millisecond'),
		    updated_at = now()
		WHERE id = $1
		RETURNING
			caller_id, idempotency_key, destination_id, destination_version,
			target_url, method, headers::text, secret_header_name, secret_env_key,
			content_type, body, timeout_ms, attempt_count, max_attempts
	`,
		id,
		workerID,
		leaseToken,
		effectiveLease.Milliseconds(),
	).Scan(
		&job.CallerID,
		&job.IdempotencyKey,
		&job.DestinationID,
		&job.DestinationVersion,
		&job.TargetURL,
		&job.Method,
		&headersJSON,
		&job.SecretHeaderName,
		&job.SecretEnvKey,
		&job.ContentType,
		&job.Body,
		&timeoutMS,
		&job.AttemptCount,
		&job.MaxAttempts,
	)
	if err != nil {
		return nil, fmt.Errorf("lease delivery job: %w", err)
	}
	job.HeadersJSON = []byte(headersJSON)
	job.Timeout = time.Duration(timeoutMS) * time.Millisecond

	if err := tx.QueryRow(ctx, `
		INSERT INTO notification_attempts (notification_id, attempt_no, lease_token)
		VALUES ($1, $2, $3)
		RETURNING id
	`, job.ID, job.AttemptCount, leaseToken).Scan(&job.AttemptID); err != nil {
		return nil, fmt.Errorf("record delivery attempt: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit delivery claim: %w", err)
	}
	return &job, nil
}

func (s *Store) CompleteAttempt(ctx context.Context, completion Completion) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin completion transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	message := truncate(completion.ErrorMessage, 1000)
	var commandTag pgconn.CommandTag

	switch completion.Kind {
	case CompletionSucceeded:
		commandTag, err = tx.Exec(ctx, `
			UPDATE notifications
			SET status = 'succeeded',
			    lease_owner = NULL,
			    lease_token = NULL,
			    lease_until = NULL,
			    last_status_code = $3,
			    last_error_code = NULL,
			    last_error_message = NULL,
			    delivered_at = now(),
			    updated_at = now()
			WHERE id = $1 AND status = 'processing' AND lease_token = $2
		`, completion.NotificationID, completion.LeaseToken, completion.HTTPStatus)
	case CompletionRetry:
		commandTag, err = tx.Exec(ctx, `
			UPDATE notifications
			SET status = 'pending',
			    lease_owner = NULL,
			    lease_token = NULL,
			    lease_until = NULL,
			    next_attempt_at = $3,
			    last_status_code = $4,
			    last_error_code = $5,
			    last_error_message = $6,
			    updated_at = now()
			WHERE id = $1 AND status = 'processing' AND lease_token = $2
		`,
			completion.NotificationID,
			completion.LeaseToken,
			completion.NextAttemptAt,
			completion.HTTPStatus,
			completion.ErrorCode,
			message,
		)
	case CompletionDead:
		commandTag, err = tx.Exec(ctx, `
			UPDATE notifications
			SET status = 'dead',
			    lease_owner = NULL,
			    lease_token = NULL,
			    lease_until = NULL,
			    last_status_code = $3,
			    last_error_code = $4,
			    last_error_message = $5,
			    updated_at = now()
			WHERE id = $1 AND status = 'processing' AND lease_token = $2
		`,
			completion.NotificationID,
			completion.LeaseToken,
			completion.HTTPStatus,
			completion.ErrorCode,
			message,
		)
	default:
		return fmt.Errorf("unsupported completion kind %q", completion.Kind)
	}
	if err != nil {
		return fmt.Errorf("update notification outcome: %w", err)
	}
	if commandTag.RowsAffected() != 1 {
		return ErrLeaseLost
	}

	attemptOutcome := string(completion.Kind)
	if completion.Kind == CompletionRetry {
		attemptOutcome = "retry_scheduled"
	}
	attemptTag, err := tx.Exec(ctx, `
		UPDATE notification_attempts
		SET finished_at = now(),
		    outcome = $3,
		    http_status = $4,
		    error_code = NULLIF($5, ''),
		    error_message = NULLIF($6, '')
		WHERE id = $1 AND lease_token = $2
	`,
		completion.AttemptID,
		completion.LeaseToken,
		attemptOutcome,
		completion.HTTPStatus,
		completion.ErrorCode,
		message,
	)
	if err != nil {
		return fmt.Errorf("complete delivery attempt: %w", err)
	}
	if attemptTag.RowsAffected() != 1 {
		return fmt.Errorf("complete delivery attempt: attempt record not found")
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit delivery outcome: %w", err)
	}
	return nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func getByIdempotencyKey(
	ctx context.Context,
	tx pgx.Tx,
	callerID string,
	idempotencyKey string,
) (Notification, []byte, error) {
	return scanStoredNotification(tx.QueryRow(ctx, `
		SELECT
			id, destination_id, destination_version, status,
			attempt_count, max_attempts, next_attempt_at,
			last_status_code, last_error_code, last_error_message,
			created_at, updated_at, delivered_at, request_hash
		FROM notifications
		WHERE caller_id = $1 AND idempotency_key = $2
	`, callerID, idempotencyKey))
}

func scanStoredNotification(row rowScanner) (Notification, []byte, error) {
	var notification Notification
	var requestHash []byte
	err := row.Scan(
		&notification.ID,
		&notification.DestinationID,
		&notification.DestinationVersion,
		&notification.Status,
		&notification.AttemptCount,
		&notification.MaxAttempts,
		&notification.NextAttemptAt,
		&notification.LastStatusCode,
		&notification.LastErrorCode,
		&notification.LastErrorMessage,
		&notification.CreatedAt,
		&notification.UpdatedAt,
		&notification.DeliveredAt,
		&requestHash,
	)
	return notification, requestHash, err
}

func truncate(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}
