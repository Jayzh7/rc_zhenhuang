package store

import (
	"errors"
	"time"
)

var (
	ErrNotFound            = errors.New("notification not found")
	ErrDestinationNotFound = errors.New("destination not found")
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with an existing request")
	ErrNoJob               = errors.New("no delivery job available")
	ErrLeaseLost           = errors.New("delivery lease lost")
)

type Submission struct {
	CallerID       string
	IdempotencyKey string
	DestinationID  string
	ContentType    string
	Body           []byte
	RequestHash    []byte
}

type Notification struct {
	ID                 string     `json:"id"`
	DestinationID      string     `json:"destinationId"`
	DestinationVersion int        `json:"destinationVersion"`
	Status             string     `json:"status"`
	AttemptCount       int        `json:"attemptCount"`
	MaxAttempts        int        `json:"maxAttempts"`
	NextAttemptAt      time.Time  `json:"nextAttemptAt"`
	LastStatusCode     *int       `json:"lastStatusCode,omitempty"`
	LastErrorCode      *string    `json:"lastErrorCode,omitempty"`
	LastErrorMessage   *string    `json:"lastErrorMessage,omitempty"`
	CreatedAt          time.Time  `json:"createdAt"`
	UpdatedAt          time.Time  `json:"updatedAt"`
	DeliveredAt        *time.Time `json:"deliveredAt,omitempty"`
}

type DeliveryJob struct {
	ID                 string
	CallerID           string
	IdempotencyKey     string
	DestinationID      string
	DestinationVersion int
	TargetURL          string
	Method             string
	HeadersJSON        []byte
	SecretHeaderName   *string
	SecretEnvKey       *string
	ContentType        string
	Body               []byte
	Timeout            time.Duration
	AttemptCount       int
	MaxAttempts        int
	LeaseToken         string
	AttemptID          int64
}

type CompletionKind string

const (
	CompletionSucceeded CompletionKind = "succeeded"
	CompletionRetry     CompletionKind = "retry"
	CompletionDead      CompletionKind = "dead"
)

type Completion struct {
	NotificationID string
	AttemptID      int64
	LeaseToken     string
	Kind           CompletionKind
	NextAttemptAt  time.Time
	HTTPStatus     *int
	ErrorCode      string
	ErrorMessage   string
}
