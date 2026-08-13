package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"rc-notifier/internal/store"
)

type fakeRepository struct {
	submit func(context.Context, store.Submission) (store.Notification, bool, error)
	get    func(context.Context, string, string) (store.Notification, error)
	replay func(context.Context, string, string) (store.Notification, error)
	stats  func(context.Context) (store.Stats, error)
	ping   func(context.Context) error
}

func (f fakeRepository) Submit(ctx context.Context, input store.Submission) (store.Notification, bool, error) {
	return f.submit(ctx, input)
}

func (f fakeRepository) GetNotification(ctx context.Context, id, callerID string) (store.Notification, error) {
	if f.get == nil {
		return store.Notification{}, store.ErrNotFound
	}
	return f.get(ctx, id, callerID)
}

func (f fakeRepository) ReplayNotification(ctx context.Context, id, callerID string) (store.Notification, error) {
	if f.replay == nil {
		return store.Notification{}, store.ErrNotFound
	}
	return f.replay(ctx, id, callerID)
}

func (f fakeRepository) GetStats(ctx context.Context) (store.Stats, error) {
	if f.stats == nil {
		return store.Stats{}, nil
	}
	return f.stats(ctx)
}

func (f fakeRepository) Ping(ctx context.Context) error {
	if f.ping == nil {
		return nil
	}
	return f.ping(ctx)
}

func TestSubmitAcceptsDurableNotification(t *testing.T) {
	t.Parallel()

	repository := fakeRepository{
		submit: func(_ context.Context, input store.Submission) (store.Notification, bool, error) {
			if input.CallerID != "orders" {
				t.Fatalf("CallerID = %q", input.CallerID)
			}
			if input.DestinationID != "crm" {
				t.Fatalf("DestinationID = %q", input.DestinationID)
			}
			if input.IdempotencyKey != "payment-123" {
				t.Fatalf("IdempotencyKey = %q", input.IdempotencyKey)
			}
			if input.ContentType != "application/json" {
				t.Fatalf("ContentType = %q", input.ContentType)
			}
			if !bytes.Equal(input.Body, []byte(`{"paid":true}`)) {
				t.Fatalf("Body = %s", input.Body)
			}
			if len(input.RequestHash) != 32 {
				t.Fatalf("RequestHash length = %d", len(input.RequestHash))
			}
			return store.Notification{
				ID:            "notification-1",
				DestinationID: "crm",
				Status:        "pending",
			}, true, nil
		},
	}

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/destinations/crm/notifications",
		bytes.NewBufferString(`{"paid":true}`),
	)
	request.Header.Set("X-Caller-ID", "orders")
	request.Header.Set("Idempotency-Key", "payment-123")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	newTestHandler(repository, 1024).Routes().ServeHTTP(response, request)

	if response.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if location := response.Header().Get("Location"); location != "/v1/notifications/notification-1" {
		t.Fatalf("Location = %q", location)
	}

	var payload struct {
		ID      string `json:"id"`
		Created bool   `json:"created"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.ID != "notification-1" || !payload.Created {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestSubmitRejectsOversizedBody(t *testing.T) {
	t.Parallel()

	repository := fakeRepository{
		submit: func(context.Context, store.Submission) (store.Notification, bool, error) {
			t.Fatal("repository should not be called")
			return store.Notification{}, false, nil
		},
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/destinations/crm/notifications",
		bytes.NewBufferString("12345"),
	)
	request.Header.Set("X-Caller-ID", "orders")
	request.Header.Set("Idempotency-Key", "payment-123")
	response := httptest.NewRecorder()

	newTestHandler(repository, 4).Routes().ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestSubmitReportsIdempotencyConflict(t *testing.T) {
	t.Parallel()

	repository := fakeRepository{
		submit: func(context.Context, store.Submission) (store.Notification, bool, error) {
			return store.Notification{}, false, store.ErrIdempotencyConflict
		},
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/destinations/crm/notifications",
		bytes.NewBufferString("{}"),
	)
	request.Header.Set("X-Caller-ID", "orders")
	request.Header.Set("Idempotency-Key", "payment-123")
	response := httptest.NewRecorder()

	newTestHandler(repository, 1024).Routes().ServeHTTP(response, request)

	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestReadyReportsDatabaseFailure(t *testing.T) {
	t.Parallel()

	repository := fakeRepository{
		submit: func(context.Context, store.Submission) (store.Notification, bool, error) {
			return store.Notification{}, false, nil
		},
		ping: func(context.Context) error {
			return errors.New("database unavailable")
		},
	}
	request := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	response := httptest.NewRecorder()

	newTestHandler(repository, 1024).Routes().ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", response.Code)
	}
}

func TestReplayRequeuesDeadNotification(t *testing.T) {
	t.Parallel()

	repository := fakeRepository{
		replay: func(_ context.Context, id, callerID string) (store.Notification, error) {
			if id != "notif-dead-1" || callerID != "orders" {
				t.Fatalf("unexpected params: id=%s, callerID=%s", id, callerID)
			}
			return store.Notification{
				ID:     "notif-dead-1",
				Status: "pending",
			}, nil
		},
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/notifications/notif-dead-1/replay", nil)
	request.Header.Set("X-Caller-ID", "orders")
	response := httptest.NewRecorder()

	newTestHandler(repository, 1024).Routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var notif store.Notification
	if err := json.NewDecoder(response.Body).Decode(&notif); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if notif.Status != "pending" {
		t.Fatalf("status = %s", notif.Status)
	}
}

func TestReplayRejectsNonDeadNotification(t *testing.T) {
	t.Parallel()

	repository := fakeRepository{
		replay: func(context.Context, string, string) (store.Notification, error) {
			return store.Notification{}, store.ErrNotDead
		},
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/notifications/notif-live/replay", nil)
	request.Header.Set("X-Caller-ID", "orders")
	response := httptest.NewRecorder()

	newTestHandler(repository, 1024).Routes().ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func newTestHandler(repository Repository, maxBodyBytes int64) *Handler {
	return NewHandler(
		repository,
		maxBodyBytes,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}
