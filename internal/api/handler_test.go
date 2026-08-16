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
	ping   func(context.Context) error
}

func (f fakeRepository) Submit(ctx context.Context, input store.Submission) (store.Notification, bool, error) {
	if f.submit == nil {
		return store.Notification{}, false, errors.New("unexpected Submit call")
	}
	return f.submit(ctx, input)
}

func (f fakeRepository) GetNotification(ctx context.Context, id, callerID string) (store.Notification, error) {
	if f.get == nil {
		return store.Notification{}, store.ErrNotFound
	}
	return f.get(ctx, id, callerID)
}

func (f fakeRepository) Ping(ctx context.Context) error {
	if f.ping == nil {
		return nil
	}
	return f.ping(ctx)
}

func TestSubmitReturnsAcceptedForCreatedNotification(t *testing.T) {
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

func TestSubmitReturnsExistingIdempotentNotification(t *testing.T) {
	t.Parallel()

	repository := fakeRepository{
		submit: func(context.Context, store.Submission) (store.Notification, bool, error) {
			return store.Notification{
				ID:            "notification-1",
				DestinationID: "crm",
				Status:        "pending",
			}, false, nil
		},
	}
	request := newSubmitRequest("/v1/destinations/crm/notifications", "{}")
	response := httptest.NewRecorder()

	newTestHandler(repository, 1024).Routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var payload struct {
		ID      string `json:"id"`
		Created bool   `json:"created"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if payload.ID != "notification-1" || payload.Created {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestSubmitValidatesPublicContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		path        string
		callerID    string
		idempotency string
		contentType string
		wantStatus  int
	}{
		{
			name:        "missing caller",
			path:        "/v1/destinations/crm/notifications",
			idempotency: "payment-123",
			contentType: "application/json",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "invalid destination",
			path:        "/v1/destinations/bad%20destination/notifications",
			callerID:    "orders",
			idempotency: "payment-123",
			contentType: "application/json",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "missing idempotency key",
			path:        "/v1/destinations/crm/notifications",
			callerID:    "orders",
			contentType: "application/json",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "unsafe idempotency key",
			path:        "/v1/destinations/crm/notifications",
			callerID:    "orders",
			idempotency: "payment key",
			contentType: "application/json",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "invalid content type",
			path:        "/v1/destinations/crm/notifications",
			callerID:    "orders",
			idempotency: "payment-123",
			contentType: "not a media type",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			repository := fakeRepository{
				submit: func(context.Context, store.Submission) (store.Notification, bool, error) {
					t.Fatal("repository should not be called")
					return store.Notification{}, false, nil
				},
			}
			request := httptest.NewRequest(http.MethodPost, test.path, bytes.NewBufferString("{}"))
			if test.callerID != "" {
				request.Header.Set("X-Caller-ID", test.callerID)
			}
			if test.idempotency != "" {
				request.Header.Set("Idempotency-Key", test.idempotency)
			}
			if test.contentType != "" {
				request.Header.Set("Content-Type", test.contentType)
			}
			response := httptest.NewRecorder()

			newTestHandler(repository, 1024).Routes().ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
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

func TestSubmitMapsRepositoryFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{
			name:       "destination missing",
			err:        store.ErrDestinationNotFound,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "internal failure",
			err:        errors.New("database unavailable"),
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			repository := fakeRepository{
				submit: func(context.Context, store.Submission) (store.Notification, bool, error) {
					return store.Notification{}, false, test.err
				},
			}
			response := httptest.NewRecorder()

			newTestHandler(repository, 1024).Routes().ServeHTTP(
				response,
				newSubmitRequest("/v1/destinations/crm/notifications", "{}"),
			)

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
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

func TestHealthEndpointsReportSuccess(t *testing.T) {
	t.Parallel()

	handler := newTestHandler(fakeRepository{}, 1024).Routes()
	for _, path := range []string{"/health/live", "/health/ready"} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()

		handler.ServeHTTP(response, request)

		if response.Code != http.StatusOK {
			t.Fatalf("%s status = %d, body = %s", path, response.Code, response.Body.String())
		}
	}
}

func TestGetNotificationContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		get        func(context.Context, string, string) (store.Notification, error)
		wantStatus int
		wantID     string
	}{
		{
			name: "returns caller notification",
			get: func(_ context.Context, id, _ string) (store.Notification, error) {
				return store.Notification{ID: id, Status: "pending"}, nil
			},
			wantStatus: http.StatusOK,
			wantID:     "notification-1",
		},
		{
			name: "not found",
			get: func(context.Context, string, string) (store.Notification, error) {
				return store.Notification{}, store.ErrNotFound
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "internal failure",
			get: func(context.Context, string, string) (store.Notification, error) {
				return store.Notification{}, errors.New("database unavailable")
			},
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			repository := fakeRepository{get: test.get}
			request := httptest.NewRequest(
				http.MethodGet,
				"/v1/notifications/notification-1",
				nil,
			)
			request.Header.Set("X-Caller-ID", "orders")
			response := httptest.NewRecorder()

			newTestHandler(repository, 1024).Routes().ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if test.wantID != "" {
				var notification store.Notification
				if err := json.NewDecoder(response.Body).Decode(&notification); err != nil {
					t.Fatalf("decode response: %v", err)
				}
				if notification.ID != test.wantID {
					t.Fatalf("notification = %+v", notification)
				}
			}

		})
	}
}

func TestGetNotificationPassesCallerIDToRepository(t *testing.T) {
	t.Parallel()

	var gotCallerID string
	repository := fakeRepository{
		get: func(_ context.Context, id, callerID string) (store.Notification, error) {
			gotCallerID = callerID
			return store.Notification{ID: id, Status: "pending"}, nil
		},
	}
	request := httptest.NewRequest(
		http.MethodGet,
		"/v1/notifications/notification-1",
		nil,
	)
	request.Header.Set("X-Caller-ID", "other-caller")
	response := httptest.NewRecorder()

	newTestHandler(repository, 1024).Routes().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if gotCallerID != "other-caller" {
		t.Fatalf("caller ID = %q", gotCallerID)
	}
}

func TestRoutesRejectUnsupportedMethod(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodDelete, "/health/live", nil)
	response := httptest.NewRecorder()

	newTestHandler(fakeRepository{}, 1024).Routes().ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", response.Code)
	}
}

func newSubmitRequest(path, body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	request.Header.Set("X-Caller-ID", "orders")
	request.Header.Set("Idempotency-Key", "payment-123")
	request.Header.Set("Content-Type", "application/json")
	return request
}

func newTestHandler(repository Repository, maxBodyBytes int64) *Handler {
	return NewHandler(
		repository,
		maxBodyBytes,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}
