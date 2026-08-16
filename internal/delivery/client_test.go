package delivery

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"rc-notifier/internal/store"
)

type staticSecrets map[string]string

func (s staticSecrets) Get(_ context.Context, name string) (string, error) {
	return s[name], nil
}

type failingSecrets struct {
	err error
}

func (s failingSecrets) Get(context.Context, string) (string, error) {
	return "", s.err
}

func TestClientDeliversOpaqueRequest(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if string(body) != `{"paid":true}` {
			t.Errorf("body = %s", body)
		}
		if value := r.Header.Get("X-Static"); value != "configured" {
			t.Errorf("X-Static = %q", value)
		}
		if value := r.Header.Get("Authorization"); value != "Bearer test" {
			t.Errorf("Authorization = %q", value)
		}
		if value := r.Header.Get("Idempotency-Key"); value != "payment-123" {
			t.Errorf("Idempotency-Key = %q", value)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	secretHeader := "Authorization"
	secretKey := "CRM_TOKEN"
	client := NewClient(true, staticSecrets{"CRM_TOKEN": "Bearer test"})
	defer client.CloseIdleConnections()

	result := client.Deliver(context.Background(), &store.DeliveryJob{
		TargetURL:        server.URL,
		Method:           http.MethodPost,
		HeadersJSON:      []byte(`{"X-Static":"configured"}`),
		SecretHeaderName: &secretHeader,
		SecretEnvKey:     &secretKey,
		ContentType:      "application/json",
		Body:             []byte(`{"paid":true}`),
		IdempotencyKey:   "payment-123",
		Timeout:          time.Second,
	})

	if !result.Success || result.StatusCode == nil || *result.StatusCode != http.StatusNoContent {
		t.Fatalf("result = %+v", result)
	}
}

func TestClientClassifiesResponses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		status    int
		retryable bool
	}{
		{name: "bad request", status: http.StatusBadRequest, retryable: false},
		{name: "request timeout", status: http.StatusRequestTimeout, retryable: true},
		{name: "too early", status: http.StatusTooEarly, retryable: true},
		{name: "too many requests", status: http.StatusTooManyRequests, retryable: true},
		{name: "server error", status: http.StatusServiceUnavailable, retryable: true},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if test.status == http.StatusTooManyRequests {
					w.Header().Set("Retry-After", "2")
				}
				w.WriteHeader(test.status)
			}))
			defer server.Close()

			client := NewClient(true, staticSecrets{})
			defer client.CloseIdleConnections()
			result := client.Deliver(context.Background(), baseJob(server.URL))

			if result.Success {
				t.Fatalf("unexpected success: %+v", result)
			}
			if result.Retryable != test.retryable {
				t.Fatalf("retryable = %v, result = %+v", result.Retryable, result)
			}
			if test.status == http.StatusTooManyRequests && result.RetryAfter != 2*time.Second {
				t.Fatalf("RetryAfter = %s", result.RetryAfter)
			}
		})
	}
}

func TestClientClassifiesTimeoutAndNetworkFailureAsRetryable(t *testing.T) {
	t.Parallel()

	t.Run("timeout", func(t *testing.T) {
		t.Parallel()

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(200 * time.Millisecond)
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()

		client := NewClient(true, staticSecrets{})
		defer client.CloseIdleConnections()
		job := baseJob(server.URL)
		job.Timeout = 50 * time.Millisecond

		result := client.Deliver(context.Background(), job)
		if !result.Retryable || result.ErrorCode != "timeout" {
			t.Fatalf("result = %+v", result)
		}
	})

	t.Run("network failure", func(t *testing.T) {
		t.Parallel()

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		address := listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatalf("close listener: %v", err)
		}

		client := NewClient(true, staticSecrets{})
		defer client.CloseIdleConnections()
		result := client.Deliver(context.Background(), baseJob("http://"+address))

		if !result.Retryable || result.ErrorCode != "network_error" {
			t.Fatalf("result = %+v", result)
		}
	})
}

func TestClientClassifiesCanceledRequest(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client := NewClient(true, staticSecrets{})
	defer client.CloseIdleConnections()
	result := client.Deliver(ctx, baseJob(server.URL))

	if !result.Retryable || result.ErrorCode != "request_canceled" {
		t.Fatalf("result = %+v", result)
	}
}

func TestClientDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()

	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	client := NewClient(true, staticSecrets{})
	defer client.CloseIdleConnections()
	result := client.Deliver(context.Background(), baseJob(redirect.URL))

	if result.Success || result.Retryable || result.StatusCode == nil || *result.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("result = %+v", result)
	}
	if targetCalls.Load() != 0 {
		t.Fatalf("redirect target calls = %d", targetCalls.Load())
	}
}

func TestClientRejectsHostnameResolvingToPrivateAddress(t *testing.T) {
	t.Parallel()

	client := NewClient(false, staticSecrets{})
	defer client.CloseIdleConnections()
	result := client.Deliver(context.Background(), baseJob("http://localhost:8080/webhook"))

	if result.Success || result.Retryable || result.ErrorCode != "invalid_destination" {
		t.Fatalf("result = %+v", result)
	}
}

func TestClientRejectsPrivateLiteralAddressByDefault(t *testing.T) {
	t.Parallel()

	client := NewClient(false, staticSecrets{})
	defer client.CloseIdleConnections()
	result := client.Deliver(context.Background(), baseJob("http://127.0.0.1:8080/webhook"))

	if result.Success || result.Retryable || result.ErrorCode != "invalid_destination" {
		t.Fatalf("result = %+v", result)
	}
}

func TestDecodeHeadersRejectsManagedHeaders(t *testing.T) {
	t.Parallel()

	_, err := decodeHeaders([]byte(`{"Content-Type":"application/json"}`))
	if err == nil || !strings.Contains(err.Error(), "managed") {
		t.Fatalf("error = %v", err)
	}
}

func TestDecodeHeadersRejectsSensitiveStaticHeader(t *testing.T) {
	t.Parallel()

	_, err := decodeHeaders([]byte(`{"Authorization":"Bearer secret"}`))
	if err == nil || !strings.Contains(err.Error(), "secret provider") {
		t.Fatalf("error = %v", err)
	}
}

func TestClientRejectsStaticAndSecretHeaderCollision(t *testing.T) {
	t.Parallel()

	secretHeader := "X-Credential"
	secretKey := "CREDENTIAL"
	client := NewClient(true, staticSecrets{"CREDENTIAL": "secret"})
	defer client.CloseIdleConnections()

	job := baseJob("http://127.0.0.1:1/webhook")
	job.HeadersJSON = []byte(`{"x-credential":"static"}`)
	job.SecretHeaderName = &secretHeader
	job.SecretEnvKey = &secretKey

	result := client.Deliver(context.Background(), job)
	if result.ErrorCode != "invalid_secret_header" || result.Retryable {
		t.Fatalf("result = %+v", result)
	}
}

func TestClientRejectsInvalidDestinationConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configure func(*store.DeliveryJob)
		client    *Client
		errorCode string
	}{
		{
			name: "nonpositive timeout",
			configure: func(job *store.DeliveryJob) {
				job.Timeout = 0
			},
			client:    NewClient(true, staticSecrets{}),
			errorCode: "invalid_timeout",
		},
		{
			name: "incomplete secret configuration",
			configure: func(job *store.DeliveryJob) {
				header := "Authorization"
				job.SecretHeaderName = &header
			},
			client:    NewClient(true, staticSecrets{}),
			errorCode: "invalid_secret_configuration",
		},
		{
			name: "missing secret provider",
			configure: func(job *store.DeliveryJob) {
				header := "Authorization"
				key := "AUTH_TOKEN"
				job.SecretHeaderName = &header
				job.SecretEnvKey = &key
			},
			client:    NewClient(true, nil),
			errorCode: "missing_secret_provider",
		},
		{
			name: "secret lookup failure",
			configure: func(job *store.DeliveryJob) {
				header := "Authorization"
				key := "AUTH_TOKEN"
				job.SecretHeaderName = &header
				job.SecretEnvKey = &key
			},
			client: NewClient(true, failingSecrets{
				err: errors.New("secret store unavailable"),
			}),
			errorCode: "missing_secret",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			defer test.client.CloseIdleConnections()

			job := baseJob("http://127.0.0.1:1/webhook")
			test.configure(job)
			result := test.client.Deliver(context.Background(), job)

			if result.Success || result.Retryable || result.ErrorCode != test.errorCode {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func baseJob(targetURL string) *store.DeliveryJob {
	return &store.DeliveryJob{
		TargetURL:      targetURL,
		Method:         http.MethodPost,
		HeadersJSON:    []byte(`{}`),
		ContentType:    "application/json",
		Body:           []byte(`{}`),
		IdempotencyKey: "event-1",
		Timeout:        time.Second,
	}
}
