package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadAPIDefaultsAndOverrides(t *testing.T) {
	clearConfigEnvironment(t)
	t.Setenv("DATABASE_URL", "postgres://example")

	defaults, err := LoadAPI()
	if err != nil {
		t.Fatalf("LoadAPI defaults: %v", err)
	}
	if defaults.DatabaseURL != "postgres://example" ||
		defaults.ListenAddr != ":8080" ||
		defaults.MaxBodyBytes != 1<<20 {
		t.Fatalf("defaults = %+v", defaults)
	}

	t.Setenv("LISTEN_ADDR", ":9090")
	t.Setenv("MAX_BODY_BYTES", "2048")
	overrides, err := LoadAPI()
	if err != nil {
		t.Fatalf("LoadAPI overrides: %v", err)
	}
	if overrides.ListenAddr != ":9090" || overrides.MaxBodyBytes != 2048 {
		t.Fatalf("overrides = %+v", overrides)
	}
}

func TestLoadAPIRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name        string
		databaseURL string
		bodyLimit   string
		errorText   string
	}{
		{
			name:      "missing database",
			errorText: "DATABASE_URL is required",
		},
		{
			name:        "invalid body limit",
			databaseURL: "postgres://example",
			bodyLimit:   "invalid",
			errorText:   "parse MAX_BODY_BYTES",
		},
		{
			name:        "nonpositive body limit",
			databaseURL: "postgres://example",
			bodyLimit:   "0",
			errorText:   "MAX_BODY_BYTES must be positive",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnvironment(t)
			t.Setenv("DATABASE_URL", test.databaseURL)
			t.Setenv("MAX_BODY_BYTES", test.bodyLimit)

			_, err := LoadAPI()
			if err == nil || !strings.Contains(err.Error(), test.errorText) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestLoadWorkerDefaultsAndOverrides(t *testing.T) {
	clearConfigEnvironment(t)
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("WORKER_ID", "worker-test")

	defaults, err := LoadWorker()
	if err != nil {
		t.Fatalf("LoadWorker defaults: %v", err)
	}
	if defaults.WorkerID != "worker-test" ||
		defaults.Concurrency != 4 ||
		defaults.PollInterval != 500*time.Millisecond ||
		defaults.LeaseDuration != 30*time.Second ||
		defaults.BackoffBase != time.Second ||
		defaults.BackoffMax != time.Hour ||
		defaults.AllowPrivateDestinations {
		t.Fatalf("defaults = %+v", defaults)
	}

	t.Setenv("WORKER_CONCURRENCY", "8")
	t.Setenv("WORKER_POLL_INTERVAL", "200ms")
	t.Setenv("WORKER_LEASE_DURATION", "45s")
	t.Setenv("RETRY_BACKOFF_BASE", "2s")
	t.Setenv("RETRY_BACKOFF_MAX", "10m")
	t.Setenv("ALLOW_PRIVATE_DESTINATIONS", "true")

	overrides, err := LoadWorker()
	if err != nil {
		t.Fatalf("LoadWorker overrides: %v", err)
	}
	if overrides.Concurrency != 8 ||
		overrides.PollInterval != 200*time.Millisecond ||
		overrides.LeaseDuration != 45*time.Second ||
		overrides.BackoffBase != 2*time.Second ||
		overrides.BackoffMax != 10*time.Minute ||
		!overrides.AllowPrivateDestinations {
		t.Fatalf("overrides = %+v", overrides)
	}
}

func TestLoadWorkerRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name      string
		variable  string
		value     string
		errorText string
	}{
		{
			name:      "invalid concurrency",
			variable:  "WORKER_CONCURRENCY",
			value:     "0",
			errorText: "WORKER_CONCURRENCY must be positive",
		},
		{
			name:      "invalid poll interval",
			variable:  "WORKER_POLL_INTERVAL",
			value:     "not-a-duration",
			errorText: "parse WORKER_POLL_INTERVAL",
		},
		{
			name:      "nonpositive lease",
			variable:  "WORKER_LEASE_DURATION",
			value:     "0s",
			errorText: "worker durations must be positive",
		},
		{
			name:      "backoff max below base",
			variable:  "RETRY_BACKOFF_MAX",
			value:     "500ms",
			errorText: "RETRY_BACKOFF_MAX",
		},
		{
			name:      "invalid private destination flag",
			variable:  "ALLOW_PRIVATE_DESTINATIONS",
			value:     "sometimes",
			errorText: "parse ALLOW_PRIVATE_DESTINATIONS",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnvironment(t)
			t.Setenv("DATABASE_URL", "postgres://example")
			t.Setenv("WORKER_ID", "worker-test")
			t.Setenv(test.variable, test.value)

			_, err := LoadWorker()
			if err == nil || !strings.Contains(err.Error(), test.errorText) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func clearConfigEnvironment(t *testing.T) {
	t.Helper()

	for _, name := range []string{
		"DATABASE_URL",
		"LISTEN_ADDR",
		"MAX_BODY_BYTES",
		"WORKER_ID",
		"WORKER_CONCURRENCY",
		"WORKER_POLL_INTERVAL",
		"WORKER_LEASE_DURATION",
		"RETRY_BACKOFF_BASE",
		"RETRY_BACKOFF_MAX",
		"ALLOW_PRIVATE_DESTINATIONS",
	} {
		t.Setenv(name, "")
	}
}
