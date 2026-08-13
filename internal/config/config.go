package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"rc-notifier/internal/identifier"
)

type API struct {
	DatabaseURL  string
	ListenAddr   string
	MaxBodyBytes int64
}

type Worker struct {
	DatabaseURL              string
	WorkerID                 string
	Concurrency              int
	PollInterval             time.Duration
	LeaseDuration            time.Duration
	BackoffBase              time.Duration
	BackoffMax               time.Duration
	AllowPrivateDestinations bool
}

func LoadAPI() (API, error) {
	databaseURL, err := required("DATABASE_URL")
	if err != nil {
		return API{}, err
	}

	maxBodyBytes, err := int64Value("MAX_BODY_BYTES", 1<<20)
	if err != nil {
		return API{}, err
	}
	if maxBodyBytes <= 0 {
		return API{}, fmt.Errorf("MAX_BODY_BYTES must be positive")
	}

	return API{
		DatabaseURL:  databaseURL,
		ListenAddr:   value("LISTEN_ADDR", ":8080"),
		MaxBodyBytes: maxBodyBytes,
	}, nil
}

func LoadWorker() (Worker, error) {
	databaseURL, err := required("DATABASE_URL")
	if err != nil {
		return Worker{}, err
	}

	concurrency, err := intValue("WORKER_CONCURRENCY", 4)
	if err != nil {
		return Worker{}, err
	}
	if concurrency <= 0 {
		return Worker{}, fmt.Errorf("WORKER_CONCURRENCY must be positive")
	}

	pollInterval, err := durationValue("WORKER_POLL_INTERVAL", 500*time.Millisecond)
	if err != nil {
		return Worker{}, err
	}
	leaseDuration, err := durationValue("WORKER_LEASE_DURATION", 30*time.Second)
	if err != nil {
		return Worker{}, err
	}
	backoffBase, err := durationValue("RETRY_BACKOFF_BASE", time.Second)
	if err != nil {
		return Worker{}, err
	}
	backoffMax, err := durationValue("RETRY_BACKOFF_MAX", time.Hour)
	if err != nil {
		return Worker{}, err
	}
	if pollInterval <= 0 || leaseDuration <= 0 || backoffBase <= 0 || backoffMax < backoffBase {
		return Worker{}, fmt.Errorf("worker durations must be positive and RETRY_BACKOFF_MAX must not be smaller than RETRY_BACKOFF_BASE")
	}

	allowPrivate, err := boolValue("ALLOW_PRIVATE_DESTINATIONS", false)
	if err != nil {
		return Worker{}, err
	}

	workerID := strings.TrimSpace(os.Getenv("WORKER_ID"))
	if workerID == "" {
		workerID, err = defaultWorkerID()
		if err != nil {
			return Worker{}, err
		}
	}

	return Worker{
		DatabaseURL:              databaseURL,
		WorkerID:                 workerID,
		Concurrency:              concurrency,
		PollInterval:             pollInterval,
		LeaseDuration:            leaseDuration,
		BackoffBase:              backoffBase,
		BackoffMax:               backoffMax,
		AllowPrivateDestinations: allowPrivate,
	}, nil
}

func defaultWorkerID() (string, error) {
	host, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("read hostname: %w", err)
	}
	id, err := identifier.New()
	if err != nil {
		return "", err
	}
	return host + "-" + id[:8], nil
}

func required(name string) (string, error) {
	result := strings.TrimSpace(os.Getenv(name))
	if result == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return result, nil
}

func value(name, fallback string) string {
	if result := strings.TrimSpace(os.Getenv(name)); result != "" {
		return result
	}
	return fallback
}

func intValue(name string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	result, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return result, nil
}

func int64Value(name string, fallback int64) (int64, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	result, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return result, nil
}

func boolValue(name string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	result, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("parse %s: %w", name, err)
	}
	return result, nil
}

func durationValue(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	result, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", name, err)
	}
	return result, nil
}
