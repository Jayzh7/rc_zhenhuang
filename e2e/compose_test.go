package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type notificationResponse struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	AttemptCount int    `json:"attemptCount"`
	Created      bool   `json:"created"`
}

func TestComposeMVP(t *testing.T) {
	if os.Getenv("RUN_COMPOSE_E2E") != "1" {
		t.Skip("RUN_COMPOSE_E2E is not set")
	}
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatalf("docker CLI is required: %v", err)
	}

	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	project := fmt.Sprintf("rc-notifier-e2e-%d", os.Getpid())

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	_, _ = runCompose(ctx, root, project, "down", "--volumes", "--remove-orphans")
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()
		if t.Failed() {
			if logs, logErr := runCompose(cleanupCtx, root, project, "logs", "--no-color", "--tail", "200"); logErr == nil {
				t.Logf("Compose logs:\n%s", logs)
			}
		}
		if output, cleanupErr := runCompose(
			cleanupCtx,
			root,
			project,
			"down",
			"--volumes",
			"--remove-orphans",
		); cleanupErr != nil {
			t.Errorf("tear down Compose stack: %v\n%s", cleanupErr, output)
		}
	})

	if output, err := runCompose(ctx, root, project, "up", "--build", "-d"); err != nil {
		t.Fatalf("start Compose stack: %v\n%s", err, output)
	}
	waitForReady(t, ctx, "http://127.0.0.1:8080/health/ready")

	t.Run("retry idempotency and audit", func(t *testing.T) {
		key := fmt.Sprintf("compose-retry-%d", time.Now().UnixNano())
		body := []byte(fmt.Sprintf(`{"event":%q}`, key))

		statusCode, created := submitNotification(t, ctx, "demo", key, body)
		if statusCode != http.StatusAccepted || !created.Created || created.ID == "" {
			t.Fatalf("create status = %d, response = %+v", statusCode, created)
		}

		delivered := waitForNotification(t, ctx, created.ID, "succeeded")
		if delivered.AttemptCount != 2 {
			t.Fatalf("delivered notification = %+v", delivered)
		}

		statusCode, duplicate := submitNotification(t, ctx, "demo", key, body)
		if statusCode != http.StatusOK || duplicate.Created || duplicate.ID != created.ID {
			t.Fatalf("duplicate status = %d, response = %+v", statusCode, duplicate)
		}

		statusCode, _ = requestNotification(t, ctx, created.ID, "other-caller")
		if statusCode != http.StatusNotFound {
			t.Fatalf("cross-caller lookup status = %d", statusCode)
		}

		statusCode, _ = submitNotification(t, ctx, "demo", key, []byte(`{"different":true}`))
		if statusCode != http.StatusConflict {
			t.Fatalf("conflict status = %d", statusCode)
		}

		query := fmt.Sprintf(
			"SELECT attempt_no, outcome, COALESCE(http_status::text, '') "+
				"FROM notification_attempts WHERE notification_id = '%s' ORDER BY attempt_no;",
			created.ID,
		)
		audit, err := runCompose(
			ctx,
			root,
			project,
			"exec",
			"-T",
			"db",
			"psql",
			"-U",
			"notifier",
			"-d",
			"notifier",
			"-At",
			"-F",
			",",
			"-c",
			query,
		)
		if err != nil {
			t.Fatalf("query attempt audit: %v\n%s", err, audit)
		}
		audit = strings.ReplaceAll(strings.TrimSpace(audit), "\r\n", "\n")
		if audit != "1,retry_scheduled,503\n2,succeeded,204" {
			t.Fatalf("attempt audit = %q", audit)
		}
	})

	t.Run("durable queue across worker restart", func(t *testing.T) {
		if output, err := runCompose(ctx, root, project, "stop", "worker"); err != nil {
			t.Fatalf("stop worker: %v\n%s", err, output)
		}

		key := fmt.Sprintf("compose-offline-worker-%d", time.Now().UnixNano())
		statusCode, created := submitNotification(
			t,
			ctx,
			"demo",
			key,
			[]byte(fmt.Sprintf(`{"event":%q}`, key)),
		)
		if statusCode != http.StatusAccepted {
			t.Fatalf("create status = %d, response = %+v", statusCode, created)
		}

		time.Sleep(time.Second)
		pending := getNotification(t, ctx, created.ID)
		if pending.Status != "pending" || pending.AttemptCount != 0 {
			t.Fatalf("notification while worker stopped = %+v", pending)
		}

		if output, err := runCompose(ctx, root, project, "start", "worker"); err != nil {
			t.Fatalf("start worker: %v\n%s", err, output)
		}
		delivered := waitForNotification(t, ctx, created.ID, "succeeded")
		if delivered.AttemptCount != 1 {
			t.Fatalf("delivered notification = %+v", delivered)
		}
	})
}

func runCompose(
	ctx context.Context,
	root string,
	project string,
	args ...string,
) (string, error) {
	commandArgs := append([]string{"compose", "-p", project}, args...)
	command := exec.CommandContext(ctx, "docker", commandArgs...)
	command.Dir = root
	output, err := command.CombinedOutput()
	return string(output), err
}

func waitForReady(t *testing.T, ctx context.Context, endpoint string) {
	t.Helper()

	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			t.Fatalf("create readiness request: %v", err)
		}
		response, err := (&http.Client{Timeout: 2 * time.Second}).Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("API did not become ready")
}

func submitNotification(
	t *testing.T,
	ctx context.Context,
	destinationID string,
	idempotencyKey string,
	body []byte,
) (int, notificationResponse) {
	t.Helper()

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		"http://127.0.0.1:8080/v1/destinations/"+destinationID+"/notifications",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("create submit request: %v", err)
	}
	request.Header.Set("X-Caller-ID", "orders")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	request.Header.Set("Content-Type", "application/json")

	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("submit notification: %v", err)
	}
	defer response.Body.Close()

	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read submit response: %v", err)
	}
	var notification notificationResponse
	if response.StatusCode != http.StatusConflict {
		if err := json.Unmarshal(raw, &notification); err != nil {
			t.Fatalf("decode submit response %q: %v", raw, err)
		}
	}
	return response.StatusCode, notification
}

func getNotification(t *testing.T, ctx context.Context, notificationID string) notificationResponse {
	t.Helper()

	statusCode, notification := requestNotification(t, ctx, notificationID, "orders")
	if statusCode != http.StatusOK {
		t.Fatalf("get status = %d", statusCode)
	}
	return notification
}

func requestNotification(
	t *testing.T,
	ctx context.Context,
	notificationID string,
	callerID string,
) (int, notificationResponse) {
	t.Helper()

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"http://127.0.0.1:8080/v1/notifications/"+notificationID,
		nil,
	)
	if err != nil {
		t.Fatalf("create status request: %v", err)
	}
	request.Header.Set("X-Caller-ID", callerID)

	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		t.Fatalf("get notification: %v", err)
	}
	defer response.Body.Close()

	var notification notificationResponse
	if response.StatusCode == http.StatusOK {
		if err := json.NewDecoder(response.Body).Decode(&notification); err != nil {
			t.Fatalf("decode notification: %v", err)
		}
	}
	return response.StatusCode, notification
}

func waitForNotification(
	t *testing.T,
	ctx context.Context,
	notificationID string,
	status string,
) notificationResponse {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		notification := getNotification(t, ctx, notificationID)
		if notification.Status == status {
			return notification
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("notification %s did not reach status %s", notificationID, status)
	return notificationResponse{}
}
