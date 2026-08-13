package api

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"time"

	"rc-notifier/internal/store"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

type Repository interface {
	Submit(context.Context, store.Submission) (store.Notification, bool, error)
	GetNotification(context.Context, string, string) (store.Notification, error)
	Ping(context.Context) error
}

type Handler struct {
	repository   Repository
	maxBodyBytes int64
	logger       *slog.Logger
}

func NewHandler(repository Repository, maxBodyBytes int64, logger *slog.Logger) *Handler {
	return &Handler{
		repository:   repository,
		maxBodyBytes: maxBodyBytes,
		logger:       logger,
	}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", h.live)
	mux.HandleFunc("GET /health/ready", h.ready)
	mux.HandleFunc("POST /v1/destinations/{destinationID}/notifications", h.submit)
	mux.HandleFunc("GET /v1/notifications/{notificationID}", h.get)
	return mux
}

func (h *Handler) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := h.repository.Ping(ctx); err != nil {
		h.logger.Warn("readiness check failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", "database is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (h *Handler) submit(w http.ResponseWriter, r *http.Request) {
	callerID, ok := callerID(w, r)
	if !ok {
		return
	}

	destinationID := r.PathValue("destinationID")
	if !identifierPattern.MatchString(destinationID) {
		writeError(w, http.StatusBadRequest, "invalid_destination", "destination identifier is invalid")
		return
	}

	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if !visibleASCII(idempotencyKey) || len(idempotencyKey) > 200 {
		writeError(w, http.StatusBadRequest, "invalid_idempotency_key", "Idempotency-Key must contain 1 to 200 visible characters")
		return
	}

	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	mediaType, parameters, err := mime.ParseMediaType(contentType)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_content_type", "Content-Type is invalid")
		return
	}
	contentType = mime.FormatMediaType(mediaType, parameters)
	if len(contentType) > 255 {
		writeError(w, http.StatusBadRequest, "invalid_content_type", "Content-Type is invalid")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "notification body exceeds the configured limit")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_body", "notification body could not be read")
		return
	}

	notification, created, err := h.repository.Submit(r.Context(), store.Submission{
		CallerID:       callerID,
		IdempotencyKey: idempotencyKey,
		DestinationID:  destinationID,
		ContentType:    contentType,
		Body:           body,
		RequestHash:    requestHash(destinationID, contentType, body),
	})
	switch {
	case errors.Is(err, store.ErrDestinationNotFound):
		writeError(w, http.StatusNotFound, "destination_not_found", "active destination was not found")
		return
	case errors.Is(err, store.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict", "Idempotency-Key was already used for a different request")
		return
	case err != nil:
		h.logger.Error("accept notification",
			"caller_id", callerID,
			"destination_id", destinationID,
			"error", err,
		)
		writeError(w, http.StatusInternalServerError, "internal_error", "notification could not be accepted")
		return
	}

	w.Header().Set("Location", "/v1/notifications/"+notification.ID)
	status := http.StatusOK
	if created {
		status = http.StatusAccepted
	}
	writeJSON(w, status, struct {
		store.Notification
		Created bool `json:"created"`
	}{
		Notification: notification,
		Created:      created,
	})
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	callerID, ok := callerID(w, r)
	if !ok {
		return
	}

	notificationID := r.PathValue("notificationID")
	if len(notificationID) < 1 || len(notificationID) > 128 || containsControl(notificationID) {
		writeError(w, http.StatusBadRequest, "invalid_notification_id", "notification identifier is invalid")
		return
	}

	notification, err := h.repository.GetNotification(r.Context(), notificationID, callerID)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "notification_not_found", "notification was not found")
	case err != nil:
		h.logger.Error("load notification",
			"caller_id", callerID,
			"notification_id", notificationID,
			"error", err,
		)
		writeError(w, http.StatusInternalServerError, "internal_error", "notification could not be loaded")
	default:
		writeJSON(w, http.StatusOK, notification)
	}
}

func callerID(w http.ResponseWriter, r *http.Request) (string, bool) {
	value := strings.TrimSpace(r.Header.Get("X-Caller-ID"))
	if !identifierPattern.MatchString(value) {
		writeError(w, http.StatusBadRequest, "invalid_caller", "X-Caller-ID is required and must be a valid identifier")
		return "", false
	}
	return value, true
}

func requestHash(destinationID, contentType string, body []byte) []byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(destinationID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(contentType))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(body)
	return hash.Sum(nil)
}

func containsControl(value string) bool {
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return true
		}
	}
	return false
}

func visibleASCII(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
