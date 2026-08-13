package delivery

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"rc-notifier/internal/store"
)

type Result struct {
	Success      bool
	Retryable    bool
	StatusCode   *int
	ErrorCode    string
	ErrorMessage string
	RetryAfter   time.Duration
}

var errBlockedDestination = errors.New("destination address is blocked")

type SecretProvider interface {
	Get(context.Context, string) (string, error)
}

type EnvSecretProvider struct{}

func (EnvSecretProvider) Get(_ context.Context, name string) (string, error) {
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		return "", fmt.Errorf("secret environment variable %q is not set", name)
	}
	return value, nil
}

type Client struct {
	httpClient               *http.Client
	secrets                  SecretProvider
	allowPrivateDestinations bool
	circuitBreaker           *CircuitBreaker
}

func NewClient(allowPrivateDestinations bool, secrets SecretProvider) *Client {
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           safeDialContext(allowPrivateDestinations),
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	return &Client{
		httpClient: &http.Client{
			Transport: transport,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		secrets:                  secrets,
		allowPrivateDestinations: allowPrivateDestinations,
		circuitBreaker:           NewCircuitBreaker(5, 30*time.Second),
	}
}

func (c *Client) CloseIdleConnections() {
	c.httpClient.CloseIdleConnections()
}

func (c *Client) Deliver(ctx context.Context, job *store.DeliveryJob) Result {
	if c.circuitBreaker != nil {
		allowed, remaining := c.circuitBreaker.Allow(job.DestinationID)
		if !allowed {
			return retryable("circuit_breaker_open", "destination circuit breaker is open due to consecutive failures", remaining)
		}
	}

	result := c.deliverRequest(ctx, job)
	if c.circuitBreaker != nil {
		if result.Success {
			c.circuitBreaker.RecordSuccess(job.DestinationID)
		} else if result.Retryable && result.ErrorCode != "request_canceled" {
			c.circuitBreaker.RecordFailure(job.DestinationID)
		}
	}
	return result
}

func (c *Client) deliverRequest(ctx context.Context, job *store.DeliveryJob) Result {
	if err := validateEndpoint(job.TargetURL, c.allowPrivateDestinations); err != nil {
		return permanent("invalid_destination", err.Error())
	}

	headers, err := decodeHeaders(job.HeadersJSON)
	if err != nil {
		return permanent("invalid_headers", err.Error())
	}

	if (job.SecretHeaderName == nil) != (job.SecretEnvKey == nil) {
		return permanent("invalid_secret_configuration", "destination secret configuration is incomplete")
	}
	if job.SecretHeaderName != nil {
		if err := validateHeader(*job.SecretHeaderName, "secret", true); err != nil {
			return permanent("invalid_secret_header", err.Error())
		}
		secretHeader := http.CanonicalHeaderKey(*job.SecretHeaderName)
		if _, exists := headers[secretHeader]; exists {
			return permanent("invalid_secret_header", fmt.Sprintf("header %q is configured as both static and secret", secretHeader))
		}
		if c.secrets == nil {
			return permanent("missing_secret_provider", "secret provider is not configured")
		}
		secret, err := c.secrets.Get(ctx, *job.SecretEnvKey)
		if err != nil {
			return permanent("missing_secret", err.Error())
		}
		if err := validateHeader(secretHeader, secret, true); err != nil {
			return permanent("invalid_secret", err.Error())
		}
		headers[secretHeader] = secret
	}

	if job.Timeout <= 0 {
		return permanent("invalid_timeout", "destination timeout must be positive")
	}

	requestCtx, cancel := context.WithTimeout(ctx, job.Timeout)
	defer cancel()

	request, err := http.NewRequestWithContext(
		requestCtx,
		job.Method,
		job.TargetURL,
		bytes.NewReader(job.Body),
	)
	if err != nil {
		return permanent("invalid_request", err.Error())
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	request.Header.Set("Content-Type", job.ContentType)
	request.Header.Set("Idempotency-Key", job.IdempotencyKey)
	request.Header.Set("User-Agent", "rc-notifier/1.0")

	response, err := c.httpClient.Do(request)
	if err != nil {
		if errors.Is(err, errBlockedDestination) {
			return permanent("invalid_destination", err.Error())
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(requestCtx.Err(), context.DeadlineExceeded) {
			return retryable("timeout", "destination request timed out", 0)
		}
		if errors.Is(err, context.Canceled) || errors.Is(requestCtx.Err(), context.Canceled) {
			return retryable("request_canceled", "destination request was canceled", 0)
		}
		return retryable("network_error", err.Error(), 0)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 32<<10))

	status := response.StatusCode
	switch {
	case status >= 200 && status < 300:
		return Result{
			Success:    true,
			StatusCode: &status,
		}
	case status == http.StatusRequestTimeout ||
		status == http.StatusTooEarly ||
		status == http.StatusTooManyRequests ||
		(status >= 500 && status <= 599):
		result := retryable("retryable_status", fmt.Sprintf("destination returned HTTP %d", status), parseRetryAfter(response.Header.Get("Retry-After")))
		result.StatusCode = &status
		return result
	default:
		result := permanent("permanent_status", fmt.Sprintf("destination returned HTTP %d", status))
		result.StatusCode = &status
		return result
	}
}

func permanent(code, message string) Result {
	return Result{
		ErrorCode:    code,
		ErrorMessage: message,
	}
}

func retryable(code, message string, retryAfter time.Duration) Result {
	return Result{
		Retryable:    true,
		ErrorCode:    code,
		ErrorMessage: message,
		RetryAfter:   retryAfter,
	}
}

func decodeHeaders(raw []byte) (map[string]string, error) {
	decoded := make(map[string]string)
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, fmt.Errorf("decode destination headers: %w", err)
		}
	}

	headers := make(map[string]string, len(decoded))
	for name, value := range decoded {
		if err := validateHeader(name, value, false); err != nil {
			return nil, err
		}
		canonicalName := http.CanonicalHeaderKey(name)
		if _, exists := headers[canonicalName]; exists {
			return nil, fmt.Errorf("header %q is configured more than once", canonicalName)
		}
		headers[canonicalName] = value
	}
	return headers, nil
}

func validateHeader(name, value string, allowSensitive bool) error {
	if !validHeaderName(name) {
		return fmt.Errorf("header name %q is invalid", name)
	}
	switch http.CanonicalHeaderKey(name) {
	case "Host", "Content-Length", "Transfer-Encoding", "Connection", "Content-Type",
		"Idempotency-Key", "Trailer", "Upgrade", "Te", "Proxy-Connection":
		return fmt.Errorf("header %q is managed by the delivery service", name)
	}
	if !allowSensitive {
		switch http.CanonicalHeaderKey(name) {
		case "Authorization", "Proxy-Authorization", "Cookie", "Set-Cookie", "X-Api-Key":
			return fmt.Errorf("header %q must use the secret provider", name)
		}
	}
	for _, character := range value {
		if character == '\r' || character == '\n' || character == 0x7f || (character < 0x20 && character != '\t') {
			return fmt.Errorf("header %q contains an invalid character", name)
		}
	}
	return nil
}

func validHeaderName(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		switch {
		case character >= 'a' && character <= 'z':
		case character >= 'A' && character <= 'Z':
		case character >= '0' && character <= '9':
		case strings.ContainsRune("!#$%&'*+-.^_`|~", character):
		default:
			return false
		}
	}
	return true
}

func validateEndpoint(raw string, allowPrivate bool) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse destination URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("destination URL must use HTTP or HTTPS")
	}
	if parsed.Hostname() == "" {
		return fmt.Errorf("destination URL must include a host")
	}
	if parsed.User != nil {
		return fmt.Errorf("destination URL must not contain user information")
	}
	if parsed.Fragment != "" {
		return fmt.Errorf("destination URL must not contain a fragment")
	}
	if strings.Contains(parsed.Hostname(), "%") {
		return fmt.Errorf("destination URL must not contain a scoped address")
	}
	if address, err := netip.ParseAddr(parsed.Hostname()); err == nil && !allowPrivate && blockedAddress(address.Unmap()) {
		return fmt.Errorf("destination URL contains a blocked address")
	}
	return nil
}

func safeDialContext(allowPrivate bool) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	if allowPrivate {
		return dialer.DialContext
	}

	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("parse destination address: %w", err)
		}

		addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("resolve destination host: %w", err)
		}

		var lastError error
		for _, candidate := range addresses {
			ip, ok := netip.AddrFromSlice(candidate.IP)
			if !ok || blockedAddress(ip.Unmap()) {
				continue
			}
			connection, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return connection, nil
			}
			lastError = err
		}

		if lastError != nil {
			return nil, fmt.Errorf("connect to destination: %w", lastError)
		}
		return nil, fmt.Errorf("%w: destination resolved only to blocked addresses", errBlockedDestination)
	}
}

var blockedPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("169.254.0.0/16"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("224.0.0.0/4"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("::/128"),
	netip.MustParsePrefix("::1/128"),
	netip.MustParsePrefix("fc00::/7"),
	netip.MustParsePrefix("fe80::/10"),
	netip.MustParsePrefix("ff00::/8"),
	netip.MustParsePrefix("2001:db8::/32"),
}

func blockedAddress(address netip.Addr) bool {
	for _, prefix := range blockedPrefixes {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		if delay := time.Until(retryAt); delay > 0 {
			return delay
		}
	}
	return 0
}
