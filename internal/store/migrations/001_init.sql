CREATE TABLE IF NOT EXISTS destinations (
    id TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version > 0),
    active BOOLEAN NOT NULL DEFAULT true,
    url TEXT NOT NULL,
    method TEXT NOT NULL CHECK (method IN ('POST', 'PUT', 'PATCH')),
    headers JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(headers) = 'object'),
    secret_header_name TEXT,
    secret_env_key TEXT,
    timeout_ms INTEGER NOT NULL DEFAULT 5000 CHECK (timeout_ms BETWEEN 100 AND 30000),
    max_attempts INTEGER NOT NULL DEFAULT 8 CHECK (max_attempts BETWEEN 1 AND 100),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, version),
    CHECK (
        (secret_header_name IS NULL AND secret_env_key IS NULL)
        OR
        (secret_header_name IS NOT NULL AND secret_env_key IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS destinations_one_active_version
    ON destinations (id)
    WHERE active;

CREATE TABLE IF NOT EXISTS notifications (
    id TEXT PRIMARY KEY,
    caller_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    request_hash BYTEA NOT NULL,
    destination_id TEXT NOT NULL,
    destination_version INTEGER NOT NULL,
    target_url TEXT NOT NULL,
    method TEXT NOT NULL,
    headers JSONB NOT NULL,
    secret_header_name TEXT,
    secret_env_key TEXT,
    content_type TEXT NOT NULL,
    body BYTEA NOT NULL,
    timeout_ms INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'succeeded', 'dead')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT,
    lease_token TEXT,
    lease_until TIMESTAMPTZ,
    last_status_code INTEGER,
    last_error_code TEXT,
    last_error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ,
    UNIQUE (caller_id, idempotency_key),
    FOREIGN KEY (destination_id, destination_version)
        REFERENCES destinations (id, version)
);

CREATE INDEX IF NOT EXISTS notifications_pending_delivery
    ON notifications (next_attempt_at, created_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS notifications_expired_lease
    ON notifications (lease_until)
    WHERE status = 'processing';

CREATE TABLE IF NOT EXISTS notification_attempts (
    id BIGSERIAL PRIMARY KEY,
    notification_id TEXT NOT NULL REFERENCES notifications (id) ON DELETE CASCADE,
    attempt_no INTEGER NOT NULL,
    lease_token TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    outcome TEXT CHECK (
        outcome IS NULL
        OR outcome IN ('succeeded', 'retry_scheduled', 'dead', 'lease_expired')
    ),
    http_status INTEGER,
    error_code TEXT,
    error_message TEXT,
    UNIQUE (notification_id, attempt_no)
);
