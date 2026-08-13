INSERT INTO destinations (
    id,
    version,
    active,
    url,
    method,
    headers,
    secret_header_name,
    secret_env_key,
    timeout_ms,
    max_attempts
)
VALUES (
    'demo',
    1,
    true,
    'http://receiver:8080/webhook',
    'POST',
    '{"X-Source":"rc-notifier"}'::jsonb,
    'Authorization',
    'DEMO_AUTH_HEADER',
    3000,
    5
)
ON CONFLICT (id, version) DO NOTHING;
