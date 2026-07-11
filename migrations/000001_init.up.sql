CREATE TABLE IF NOT EXISTS endpoints (
    id  UUID PRIMARY KEY,
    name    VARCHAR(255) NOT NULL,
    target_url  TEXT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS requests (
    id      UUID PRIMARY KEY,
    endpoint_id     UUID REFERENCES endpoints(id) ON DELETE CASCADE,
    method      VARCHAR(10) NOT NULL,
    headers     JSONB NOT NULL,
    body    TEXT,
    source_ip   VARCHAR(50),
    received_at     TIMESTAMPTZ DEFAULT NOW()   
);

CREATE TABLE IF NOT EXISTS delivery_jobs (
    id      UUID PRIMARY KEY,
    request_id      UUID REFERENCES requests(id) ON DELETE CASCADE,
    target_url      TEXT NOT NULL,
    status      VARCHAR(20) DEFAULT 'pending',
    attempts    INT DEFAULT 0,
    last_attempt    TIMESTAMPTZ,
    next_attempt    TIMESTAMPTZ DEFAULT NOW(),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);