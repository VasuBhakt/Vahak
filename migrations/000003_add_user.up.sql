CREATE TABLE IF NOT EXISTS users (
    id  UUID PRIMARY KEY,
    email    VARCHAR(255) NOT NULL UNIQUE,
    username VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    password VARCHAR(100) NOT NULL,
    verification_token VARCHAR(255),
    verification_token_expiry TIMESTAMPTZ,
    verified  BOOLEAN DEFAULT FALSE,
    forgot_password_token VARCHAR(255),
    forgot_password_token_expiry TIMESTAMPTZ,
    endpoints VARCHAR[] DEFAULT '{}',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE endpoints ADD COLUMN user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE;