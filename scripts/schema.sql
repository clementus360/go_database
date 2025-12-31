-- 1. Enums
CREATE TYPE user_status AS ENUM ('ACTIVE', 'SUSPENDED', 'BANNED');
CREATE TYPE stream_status AS ENUM ('LIVE', 'OFFLINE', 'COMPLETE', 'ERROR');

-- 2. Users Table (No changes needed, but ensuring it's here for context)
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    roles TEXT[] DEFAULT '{user}' NOT NULL,
    status user_status DEFAULT 'ACTIVE' NOT NULL,
    is_verified BOOLEAN DEFAULT FALSE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    deleted_at TIMESTAMPTZ
);

-- 3. Channels Table (The Persistent "Home" for a Streamer)
CREATE TABLE channels (
    id SERIAL PRIMARY KEY,
    user_id BIGINT UNIQUE NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    stream_key VARCHAR(100) UNIQUE NOT NULL,
    title VARCHAR(255),
    description TEXT,
    is_live BOOLEAN DEFAULT FALSE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- 4. Stream Sessions Table (The History / Analytics)
CREATE TABLE stream_sessions (
    id BIGSERIAL PRIMARY KEY,
    channel_id INT NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    status stream_status DEFAULT 'LIVE' NOT NULL,
    resolution VARCHAR(20),
    bitrate_kbps INT,
    codec VARCHAR(20),
    view_count INT DEFAULT 0 NOT NULL,
    start_time TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    end_time TIMESTAMPTZ -- NULL while the stream is active
);

-- 5. Indexes for Performance
CREATE INDEX idx_users_username_active ON users(username) WHERE deleted_at IS NULL;
CREATE INDEX idx_channels_user_id ON channels(user_id);
CREATE INDEX idx_channels_stream_key ON channels(stream_key);
CREATE INDEX idx_sessions_active ON stream_sessions(channel_id) WHERE end_time IS NULL;

ALTER TABLE users ALTER COLUMN roles SET DEFAULT '[]'::jsonb; 