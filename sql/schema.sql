-- PostgreSQL schema for the streaming pipeline (idempotent: safe to run repeatedly)

CREATE TABLE IF NOT EXISTS content_metadata (
    content_id     VARCHAR(20) PRIMARY KEY,
    content_title  VARCHAR(200) NOT NULL,
    genre          VARCHAR(50),
    release_year   INTEGER,
    content_type   VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS streaming_events (
    event_id          VARCHAR(30) PRIMARY KEY,
    user_id           VARCHAR(30) NOT NULL,
    content_id        VARCHAR(20) NOT NULL REFERENCES content_metadata (content_id),
    event_type        VARCHAR(20) NOT NULL
                      CHECK (event_type IN ('PLAY','PAUSE','RESUME','COMPLETE','STOP')),
    event_timestamp   TIMESTAMP NOT NULL,
    event_date        DATE NOT NULL,
    event_hour        SMALLINT NOT NULL CHECK (event_hour BETWEEN 0 AND 23),
    device_type       VARCHAR(50),
    region            VARCHAR(50),
    playback_position BIGINT NOT NULL CHECK (playback_position >= 0)
);
CREATE INDEX IF NOT EXISTS idx_events_date    ON streaming_events (event_date);
CREATE INDEX IF NOT EXISTS idx_events_content ON streaming_events (content_id);
CREATE INDEX IF NOT EXISTS idx_events_user    ON streaming_events (user_id);

CREATE TABLE IF NOT EXISTS viewing_sessions (
    session_id     VARCHAR(20) PRIMARY KEY,
    user_id        VARCHAR(30) NOT NULL,
    content_id     VARCHAR(20) NOT NULL REFERENCES content_metadata (content_id),
    start_ts       TIMESTAMP NOT NULL,
    end_ts         TIMESTAMP NOT NULL,
    watch_seconds  INTEGER NOT NULL,
    event_count    INTEGER NOT NULL,
    status         VARCHAR(12) NOT NULL CHECK (status IN ('COMPLETED','STOPPED','INCOMPLETE')),
    region         VARCHAR(50),
    device_type    VARCHAR(50),
    issue          VARCHAR(50)
);
CREATE INDEX IF NOT EXISTS idx_sessions_content ON viewing_sessions (content_id);

CREATE TABLE IF NOT EXISTS rejected_events (
    reject_id         SERIAL PRIMARY KEY,
    run_id            VARCHAR(20) NOT NULL,
    event_id          VARCHAR(30),
    user_id           VARCHAR(30),
    content_id        VARCHAR(20),
    event_type        VARCHAR(50),
    event_timestamp   VARCHAR(50),
    device_type       VARCHAR(50),
    region            VARCHAR(50),
    playback_position VARCHAR(50),
    source_file       VARCHAR(100),
    rejection_reasons VARCHAR(300)
);

CREATE TABLE IF NOT EXISTS data_quality_logs (
    log_id          SERIAL PRIMARY KEY,
    run_id          VARCHAR(20) NOT NULL,
    run_timestamp   TIMESTAMP NOT NULL,
    check_name      VARCHAR(60) NOT NULL,
    records_failed  INTEGER NOT NULL,
    records_total   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS viewing_analytics (
    event_date          DATE NOT NULL,
    content_id          VARCHAR(20) NOT NULL REFERENCES content_metadata (content_id),
    total_events        INTEGER NOT NULL,
    play_events         INTEGER NOT NULL,
    unique_viewers      INTEGER NOT NULL,
    sessions            INTEGER NOT NULL,
    completed_sessions  INTEGER NOT NULL,
    total_watch_seconds BIGINT NOT NULL,
    PRIMARY KEY (event_date, content_id)
);
