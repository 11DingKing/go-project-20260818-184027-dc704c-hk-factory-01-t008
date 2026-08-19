-- Schema version tracking for migration support
CREATE TABLE IF NOT EXISTS schema_version (
    version    INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
);

-- Enterprise registry: core business entity
CREATE TABLE IF NOT EXISTS enterprises (
    id                   TEXT PRIMARY KEY,
    name                 TEXT NOT NULL,
    legal_representative TEXT NOT NULL,
    unified_credit_code  TEXT NOT NULL UNIQUE,
    registered_capital   TEXT NOT NULL,
    business_scope       TEXT NOT NULL,
    industry_code        TEXT NOT NULL,
    status               TEXT NOT NULL DEFAULT 'active',
    version              INTEGER NOT NULL DEFAULT 1,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL
);

-- Registration changes submitted by intake clerks
CREATE TABLE IF NOT EXISTS changes (
    id                 TEXT PRIMARY KEY,
    enterprise_id      TEXT NOT NULL,
    change_type        TEXT NOT NULL,
    before_snapshot    TEXT NOT NULL,
    after_snapshot     TEXT NOT NULL,
    evidence_materials TEXT,
    event_time         INTEGER NOT NULL,
    status             TEXT NOT NULL DEFAULT 'draft',
    submitted_by       TEXT NOT NULL,
    revoked_reason     TEXT,
    resolution_order   INTEGER NOT NULL DEFAULT 0,
    created_at         INTEGER NOT NULL,
    updated_at         INTEGER NOT NULL,
    FOREIGN KEY (enterprise_id) REFERENCES enterprises(id)
);

-- Dispatch tasks: one change dispatched to one department
CREATE TABLE IF NOT EXISTS dispatch_tasks (
    id             TEXT PRIMARY KEY,
    change_id      TEXT NOT NULL,
    department_code TEXT NOT NULL,
    topic          TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'pending',
    log_offset     INTEGER NOT NULL,
    attempt_count  INTEGER NOT NULL DEFAULT 0,
    max_attempts   INTEGER NOT NULL DEFAULT 3,
    next_retry_at  INTEGER,
    last_error     TEXT,
    acked_by       TEXT,
    acked_at       INTEGER,
    result         TEXT,
    result_error   TEXT,
    completed_at   INTEGER,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL,
    FOREIGN KEY (change_id) REFERENCES changes(id)
);

-- Append-only event log: source of truth for all state transitions
CREATE TABLE IF NOT EXISTS event_log (
    offset          INTEGER PRIMARY KEY AUTOINCREMENT,
    topic           TEXT NOT NULL,
    change_id       TEXT NOT NULL,
    department_code TEXT,
    event_type      TEXT NOT NULL,
    payload         TEXT NOT NULL,
    event_time      INTEGER NOT NULL,
    sequence        INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL
);

-- Subscriber consumption offsets (per subscriber per topic)
CREATE TABLE IF NOT EXISTS subscriber_offsets (
    subscriber_id    TEXT NOT NULL,
    topic            TEXT NOT NULL,
    committed_offset INTEGER NOT NULL DEFAULT 0,
    last_seen_at     INTEGER,
    PRIMARY KEY (subscriber_id, topic)
);

-- Audit trail: who did what when
CREATE TABLE IF NOT EXISTS audit_records (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    actor       TEXT NOT NULL,
    action      TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    details     TEXT,
    created_at  INTEGER NOT NULL
);

-- Dead letters: permanently failed dispatches
CREATE TABLE IF NOT EXISTS dead_letters (
    id               TEXT PRIMARY KEY,
    dispatch_task_id TEXT NOT NULL,
    change_id        TEXT NOT NULL,
    department_code  TEXT NOT NULL,
    last_error       TEXT NOT NULL,
    attempt_count    INTEGER NOT NULL,
    created_at       INTEGER NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',
    FOREIGN KEY (change_id) REFERENCES changes(id)
);

-- Compensation records: rollback actions for partial failures
CREATE TABLE IF NOT EXISTS compensation_records (
    id              TEXT PRIMARY KEY,
    change_id       TEXT NOT NULL,
    department_code TEXT NOT NULL,
    action          TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      INTEGER NOT NULL,
    completed_at    INTEGER,
    FOREIGN KEY (change_id) REFERENCES changes(id)
);

-- Upstream request/response traces
CREATE TABLE IF NOT EXISTS upstream_traces (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    upstream_name TEXT NOT NULL,
    method       TEXT NOT NULL,
    path         TEXT NOT NULL,
    request_body TEXT,
    response_body TEXT,
    status_code  INTEGER,
    duration_ms  INTEGER,
    error        TEXT,
    created_at   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_changes_enterprise ON changes(enterprise_id);
CREATE INDEX IF NOT EXISTS idx_changes_status ON changes(status);
CREATE INDEX IF NOT EXISTS idx_changes_event_time ON changes(event_time);
CREATE INDEX IF NOT EXISTS idx_dispatch_change ON dispatch_tasks(change_id);
CREATE INDEX IF NOT EXISTS idx_dispatch_department ON dispatch_tasks(department_code);
CREATE INDEX IF NOT EXISTS idx_dispatch_status ON dispatch_tasks(status);
CREATE INDEX IF NOT EXISTS idx_dispatch_next_retry ON dispatch_tasks(next_retry_at);
CREATE INDEX IF NOT EXISTS idx_event_log_topic ON event_log(topic);
CREATE INDEX IF NOT EXISTS idx_event_log_change ON event_log(change_id);
CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_records(entity_type, entity_id);
CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_records(created_at);
CREATE INDEX IF NOT EXISTS idx_dead_letter_status ON dead_letters(status);
CREATE INDEX IF NOT EXISTS idx_compensation_change ON compensation_records(change_id);
