-- MeshFS metadata schema for Cloudflare D1 (SQLite dialect)

PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS tenants (
  tenant_id TEXT PRIMARY KEY,
  plan_tier TEXT NOT NULL,
  retention_policy_json TEXT NOT NULL,
  next_cursor INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS nodes (
  tenant_id TEXT NOT NULL,
  node_id TEXT PRIMARY KEY,
  parent_id TEXT,
  name TEXT NOT NULL,
  path TEXT NOT NULL,
  kind TEXT NOT NULL,
  logical_clock INTEGER NOT NULL,
  deleted_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_tenant_path ON nodes(tenant_id, path);

CREATE TABLE IF NOT EXISTS file_versions (
  tenant_id TEXT NOT NULL,
  version_id TEXT PRIMARY KEY,
  node_id TEXT NOT NULL,
  blob_key TEXT NOT NULL,
  size INTEGER NOT NULL,
  content_hash TEXT,
  writer_device_id TEXT,
  committed_at TEXT NOT NULL,
  overwrite_of_version_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_file_versions_tenant_node ON file_versions(tenant_id, node_id, committed_at);

CREATE TABLE IF NOT EXISTS head_versions (
  tenant_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  version_id TEXT NOT NULL,
  PRIMARY KEY (tenant_id, node_id)
);

CREATE TABLE IF NOT EXISTS change_events (
  tenant_id TEXT NOT NULL,
  event_id TEXT PRIMARY KEY,
  node_id TEXT NOT NULL,
  op TEXT NOT NULL,
  version_id TEXT,
  ts TEXT NOT NULL,
  actor TEXT NOT NULL,
  cursor INTEGER NOT NULL,
  path TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_change_events_tenant_cursor ON change_events(tenant_id, cursor);

CREATE TABLE IF NOT EXISTS uploads (
  upload_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  path TEXT NOT NULL,
  blob_key TEXT NOT NULL,
  content_hash TEXT,
  writer_device_id TEXT,
  size_hint INTEGER,
  parts_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
  id_key TEXT PRIMARY KEY,
  response_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS device_sessions (
  device_code TEXT PRIMARY KEY,
  user_code TEXT NOT NULL,
  approved_json TEXT,
  expires_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_device_sessions_user_code ON device_sessions(user_code);

CREATE TABLE IF NOT EXISTS refresh_sessions (
  refresh_token TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  plan_tier TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  revoked_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_refresh_sessions_tenant ON refresh_sessions(tenant_id);

CREATE TABLE IF NOT EXISTS audit_events (
  audit_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  action TEXT NOT NULL,
  resource TEXT,
  outcome TEXT NOT NULL,
  ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_ts ON audit_events(tenant_id, ts);

CREATE TABLE IF NOT EXISTS meshfs_metadata_snapshot (
  snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1),
  snapshot_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
