# Temporary Production Implementation Checklist

This document tracks the implementation scope requested in this cycle.
It is temporary and focused on execution + verification.

## Scope

1. Persistent object storage integration for control-plane runtime.
2. Client sync engine applies remote events into a local workspace.
3. Mount command becomes functional (non-placeholder behavior).
4. Auth hardening: refresh token, revoke/logout, stricter permission checks, audit log.
5. Reliability controls: retry/timeout strategy, tenant rate limit, tenant storage quota guard.
6. Cloudflare free-tier direct-worker runtime enhancement.
7. Expanded unit tests for all newly added behaviors.

## Acceptance Criteria

### 1) Persistent object storage integration

- Control-plane supports selecting object backend via env/config.
- Backend options include in-memory and S3-compatible (AWS S3 / Cloudflare R2).
- Default remains safe for local dev, with explicit production path via env.
- Unit tests cover config parsing and backend selection.

### 2) Client sync applies events locally

- `meshfs sync` can mirror server-side changes to a local target directory.
- Pull and WebSocket events both apply to local files/dirs.
- Cursor and node-path mapping are persisted in local SQLite for restart recovery.
- Unit tests cover local state persistence helpers and event-application edge cases.

### 3) Mount command functional

- `meshfs mount` no longer returns placeholder-only output.
- It supports kernel-level FUSE mount flow in OSS client (`--features fuse`).
- Behavior and host prerequisites are documented in CLI help/README text.

### 4) Auth hardening

- Refresh token issuance persists server-side and supports rotation/exchange.
- Logout/revoke invalidates refresh token.
- Sensitive operations validate tenant ownership (no cross-tenant apply).
- Audit records are captured for critical operations.
- Unit tests cover refresh, revoke, permission check, and audit persistence.

### 5) Reliability controls

- Client uses bounded timeout + retry for key sync/http operations.
- Server enforces per-tenant rate limit for mutating/sync-heavy operations.
- Server enforces per-tenant storage quota during upload commit.
- Unit tests cover rate limit and quota enforcement.

### 6) Cloudflare free-tier direct-worker runtime enhancement

- Worker runtime is direct mode (no gateway mode).
- Capability endpoint reflects direct-worker runtime metadata/object bindings.
- D1 schema is aligned to current metadata surface.
- Unit-level behavioral checks are added where possible.

### 7) Tests and gate

- New unit tests added for all new core behaviors in this document.
- `cargo test` passes for workspace.
- Deployment command help/syntax checks pass.

## Status

- [x] 1) Persistent object storage integration
- [x] 2) Client sync applies events locally
- [x] 3) Mount command functional
- [x] 4) Auth hardening
- [x] 5) Reliability controls
- [x] 6) Cloudflare free-tier direct-worker runtime enhancement
- [x] 7) Tests and gate

## Next Phase (Direct Worker, No Gateway Mode)

This phase is queued after FUSE mount completion.

### Goals

1. Split business logic from native runtime into `meshfs-control-plane-core`.
2. Add a Worker-native runtime entry (`fetch`) for Rust/Wasm, instead of native TCP-only hosting.
3. Replace metadata persistence abstraction for Worker path with Cloudflare D1 adapter.
4. Replace object storage abstraction for Worker path with Cloudflare R2 adapter.

### Status

- [x] 1) Extract `control-plane-core` from axum/tokio runtime
- [x] 2) Implement Worker-native runtime (Rust/Wasm `fetch` entry)
- [x] 3) Implement D1 metadata adapter
- [x] 4) Implement R2 object storage adapter
