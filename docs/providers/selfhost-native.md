# selfhost-native

Status: planned (documentation-only, not implemented yet).

## Positioning

First planned provider priority for teams that want full infra control and low cloud lock-in.

## Expected runtime and deployment shape

- Runtime target: `meshfs_control_plane_runtime_native`
- Suggested form: single-process or containerized self-host deployment
- Suggested deploy artifacts: systemd unit and/or container image template

## Suggested default backends

- Metadata: PostgreSQL (primary) with SQLite as lightweight fallback
- Objects: S3-compatible storage (MinIO, Ceph RGW, or cloud S3-compatible endpoint)

## Review focus

- Native runtime production hardening (health checks, graceful shutdown, observability)
- Stateful service upgrade and rollback strategy
- On-prem network and security baseline
