# cloudflare-workers-free-tier

Status: implemented.

## Positioning

Primary OSS bootstrap provider with zero-cost target on Cloudflare Free tier.

## Runtime and deployment shape

- Runtime: `meshfs_control_plane_runtime_cloudflare_workers`
- Deployment entry: `meshfs deploy cloudflare-workers-free-tier`
- Flow: upload prebuilt Rust/Wasm bundle via Cloudflare API and configure required bindings.
- Default bundle path: `deploy/providers/cloudflare-workers-free-tier/worker-bundle/`
- Local fallback: `--build-worker-local`

## Default backends

- Metadata: Cloudflare D1 (default on; can disable with `--no-d1`)
- Objects: Cloudflare R2 (default on; can disable with `--no-r2`)

## Current scope

- Core API routes
- Cursor-based `sync/ws` push channel
- Snapshot-mode `sync/stream` SSE
- D1 snapshot persistence and R2 object adapter
