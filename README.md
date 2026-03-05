# MeshFS

MeshFS is a cloud filesystem for human-agent collaboration.

It gives you:
- near real-time multi-device synchronization,
- deterministic overwrite behavior (last-write-wins),
- full version history with restore,
- both filesystem-style access and REST API access.

## What MeshFS Provides

- Authentication with device code flow for CLI clients.
- File APIs: upload, download, list, metadata, mkdir, rename, delete.
- Sync APIs:
  - `sync/ws` for push-style updates,
  - `sync/pull` for cursor-based catch-up,
  - `sync/stream` (SSE snapshot mode in Worker runtime).
- Versioning:
  - every write creates a new version,
  - head version is always deterministic,
  - historical restore is supported.
- Safety controls:
  - refresh token rotation + revoke/logout,
  - tenant rate limit,
  - tenant storage quota,
  - audit events.

## Quick Start for Release Package Users

Use this path if you downloaded a prebuilt release package and do not want to build from source.

### Install the client

1. Download the release archive for your OS/CPU.
2. Extract it.
3. Install `meshfs` (`meshfs.exe` on Windows) into your `PATH`.
4. Verify:

```bash
meshfs --help
```

### Set up Cloudflare free tier server

From release package root (or repository root), run:

```bash
meshfs deploy cloudflare-workers-free-tier \
  --token <CLOUDFLARE_API_TOKEN>
```

This command uses a prebuilt worker bundle if available at:

- `deploy/providers/cloudflare-workers-free-tier/worker-bundle/`

Then use the deployed endpoint as your client server URL:

```bash
meshfs --server https://<your-worker>.workers.dev login
meshfs --server https://<your-worker>.workers.dev sync --target ./meshfs-mirror
```

Cloudflare token permissions required for free-tier setup:
- Account: `Workers Scripts:Edit`
- User: `Memberships:Read` (if `--account-id` is not provided)
- User: `User Details:Read` (if `--account-id` is not provided)
- Account: `D1:Edit` (unless using `--no-d1`)
- Account: `Workers R2 Storage:Edit` (unless using `--no-r2`)

## Build from Source

### Prerequisites

- Rust stable toolchain (`cargo`, `rustup`).
- Optional for Cloudflare Worker deployment only:
  - Needed when you use `--build-worker-local` (no prebuilt bundle available).
  - WebAssembly build target `wasm32-unknown-unknown`.
  - Install with:

```bash
rustup target add wasm32-unknown-unknown
```

### Install Server and Client

From repository root:

```bash
cargo install --path crates/meshfs-control-plane
cargo install --path crates/meshfs
```

Or run directly from source without installing:

```bash
cargo build
```

## Local Setup (Native Server + Client)

### 1) Start the server

```bash
cargo run -p meshfs-control-plane
```

Default bind address: `127.0.0.1:8787`

### 2) Login from client

```bash
cargo run -p meshfs -- login --auto-activate
```

Client local state:
- `~/.meshfs/client.db`

### 3) Run sync mirror

One-shot sync:

```bash
cargo run -p meshfs -- sync --once --target ./meshfs-mirror
```

Continuous sync:

```bash
cargo run -p meshfs -- sync --target ./meshfs-mirror
```

## Filesystem Mount (FUSE)

MeshFS mount mode uses standard user-space filesystem integration:
- Linux: FUSE
- macOS: macFUSE
- Windows: WinFsp

Current OSS MeshFS mount implementation is wired for Linux/macOS builds.

Linux prerequisites:
- FUSE userspace library (`libfuse`)
- `pkg-config`

macOS prerequisites:
- macFUSE
- `pkg-config` (`brew install pkgconf`)

Mount command:

```bash
cargo run -p meshfs --features fuse -- mount \
  --remote http://127.0.0.1:8787 \
  --target ./meshfs-mount
```

Optional flags:
- `--allow-other`
- `--auto-unmount`
- `--read-only`

## Cloudflare Free-Tier Deployment (Rust Worker Runtime)

One command:

```bash
meshfs deploy cloudflare-workers-free-tier \
  --token <CLOUDFLARE_API_TOKEN>
```

Default behavior:
- deploys Rust/Wasm Worker runtime (`meshfs-control-plane-runtime-cloudflare-workers`),
- auto creates/reuses D1 metadata database,
- auto creates/reuses R2 object bucket,
- uploads worker modules directly through Cloudflare API (no `wrangler` required).
- uses prebuilt bundle from `deploy/providers/cloudflare-workers-free-tier/worker-bundle/` when present.

Common options:

```bash
# custom D1 name
meshfs deploy cloudflare-workers-free-tier --token <TOKEN> --d1-database-name <DB_NAME>

# custom R2 bucket
meshfs deploy cloudflare-workers-free-tier --token <TOKEN> --r2-bucket-name <BUCKET_NAME>

# disable D1
meshfs deploy cloudflare-workers-free-tier --token <TOKEN> --no-d1

# disable R2
meshfs deploy cloudflare-workers-free-tier --token <TOKEN> --no-r2

# use custom prebuilt worker bundle directory
meshfs deploy cloudflare-workers-free-tier --token <TOKEN> --worker-bundle <BUNDLE_DIR>

# fallback to local worker build when prebuilt bundle is missing
meshfs deploy cloudflare-workers-free-tier --token <TOKEN> --build-worker-local
```

Required Cloudflare token permissions:
- Account: `Workers Scripts:Edit`
- User: `Memberships:Read` (if `--account-id` is not provided)
- User: `User Details:Read` (if `--account-id` is not provided)
- Account: `D1:Edit` (unless using `--no-d1`)
- Account: `Workers R2 Storage:Edit` (unless using `--no-r2`)

End-user walkthrough (client + first deployment):
- [`docs/user-guide.md`](docs/user-guide.md)

## Native Server Configuration

Main environment variables:

- `MESHFS_BIND_ADDR` (default `127.0.0.1:8787`)
- `MESHFS_JWT_SECRET`
- `MESHFS_TOKEN_TTL_SECONDS` (default `3600`)
- `MESHFS_REFRESH_TOKEN_TTL_SECONDS` (default `2592000`)
- `MESHFS_METADATA_SQLITE_PATH` (default OS data dir: `meshfs/control-plane/metadata.db`)
- `MESHFS_RATE_LIMIT_PER_MINUTE` (default `1200`)
- `MESHFS_TENANT_STORAGE_QUOTA_BYTES` (default `10737418240`)
- `MESHFS_OBJECT_STORE_BACKEND` (`in-memory` or `s3-compatible`)
- `MESHFS_OBJECT_STORE_BUCKET`
- `MESHFS_OBJECT_STORE_REGION`
- `MESHFS_OBJECT_STORE_ENDPOINT`
- `MESHFS_OBJECT_STORE_ACCESS_KEY_ID`
- `MESHFS_OBJECT_STORE_SECRET_ACCESS_KEY`
- `MESHFS_OBJECT_STORE_FORCE_PATH_STYLE`
- `MESHFS_OBJECT_STORE_R2_ACCOUNT_ID`

## API Surface

- Auth:
  - `/auth/device/start`
  - `/auth/device/poll`
  - `/auth/device/activate`
  - `/auth/refresh`
  - `/auth/logout`
- Files:
  - `/files/upload/init`
  - `/files/upload/part`
  - `/files/upload/commit`
  - `/files/mkdir`
  - `/files/rename`
  - `/files` (DELETE)
  - `/files/meta`
  - `/files/list`
  - `/files/download`
- Versions:
  - `/files/{node_id}/versions`
  - `/files/{node_id}/versions/{version_id}/restore`
- Sync:
  - `/sync/pull`
  - `/sync/stream`
  - `/sync/ws`
- Plan / Retention / Audit:
  - `/plans/current`
  - `/retention/policy`
  - `/retention/apply`
  - `/audit/recent`

## Testing and Coverage

```bash
cargo fmt --all
cargo check
cargo test --workspace --exclude meshfs-integration-tests
```

Core integration tests (requires built binaries):

```bash
cargo build -p meshfs -p meshfs-control-plane
MESHFS_TEST_CLIENT_BIN=target/debug/meshfs \
MESHFS_TEST_SERVER_BIN=target/debug/meshfs-control-plane \
cargo test -p meshfs-integration-tests --test core -- --nocapture
```

Linux MinIO integration:

```bash
MESHFS_TEST_ENABLE_MINIO=1 \
MESHFS_TEST_MINIO_ENDPOINT=http://127.0.0.1:9000 \
MESHFS_TEST_MINIO_ACCESS_KEY=minioadmin \
MESHFS_TEST_MINIO_SECRET_KEY=minioadmin \
MESHFS_TEST_MINIO_BUCKET=meshfs-int \
MESHFS_TEST_CLIENT_BIN=target/debug/meshfs \
MESHFS_TEST_SERVER_BIN=target/debug/meshfs-control-plane \
cargo test -p meshfs-integration-tests --test minio_s3 -- --nocapture
```

Coverage summary:

```bash
cargo llvm-cov --workspace --exclude meshfs-integration-tests --summary-only
```

Generate LCOV:

```bash
mkdir -p target/coverage
cargo llvm-cov --workspace --exclude meshfs-integration-tests --lcov --output-path target/coverage/lcov.info
```

CI workflows:
- `.github/workflows/ci-pr.yml`
- `.github/workflows/ci-heavy.yml`
- `.github/workflows/ci-coverage.yml`

## Release Automation

GitHub release automation is defined at:

- `.github/workflows/release.yml`

When a tag like `v0.1.0` is pushed, the workflow:
- builds `meshfs` binaries for Linux/macOS/Windows,
- builds Cloudflare worker prebuilt bundle (`index.js` + `index_bg.wasm`),
- packages the bundle into release artifacts and embeds it into each platform package.

## Current OSS Constraints

- Cloudflare free-tier path is for zero-cost bootstrap, not enterprise durability.
- No managed multi-region failover for stateful data in OSS.
- OSS transport is HTTP/WebSocket oriented.
- Worker metadata persistence uses D1 snapshot mode in current OSS runtime.

## More Docs

- User guide (client + first server deployment):
  - [`docs/user-guide.md`](docs/user-guide.md)
- Provider planning and review map:
  - [`docs/provider-user-guide.md`](docs/provider-user-guide.md)
- Architecture and naming:
  - [`docs/control-plane-architecture.md`](docs/control-plane-architecture.md)
- Deployment provider map:
  - [`docs/deployment-providers.md`](docs/deployment-providers.md)
- OSS vs commercial scope:
  - [`docs/editions.md`](docs/editions.md)
- Name reservation playbook (Chinese):
  - [`docs/meshfs-name-reservation-playbook.zh-CN.md`](docs/meshfs-name-reservation-playbook.zh-CN.md)

## License

Apache License 2.0 (`Apache-2.0`)

## Acknowledgement

Respect to the [JuiceFS](https://github.com/juicedata/juicefs) project for advancing the cloud filesystem ecosystem.
