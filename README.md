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
3. Install `meshfs-client` (`meshfs-client.exe` on Windows) into your `PATH`.
4. Verify:

```bash
meshfs-client --help
```

### Set up Cloudflare free tier server

If your release package includes deployment scripts, run from the package root:

```bash
./scripts/deploy-provider.sh \
  --provider cloudflare-workers-free-tier \
  --token <CLOUDFLARE_API_TOKEN>
```

Then use the deployed endpoint as your client server URL:

```bash
meshfs-client --server https://<your-worker>.workers.dev login
meshfs-client --server https://<your-worker>.workers.dev sync --target ./meshfs-mirror
```

If your release package does not include scripts, clone this repository and run the same deploy command from the repository root.

Cloudflare token permissions required for free-tier setup:
- Account: `Workers Scripts:Edit`
- User: `Memberships:Read` (if `--account-id` is not provided)
- User: `User Details:Read` (if `--account-id` is not provided)
- Account: `D1:Edit` (unless using `--no-d1`)
- Account: `Workers R2 Storage:Edit` (unless using `--no-r2`)

## Build from Source

### Prerequisites

- Rust stable toolchain (`cargo`, `rustup`).
- For Cloudflare Worker deployment only:
  - WebAssembly build target `wasm32-unknown-unknown`.
  - This is the Rust compilation target used to build Worker-compatible Wasm binaries.
  - Install with:

```bash
rustup target add wasm32-unknown-unknown
```

### Install Server and Client

From repository root:

```bash
cargo install --path crates/meshfs-control-plane
cargo install --path crates/meshfs-client
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
cargo run -p meshfs-client -- login --auto-activate
```

Client local state:
- `~/.meshfs/client.db`

### 3) Run sync mirror

One-shot sync:

```bash
cargo run -p meshfs-client -- sync --once --target ./meshfs-mirror
```

Continuous sync:

```bash
cargo run -p meshfs-client -- sync --target ./meshfs-mirror
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
cargo run -p meshfs-client --features fuse -- mount \
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
./scripts/deploy-provider.sh \
  --provider cloudflare-workers-free-tier \
  --token <CLOUDFLARE_API_TOKEN>
```

Default behavior:
- deploys Rust/Wasm Worker runtime (`meshfs-control-plane-runtime-cloudflare-workers`),
- auto creates/reuses D1 metadata database,
- auto creates/reuses R2 object bucket,
- runs Wrangler through `npx` (no local `npm install` required).

Common options:

```bash
# custom D1 name
./scripts/deploy-provider.sh --provider cloudflare-workers-free-tier --token <TOKEN> --d1-database-name <DB_NAME>

# custom R2 bucket
./scripts/deploy-provider.sh --provider cloudflare-workers-free-tier --token <TOKEN> --r2-bucket-name <BUCKET_NAME>

# disable D1
./scripts/deploy-provider.sh --provider cloudflare-workers-free-tier --token <TOKEN> --no-d1

# disable R2
./scripts/deploy-provider.sh --provider cloudflare-workers-free-tier --token <TOKEN> --no-r2
```

Required Cloudflare token permissions:
- Account: `Workers Scripts:Edit`
- User: `Memberships:Read` (if `--account-id` is not provided)
- User: `User Details:Read` (if `--account-id` is not provided)
- Account: `D1:Edit` (unless using `--no-d1`)
- Account: `Workers R2 Storage:Edit` (unless using `--no-r2`)

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
cargo test
```

Coverage summary:

```bash
cargo llvm-cov --workspace --summary-only
```

Generate LCOV:

```bash
mkdir -p target/coverage
cargo llvm-cov --workspace --lcov --output-path target/coverage/lcov.info
```

## Current OSS Constraints

- Cloudflare free-tier path is for zero-cost bootstrap, not enterprise durability.
- No managed multi-region failover for stateful data in OSS.
- OSS transport is HTTP/WebSocket oriented.
- Worker metadata persistence uses D1 snapshot mode in current OSS runtime.

## More Docs

- Architecture and naming:
  - [`docs/control-plane-architecture.md`](docs/control-plane-architecture.md)
- Deployment provider map:
  - [`docs/deployment-providers.md`](docs/deployment-providers.md)
- OSS vs commercial scope:
  - [`docs/editions.md`](docs/editions.md)

## License

Apache License 2.0 (`Apache-2.0`)

## Acknowledgement

Respect to the [JuiceFS](https://github.com/juicedata/juicefs) project for advancing the cloud filesystem ecosystem.
