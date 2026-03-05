# Deployment Providers

MeshFS keeps deployment provider support modular. No single cloud vendor is the architecture center.

## Current providers

- `cloudflare-workers-free-tier` ([doc](providers/cloudflare-workers-free-tier.md))
  - Type: direct worker runtime deployment
  - Cost target: zero-cost start on Cloudflare Free tier
  - Scope: OSS deploy path for Rust/Wasm worker runtime (`meshfs-control-plane-runtime-cloudflare-workers`) with core API routes, D1 snapshot persistence, R2 object adapter, cursor-based `sync/ws` push, and snapshot-mode `sync/stream` SSE
  - Default metadata backend: Cloudflare D1 (auto create/reuse by token)
  - Default object backend: Cloudflare R2 (auto create/reuse by token)

## Planned providers (not implemented yet)

- `selfhost-native` ([doc](providers/selfhost-native.md))
- `cloudflare-containers` ([doc](providers/cloudflare-containers.md))
- `aws-ecs` ([doc](providers/aws-ecs.md))
- `aws-ec2` ([doc](providers/aws-ec2.md))
- `gcp-cloud-run` ([doc](providers/gcp-cloud-run.md))
- `azure-container-apps` ([doc](providers/azure-container-apps.md))

Note: the previous placeholder `cloudflare-workers` has been replaced by `cloudflare-containers` to match Cloudflare's container product naming.

## Provider detail docs

See [Provider Docs](providers/README.md) for all provider-specific review documents.

## Unified deployment entrypoint

Use the unified deploy subcommand:

```bash
meshfs deploy <provider_id> [provider options]
```

Example:

```bash
meshfs deploy cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN>
```

With custom D1 database name:

```bash
meshfs deploy cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN> --d1-database-name <DATABASE_NAME>
```

Disable D1:

```bash
meshfs deploy cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN> --no-d1
```

Disable R2:

```bash
meshfs deploy cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN> --no-r2
```

Use custom prebuilt worker bundle:

```bash
meshfs deploy cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN> --worker-bundle <BUNDLE_DIR>
```

Fallback to local worker build:

```bash
meshfs deploy cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN> --build-worker-local
```

## Adding a new provider

1. Add provider assets under `deploy/providers/<provider-id>/`.
2. Implement provider deployment flow under `crates/meshfs/src/deploy.rs`.
3. Register provider subcommand under `meshfs deploy`.
4. Add `docs/providers/<provider-id>.md`.
5. Update this document and the root README provider table.
