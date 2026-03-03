# Deployment Providers

MeshFS keeps deployment provider support modular. No single cloud vendor is the architecture center.

## Current providers

- `cloudflare-workers-free-tier`
  - Type: direct worker runtime deployment
  - Cost target: zero-cost start on Cloudflare Free tier
  - Scope: OSS deploy path for Rust/Wasm worker runtime (`meshfs-control-plane-runtime-cloudflare-workers`) with core API routes, D1 snapshot persistence, R2 object adapter, cursor-based `sync/ws` push, and snapshot-mode `sync/stream` SSE
  - Default metadata backend: Cloudflare D1 (auto create/reuse by token)
  - Default object backend: Cloudflare R2 (auto create/reuse by token)

## Planned providers (not implemented yet)

- `cloudflare-workers`
- `selfhost-native`
- `aws-ecs`
- `aws-ec2`
- `gcp-cloud-run`
- `azure-container-apps`

## Unified deployment entrypoint

Use the provider dispatcher script:

```bash
./scripts/deploy-provider.sh --provider <provider_id> [provider options]
```

Example:

```bash
./scripts/deploy-provider.sh --provider cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN>
```

With custom D1 database name:

```bash
./scripts/deploy-provider.sh --provider cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN> --d1-database-name <DATABASE_NAME>
```

Disable D1:

```bash
./scripts/deploy-provider.sh --provider cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN> --no-d1
```

Disable R2:

```bash
./scripts/deploy-provider.sh --provider cloudflare-workers-free-tier --token <CLOUDFLARE_API_TOKEN> --no-r2
```

## Adding a new provider

1. Add provider assets under `deploy/providers/<provider-id>/`.
2. Add provider deployment script at `scripts/providers/deploy-<provider-id>.sh`.
3. Register provider in `scripts/deploy-provider.sh`.
4. Update this document and the root README provider table.
